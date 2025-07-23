DECLARE
  v_secret_api_key text := get_secret('PAYMENTS_KEY');

  -- Для "user" варианта:
  v_user_phone text;
  v_user_email text;
  v_user_exists boolean;
  v_now            timestamptz := now();
  v_new_payment_id uuid := uuid_generate_v4();

  v_product_price  numeric;
  v_final_price    numeric;

  v_coupon_strapi_document_id   text;
  v_discount_type  text;
  v_discount_value numeric;
  v_discount_strapi_document_id text;
  v_discount numeric := 0;

  -- Для "lead" варианта:
  v_lead_id text;
  v_lead_user_strapi_document_id text;
BEGIN
  -----------------------------------------------------------------
  -- 1) Проверяем API-ключ
  -----------------------------------------------------------------
  IF _api_key IS NULL OR _api_key <> v_secret_api_key THEN
    RAISE EXCEPTION 'Access Denied: invalid API key (create_payment)';
  END IF;

  -----------------------------------------------------------------
  -- 2) Разделяем логику:
  --    Если _user_strapi_document_id непустой => работаем с users
  --    Иначе => старый механизм с leads
  -----------------------------------------------------------------
  IF _user_strapi_document_id IS NOT NULL AND trim(_user_strapi_document_id) <> '' THEN
    -----------------------------------------------------------------
    -- Вариант с user_strapi_document_id
    -----------------------------------------------------------------
    SELECT phone, email
      INTO v_user_phone, v_user_email
      FROM users
     WHERE strapi_document_id = _user_strapi_document_id
     LIMIT 1;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'User not found for user_strapi_document_id=%', _user_strapi_document_id;
    END IF;

    -- Если нужно, можно решать, что приоритетнее:
    -- телефон, пришедший во входных параметрах, или из users
    -- но по условию: «В таком случае берем из users»
    _phone := v_user_phone;
    _email := v_user_email;

    -- В lead_supabase_id ничего не кладём, т.к. логику лидов пропускаем
    -- v_lead_id := NULL;

  ELSE
    -----------------------------------------------------------------
    -- Вариант без _user_strapi_document_id => старый механизм с leads
    -----------------------------------------------------------------
    SELECT supabase_id,
           user_strapi_document_id
      INTO v_lead_id,
           v_lead_user_strapi_document_id
      FROM leads
     WHERE (phone = _phone AND _phone IS NOT NULL AND trim(_phone) <> '')
        OR (email = _email AND _email IS NOT NULL AND trim(_email) <> '')
        OR (telegram_id = _telegram_id AND _telegram_id IS NOT NULL AND _telegram_id <> 0)
        OR (telegram_username = _telegram_username AND _telegram_username IS NOT NULL AND trim(_telegram_username) <> '')
     LIMIT 1;

    IF v_lead_id IS NULL THEN
      v_lead_id := uuid_generate_v4()::text;
      INSERT INTO leads (
        supabase_id,
        created_at,
        name,
        surname,
        telegram_username,
        email,
        telegram_id,
        strapi_document_id,
        phone,
        utm_source,
        utm_content,
        utm_campaign,
        utm_term,
        user_strapi_document_id
      )
      VALUES (
        v_lead_id,
        v_now,
        NULL,
        NULL,
        _telegram_username,
        _email,
        _telegram_id,
        NULL,
        _phone,
        _utm_source,
        _utm_content,
        _utm_campaign,
        _utm_term,
        NULL
      );
    END IF;
  END IF;

  -----------------------------------------------------------------
  -- 3) Найти product
  -----------------------------------------------------------------
  IF _product_strapi_document_id IS NULL OR trim(_product_strapi_document_id) = '' THEN
    RAISE EXCEPTION 'No product_strapi_document_id provided';
  END IF;

  SELECT price
    INTO v_product_price
    FROM products
   WHERE strapi_document_id = _product_strapi_document_id
   LIMIT 1;

  IF v_product_price IS NULL THEN
    RAISE EXCEPTION 'Product not found for strapi_document_id=%', _product_strapi_document_id;
  END IF;

  v_final_price := v_product_price;

  -----------------------------------------------------------------
  -- 4) Применяем купон (не изменяем логику)
  -----------------------------------------------------------------
  IF _coupon_name IS NOT NULL AND trim(_coupon_name) <> '' THEN
    SELECT discount_strapi_document_id
      INTO v_discount_strapi_document_id
      FROM coupons
     WHERE name = _coupon_name
       AND is_active = true
       AND (start_datetime IS NULL OR start_datetime <= v_now)
       AND (end_datetime   IS NULL OR end_datetime   >= v_now)
     LIMIT 1;

    IF v_discount_strapi_document_id IS NOT NULL THEN
      SELECT discount_type, value
        INTO v_discount_type, v_discount_value
        FROM discounts
       WHERE strapi_document_id = v_discount_strapi_document_id
       LIMIT 1;

      IF v_discount_type = 'PERCENT' THEN
        v_discount := v_product_price * (v_discount_value / 100);
      ELSIF v_discount_type = 'AMOUNT' THEN
        v_discount := v_discount_value;
      ELSE
        v_discount := 0;
      END IF;

      v_final_price := v_final_price - v_discount;
      IF v_final_price < 0 THEN
        v_final_price := 0;
      END IF;
    END IF;
  END IF;

  IF _coupon_name IS NOT NULL AND trim(_coupon_name) <> '' THEN
    SELECT strapi_document_id
      INTO v_coupon_strapi_document_id
      FROM coupons
     WHERE name = _coupon_name
       AND is_active = true
       AND (start_datetime IS NULL OR start_datetime <= v_now)
       AND (end_datetime   IS NULL OR end_datetime   >= v_now)
     LIMIT 1;
  ELSE
    v_coupon_strapi_document_id := NULL;
  END IF;

  -----------------------------------------------------------------
  -- 5) Создаём запись в payments
  --    Если _user_strapi_document_id есть (и не пуст),
  --    тогда lead_supabase_id = NULL, user_strapi_document_id = ...
  --    Иначе lead_supabase_id = v_lead_id
  -----------------------------------------------------------------
  INSERT INTO payments(
    supabase_id,
    created_at,
    payment_status,
    amount,
    amount_before_discount,
    lead_supabase_id,
    user_strapi_document_id,
    coupon_strapi_document_id,
    product_strapi_document_id

  )
  VALUES (
    v_new_payment_id,
    v_now,
    'CREATED',
    v_final_price,
    v_product_price,
    v_lead_id,
    CASE 
      WHEN _user_strapi_document_id IS NOT NULL 
           AND trim(_user_strapi_document_id) <> '' 
      THEN _user_strapi_document_id 
      ELSE v_lead_user_strapi_document_id 
    END,
    v_coupon_strapi_document_id,
    _product_strapi_document_id
  );

  -----------------------------------------------------------------
  -- 6) Возвращаем JSON
  -----------------------------------------------------------------
  RETURN jsonb_build_object(
    'payment_supabase_id', v_new_payment_id,
    'lead_supabase_id',    v_lead_id,
    'amount',              v_final_price,
    'discount_applied',    v_discount
  );
END;
