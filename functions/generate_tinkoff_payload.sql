DECLARE
  v_created_payment jsonb;
  v_payment_supabase_id uuid;
  v_lead_supabase_id    text;
  v_amount              numeric;

  v_terminal_key   text := get_secret('TINKOFF_TERMINAL_KEY');
  v_secret_tinkoff text := get_secret('TINKOFF_SECRET');

  v_concat_str  text;
  v_sha256_hex  text;
  v_amount_kopecks bigint;
  v_payload jsonb;
  v_receipt jsonb;
  v_payment_status text := 'CREATED';
  v_user_phone text;
  v_user_email text;

  -- Новая переменная для хранения имени товара
  v_product_name text;
BEGIN
  ------------------------------------------------------------------
  -- Сначала всё без изменений, создаём платёж
  ------------------------------------------------------------------
  v_created_payment := create_payment(
    _api_key,
    _phone,
    _email,
    _telegram_id,
    _telegram_username,
    _utm_source,
    _utm_content,
    _product_strapi_document_id,
    _coupon_name,
    _utm_campaign,
    _utm_term,
    _initiator_type,
    _user_strapi_document_id
  );

  v_payment_supabase_id := (v_created_payment->>'payment_supabase_id')::uuid;
  v_lead_supabase_id    :=  v_created_payment->>'lead_supabase_id';
  v_amount              := (v_created_payment->>'amount')::numeric;
 


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

  ELSE
  ------------------------------------------------------------------
  -- Проверка, что есть телефон или e-mail (как было)
  ------------------------------------------------------------------
  IF (_phone IS NULL OR trim(_phone) = '')
     AND (_email IS NULL OR trim(_email) = '') THEN
    RAISE EXCEPTION 'You must provide at least _phone or _email to generate Tinkoff receipt';
  END IF;
 END IF;
  ------------------------------------------------------------------
  -- 1) Изменение: Берём имя товара из products (по _product_strapi_document_id)
  ------------------------------------------------------------------
  SELECT name
    INTO v_product_name
    FROM products
   WHERE strapi_document_id = _product_strapi_document_id
   LIMIT 1;

  -- На случай, если не найден, можно подстраховаться:
  IF v_product_name IS NULL THEN
    v_product_name := 'Товар без названия'; --fallback, если нужно
  END IF;

  ------------------------------------------------------------------
  -- Формируем чек (Receipt). Меняем только "Name" -> 'Оплата за '||v_product_name
  ------------------------------------------------------------------
  v_amount_kopecks := CAST(v_amount * 100 AS bigint);

  v_receipt := jsonb_build_object(
    'FfdVersion','1.2',
    'Taxation','usn_income',
    'Items', jsonb_build_array(
       jsonb_build_object(
         'Name',       'Оплата за ' || v_product_name,  -- Изменение
         'Price',      v_amount_kopecks,
         'Quantity',   1,
         'Amount',     v_amount_kopecks,
         'Tax',        'none',
         'PaymentMethod','full_prepayment',
         'PaymentObject','intellectual_activity',
         'MeasurementUnit','шт'
       )
    )
  );

  IF _email IS NOT NULL AND trim(_email) <> '' THEN
    v_receipt := v_receipt || jsonb_build_object('Email', _email);
  END IF;
  IF _phone IS NOT NULL AND trim(_phone) <> '' THEN
    v_receipt := v_receipt || jsonb_build_object('Phone', _phone);
  END IF;

  ------------------------------------------------------------------
  -- Считаем токен (без изменений)
  ------------------------------------------------------------------
  v_concat_str := CONCAT(
    v_amount_kopecks,
    'Оплата по платежу ',
    v_payment_supabase_id::text,
    v_secret_tinkoff,
    v_terminal_key
  );
  SELECT encode(digest(v_concat_str, 'sha256'), 'hex')
    INTO v_sha256_hex;

  ------------------------------------------------------------------
  -- Формируем tinkoff_payload (меняем только DATA)
  ------------------------------------------------------------------
  v_payload := jsonb_build_object(
    'TerminalKey', v_terminal_key,
    'Amount',      v_amount_kopecks,
    'OrderId',     v_payment_supabase_id::text,
    'Description', 'Оплата заказа '||v_payment_supabase_id::text,
    'Receipt',     v_receipt,
    'Token',       v_sha256_hex,
    'DATA', jsonb_build_object(
      'PaymentSupabaseId', v_payment_supabase_id::text,
      'LeadSupabaseId',    v_lead_supabase_id,

      -- 2) Изменение: добавляем user_strapi_document_id
      'UserStrapiDocumentID', _user_strapi_document_id
    )
  );

  RETURN jsonb_build_object(
    'payment_supabase_id', v_payment_supabase_id,
    'lead_supabase_id',    v_lead_supabase_id,
    'amount',              v_amount,
    'payment_status',      v_payment_status,
    'tinkoff_payload',     v_payload,
    'user_strapi_document_id', _user_strapi_document_id
  );
END;
