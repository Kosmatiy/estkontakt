DECLARE
    v_secret_api_key     text := get_secret('PAYMENTS_KEY');
    v_now                timestamptz := now();
    v_payment_supabase_id uuid;
    v_tinkoff_payment_id bigint;
    v_status            text;
    v_error_code        text;
    v_amount_kopecks    bigint;    -- приходит от Tinkoff в копейках
    v_db_amount         numeric;   -- хранится у нас в payments.amount (рубли/число)
    v_payment_method    text;
    v_card_id           bigint;
    v_pan               text;
    v_exp_date          text;
    v_info_email        text;

    v_lead_supabase_id  text;
    v_found             boolean;
    v_message           text;

    -- Для расшифровки error_code из таблицы tinkoff_errors
    v_error_details     text;
BEGIN
    -- 1) Проверяем API-ключ
    IF _api_key IS NULL OR _api_key <> v_secret_api_key THEN
        RAISE EXCEPTION 'Access Denied: invalid API key (update_payment_from_tinkoff_webhook)';
    END IF;

    -- 2) Извлекаем основные поля из JSON
    v_payment_supabase_id := (_payload->>'OrderId')::uuid;
    v_tinkoff_payment_id  := _payload->>'PaymentId';
    v_status              := _payload->>'Status';
    v_error_code          := _payload->>'ErrorCode';
    v_amount_kopecks      := (_payload->>'Amount')::bigint;
    v_payment_method      := _payload->'Data'->>'Source';
    v_card_id             := _payload->>'CardId';
    v_pan                 := _payload->>'Pan';
    v_exp_date            := _payload->>'ExpDate';
    v_info_email          := _payload->'Data'->>'INFO_EMAIL';
    v_lead_supabase_id    := _payload->'Data'->>'LeadSupabaseId';

    -- 3) Проверяем наличие платежа
    SELECT amount
      INTO v_db_amount
      FROM payments
     WHERE supabase_id = v_payment_supabase_id
     LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No payment found for supabase_id=%', v_payment_supabase_id;
    END IF;

    -- 4) Сравниваем суммы
    IF (v_db_amount * 100) <> v_amount_kopecks THEN
       v_message := 'Сумма не совпадает. В БД='||v_db_amount::text||
                    ', Tinkoff='||(v_amount_kopecks::numeric / 100)::text||
                    '. Полный JSON: '|| _payload::text;

       INSERT INTO admin_messages (message_text, created_at)
       VALUES (v_message, v_now);
    END IF;

    ----------------------------------------------------------------------------
    -- 4.1) Если пришёл ErrorCode != '0', пытаемся найти расшифровку в tinkoff_errors
    ----------------------------------------------------------------------------
    IF v_error_code IS NOT NULL 
       AND v_error_code <> '' 
       AND v_error_code <> '0'
    THEN
      BEGIN
        SELECT error_details
          INTO v_error_details
          FROM tinkoff_errors
         WHERE error_code = v_error_code::int
         LIMIT 1;

        IF NOT FOUND THEN
          -- Если такой код не нашёлся, ставим что-то вроде "Unknown code"
          v_error_details := 'Неизвестная ошибка Tinkoff: '||v_error_code;
        END IF;
      EXCEPTION WHEN others THEN
        -- На случай, если v_error_code не цифры, или другая ошибка
        v_error_details := 'Неизвестная ошибка Tinkoff: '||v_error_code;
      END;
    ELSE
      -- Если ErrorCode = 0 или пустой, то считаем, что ошибки нет
      v_error_details := NULL;
    END IF;

    ----------------------------------------------------------------------------
    -- 5) Обновляем поля в payments
    --    Добавляем поля:
    --     - error_details = v_error_details
    ----------------------------------------------------------------------------
    UPDATE payments
       SET payment_status     = COALESCE(v_status, payment_status),
           error_code         = COALESCE(v_error_code, error_code),
           -- Тут записываем расшифровку, если есть
           error_details      = COALESCE(v_error_details, error_details),
          --  tinkoff_payment_id = COALESCE(v_tinkoff_payment_id, tinkoff_payment_id),
           payment_method     = COALESCE(v_payment_method, payment_method),
           card_id            = COALESCE(v_card_id, card_id),
           pan                = COALESCE(v_pan, pan),
           exp_date           = COALESCE(v_exp_date, exp_date),
           info_email         = COALESCE(v_info_email, info_email),
           updated_at         = v_now,
           raw_responce_after_pay = _payload
     WHERE supabase_id = v_payment_supabase_id AND tinkoff_payment_id = v_tinkoff_payment_id
     RETURNING 1 INTO v_found;

    IF NOT v_found THEN
        RAISE EXCEPTION 'No payment found for supabase_id=%', v_payment_supabase_id;
    END IF;

    -- 6) (Опционально) обновить leads, если нужно

    -- 7) Возвращаем результат
    RETURN jsonb_build_object(
      'success',    true,
      'payment_id', v_payment_supabase_id::text,
      'new_status', v_status
    );
END;
