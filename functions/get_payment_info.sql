DECLARE
    v_secret_api_key text := get_secret('PAYMENTS_KEY');
    v_payment_record record;
BEGIN
    --------------------------------------------------------------------
    -- 1) Проверка API-ключа
    --------------------------------------------------------------------
    IF _api_key IS NULL OR _api_key <> v_secret_api_key THEN
        RAISE EXCEPTION 'Access Denied: invalid API key (get_payment_info)';
    END IF;

    --------------------------------------------------------------------
    -- 2) Выбираем данные из payments
    --------------------------------------------------------------------
    SELECT *
      INTO v_payment_record
      FROM payments
     WHERE supabase_id = _payment_id
     LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Payment ID not found: %', _payment_id;
    END IF;

    --------------------------------------------------------------------
    -- 3) Возвращаем строку в формате JSON
    --------------------------------------------------------------------
    RETURN row_to_json(v_payment_record);
END;
