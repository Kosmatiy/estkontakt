DECLARE
    v_secret_api_key text := get_secret('PAYMENTS_KEY');
    v_now            timestamp := now();
    v_found          boolean;
    v_updated_count  integer;
BEGIN
    --------------------------------------------------------------------
    -- 1) Проверяем API-ключ
    --------------------------------------------------------------------
    IF _api_key IS NULL OR _api_key <> v_secret_api_key THEN
        RAISE EXCEPTION 'Access Denied: invalid API key (update_payment)';
    END IF;

    --------------------------------------------------------------------
    -- 2) Проверяем, существует ли платёж
    --------------------------------------------------------------------
    SELECT TRUE
      FROM payments
     WHERE supabase_id = _payment_id
     LIMIT 1
     INTO v_found;

    IF NOT FOUND OR v_found IS NOT TRUE THEN
        RAISE EXCEPTION 'No payment found for supabase_id=%', _payment_id;
    END IF;

    --------------------------------------------------------------------
    -- 3) Делаем UPDATE с учётом COALESCE / CASE для raw_response
    --------------------------------------------------------------------
    UPDATE payments
       SET payment_status = COALESCE(_new_status, payment_status),
           payment_url    = COALESCE(_payment_url, payment_url),
           error_code     = COALESCE(_error_code, error_code),
           tinkoff_payment_id = COALESCE(_tinkoff_payment_id,tinkoff_payment_id),
           raw_response   = CASE
                                WHEN _raw_response IS NOT NULL THEN _raw_response
                                ELSE raw_response
                            END,
           updated_at     = v_now
     WHERE supabase_id = _payment_id
     RETURNING 1
     INTO v_updated_count;

    IF v_updated_count IS NULL THEN
        RAISE EXCEPTION 'No payment found for supabase_id=%', _payment_id;
    END IF;

    --------------------------------------------------------------------
    -- 4) Возвращаем ответ
    --------------------------------------------------------------------
    RETURN jsonb_build_object(
      'success',    true,
      'payment_id', _payment_id::text,
      'new_status', _new_status
    );
END;
