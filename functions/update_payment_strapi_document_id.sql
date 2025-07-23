DECLARE
    v_secret_api_key text := get_secret('PAYMENTS_KEY');
    v_found          boolean;
    v_updated_count  text;
    v_now            timestamp := now();
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
       SET strapi_document_id = COALESCE(_strapi_document_id, strapi_document_id),
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
      'payment_id', _payment_id::text
    );
END;
