BEGIN
    PERFORM test_log('INFO','TEST-5','=== TEST-5: Подтверждение оплаты (STREAM_PASS) - START ===');
    -- user2, strikes_limit=5, product STREAM_PASS (entity_id='stream_doc_123'), payment_status='NEW'

    UPDATE payments
       SET payment_status='CONFIRMED'
     WHERE user_strapi_document_id='user2'
       AND payment_status='NEW';

    IF NOT EXISTS(
       SELECT 1
         FROM user_stream_access
        WHERE user_strapi_document_id='user2'
          AND is_active=true
    ) THEN
        RAISE EXCEPTION 'TEST-5 FAIL: user_stream_access не стало is_active=true для user2.';
    END IF;

    -- strikes_limit у user2 не меняется

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE user_strapi_document_id='user2'
          AND message LIKE '%CONFIRMED => activate resources%'
    ) THEN
        RAISE EXCEPTION 'TEST-5 FAIL: Не найден лог CONFIRMED => activate resources для user2.';
    END IF;

    PERFORM test_log('INFO','TEST-5','=== TEST-5 PASSED ===');
END;
