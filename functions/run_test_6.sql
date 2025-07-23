BEGIN
    PERFORM test_log('INFO','TEST-6','=== TEST-6: Отмена (STREAM_PASS) - START ===');
    -- user_stream_access(user2, stream_doc_123, is_active=true), payment_status='CONFIRMED'

    UPDATE payments
       SET payment_status='CANCELED'
     WHERE user_strapi_document_id='user2'
       AND payment_status='CONFIRMED';

    IF NOT EXISTS(
       SELECT 1
         FROM user_stream_access
        WHERE user_strapi_document_id='user2'
          AND is_active=false
    ) THEN
        RAISE EXCEPTION 'TEST-6 FAIL: user_stream_access не стал is_active=false.';
    END IF;

    PERFORM test_log('INFO','TEST-6','=== TEST-6 PASSED ===');
END;
