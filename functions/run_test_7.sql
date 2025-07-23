BEGIN
    PERFORM test_log('INFO','TEST-7','=== TEST-7: Подтверждение оплаты (MEETING_PASS) - START ===');
    -- user3, product=MEETING_PASS(meeting_doc_456), payment_status='NEW'

    UPDATE payments
       SET payment_status='CONFIRMED'
     WHERE user_strapi_document_id='user3'
       AND payment_status='NEW';

    IF NOT EXISTS(
       SELECT 1
         FROM user_meeting_access
        WHERE user_strapi_document_id='user3'
          AND is_active=true
    ) THEN
        RAISE EXCEPTION 'TEST-7 FAIL: user_meeting_access не стало is_active=true.';
    END IF;

    -- Логи соответствуют CONFIRMED
    PERFORM test_log('INFO','TEST-7','=== TEST-7 PASSED ===');
END;
