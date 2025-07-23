BEGIN
    PERFORM test_log('INFO','TEST-9','=== TEST-9: Неуспешный статус (REJECTED) - START ===');
    -- Платёж P4, изначально NEW, => REJECTED

    UPDATE payments
       SET payment_status='REJECTED'
     WHERE payment_status='NEW';

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE level='DEBUG'
          AND message LIKE '%Unsuccessful status: REJECTED%'
    ) THEN
        RAISE EXCEPTION 'TEST-9 FAIL: Нет лога "Unsuccessful status: REJECTED"';
    END IF;

    PERFORM test_log('INFO','TEST-9','=== TEST-9 PASSED ===');
END;
