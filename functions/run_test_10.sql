BEGIN
    PERFORM test_log('INFO','TEST-10','=== TEST-10: Неизвестный статус FOO_BAR - START ===');
    UPDATE payments
       SET payment_status='FOO_BAR'
     WHERE payment_status='NEW';
    

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE level='WARNING'
          AND message LIKE '%Unknown status=FOO_BAR. No logic applied%'
    ) THEN
        RAISE EXCEPTION 'TEST-10 FAIL: Не найден WARNING лог Unknown status=FOO_BAR.';
    END IF;

    PERFORM test_log('INFO','TEST-10','=== TEST-10 PASSED ===');
END;
