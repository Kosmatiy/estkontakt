DECLARE
    v_test_payment uuid := gen_random_uuid();
BEGIN
    PERFORM test_log('INFO','TEST-11','=== TEST-11: Проверка логирования (enable_payment_logs) - START ===');

    -- 1) Убедимся, что enable_payment_logs=true
    UPDATE global_vars
       SET value='true'
     WHERE key='enable_payment_logs';

    -- Создаём платёж
    INSERT INTO payments(id, payment_status, user_strapi_document_id)
    VALUES(v_test_payment, 'NEW', 'test_user_11');

    -- Ставим CONFIRMED => должны появиться логи
    UPDATE payments
       SET payment_status='CONFIRMED'
     WHERE id=v_test_payment;

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE payment_id=v_test_payment
    ) THEN
        RAISE EXCEPTION 'TEST-11 FAIL: Не появились логи при enable_payment_logs=true.';
    END IF;

    PERFORM test_log('DEBUG','TEST-11','OK: Логи есть при enable_payment_logs=true.');

    -- 2) Выключаем
    UPDATE global_vars
       SET value='false'
     WHERE key='enable_payment_logs';

    -- Меняем статус => НЕ должны появиться логи
    UPDATE payments
       SET payment_status='REFUNDED'
     WHERE id=v_test_payment;

    IF EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE payment_id=v_test_payment
          AND level='INFO'
          AND message LIKE '%resources deactivated%'
    ) THEN
        RAISE EXCEPTION 'TEST-11 FAIL: Логи появились при enable_payment_logs=false.';
    END IF;

    PERFORM test_log('DEBUG','TEST-11','OK: Логи не появились при enable_payment_logs=false => всё верно.');

    -- Возвращаем true
    UPDATE global_vars
       SET value='true'
     WHERE key='enable_payment_logs';

    DELETE FROM payments WHERE id=v_test_payment;

    PERFORM test_log('INFO','TEST-11','=== TEST-11 PASSED ===');
END;
