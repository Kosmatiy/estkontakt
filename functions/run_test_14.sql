DECLARE
    v_payment_id uuid := gen_random_uuid();
BEGIN
    PERFORM test_log('INFO','TEST-14','=== TEST-14: Некорректная ссылка на product_id - START ===');

    INSERT INTO payments(id, payment_status, user_strapi_document_id, product_id)
    VALUES(v_payment_id,'NEW','user_bad_product',99999999);

    UPDATE payments
       SET payment_status='CONFIRMED'
     WHERE id=v_payment_id;

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE payment_id=v_payment_id
          AND level='WARNING'
          AND message LIKE '%No rows found in products%'
    ) THEN
        RAISE EXCEPTION 'TEST-14 FAIL: Не найден WARNING "No rows found in products"';
    END IF;

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE payment_id=v_payment_id
          AND level='WARNING'
          AND message LIKE '%Unknown entity_type%'
    ) THEN
        RAISE EXCEPTION 'TEST-14 FAIL: Не найден WARNING "Unknown entity_type= for CONFIRMED"';
    END IF;

    DELETE FROM payments WHERE id=v_payment_id;
    PERFORM test_log('INFO','TEST-14','=== TEST-14 PASSED ===');
END;
