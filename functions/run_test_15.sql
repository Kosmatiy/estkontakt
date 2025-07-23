DECLARE
    v_payment_id uuid := gen_random_uuid();
BEGIN
    PERFORM test_log('INFO','TEST-15','=== TEST-15: Платёж без user_strapi_document_id - START ===');

    INSERT INTO payments(id, payment_status, user_strapi_document_id, product_id)
    VALUES(v_payment_id,'NEW',NULL,1);

    UPDATE payments
       SET payment_status='CONFIRMED'
     WHERE id=v_payment_id;

    -- Проверяем, что не создалось записей
    IF EXISTS(
       SELECT 1
         FROM user_strikes_purchases
        WHERE payment_id=v_payment_id
    ) THEN
        RAISE EXCEPTION 'TEST-15 FAIL: user_strikes_purchases появилась при user=NULL.';
    END IF;

    IF EXISTS(
       SELECT 1
         FROM user_stream_access
        WHERE user_strapi_document_id IS NULL
          AND is_active=true
    ) THEN
        RAISE EXCEPTION 'TEST-15 FAIL: user_stream_access создалась для NULL user.';
    END IF;

    IF EXISTS(
       SELECT 1
         FROM user_meeting_access
        WHERE user_strapi_document_id IS NULL
          AND is_active=true
    ) THEN
        RAISE EXCEPTION 'TEST-15 FAIL: user_meeting_access создалась для NULL user.';
    END IF;

    DELETE FROM payments WHERE id=v_payment_id;
    PERFORM test_log('INFO','TEST-15','=== TEST-15 PASSED ===');
END;
