DECLARE
    v_strikes_before int;
    v_strikes_after  int;
BEGIN
    PERFORM test_log('INFO','TEST-3','=== TEST-3: Отмена платежа (LIVE_PACK) - REFUNDED - START ===');
    -- user1.strikes_limit=4, user_strikes_purchases(...some_uuid..., is_active=true), payment_status='CONFIRMED'

    SELECT strikes_limit
      INTO v_strikes_before
      FROM public.users
     WHERE strapi_document_id='user1';

    UPDATE payments
       SET payment_status='REFUNDED'
     WHERE id='some_uuid';

    IF NOT EXISTS(
       SELECT 1
         FROM user_strikes_purchases
        WHERE payment_id='some_uuid'
          AND user_strapi_document_id='user1'
          AND is_active=false
    ) THEN
        RAISE EXCEPTION 'TEST-3 FAIL: user_strikes_purchases не стало is_active=false.';
    END IF;

    SELECT strikes_limit
      INTO v_strikes_after
      FROM public.users
     WHERE strapi_document_id='user1';

    IF v_strikes_after<>(v_strikes_before-1) THEN
        RAISE EXCEPTION 'TEST-3 FAIL: ожидалось %, фактически %', v_strikes_before-1, v_strikes_after;
    END IF;

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE payment_id='some_uuid'
          AND level='DEBUG'
          AND message LIKE '%Refund/Cancel => deactivate resources%'
    ) THEN
        RAISE EXCEPTION 'TEST-3 FAIL: Не найден DEBUG лог Refund/Cancel => deactivate resources';
    END IF;

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE payment_id='some_uuid'
          AND level='INFO'
          AND message LIKE '%Payment refunded/canceled, resources deactivated%'
    ) THEN
        RAISE EXCEPTION 'TEST-3 FAIL: Не найден INFO лог Payment refunded/canceled, resources deactivated';
    END IF;

    PERFORM test_log('INFO','TEST-3','=== TEST-3 PASSED ===');
END;
