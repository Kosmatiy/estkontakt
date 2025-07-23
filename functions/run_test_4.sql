DECLARE
    v_strikes_before int;
    v_strikes_after  int;
BEGIN
    PERFORM test_log('INFO','TEST-4','=== TEST-4: Повторная отмена (LIVE_PACK) - START ===');
    -- user_strikes_purchases(...some_uuid..., is_active=false), strikes_limit=3, payment_status='REFUNDED'

    SELECT strikes_limit
      INTO v_strikes_before
      FROM public.users
     WHERE strapi_document_id='user1';

    UPDATE payments
       SET payment_status='REFUNDED'
     WHERE id='some_uuid';

    SELECT strikes_limit
      INTO v_strikes_after
      FROM public.users
     WHERE strapi_document_id='user1';

    IF v_strikes_after<>v_strikes_before THEN
        RAISE EXCEPTION 'TEST-4 FAIL: strikes_limit изменился повторно, хотя is_active уже false.';
    END IF;

    PERFORM test_log('DEBUG','TEST-4','strikes_limit осталось % => OK', v_strikes_after::text);
    PERFORM test_log('INFO','TEST-4','=== TEST-4 PASSED ===');
END;
