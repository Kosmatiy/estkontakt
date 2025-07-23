DECLARE
    v_strikes_before int;
    v_strikes_after  int;
BEGIN
    PERFORM test_log('INFO','TEST-2','=== TEST-2: Повторный CONFIRMED (LIVE_PACK) - START ===');

    -- Предполагается, что user1.strikes_limit=4, user_strikes_purchases(...some_uuid..., is_active=true),
    -- payment_status='CONFIRMED' уже.

    SELECT strikes_limit
      INTO v_strikes_before
      FROM public.users
     WHERE strapi_document_id='user1';

    PERFORM test_log('DEBUG','TEST-2','Снова ставим payment_status=CONFIRMED');
    UPDATE payments
       SET payment_status='CONFIRMED'
     WHERE id='some_uuid';

    SELECT strikes_limit
      INTO v_strikes_after
      FROM public.users
     WHERE strapi_document_id='user1';

    IF v_strikes_after<>v_strikes_before THEN
        RAISE EXCEPTION 'TEST-2 FAIL: strikes_limit изменился повторно, ожидалось без изменений.';
    END IF;

    PERFORM test_log('DEBUG','TEST-2','strikes_limit остался % => OK', v_strikes_after::text);
    PERFORM test_log('INFO','TEST-2','=== TEST-2 PASSED ===');
END;
