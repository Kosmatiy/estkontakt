DECLARE
    v_strikes_before int;
    v_strikes_after  int;
BEGIN
    PERFORM test_log('INFO','TEST-13','=== TEST-13: Повторная деактивация (is_active=false->false) - START ===');
    -- user_strikes_purchases(payment_id='X', user1, added_strikes=2, is_active=false), user1.strikes_limit=10

    SELECT strikes_limit
      INTO v_strikes_before
      FROM users
     WHERE strapi_document_id='user1';

    UPDATE user_strikes_purchases
       SET is_active=false
     WHERE payment_id='X'
       AND user_strapi_document_id='user1';

    SELECT strikes_limit
      INTO v_strikes_after
      FROM users
     WHERE strapi_document_id='user1';

    IF v_strikes_after<>v_strikes_before THEN
        RAISE EXCEPTION 'TEST-13 FAIL: strikes_limit изменился, а ожидалось без изменений.';
    END IF;

    PERFORM test_log('INFO','TEST-13','=== TEST-13 PASSED: is_active уже был false => strikes_limit не меняется ===');
END;
