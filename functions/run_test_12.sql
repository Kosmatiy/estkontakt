DECLARE
    v_strikes_before int;
    v_strikes_after  int;
BEGIN
    PERFORM test_log('INFO','TEST-12','=== TEST-12: user_strikes_purchases (false->true) - START ===');
    -- user_strikes_purchases(payment_id='X', user1, added_strikes=2, is_active=false), user1.strikes_limit=10

    SELECT strikes_limit
      INTO v_strikes_before
      FROM users
     WHERE strapi_document_id='user1';

    PERFORM test_log('DEBUG','TEST-12','strikes_before='||COALESCE(v_strikes_before::text,'NULL'));

    UPDATE user_strikes_purchases
       SET is_active=true
     WHERE payment_id='X'
       AND user_strapi_document_id='user1';

    SELECT strikes_limit
      INTO v_strikes_after
      FROM users
     WHERE strapi_document_id='user1';

    IF v_strikes_after<>(v_strikes_before+2) THEN
        RAISE EXCEPTION 'TEST-12 FAIL: ожидалось %, фактически %', v_strikes_before+2, v_strikes_after;
    END IF;

    IF NOT EXISTS(
       SELECT 1
         FROM user_strikes_purchases
        WHERE payment_id='X'
          AND user_strapi_document_id='user1'
          AND is_active=true
    ) THEN
        RAISE EXCEPTION 'TEST-12 FAIL: user_strikes_purchases не стало is_active=true.';
    END IF;

    PERFORM test_log('INFO','TEST-12','=== TEST-12 PASSED: добавлено 2 страйка ===');
END;
