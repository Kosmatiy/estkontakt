DECLARE
    v_strikes_before int;
    v_strikes_after  int;
BEGIN
    PERFORM test_log('INFO','TEST-1','=== TEST-1: Оплата подтверждена (LIVE_PACK) - START ===');

    -- Предполагаем, что payment_id='some_uuid' со status='NEW', user='user1' 
    -- (u1.strikes_limit=3), lead='lead1', product_id => entity_type=LIVE_PACK

    PERFORM test_log('DEBUG','TEST-1','Считываем текущий strikes_limit для user1...');
    SELECT strikes_limit 
      INTO v_strikes_before
      FROM public.users
     WHERE strapi_document_id='user1';

    IF v_strikes_before IS NULL THEN
        RAISE EXCEPTION 'TEST-1 FAIL: user1 not found or strikes_limit IS NULL.';
    END IF;
    PERFORM test_log('DEBUG','TEST-1','Текущий strikes_before='||v_strikes_before);

    PERFORM test_log('DEBUG','TEST-1','UPDATE payments SET payment_status=CONFIRMED WHERE id=''some_uuid''...');
    UPDATE payments
       SET payment_status='CONFIRMED'
     WHERE id='some_uuid';

    PERFORM test_log('DEBUG','TEST-1','Проверяем запись в user_strikes_purchases...');
    IF NOT EXISTS(
       SELECT 1 
         FROM public.user_strikes_purchases
        WHERE payment_id='some_uuid'
          AND user_strapi_document_id='user1'
          AND is_active=true
    ) THEN
        RAISE EXCEPTION 'TEST-1 FAIL: нет записи is_active=true в user_strikes_purchases.';
    END IF;

    -- Проверяем strikes_limit = 3+1=4
    SELECT strikes_limit
      INTO v_strikes_after
      FROM public.users
     WHERE strapi_document_id='user1';

    IF v_strikes_after<>(v_strikes_before+1) THEN
        RAISE EXCEPTION 'TEST-1 FAIL: ожидалось %, а фактически %', v_strikes_before+1, v_strikes_after;
    END IF;

    PERFORM test_log('DEBUG','TEST-1','OK: strikes_limit=% (было % + 1)', v_strikes_after::text, v_strikes_before::text);

    -- Проверяем логи
    IF NOT EXISTS(
       SELECT 1 
         FROM payment_logs
        WHERE payment_id='some_uuid'
          AND level='DEBUG'
          AND message LIKE '%CONFIRMED => activate resources%'
    ) THEN
        RAISE EXCEPTION 'TEST-1 FAIL: не найден DEBUG лог "CONFIRMED => activate resources"';
    END IF;

    IF NOT EXISTS(
       SELECT 1
         FROM payment_logs
        WHERE payment_id='some_uuid'
          AND level='INFO'
          AND message LIKE '%Payment confirmed and resources activated%'
    ) THEN
        RAISE EXCEPTION 'TEST-1 FAIL: не найден INFO лог "Payment confirmed and resources activated"';
    END IF;

    PERFORM test_log('INFO','TEST-1','=== TEST-1 PASSED ===');
END;
