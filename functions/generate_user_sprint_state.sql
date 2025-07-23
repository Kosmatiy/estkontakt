DECLARE
    /* ── текущий спринт и stream ──────────────────────────────── */
    v_sprint        sprints%ROWTYPE;
    v_stream_id     TEXT;

    /* ── счётчики ─────────────────────────────────────────────── */
    v_duels_cnt     INT;
    v_total_users   INT;
    v_double_cnt    INT;
    v_rows_affected INT := 0;
    v_batch_cnt     INT;

    /* ── список «удвоителей» для отчёта ───────────────────────── */
    v_double_list   TEXT;

    /* ── курсор по пользователям потока ───────────────────────── */
    r_user          RECORD;
BEGIN
    /* 0. Проверяем спринт */
    SELECT * INTO v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object(
            'result' ,'error',
            'message', format('Спринт %s не найден', in_sprint_document_id)
        );
    END IF;

    v_stream_id := v_sprint.stream_strapi_document_id;

    /* 1. Очистка при CLEANSLATE */
    IF mode = 'CLEANSLATE' THEN
        DELETE FROM user_sprint_state uss
        USING duels d
        WHERE uss.duel_strapi_document_id = d.strapi_document_id
          AND d.sprint_strapi_document_id = v_sprint.strapi_document_id;
    END IF;

    /* 2. Соберём дуэли спринта */
    SELECT COUNT(*) INTO v_duels_cnt
    FROM   duels
    WHERE  sprint_strapi_document_id = v_sprint.strapi_document_id;

    IF v_duels_cnt = 0 THEN
        RETURN json_build_object(
            'result' ,'error',
            'message','В спринте нет дуэлей'
        );
    END IF;

    /* 3. Пользователи потока */
    SELECT COUNT(*) INTO v_total_users
    FROM   users
    WHERE  dismissed_at IS NULL
      AND  stream_strapi_document_id = v_stream_id;

    IF v_total_users = 0 THEN
        RETURN json_build_object(
            'result' ,'error',
            'message','В потоке нет активных пользователей'
        );
    END IF;

    /* 4. Формируем таблицу «удвоителей» */
    v_double_cnt := CEIL(v_total_users * in_percent / 100.0)::INT;

    CREATE TEMP TABLE _double_users(user_id TEXT PRIMARY KEY) ON COMMIT DROP;

    INSERT INTO _double_users(user_id)
    SELECT strapi_document_id
    FROM   users
    WHERE  dismissed_at IS NULL
      AND  stream_strapi_document_id = v_stream_id
    ORDER  BY random()
    LIMIT  v_double_cnt;

    /* Список в человекочитаемом виде */
    SELECT string_agg(format('%s %s (@%s)', name, surname, telegram_username), ', ')
    INTO   v_double_list
    FROM   users
    WHERE  strapi_document_id IN (SELECT user_id FROM _double_users);

    /* 5. Основной цикл по пользователям потока */
    FOR r_user IN
        SELECT *
        FROM   users
        WHERE  dismissed_at IS NULL
          AND  stream_strapi_document_id = v_stream_id
    LOOP
        /* TRUE/FALSE для is_repeats_ok */
        PERFORM 1 FROM _double_users WHERE user_id = r_user.strapi_document_id;
        -- found → TRUE, иначе FALSE
        INSERT INTO user_sprint_state(
               user_strapi_document_id,
               duel_strapi_document_id,
               is_chosen,
               is_repeats_ok,
               created_at)
        SELECT r_user.strapi_document_id,
               d.strapi_document_id,
               FALSE,
               FOUND,                       -- ← результат PERFORM
               NOW()
        FROM   duels d
        WHERE  d.sprint_strapi_document_id = v_sprint.strapi_document_id
        ON CONFLICT (user_strapi_document_id, duel_strapi_document_id)
        DO UPDATE
           SET is_repeats_ok = EXCLUDED.is_repeats_ok,
               is_chosen     = FALSE;

        GET DIAGNOSTICS v_batch_cnt = ROW_COUNT;
        v_rows_affected := v_rows_affected + v_batch_cnt;
    END LOOP;

    /* 6. Финальный ответ */
    RETURN json_build_object(
        'result'           , 'success',
        'generated_records', v_rows_affected,
        'double_users'     , COALESCE(v_double_list,'нет'),
       'message' , format(
    'user_sprint_state: спринт «%s» (%s дуэлей). Пользователей %s, is_repeats_ok=TRUE у %s (%s%%). Режим: %s.',
    v_sprint.sprint_name,          -- %s
    v_duels_cnt::text,             -- %s
    v_total_users::text,           -- %s
    v_double_cnt::text,            -- %s
    in_percent::text,              -- %s  ← вместо %.0f
    mode                           -- %s
)
    );

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
        'result' ,'error',
        'message', SQLERRM
    );
END;
