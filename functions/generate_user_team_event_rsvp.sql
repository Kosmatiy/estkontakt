DECLARE
    /* спринт / stream / event */
    v_sprint      sprints%ROWTYPE;
    v_stream_id   TEXT;
    v_event_id    TEXT;

    /* счётчики */
    v_total_users   INT;
    v_fail_count    INT;
    v_rows_inserted INT := 0;
    v_rc            INT;

    /* вспомогательные массивы */
    fail_users     TEXT[] := ARRAY[]::TEXT[];  -- FIX: инициализируем пустым
    team_players   TEXT[];
    candidate_arr  TEXT[];
    voter_arr      TEXT[];
    v_voter_id     TEXT;                       -- FIX: правильное имя

    /* рабочие переменные */
    t_team_id      TEXT;
    k_yes          INT;
BEGIN
    /* 0. спринт и stream */
    SELECT *
    INTO   v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                                 'message', format('Спринт %s не найден',
                                                   in_sprint_document_id));
    END IF;

    v_stream_id := v_sprint.stream_strapi_document_id;

    /* event спринта (предполагаем 1 запись) */
    SELECT strapi_document_id
    INTO   v_event_id
    FROM   team_events
    WHERE  sprint_strapi_document_id = in_sprint_document_id
    LIMIT  1;

    IF v_event_id IS NULL THEN
        RETURN json_build_object('result','error',
                                 'message','team_event для спринта не найден');
    END IF;

    /* нормализуем min/max */
    IF in_min_candidates < 1                THEN in_min_candidates := 1; END IF;
    IF in_max_candidates < in_min_candidates THEN in_max_candidates := in_min_candidates; END IF;

    /* 1. очистка при CLEANSLATE */
    IF mode = 'CLEANSLATE' THEN
        DELETE FROM user_team_events
        WHERE team_event_strapi_document_id = v_event_id;
    END IF;

    /* 2. список всех активных пользователей потока */
    SELECT array_agg(strapi_document_id)
    INTO   fail_users
    FROM   users
    WHERE  stream_strapi_document_id = v_stream_id
      AND  dismissed_at IS NULL;

    v_total_users := COALESCE(array_length(fail_users,1),0);

    IF v_total_users = 0 THEN
        RETURN json_build_object('result','error',
                                 'message','В потоке нет активных пользователей');
    END IF;

    /* 3. выбираем, кто НЕ ответит */
    v_fail_count := ceil(v_total_users * in_fail_percent / 100.0)::INT;

    SELECT COALESCE(array_agg(uid), ARRAY[]::TEXT[])      -- FIX: пустой массив вместо NULL
    INTO   fail_users
    FROM (
        SELECT unnest(fail_users) AS uid ORDER BY random() LIMIT v_fail_count
    ) f;

    /* 4. цикл по командам потока */
    FOR t_team_id IN
        SELECT DISTINCT team_strapi_document_id
        FROM   users
        WHERE  stream_strapi_document_id = v_stream_id
          AND  dismissed_at IS NULL
          AND  team_strapi_document_id IS NOT NULL
    LOOP
        /* игроки команды, не попавшие в fail-список */
        SELECT array_agg(strapi_document_id)
        INTO   team_players
        FROM   users
        WHERE  team_strapi_document_id = t_team_id
          AND  dismissed_at IS NULL
          AND  (fail_users = '{}' OR strapi_document_id <> ALL(fail_users)); -- FIX: условие валидно и при пустом массиве

        IF team_players IS NULL THEN
            CONTINUE;
        END IF;

        /* кандидаты */
        k_yes := LEAST(
                    in_min_candidates +
                    floor(random()*(in_max_candidates - in_min_candidates + 1))::INT,
                    array_length(team_players,1));

        SELECT array_agg(uid)
        INTO   candidate_arr
        FROM (
            SELECT unnest(team_players) AS uid ORDER BY random() LIMIT k_yes
        ) sub;

        voter_arr := candidate_arr;  -- отвечают «да»

        /* вставка RSVP «да» */
        FOREACH v_voter_id IN ARRAY voter_arr
        LOOP
            IF mode = 'GOON' THEN
                PERFORM 1
                FROM   user_team_events
                WHERE  user_strapi_document_id  = v_voter_id   -- FIX: v_voter_id
                  AND  team_event_strapi_document_id = v_event_id;
                IF FOUND THEN CONTINUE; END IF;
            END IF;

            INSERT INTO user_team_events(
                user_strapi_document_id,
                team_event_strapi_document_id,
                will_attend)
            VALUES (v_voter_id, v_event_id, TRUE)            -- FIX: v_voter_id
            ON CONFLICT (user_strapi_document_id, team_event_strapi_document_id)
            DO UPDATE SET will_attend = TRUE;

            GET DIAGNOSTICS v_rc = ROW_COUNT;
            v_rows_inserted := v_rows_inserted + v_rc;
        END LOOP;
    END LOOP;

    RETURN json_build_object(
        'result'           , 'success',
        'generated_records', v_rows_inserted,
        'message'          , format(
            'RSVP: поток %s, добавлено %s ответов «да». Пропущено %s из %s (%s%%). Режим %s.',
            v_stream_id,
            v_rows_inserted,
            v_fail_count,
            v_total_users,
            round(in_fail_percent)::int,
            mode
        )
    );

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object('result','error','message',SQLERRM);
END;
