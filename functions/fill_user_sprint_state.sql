DECLARE
    /*--- спринты ---*/
    v_curr         sprints%ROWTYPE;
    v_prev         sprints%ROWTYPE;
    v_prev_ok      boolean := false;

    /*--- пользователи ---*/
    r_user         users%ROWTYPE;

    /*--- счётчики ---*/
    v_rep_ok       boolean;
    v_duels_cnt    int;
    v_exist_cnt    int;
    v_inserted_cnt int;
    v_rows_total   int := 0;

    /*--- журнал ---*/
    v_log          text[] := '{}'::text[];
BEGIN
    /* 1. текущий спринт */
    SELECT * INTO v_curr
    FROM   sprints
    WHERE  strapi_document_id = v_sprint_strapi_document_id;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'created_total', 0,
            'logs', to_jsonb(ARRAY[
              format('❌ Спринт с id=%s не найден', v_sprint_strapi_document_id)
            ])
        );
    END IF;

    v_log := array_append(
        v_log,
        format('✔ Найден текущий спринт №%s (%s)',
               v_curr.sprint_number, v_curr.sprint_name)
    );

    /* 2. количество дуэлей */
    SELECT count(*) INTO v_duels_cnt
    FROM   duels
    WHERE  sprint_strapi_document_id = v_curr.strapi_document_id;

    IF v_duels_cnt = 0 THEN
        v_log := array_append(v_log,
                 '⚠ В этом спринте нет дуэлей — нечего вставлять');
        RETURN jsonb_build_object('created_total', 0, 'logs', to_jsonb(v_log));
    END IF;

    v_log := array_append(
        v_log,
        format('✔ В спринте дуэлей: %s', v_duels_cnt)
    );

    /* 3. предыдущий спринт того же потока */
    SELECT * INTO v_prev
    FROM   sprints
    WHERE  stream_strapi_document_id = v_curr.stream_strapi_document_id
      AND  sprint_number             = v_curr.sprint_number - 1;

    IF FOUND THEN
        v_prev_ok := true;
        v_log := array_append(
            v_log,
            format('✔ Найден предыдущий спринт №%s', v_prev.sprint_number)
        );
    ELSE
        v_log := array_append(
            v_log,
            '⚠ Предыдущего спринта того же потока не найден — is_repeats_ok будет false'
        );
    END IF;

    /* 4. цикл по активным пользователям */
    FOR r_user IN
        SELECT *
        FROM   users
        WHERE  dismissed_at IS NULL
    LOOP
        v_rep_ok := false;

        IF v_prev_ok THEN
            SELECT uss.is_repeats_ok
            INTO   v_rep_ok
            FROM   user_sprint_state uss
            JOIN   duels d ON d.strapi_document_id = uss.duel_strapi_document_id
            WHERE  uss.user_strapi_document_id = r_user.strapi_document_id
              AND  d.sprint_strapi_document_id  = v_prev.strapi_document_id
            LIMIT  1;

            IF NOT FOUND THEN
                v_rep_ok := false;
            END IF;
        END IF;

        /* сколько строк уже было */
        SELECT count(*)
        INTO   v_exist_cnt
        FROM   user_sprint_state uss
        JOIN   duels d ON d.strapi_document_id = uss.duel_strapi_document_id
        WHERE  uss.user_strapi_document_id = r_user.strapi_document_id
          AND  d.sprint_strapi_document_id = v_curr.strapi_document_id;

        /* вставляем отсутствующие строки */
        INSERT INTO user_sprint_state (
               user_strapi_document_id,
               duel_strapi_document_id,
               is_chosen,
               is_repeats_ok,
               created_at)
        SELECT r_user.strapi_document_id,
               d.strapi_document_id,
               false,
               v_rep_ok,
               now()
        FROM   duels d
        WHERE  d.sprint_strapi_document_id = v_curr.strapi_document_id
          AND  NOT EXISTS (
                SELECT 1
                FROM   user_sprint_state uss
                WHERE  uss.user_strapi_document_id = r_user.strapi_document_id
                  AND  uss.duel_strapi_document_id = d.strapi_document_id);

        GET DIAGNOSTICS v_inserted_cnt = ROW_COUNT;
        v_rows_total := v_rows_total + v_inserted_cnt;

        /* обновляем существующие */
        UPDATE user_sprint_state uss
        SET    is_repeats_ok = v_rep_ok,
               is_chosen     = false
        FROM   duels d
        WHERE  uss.user_strapi_document_id = r_user.strapi_document_id
          AND  uss.duel_strapi_document_id = d.strapi_document_id
          AND  d.sprint_strapi_document_id = v_curr.strapi_document_id;

        /* логируем результат по пользователю */
        IF v_inserted_cnt = 0 THEN
            v_log := array_append(
                v_log,
                format('⤵ %s %s: новых строк 0 (всё уже было), обновлено %s',
                       r_user.name, r_user.surname, v_exist_cnt)
            );
        ELSE
            v_log := array_append(
                v_log,
                format('⤴ %s %s: вставлено %s строк, обновлено %s',
                       r_user.name, r_user.surname,
                       v_inserted_cnt, v_exist_cnt)
            );
        END IF;
    END LOOP;

    IF v_rows_total = 0 THEN
        v_log := array_append(v_log, 'ℹ Итог: новых строк не добавлено');
    ELSE
        v_log := array_append(
                   v_log,
                   format('✔ Итог: вставлено %s строк', v_rows_total)
                 );
    END IF;

    RETURN jsonb_build_object(
        'created_total', v_rows_total,
        'logs'         , to_jsonb(v_log)
    );
END;
