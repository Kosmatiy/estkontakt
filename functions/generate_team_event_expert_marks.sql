DECLARE
    /* ── спринт / stream / event ─────────────────────────────── */
    v_sprint      sprints%ROWTYPE;
    v_stream_id   TEXT;
    v_event       team_events%ROWTYPE;
    v_user_id     TEXT;

    /* ── эксперт ────────────────────────────────────────────── */
    v_expert_id   TEXT;

    /* ── цикл по командам ───────────────────────────────────── */
    team_id       TEXT;
    present_arr   TEXT[];
    sample_arr    TEXT[];
    sample_size   INT;

    /* ── счётчик ────────────────────────────────────────────── */
    v_rows_inserted INT := 0;
    v_rc            INT;
BEGIN
    /* 0. спринт и stream */
    SELECT * INTO v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                                 'message', format('Спринт %s не найден',
                                                   in_sprint_document_id));
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* 1. единственный team_event спринта */
    SELECT * INTO v_event
    FROM   team_events
    WHERE  sprint_strapi_document_id = in_sprint_document_id
    LIMIT  1;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                                 'message','team_event для спринта не найден');
    END IF;

    /* 2. случайный эксперт */
    SELECT strapi_document_id
      INTO v_expert_id
      FROM experts
     WHERE dismissed_at IS NULL
     ORDER BY random()
     LIMIT 1;

    IF v_expert_id IS NULL THEN
        RETURN json_build_object('result','error',
                                 'message','нет доступных экспертов');
    END IF;

    /* 3. цикл по командам потока */
    FOR team_id IN
        SELECT DISTINCT team_strapi_document_id
        FROM   users u
        WHERE  u.stream_strapi_document_id = v_stream_id
          AND  u.dismissed_at IS NULL
          AND  team_strapi_document_id IS NOT NULL
    LOOP
        /* присутствующие will_attend=TRUE */
        SELECT array_agg(u.strapi_document_id) INTO present_arr
        FROM   users u
        JOIN   user_team_events ute
               ON ute.user_strapi_document_id   = u.strapi_document_id
        WHERE  u.team_strapi_document_id       = team_id
          AND  ute.team_event_strapi_document_id = v_event.strapi_document_id
          AND  ute.will_attend
          AND  u.dismissed_at IS NULL;

        IF present_arr IS NULL THEN CONTINUE; END IF;

        sample_size :=
            LEAST(v_event.minimum_participants_from_team,
                  array_length(present_arr,1));

        /* случайная выборка sample_size игроков */
        SELECT array_agg(uid)
          INTO sample_arr
          FROM (
              SELECT unnest(present_arr) AS uid
              ORDER  BY random()
              LIMIT  sample_size
          ) t;

        /* вставляем оценки */
        FOREACH  v_user_id IN ARRAY sample_arr
        LOOP
            INSERT INTO expert_team_events_marks(
                expert_strapi_document_id,
                user_strapi_document_id,
                team_event_strapi_document_id,
                mark,
                created_at
            )
            VALUES(
                v_expert_id,
                v_user_id,
                v_event.strapi_document_id,
                CASE WHEN team_id = in_team_document_id THEN 20 ELSE -5 END,
                NOW()
            )
            ON CONFLICT (expert_strapi_document_id,
                         user_strapi_document_id,
                         team_event_strapi_document_id)
            DO UPDATE SET mark = EXCLUDED.mark;

            GET DIAGNOSTICS v_rc = ROW_COUNT;
            v_rows_inserted := v_rows_inserted + v_rc;
        END LOOP;
    END LOOP;

    /* 4. итоговый JSON */
    RETURN json_build_object(
        'result'           , 'success',
        'generated_records', v_rows_inserted,
        'message'          , format(
           'Оценки эксперта %s для события %s: добавлено %s строк. Победитель – команда %s.',
           v_expert_id, v_event.strapi_document_id,
           v_rows_inserted, in_team_document_id)
    );

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object('result','error','message',SQLERRM);
END;
