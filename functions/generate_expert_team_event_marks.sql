DECLARE
    v_stream_id  TEXT;
    v_event_id   TEXT;
    v_min_part   INT;

    -- счётчики
    marks_added  INT := 0;
    res_added    INT := 0;

    -- внутр. переменные
    rec_team     RECORD;
    attended_cnt INT;
BEGIN
/* 1. stream и team_event --------------------------------------------------- */
    SELECT stream_strapi_document_id
      INTO v_stream_id
      FROM sprints
     WHERE strapi_document_id = in_sprint_id;

    IF v_stream_id IS NULL THEN
        RETURN json_build_object('result','error',
                                 'message','Спринт не найден или без stream');
    END IF;

    SELECT strapi_document_id,
           minimum_participants_from_team
      INTO v_event_id, v_min_part
      FROM team_events
     WHERE sprint_strapi_document_id = in_sprint_id
     LIMIT 1;

    IF v_event_id IS NULL THEN
        RETURN json_build_object('result','error',
                                 'message','team_event для спринта не найден');
    END IF;

/* 2. Стираем старые данные эксперта по этому событию ---------------------- */
    DELETE FROM expert_team_events_marks
     WHERE expert_strapi_document_id      = in_expert_id
       AND team_event_strapi_document_id  = v_event_id;

    DELETE FROM expert_team_event_results
     WHERE expert_strapi_document_id      = in_expert_id
       AND team_event_strapi_document_id  = v_event_id;

/* 3. Временная таблица всех игроков stream’а ------------------------------ */
    DROP TABLE IF EXISTS _team_users;
    CREATE TEMP TABLE _team_users ON COMMIT DROP AS
    SELECT
        team_strapi_document_id AS team_id,
        strapi_document_id      AS user_id,
        FALSE                   AS attended,
        FALSE                   AS played
    FROM   users
    WHERE  stream_strapi_document_id = v_stream_id
      AND  team_strapi_document_id IS NOT NULL;

/* 4. Обрабатываем каждую команду ------------------------------------------ */
    FOR rec_team IN
        SELECT team_id,
               ARRAY_AGG(user_id) AS members
        FROM   _team_users
        GROUP  BY team_id
    LOOP
        /* 4.1. Случайное attended ( p = 0.7 ) */
        UPDATE _team_users
           SET attended = (random() < 0.7)
         WHERE team_id = rec_team.team_id;

        /* 4.2. Гарантируем минимум присутствующих */
        SELECT COUNT(*) INTO attended_cnt
        FROM   _team_users
        WHERE  team_id = rec_team.team_id
          AND  attended;

        IF attended_cnt < v_min_part THEN
            UPDATE _team_users
               SET attended = TRUE
             WHERE ctid IN (
                   SELECT ctid
                     FROM _team_users
                    WHERE team_id = rec_team.team_id
                      AND attended = FALSE
                    ORDER BY random()
                    LIMIT (v_min_part - attended_cnt)
             );
        END IF;

        /* 4.3. Выбираем played = TRUE ровно min_part среди attended */
        UPDATE _team_users
           SET played = FALSE
         WHERE team_id = rec_team.team_id;

        UPDATE _team_users
           SET played = TRUE
         WHERE ctid IN (
               SELECT ctid
                 FROM _team_users
                WHERE team_id = rec_team.team_id
                  AND attended
                ORDER BY random()
                LIMIT v_min_part
         );

        /* 4.4. Вставляем marks для всех членов команды */
        INSERT INTO expert_team_events_marks(
            expert_strapi_document_id,
            user_strapi_document_id,
            team_event_strapi_document_id,
            mark,
            attended,
            played
        )
        SELECT
            in_expert_id,
            user_id,
            v_event_id,
            CASE
              WHEN played AND team_id = in_winner_team_id THEN 20
              WHEN played AND team_id <> in_winner_team_id THEN -5
              ELSE NULL
            END,
            attended,
            played
        FROM _team_users
        WHERE team_id = rec_team.team_id;

        GET DIAGNOSTICS attended_cnt = ROW_COUNT;
        marks_added := marks_added + attended_cnt;

        /* 4.5. Результаты команды (won) */
        INSERT INTO expert_team_event_results(
            expert_strapi_document_id,
            team_strapi_document_id,
            team_event_strapi_document_id,
            won
        )
        VALUES (
            in_expert_id,
            rec_team.team_id,
            v_event_id,
            (rec_team.team_id = in_winner_team_id)
        );

        res_added := res_added + 1;
    END LOOP;

/* 5. Финальный ответ ------------------------------------------------------ */
    RETURN json_build_object(
        'result'          ,'success',
        'marks_inserted'  ,marks_added,
        'results_inserted',res_added,
        'message'         ,format(
            'Событие %s: вставлено %s оценок и %s результатов.',
            v_event_id, marks_added, res_added)
    );
END;
