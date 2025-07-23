DECLARE
    v_log             JSONB := '[]'::JSONB;
    v_error           JSONB;
    v_filled          INT;
    v_needed          INT;
    v_test_result     JSONB;
    v_selected_review TEXT;
    v_slot            RECORD;
    stage             INT;
BEGIN
    -- 1) Проверяем режим
    IF p_mode NOT IN ('CLEANSLATE','GOON') THEN
        RAISE EXCEPTION 'Invalid mode: %', p_mode;
    END IF;

    BEGIN
        -- 2) В режиме CLEANSLATE удаляем старые распределения
        IF p_mode = 'CLEANSLATE' THEN
            WITH sprint_duels AS (
                SELECT strapi_document_id
                  FROM duels
                 WHERE sprint_strapi_document_id = p_sprint_id
            )
            DELETE FROM user_duel_to_review
             WHERE duel_strapi_document_id IN (SELECT strapi_document_id FROM sprint_duels);
        END IF;

        -- 3) Создаём слоты: по 6 на каждого участника каждой дуэли
        CREATE TEMP TABLE review_slots (
            duel_id        TEXT,
            hash           TEXT,
            participant_id TEXT,
            slot_no        INT,
            reviewer_id    TEXT     DEFAULT NULL,
            stage_assigned INT      DEFAULT NULL
        ) ON COMMIT DROP;

        WITH active_users AS (
            SELECT u.strapi_document_id AS user_id
              FROM users u
              JOIN user_stream_links usl
                ON usl.user_strapi_document_id = u.strapi_document_id
              JOIN sprints s
                ON s.strapi_document_id = p_sprint_id
             WHERE u.dismissed_at IS NULL
               AND usl.is_active
               AND usl.stream_strapi_document_id = s.stream_strapi_document_id
        ),
        units AS (
            SELECT uda.duel_strapi_document_id AS duel_id,
                   uda.hash,
                   uda.user_strapi_document_id  AS participant_id
              FROM user_duel_answers uda
              JOIN duels d
                ON d.strapi_document_id = uda.duel_strapi_document_id
              JOIN active_users au
                ON au.user_id = uda.user_strapi_document_id
             WHERE d.sprint_strapi_document_id = p_sprint_id
        )
        INSERT INTO review_slots (duel_id, hash, participant_id, slot_no)
        SELECT u.duel_id, u.hash, u.participant_id, gs
          FROM units u
    CROSS JOIN generate_series(1,6) AS gs;

        -- 4) В режиме GOON отмечаем существующие записи
        IF p_mode = 'GOON' THEN
            WITH exist AS (
                SELECT reviewer_user_strapi_document_id AS reviewer,
                       duel_strapi_document_id        AS duel_id,
                       user_strapi_document_id         AS participant_id,
                       hash
                  FROM user_duel_to_review
                 WHERE duel_strapi_document_id IN (
                     SELECT strapi_document_id
                       FROM duels
                      WHERE sprint_strapi_document_id = p_sprint_id
                 )
            )
            UPDATE review_slots rs
               SET reviewer_id    = e.reviewer,
                   stage_assigned = 0
              FROM exist e
             WHERE rs.duel_id        = e.duel_id
               AND rs.hash           = e.hash
               AND rs.participant_id = e.participant_id
               AND rs.slot_no IN (
                   SELECT MIN(rs2.slot_no)
                     FROM review_slots rs2
                    WHERE rs2.duel_id        = rs.duel_id
                      AND rs2.hash           = rs.hash
                      AND rs2.participant_id = rs.participant_id
                      AND rs2.reviewer_id IS NULL
               );
        END IF;

        -- 5) Инициализируем квоты и загрузки ревьюеров
        CREATE TEMP TABLE reviewer_loads (
            reviewer_id   TEXT,
            quota         INT,
            assigned      INT      DEFAULT 0,
            assigned_pairs JSONB   DEFAULT '{}'::JSONB
        ) ON COMMIT DROP;

        WITH active_users AS (
            SELECT u.strapi_document_id AS user_id
              FROM users u
              JOIN user_stream_links usl
                ON usl.user_strapi_document_id = u.strapi_document_id
              JOIN sprints s
                ON s.strapi_document_id = p_sprint_id
             WHERE u.dismissed_at IS NULL
               AND usl.is_active
               AND usl.stream_strapi_document_id = s.stream_strapi_document_id
        ),
        quotas AS (
            SELECT au.user_id,
                   COUNT(uda.duel_answer_id) * 3 AS quota
              FROM active_users au
              LEFT JOIN user_duel_answers uda
                ON uda.user_strapi_document_id = au.user_id
              JOIN duels d
                ON d.strapi_document_id = uda.duel_strapi_document_id
             WHERE d.sprint_strapi_document_id = p_sprint_id
             GROUP BY au.user_id
        )
        INSERT INTO reviewer_loads (reviewer_id, quota)
        SELECT user_id, quota FROM quotas;

        -- 6) Три стадии назначения
        FOR stage IN 1..3 LOOP
            v_log := v_log || jsonb_build_object('stage', stage, 'assignments', '[]'::JSONB);
            LOOP
                -- выбираем следующий пустой слот
                SELECT * INTO v_slot
                  FROM review_slots
                 WHERE reviewer_id IS NULL
                 ORDER BY duel_id, hash, participant_id, slot_no
                 LIMIT 1;
                EXIT WHEN NOT FOUND;

                -- ищем одного кандидата по текущей стадии
                SELECT c.reviewer_id
                  INTO v_selected_review
                  FROM (
                      SELECT rl.reviewer_id,
                             rl.assigned,
                             (rl.quota - rl.assigned) AS free_slots,
                             (
                              SELECT COUNT(*)
                                FROM user_duel_answers ua2
                               WHERE ua2.user_strapi_document_id = rl.reviewer_id
                                 AND ua2.duel_strapi_document_id  = v_slot.duel_id
                             ) AS played
                        FROM reviewer_loads rl
                       WHERE rl.assigned < rl.quota
                         AND rl.reviewer_id <> v_slot.participant_id
                         AND NOT (rl.assigned_pairs ? (v_slot.duel_id || '_' || v_slot.hash))
                  ) c
                 WHERE (stage = 1 AND c.played > 0)
                    OR (stage = 2)
                    OR (stage = 3)
                 ORDER BY c.assigned, c.reviewer_id
                 LIMIT 1;

                IF v_selected_review IS NULL THEN
                    EXIT;  -- больше нет кандидатов на этой стадии
                END IF;

                -- назначаем ревьюера
                UPDATE review_slots
                   SET reviewer_id    = v_selected_review,
                       stage_assigned = stage
                 WHERE duel_id        = v_slot.duel_id
                   AND hash           = v_slot.hash
                   AND participant_id = v_slot.participant_id
                   AND slot_no        = v_slot.slot_no;

                -- обновляем нагрузку
                UPDATE reviewer_loads
                   SET assigned       = assigned + 1,
                       assigned_pairs = assigned_pairs || jsonb_build_object(
                           v_slot.duel_id || '_' || v_slot.hash, TRUE
                       )
                 WHERE reviewer_id = v_selected_review;

                -- логируем назначение
                v_log := jsonb_set(
                    v_log,
                    '{-1,assignments,-1}',
                    jsonb_build_object(
                        'slot', jsonb_build_object(
                            'duel',        v_slot.duel_id,
                            'hash',        v_slot.hash,
                            'participant', v_slot.participant_id,
                            'slot_no',     v_slot.slot_no
                        ),
                        'reviewer', v_selected_review
                    )
                );
            END LOOP;
        END LOOP;

        -- 7) Проверяем, что все слоты заполнены
        SELECT COUNT(*) INTO v_filled FROM review_slots WHERE reviewer_id IS NOT NULL;
        SELECT COUNT(*) INTO v_needed FROM review_slots;
        IF v_filled <> v_needed THEN
            v_error := jsonb_build_object(
                'status', 'FAIL',
                'reason', 'Not all slots filled',
                'filled', v_filled,
                'needed', v_needed,
                'log',    v_log
            );
            RAISE EXCEPTION '%', v_error;
        END IF;

        -- 8) Записываем новые назначения в основную таблицу
        INSERT INTO user_duel_to_review (
            reviewer_user_strapi_document_id,
            duel_strapi_document_id,
            user_strapi_document_id,
            hash,
            created_at
        )
        SELECT
            reviewer_id,
            duel_id,
            participant_id,
            hash,
            NOW()
          FROM review_slots
         WHERE stage_assigned > 0
        ON CONFLICT DO NOTHING;

        -- 9) Финальный тест
        SELECT test_user_duel_to_review(p_sprint_id) INTO v_test_result;
        IF (v_test_result->>'status')   <> 'OK'
           OR (v_test_result->>'bad_pairs')::INT     <> 0
           OR (v_test_result->>'duplicates')::INT     <> 0
           OR (v_test_result->>'quota_violations')::INT <> 0
        THEN
            v_error := jsonb_build_object(
                'status',      'FAIL',
                'reason',      'Test failed',
                'test_result', v_test_result,
                'log',         v_log
            );
            RAISE EXCEPTION '%', v_error;
        END IF;

        RETURN 'OK';

    EXCEPTION WHEN OTHERS THEN
        IF v_error IS NULL THEN
            v_error := jsonb_build_object(
                'status','FAIL',
                'reason', SQLERRM,
                'log',    v_log
            );
        END IF;
        RAISE NOTICE '%', v_error;
        RETURN v_error::TEXT;
    END;
END;
