DECLARE
    max_attempts      INT := 5;    -- Сколько раз пытаемся (для примера)
    attempt_count     INT;
    v_stream_id       TEXT;
    v_msg             TEXT;
    v_needed          INT := 6;    
    v_deleted_cnt     INT;
    v_duel_count      INT;
    v_assigned        INT;
    v_short           INT;
    temp_count        INT;
    rec_duel          RECORD;
    rec_ans           RECORD;
    v_teamA           TEXT;
    v_teamB           TEXT;
    v_candidates      TEXT[];
    v_idx             INT;
    v_reviewer_id     TEXT;
    v_bal_need_a      INT;
    v_bal_need_b      INT;
    distribution_failed BOOLEAN;
    -- Дополнительно для логирования/диагностики
    v_found_candidates INT;
BEGIN
    /*
      0) Дополнительная быстрая проверка (необязательная) — смотрим, хватает ли слотов вообще
    */
    SELECT s.stream_strapi_document_id
      INTO v_stream_id
      FROM sprints s
     WHERE s.strapi_document_id = p_sprint_id
     LIMIT 1;
    IF v_stream_id IS NULL THEN
        v_msg := format('Sprint %s not found => stop distribution', p_sprint_id);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;
        RETURN;
    END IF;

    -- Считаем D = кол-во дуэлей
    SELECT COUNT(*) INTO v_duel_count
    FROM duels
    WHERE sprint_strapi_document_id = p_sprint_id;

    IF v_duel_count = 0 THEN
        v_msg := format('No duels in sprint=%s => nothing to do', p_sprint_id);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;
        RETURN;
    END IF;

    -- Пример дополнительной проверки slотов:
    DROP TABLE IF EXISTS tmp_users_played;
    CREATE TEMP TABLE tmp_users_played ON COMMIT DROP AS
    SELECT
      u.strapi_document_id AS user_id,
      COALESCE(SUM( CASE WHEN d.sprint_strapi_document_id = p_sprint_id THEN 1 ELSE 0 END ),0) AS total_played
    FROM users u
    LEFT JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
    LEFT JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE u.stream_strapi_document_id = v_stream_id
      AND u.dismissed_at IS NULL
    GROUP BY u.strapi_document_id;

    SELECT COALESCE(SUM(total_played * 3),0)
      INTO temp_count
    FROM tmp_users_played;

    IF temp_count < (v_duel_count * 6) THEN
        v_msg := format('WARNING: total possible slots=%s < needed=%s => might be impossible!',
                        temp_count, v_duel_count*6);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;
    ELSE
        v_msg := format('OK: total possible slots=%s >= needed=%s => might be feasible in theory',
                        temp_count, v_duel_count*6);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;
    END IF;

    /*
      1..max_attempts: основная попытка распределения
    */
    FOR attempt_count IN 1..max_attempts LOOP
        distribution_failed := false;

        v_msg := format('=== Attempt #%s distribution for sprint=%s ===', attempt_count, p_sprint_id);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;

        -- Удаляем назначения, чтобы начать заново
        SELECT COUNT(*) INTO v_deleted_cnt
        FROM user_duel_to_review
        WHERE duel_strapi_document_id IN (
            SELECT d.strapi_document_id
            FROM duels d
            WHERE d.sprint_strapi_document_id = p_sprint_id
        );

        DELETE FROM user_duel_to_review
        WHERE duel_strapi_document_id IN (
            SELECT d.strapi_document_id
            FROM duels d
            WHERE d.sprint_strapi_document_id = p_sprint_id
        );

        v_msg := format('Deleted %s existing records from user_duel_to_review for sprint=%s', v_deleted_cnt, p_sprint_id);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;

        -- Создаём user_duel_review_counts
        DROP TABLE IF EXISTS user_duel_review_counts;
        CREATE TEMP TABLE user_duel_review_counts ON COMMIT DROP AS
        WITH all_pairs AS (
          SELECT
            u.strapi_document_id AS user_id,
            d.strapi_document_id AS duel_id
          FROM users u
          CROSS JOIN duels d
          WHERE u.stream_strapi_document_id = v_stream_id
            AND u.dismissed_at IS NULL
            AND d.sprint_strapi_document_id = p_sprint_id
        ),
        played_cte AS (
          SELECT
            uda.user_strapi_document_id AS user_id,
            uda.duel_strapi_document_id AS duel_id,
            COUNT(*) AS played_count
          FROM user_duel_answers uda
          JOIN duels dd ON dd.strapi_document_id = uda.duel_strapi_document_id
          WHERE dd.sprint_strapi_document_id = p_sprint_id
          GROUP BY uda.user_strapi_document_id, uda.duel_strapi_document_id
        )
        SELECT
          ap.user_id,
          ap.duel_id,
          COALESCE(p.played_count, 0) AS played_count,
          (COALESCE(p.played_count, 0) * 3) AS max_review_count,
          0 AS review_count
        FROM all_pairs ap
        LEFT JOIN played_cte p ON p.user_id = ap.user_id AND p.duel_id = ap.duel_id;

        v_msg := format('Created user_duel_review_counts for sprint=%s', p_sprint_id);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;

        -- user_duel_answers_review_counts
        DROP TABLE IF EXISTS user_duel_answers_review_counts;
        CREATE TEMP TABLE user_duel_answers_review_counts ON COMMIT DROP AS
        WITH cte_latest AS (
          SELECT
            uda.*,
            ROW_NUMBER() OVER (
              PARTITION BY LEAST(uda.user_strapi_document_id, uda.rival_user_strapi_document_id),
                           GREATEST(uda.user_strapi_document_id, uda.rival_user_strapi_document_id),
                           uda.duel_strapi_document_id
              ORDER BY uda.created_at DESC
            ) AS rn
          FROM user_duel_answers uda
          JOIN duels d2 ON d2.strapi_document_id = uda.duel_strapi_document_id
          WHERE d2.sprint_strapi_document_id = p_sprint_id
        )
        SELECT
          t.duel_answer_id AS user_duel_answer_id,
          LEAST(t.user_strapi_document_id, t.rival_user_strapi_document_id) AS owner_a,
          GREATEST(t.user_strapi_document_id, t.rival_user_strapi_document_id) AS owner_b,
          t.duel_strapi_document_id,
          (SELECT team_strapi_document_id FROM users WHERE strapi_document_id = LEAST(t.user_strapi_document_id, t.rival_user_strapi_document_id)) AS teamA,
          (SELECT team_strapi_document_id FROM users WHERE strapi_document_id = GREATEST(t.user_strapi_document_id, t.rival_user_strapi_document_id)) AS teamB,
          (
            LEAST(t.user_strapi_document_id, t.rival_user_strapi_document_id)
            || '_' ||
            GREATEST(t.user_strapi_document_id, t.rival_user_strapi_document_id)
          ) AS hash
        FROM cte_latest t
        WHERE t.rn=1;

        v_msg := format('Created user_duel_answers_review_counts for sprint=%s', p_sprint_id);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;

        -- duels_order (упрощённый)
        DROP TABLE IF EXISTS duels_order;
        CREATE TEMP TABLE duels_order ON COMMIT DROP AS
        SELECT DISTINCT
          d.strapi_document_id AS duel_id
        FROM duels d
        WHERE d.sprint_strapi_document_id = p_sprint_id;

        SELECT COUNT(*) INTO v_duel_count FROM duels_order;
        v_msg := format('duels_order built for sprint=%s, total duels=%s', p_sprint_id, v_duel_count);
        INSERT INTO distribution_logs(log_message) VALUES(v_msg);
        RAISE NOTICE '%', v_msg;

        -- Если нет дуэлей => всё
        IF v_duel_count=0 THEN
           v_msg := format('No duels found for sprint=%s => done (attempt=%s)', p_sprint_id, attempt_count);
           INSERT INTO distribution_logs(log_message) VALUES(v_msg);
           RAISE NOTICE '%', v_msg;
           RETURN;
        END IF;

        /*
          Основной цикл: Pass1 (чужие), Pass2 (A,B), Pass3 (все)
        */
        FOR rec_duel IN (
          SELECT * FROM duels_order
        ) LOOP
            FOR rec_ans IN (
              SELECT * 
              FROM user_duel_answers_review_counts
              WHERE duel_strapi_document_id = rec_duel.duel_id
            ) LOOP
                v_assigned := 0;
                v_teamA := rec_ans.teamA;
                v_teamB := rec_ans.teamB;

                /*
                  PASS1: "чужие" команды
                */
                SELECT array_agg(sub.user_id)
                  INTO v_candidates
                FROM (
                  SELECT udrc.user_id
                  FROM user_duel_review_counts udrc
                  JOIN users u ON u.strapi_document_id = udrc.user_id
                  WHERE udrc.duel_id = rec_ans.duel_strapi_document_id
                    AND udrc.review_count < udrc.max_review_count
                    AND u.team_strapi_document_id NOT IN (v_teamA, v_teamB)
                    AND udrc.user_id NOT IN (rec_ans.owner_a, rec_ans.owner_b)
                  ORDER BY random()
                ) sub;

                v_found_candidates := COALESCE(array_length(v_candidates,1),0);
                v_msg := format('Pass1: Duel=%s -> foreign candidates found=%s', rec_ans.hash, v_found_candidates);
                INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                RAISE NOTICE '%', v_msg;

                v_idx := 1;
                WHILE v_idx <= v_found_candidates AND v_assigned < v_needed LOOP
                    v_reviewer_id := v_candidates[v_idx];

                    INSERT INTO user_duel_to_review(
                      reviewer_user_strapi_document_id,
                      user_strapi_document_id,
                      duel_strapi_document_id,
                      hash,
                      created_at
                    )
                    VALUES
                      (v_reviewer_id, rec_ans.owner_a, rec_ans.duel_strapi_document_id, rec_ans.hash, now()),
                      (v_reviewer_id, rec_ans.owner_b, rec_ans.duel_strapi_document_id, rec_ans.hash, now());

                    UPDATE user_duel_review_counts
                    SET review_count = review_count + 1
                    WHERE user_id = v_reviewer_id
                      AND duel_id = rec_ans.duel_strapi_document_id;

                    v_assigned := v_assigned + 1;
                    v_idx := v_idx + 1;
                END LOOP;

                v_msg := format('Pass1 done: Duel=%s -> assigned=%s from foreign, needed=%s',
                                rec_ans.hash, v_assigned, v_needed);
                INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                RAISE NOTICE '%', v_msg;

                /*
                  PASS2: команда A,B поровну
                */
                IF v_assigned < v_needed THEN
                   v_short := v_needed - v_assigned;
                   v_bal_need_a := CEIL(v_short / 2.0);
                   v_bal_need_b := v_short - v_bal_need_a;

                   -- A
                   IF v_bal_need_a > 0 THEN
                      SELECT array_agg(sub.user_id)
                        INTO v_candidates
                      FROM (
                        SELECT udrc.user_id
                        FROM user_duel_review_counts udrc
                        JOIN users u ON u.strapi_document_id = udrc.user_id
                        WHERE udrc.duel_id = rec_ans.duel_strapi_document_id
                          AND udrc.review_count < udrc.max_review_count
                          AND u.team_strapi_document_id = v_teamA
                          AND udrc.user_id NOT IN (rec_ans.owner_a, rec_ans.owner_b)
                        ORDER BY random()
                        LIMIT v_bal_need_a
                      ) sub;

                      v_found_candidates := COALESCE(array_length(v_candidates,1),0);
                      v_msg := format('Pass2(A): Duel=%s -> teamA candidates=%s, need=%s',
                                      rec_ans.hash, v_found_candidates, v_bal_need_a);
                      INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                      RAISE NOTICE '%', v_msg;

                      v_idx := 1;
                      WHILE v_idx <= v_found_candidates AND v_assigned < v_needed LOOP
                          v_reviewer_id := v_candidates[v_idx];

                          INSERT INTO user_duel_to_review(
                            reviewer_user_strapi_document_id,
                            user_strapi_document_id,
                            duel_strapi_document_id,
                            hash,
                            created_at
                          )
                          VALUES
                            (v_reviewer_id, rec_ans.owner_a, rec_ans.duel_strapi_document_id, rec_ans.hash, now()),
                            (v_reviewer_id, rec_ans.owner_b, rec_ans.duel_strapi_document_id, rec_ans.hash, now());

                          UPDATE user_duel_review_counts
                          SET review_count = review_count + 1
                          WHERE user_id = v_reviewer_id
                            AND duel_id = rec_ans.duel_strapi_document_id;

                          v_assigned := v_assigned + 1;
                          v_idx := v_idx + 1;
                      END LOOP;
                   END IF;

                   -- B
                   IF v_assigned < v_needed AND v_bal_need_b > 0 THEN
                      SELECT array_agg(sub.user_id)
                        INTO v_candidates
                      FROM (
                        SELECT udrc.user_id
                        FROM user_duel_review_counts udrc
                        JOIN users u ON u.strapi_document_id = udrc.user_id
                        WHERE udrc.duel_id = rec_ans.duel_strapi_document_id
                          AND udrc.review_count < udrc.max_review_count
                          AND u.team_strapi_document_id = v_teamB
                          AND udrc.user_id NOT IN (rec_ans.owner_a, rec_ans.owner_b)
                        ORDER BY random()
                        LIMIT v_bal_need_b
                      ) sub;

                      v_found_candidates := COALESCE(array_length(v_candidates,1),0);
                      v_msg := format('Pass2(B): Duel=%s -> teamB candidates=%s, need=%s',
                                      rec_ans.hash, v_found_candidates, v_bal_need_b);
                      INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                      RAISE NOTICE '%', v_msg;

                      v_idx := 1;
                      WHILE v_idx <= v_found_candidates AND v_assigned < v_needed LOOP
                          v_reviewer_id := v_candidates[v_idx];

                          INSERT INTO user_duel_to_review(
                            reviewer_user_strapi_document_id,
                            user_strapi_document_id,
                            duel_strapi_document_id,
                            hash,
                            created_at
                          )
                          VALUES
                            (v_reviewer_id, rec_ans.owner_a, rec_ans.duel_strapi_document_id, rec_ans.hash, now()),
                            (v_reviewer_id, rec_ans.owner_b, rec_ans.duel_strapi_document_id, rec_ans.hash, now());

                          UPDATE user_duel_review_counts
                          SET review_count = review_count + 1
                          WHERE user_id = v_reviewer_id
                            AND duel_id = rec_ans.duel_strapi_document_id;

                          v_assigned := v_assigned + 1;
                          v_idx := v_idx + 1;
                      END LOOP;
                   END IF;
                END IF;

                v_msg := format('Pass2 done: Duel=%s -> assigned=%s so far, needed=%s',
                                rec_ans.hash, v_assigned, v_needed);
                INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                RAISE NOTICE '%', v_msg;

                /*
                  PASS3: добираем всеми
                */
                IF v_assigned < v_needed THEN
                   v_short := v_needed - v_assigned;
                   SELECT array_agg(sub.user_id)
                     INTO v_candidates
                   FROM (
                     SELECT udrc.user_id
                     FROM user_duel_review_counts udrc
                     JOIN users u ON u.strapi_document_id = udrc.user_id
                     WHERE udrc.duel_id = rec_ans.duel_strapi_document_id
                       AND udrc.review_count < udrc.max_review_count
                       AND udrc.user_id NOT IN (rec_ans.owner_a, rec_ans.owner_b)
                     ORDER BY random()
                   ) sub;

                   v_found_candidates := COALESCE(array_length(v_candidates,1),0);
                   v_msg := format('Pass3: Duel=%s -> ANY-team candidates found=%s, need=%s',
                                   rec_ans.hash, v_found_candidates, v_short);
                   INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                   RAISE NOTICE '%', v_msg;

                   v_idx := 1;
                   WHILE v_idx <= v_found_candidates AND v_assigned < v_needed LOOP
                       v_reviewer_id := v_candidates[v_idx];

                       INSERT INTO user_duel_to_review(
                         reviewer_user_strapi_document_id,
                         user_strapi_document_id,
                         duel_strapi_document_id,
                         hash,
                         created_at
                       )
                       VALUES
                         (v_reviewer_id, rec_ans.owner_a, rec_ans.duel_strapi_document_id, rec_ans.hash, now()),
                         (v_reviewer_id, rec_ans.owner_b, rec_ans.duel_strapi_document_id, rec_ans.hash, now());

                       UPDATE user_duel_review_counts
                       SET review_count = review_count + 1
                       WHERE user_id = v_reviewer_id
                         AND duel_id = rec_ans.duel_strapi_document_id;

                       v_assigned := v_assigned + 1;
                       v_idx := v_idx + 1;
                   END LOOP;
                END IF;

                -- Итог
                IF v_assigned < v_needed THEN
                   v_msg := format('Duel hash=%s got only %s reviewers instead of %s', rec_ans.hash, v_assigned, v_needed);
                   INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                   RAISE NOTICE '%', v_msg;
                END IF;

                SELECT COUNT(*) INTO temp_count
                FROM user_duel_to_review
                WHERE duel_strapi_document_id = rec_ans.duel_strapi_document_id
                  AND hash = rec_ans.hash;

                IF temp_count <> v_needed * 2 THEN
                   v_msg := format('Duel hash=%s total rows in user_duel_to_review=%s (expected %s)',
                                   rec_ans.hash, temp_count, v_needed*2);
                   INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                   RAISE NOTICE '%', v_msg;
                ELSE
                   v_msg := format('Duel hash=%s assigned %s reviewers (=%s rows)',
                                   rec_ans.hash, v_assigned, temp_count);
                   INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                   RAISE NOTICE '%', v_msg;
                END IF;

            END LOOP; -- rec_ans
        END LOOP; -- rec_duel

        ------------------------------------------------------------------
        -- 7) Самопроверка
        ------------------------------------------------------------------
        FOR rec_duel IN (
          SELECT * FROM duels_order
        ) LOOP
            FOR rec_ans IN (
              SELECT *
              FROM user_duel_answers_review_counts
              WHERE duel_strapi_document_id = rec_duel.duel_id
            ) LOOP
                -- Сколько unique reviewers?
                SELECT COUNT(DISTINCT reviewer_user_strapi_document_id)
                  INTO temp_count
                FROM user_duel_to_review
                WHERE duel_strapi_document_id = rec_ans.duel_strapi_document_id
                  AND hash = rec_ans.hash;

                IF temp_count < 6 THEN
                   distribution_failed := true;
                   v_msg := format('Fail check: Duel hash=%s has only %s unique reviewers <6', rec_ans.hash, temp_count);
                   INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                   RAISE NOTICE '%', v_msg;
                END IF;

                -- Проверка, чтобы владелец не проверял
                IF EXISTS(
                   SELECT 1
                   FROM user_duel_to_review
                   WHERE duel_strapi_document_id = rec_ans.duel_strapi_document_id
                     AND hash = rec_ans.hash
                     AND reviewer_user_strapi_document_id IN (rec_ans.owner_a, rec_ans.owner_b)
                ) THEN
                   distribution_failed := true;
                   v_msg := format('Fail check: Duel hash=%s => owner is reviewing own duel', rec_ans.hash);
                   INSERT INTO distribution_logs(log_message) VALUES(v_msg);
                   RAISE NOTICE '%', v_msg;
                END IF;
            END LOOP;
        END LOOP;

        -- Проверка лимитов
        FOR rec_duel IN (
          SELECT * FROM user_duel_review_counts
        ) LOOP
            IF rec_duel.review_count > rec_duel.max_review_count THEN
               distribution_failed := true;
               v_msg := format('Fail check: user=%s duel=%s => assigned=%s exceeds max=%s',
                               rec_duel.user_id, rec_duel.duel_id, rec_duel.review_count, rec_duel.max_review_count);
               INSERT INTO distribution_logs(log_message) VALUES(v_msg);
               RAISE NOTICE '%', v_msg;
            END IF;
        END LOOP;

        IF distribution_failed = false THEN
           v_msg := format('=== SUCCESS distribution on attempt %s for sprint=%s ===', attempt_count, p_sprint_id);
           INSERT INTO distribution_logs(log_message) VALUES(v_msg);
           RAISE NOTICE '%', v_msg;
           RETURN;
        ELSE
           v_msg := format('=== FAIL distribution on attempt %s => will re-run ===', attempt_count);
           INSERT INTO distribution_logs(log_message) VALUES(v_msg);
           RAISE NOTICE '%', v_msg;
        END IF;

    END LOOP;  -- 1..max_attempts

    -- Если дошли сюда => все попытки провалились
    v_msg := format('Distribution for sprint=%s => cannot satisfy conditions after %s attempts',
                    p_sprint_id, max_attempts);
    INSERT INTO distribution_logs(log_message) VALUES(v_msg);
    RAISE NOTICE '%', v_msg;

END;
