DECLARE
    v_total                INT;      -- общее число пользователей (отфильтрованных)
    v_distance_min         INT;      -- 10% от общего числа
    v_distance_max         INT;      -- 30% от общего числа
    v_distance             INT;      -- текущая дистанция по рангу
    v_main_user_rank       INT;      -- rank пользователя p_user_id
    v_found                BOOLEAN := FALSE; 
    v_candidate            RECORD;
    v_left_rank            INT;
    v_right_rank           INT;

    v_is_repeat    BOOLEAN := FALSE; 
    v_count_existing INT;

    /*
      Шаги 2.4, 2.5 ТЗ:
      - Сперва пытаемся найти волонтёров (is_repeats_ok=TRUE)
        в диапазоне 10% (min) -> 30% (max) по user_duels_total_rank
      - Если нашли — выбираем самого подходящего (чтобы не превышал лимит 2 оппонентов),
        проверяем, был ли уже бой => is_repeat = (был ли?)
        create_duel_distribution(... is_extra=TRUE, is_repeat=..., is_late=TRUE ...)
      - Если не нашли волонтёров — переходим к НЕ-волонтёрам (тоже 2.5, см. ТЗ),
        снова идём в диапазон [10%..30%].
      - Если добрались до 30% и ничего не нашли — логируем в admin_messages
        (или, по ТЗ, пытаемся распределить по «самому меньшему весу» и т. д.).
    */
BEGIN
    PERFORM log_message(format(
      'handle_no_opponent_found_base(duel=%s, user=%s) START', 
      p_duel_strapi_id, p_user_id
    ));

    -- 2.1–2.3: У нас уже есть filter_users_for_sprint(p_sprint_id), 
    --           которая учитывает dismissed_at, strikes, stream_strapi_document_id.

    -- 1) Посчитаем общее число
    WITH cte_all AS (
       SELECT * 
         FROM filter_users_for_sprint(p_sprint_id)
    )
    SELECT COUNT(*) 
      INTO v_total
      FROM cte_all;

    IF v_total <= 1 THEN
       PERFORM log_message('   no or single user => cannot find extended opponent => admin_messages');
       INSERT INTO admin_messages(message_text, sprint_strapi_document_id, created_at)
       VALUES(
         format('No extended opponent found (<=1 user total) for user=%s in duel=%s', 
                 p_user_id, p_duel_strapi_id),
         p_sprint_id,
         now()
       );
       RETURN;
    END IF;

    -- Определяем rank пользователя (через view_rank_scores.user_duels_total_rank)
    SELECT vs.user_duels_total_rank
      INTO v_main_user_rank
      FROM view_rank_scores vs
      JOIN sprints s ON s.strapi_document_id = p_sprint_id
    WHERE vs.user_strapi_document_id = p_user_id
      AND vs.stream_strapi_document_id = s.stream_strapi_document_id
      AND vs.sprint_strapi_document_id = CONCAT('total_', s.stream_strapi_document_id)
    LIMIT 1;


    IF v_main_user_rank IS NULL THEN
        PERFORM log_message(format(
          '   user=%s not found in view_rank_scores => admin_messages',
           p_user_id
        ));
        INSERT INTO admin_messages(message_text, sprint_strapi_document_id, created_at)
        VALUES(
          format('No extended opponent found: user=%s not in ranking', p_user_id),
          p_sprint_id,
          now()
        );
        RETURN;
    END IF;

    -- distance_min=10%, distance_max=30%
    v_distance_min := GREATEST(1, FLOOR(0.10 * v_total));
    v_distance_max := GREATEST(1, FLOOR(0.30 * v_total));

    -------------------------------------------------------------------
    -- 2.4 - СНАЧАЛА ИЩЕМ ВОЛОНТЁРОВ (is_repeats_ok=TRUE)
    -------------------------------------------------------------------
    v_distance := v_distance_min;
    WHILE (v_distance <= v_distance_max) AND (NOT v_found) LOOP

        v_left_rank  := v_main_user_rank - v_distance;
        v_right_rank := v_main_user_rank + v_distance;

        -- Корректируем границы (как в предыдущем примере)
        IF v_left_rank < 1 THEN
            DECLARE v_diff INT := 1 - v_left_rank;
            BEGIN
               v_left_rank := 1;
               v_right_rank := v_right_rank + v_diff;
            END;
        END IF;

        IF v_right_rank > v_total THEN
            DECLARE v_diff INT := v_right_rank - v_total;
            BEGIN
               v_right_rank := v_total;
               v_left_rank := v_left_rank - v_diff;
               IF v_left_rank < 1 THEN
                   v_left_rank := 1;
               END IF;
            END;
        END IF;

        FOR v_candidate IN
            WITH cte_volunteers AS (
              SELECT s.user_strapi_document_id,
                    s.weight,
                    vs.user_duels_total_rank,
                    s.team_id,
                    uss.is_repeats_ok
                FROM filter_users_for_sprint(p_sprint_id) s
                JOIN view_rank_scores vs 
                  ON vs.user_strapi_document_id = s.user_strapi_document_id
                JOIN sprints sp ON sp.strapi_document_id = p_sprint_id
                LEFT JOIN user_sprint_state uss
                  ON uss.user_strapi_document_id = s.user_strapi_document_id
                    AND uss.duel_strapi_document_id = p_duel_strapi_id
              WHERE vs.user_duels_total_rank BETWEEN v_left_rank AND v_right_rank
                AND s.user_strapi_document_id <> p_user_id
                AND COALESCE(uss.is_repeats_ok, FALSE) = TRUE
                AND vs.stream_strapi_document_id = sp.stream_strapi_document_id
                AND vs.sprint_strapi_document_id = CONCAT('total_', sp.stream_strapi_document_id)
            )
            SELECT v.*
              FROM cte_volunteers v
              WHERE v.team_id <> (
                 SELECT u.team_strapi_document_id 
                   FROM users u
                  WHERE u.strapi_document_id = p_user_id
              )
              -- Проверяем, что кандидат ещё может иметь <2 оппонентов в ЭТОЙ дуэли:
              AND (
                SELECT COUNT(DISTINCT dd.rival_strapi_document_id)
                  FROM duel_distributions dd
                 WHERE dd.duel_strapi_document_id = p_duel_strapi_id
                   AND dd.user_strapi_document_id = v.user_strapi_document_id
                   AND dd.is_failed=FALSE
              ) < 2
              ORDER BY v.weight ASC  -- По условию ТЗ: "…у кого наименьший вес" (см. 2.4/2.5)
        LOOP
             ------------------------------------------------------------------------------
              -- ДОБАВЛЯЕМ проверку, что пара не появлялась в ЭТОМ спринте
              ------------------------------------------------------------------------------
              IF EXISTS (
                  SELECT 1
                    FROM duel_distributions dd
                    JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
                  WHERE d.sprint_strapi_document_id = p_sprint_id
                    AND (
                        (dd.user_strapi_document_id = p_user_id
                          AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
                      OR (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
                          AND dd.rival_strapi_document_id = p_user_id)
                    )
              ) THEN
                  PERFORM log_message(format(
                    '   SKIP volunteer=%s => pair was already in this sprint => continue...',
                    v_candidate.user_strapi_document_id
                  ));
                  CONTINUE;
              END IF;
              ------------------------------------------------------------------------------
            -- Проверим, была ли у данного пользователя и p_user_id уже схватка (is_repeat?).
            SELECT COUNT(*)
              INTO v_count_existing
              FROM duel_distributions dd
             WHERE dd.duel_strapi_document_id = p_duel_strapi_id
               AND dd.is_failed = FALSE
               AND (
                 (dd.user_strapi_document_id = p_user_id 
                  AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
                 OR
                 (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
                  AND dd.rival_strapi_document_id = p_user_id)
               );

            v_is_repeat := (v_count_existing > 0);

            IF can_pair_extended(
                 p_duel_strapi_id,
                 p_user_id,
                 v_candidate.user_strapi_document_id
               )
            THEN
                PERFORM log_message(format(
                  '   found volunteer user=%s => create extra. repeat=%s, is_late=TRUE',
                   v_candidate.user_strapi_document_id, v_is_repeat
                ));

                -- is_extra=TRUE, is_repeat=(считаем выше), is_late=TRUE
                PERFORM create_duel_distribution(
                  p_duel_strapi_id,
                  p_user_id,
                  v_candidate.user_strapi_document_id,
                  p_sprint_id,
                  v_is_repeat,   -- p_is_repeat
                  p_is_late,         -- p_is_late
                  1/20.0         -- p_weight_coef
                );

                v_found := TRUE;
                EXIT; -- выходим из цикла по волонтёрам
            END IF;
        END LOOP; -- FOR v_candidate

        IF NOT v_found THEN
            v_distance := v_distance + 1;
        END IF;
    END LOOP; -- WHILE (v_distance <= v_distance_max) for volunteers

    IF v_found THEN
        -- Если нашли волонтёра, то всё — завершаем
        PERFORM log_message('   handle_no_opponent_found_base => found volunteer => done');
        RETURN;
    END IF;

    -------------------------------------------------------------------
    -- 2.5 - ЕСЛИ НЕ НАШЛИ ВОЛОНТЁРА, ИЩЕМ СРЕДИ ОСТАЛЬНЫХ
    -------------------------------------------------------------------
    v_distance := v_distance_min;
    WHILE (v_distance <= v_distance_max) AND (NOT v_found) LOOP

        v_left_rank  := v_main_user_rank - v_distance;
        v_right_rank := v_main_user_rank + v_distance;

        -- Снова корректируем границы [1..v_total]:
        IF v_left_rank < 1 THEN
            DECLARE v_diff INT := 1 - v_left_rank;
            BEGIN
               v_left_rank := 1;
               v_right_rank := v_right_rank + v_diff;
            END;
        END IF;
        IF v_right_rank > v_total THEN
            DECLARE v_diff INT := v_right_rank - v_total;
            BEGIN
               v_right_rank := v_total;
               v_left_rank := v_left_rank - v_diff;
               IF v_left_rank < 1 THEN
                   v_left_rank := 1;
               END IF;
            END;
        END IF;

        FOR v_candidate IN
            WITH cte_nonvolunteers AS (
              SELECT s.user_strapi_document_id,
                    s.weight,
                    vs.user_duels_total_rank,
                    s.team_id
                FROM filter_users_for_sprint(p_sprint_id) s
                JOIN view_rank_scores vs 
                  ON vs.user_strapi_document_id = s.user_strapi_document_id
                JOIN sprints sp ON sp.strapi_document_id = p_sprint_id  -- ← ДОБАВЛЕНО
                LEFT JOIN user_sprint_state uss
                  ON uss.user_strapi_document_id = s.user_strapi_document_id
                    AND uss.duel_strapi_document_id = p_duel_strapi_id
              WHERE vs.user_duels_total_rank BETWEEN v_left_rank AND v_right_rank
                AND s.user_strapi_document_id <> p_user_id
                AND NOT COALESCE(uss.is_repeats_ok, FALSE)
                AND vs.stream_strapi_document_id = sp.stream_strapi_document_id     -- ← ДОБАВЛЕНО
                AND vs.sprint_strapi_document_id = CONCAT('total_', sp.stream_strapi_document_id)  -- ← ДОБАВЛЕНО
            )
            SELECT n.*
              FROM cte_nonvolunteers n
              WHERE n.team_id <> (
                 SELECT u.team_strapi_document_id
                   FROM users u
                  WHERE u.strapi_document_id = p_user_id
              )
              -- Проверяем лимит "не более 2 оппонентов в данной дуэли"
              AND (
                SELECT COUNT(DISTINCT dd.rival_strapi_document_id)
                  FROM duel_distributions dd
                 WHERE dd.duel_strapi_document_id = p_duel_strapi_id
                   AND dd.user_strapi_document_id = n.user_strapi_document_id
                   AND dd.is_failed=FALSE
              ) < 2
              -- Доп. условие ТЗ: "…для того, у кого наименьший вес по схваткам"
              ORDER BY n.weight ASC
        LOOP
            ------------------------------------------------------------------------------
            -- ДОБАВЛЯЕМ проверку, что пара не появлялась в ЭТОМ спринте
            ------------------------------------------------------------------------------
            IF EXISTS (
                SELECT 1
                  FROM duel_distributions dd
                  JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
                WHERE d.sprint_strapi_document_id = p_sprint_id
                  AND (
                      (dd.user_strapi_document_id = p_user_id
                        AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
                    OR (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
                        AND dd.rival_strapi_document_id = p_user_id)
                  )
            ) THEN
                PERFORM log_message(format(
                  '   SKIP non-volunteer=%s => pair was already in this sprint => continue...',
                  v_candidate.user_strapi_document_id
                ));
                CONTINUE;
            END IF;
            ------------------------------------------------------------------------------
            -- Смотрим, была ли уже схватка => is_repeat
            SELECT COUNT(*)
              INTO v_count_existing
              FROM duel_distributions dd
             WHERE dd.duel_strapi_document_id = p_duel_strapi_id
               AND dd.is_failed = FALSE
               AND (
                 (dd.user_strapi_document_id = p_user_id
                  AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
                 OR
                 (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
                  AND dd.rival_strapi_document_id = p_user_id)
               );
            v_is_repeat := (v_count_existing > 0);

            IF can_pair_extended(
                 p_duel_strapi_id,
                 p_user_id,
                 v_candidate.user_strapi_document_id
               )
            THEN
                -- Если distance уже >= v_distance_max (т.е. 30%), 
                -- это означает "назначаем повторную схватку, если не было — всё равно is_repeat=FALSE/TRUE"
                -- но фактически "если дошли до 30% => всё, берём, что есть"
                
                PERFORM log_message(format(
                  '   found non-volunteer user=%s => create extra. repeat=%s, is_late=TRUE',
                   v_candidate.user_strapi_document_id, v_is_repeat
                ));

                PERFORM create_duel_distribution(
                    p_duel_strapi_id,
                    p_user_id,
                    v_candidate.user_strapi_document_id,
                    p_sprint_id,
                    v_is_repeat,   -- p_is_repeat
                    p_is_late,     -- p_is_late
                    1/20.0         -- p_weight_coef
                );

                v_found := TRUE;
                EXIT;
            END IF;
        END LOOP; -- FOR v_candidate

        IF NOT v_found THEN
            v_distance := v_distance + 1;
        END IF;
    END LOOP; -- WHILE

    -- Если дошли сюда и всё ещё не v_found, значит никого не нашли
    IF NOT v_found THEN
        PERFORM log_message('   handle_no_opponent_found_base => no volunteers, no non-volunteers => admin_messages');

        INSERT INTO admin_messages(message_text, sprint_strapi_document_id, created_at)
        VALUES(
          format(
            'No extended opponent found for user=%s on base scenario (distance up to 30%% exhausted), duel=%s', 
            p_user_id, 
            p_duel_strapi_id
          ),
          p_sprint_id,
          now()
        );
    END IF;

    PERFORM log_message(format(
      'handle_no_opponent_found_base(duel=%s, user=%s) END', 
      p_duel_strapi_id, p_user_id
    ));
END;
