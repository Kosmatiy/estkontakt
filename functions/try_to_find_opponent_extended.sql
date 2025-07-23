DECLARE
    -- Общее число пользователей, которых можно рассматривать (filter_users_for_sprint)
    v_total INT;

    -- Ранг (user_duels_total_rank) нашего p_user_id из view_rank_scores
    v_main_user_rank INT;

    -- Границы расстояния (rank) для поиска
    v_distance_min INT;
    v_distance_max INT;
    v_distance     INT;

    -- Кандидат, которого будем перебирать
    v_candidate RECORD;

    -- Флаг, нашли ли оппонента
    v_found BOOLEAN := FALSE;

    -- Проверим, была ли уже схватка => is_repeat
    v_count_existing INT;
    v_is_repeat BOOLEAN := FALSE;

    -- Для сортировки «у кого наименьший sum_match_weight по уже сыгранным боям»
    -- (если в конце придётся смотреть «самого лёгкого по схваткам»):
    v_min_sum_weight NUMERIC;
BEGIN
    PERFORM log_message(format(
      'try_to_find_opponent_extended(duel=%s, user=%s, sprint=%s, is_late=%s) START',
       p_duel_id, p_user_id, p_sprint_id, p_is_late
    ));

    -------------------------------------------------------------------------
    -- 1) Считаем, сколько всего игроков (учитывая dismissed_at, strikes, stream)
    -------------------------------------------------------------------------
    WITH cte_all AS (
       SELECT * FROM filter_users_for_sprint(p_sprint_id)
    )
    SELECT COUNT(*)
      INTO v_total
      FROM cte_all;

    IF v_total <= 1 THEN
       PERFORM log_message('   no or single user => cannot find extended opponent => return');
       RETURN;
    END IF;

    -------------------------------------------------------------------------
    -- 2) rank пользователя (view_rank_scores.user_duels_total_rank)
    -------------------------------------------------------------------------
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
         '   user=%s not found in view_rank_scores => no extended match possible', 
         p_user_id
       ));
       RETURN;
    END IF;

    -------------------------------------------------------------------------
    -- 3) distance_min=10%, distance_max=30% (по ТЗ для extended можно брать 10–30)
    -------------------------------------------------------------------------
    v_distance_min := GREATEST(1, FLOOR(0.25 * v_total));
    v_distance_max := GREATEST(1, FLOOR(0.50 * v_total));

    -------------------------------------------------------------------------
    -- 4) СНАЧАЛА ИЩЕМ СРЕДИ ВОЛОНТЁРОВ (is_repeats_ok=TRUE)
    --    (ТЗ 2.4: "сначала смотрим добровольцев на повторные схватки")
    -------------------------------------------------------------------------
    v_distance := v_distance_min;  -- начинаем с 10%
    WHILE (v_distance <= v_distance_max) AND (NOT v_found) LOOP

        ---------------------------------------------------------------------
        -- 4.1) Считаем границы по rank (v_left_rank..v_right_rank)
        ---------------------------------------------------------------------
        DECLARE
            v_left INT := v_main_user_rank - v_distance;
            v_right INT := v_main_user_rank + v_distance;
            v_diff INT;
        BEGIN
            IF v_left < 1 THEN
               v_diff := 1 - v_left;
               v_left := 1;
               v_right := v_right + v_diff;
            END IF;

            IF v_right > v_total THEN
               v_diff := v_right - v_total;
               v_right := v_total;
               v_left := v_left - v_diff;
               IF v_left < 1 THEN
                  v_left := 1;
               END IF;
            END IF;

            -----------------------------------------------------------------
            -- 4.2) Перебираем кандидатов-волонтёров, упорядоченных:
            --      "…с кем не более 2 оппонентов, команда другая, 
            --        и ORDER BY наименьший weight, или sum_match_weight"
            -----------------------------------------------------------------
            FOR v_candidate IN
                WITH cte_volunteers AS (
              SELECT 
                s.user_strapi_document_id,
                s.team_id,
                s.weight,
                vs.user_duels_total_rank,
                uss.is_repeats_ok,
                -- (Опционально) считаем сумму сыгранных боёв:
                COALESCE((
                  SELECT SUM(dd.weight)
                    FROM duel_distributions dd
                    JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
                  WHERE dd.is_failed = FALSE
                    AND (
                      (dd.user_strapi_document_id = s.user_strapi_document_id
                        AND dd.rival_strapi_document_id = p_user_id)
                      OR
                      (dd.user_strapi_document_id = p_user_id
                        AND dd.rival_strapi_document_id = s.user_strapi_document_id)
                    )
                    AND d.sprint_strapi_document_id = p_sprint_id
                ),0) AS sum_match_weight
              FROM filter_users_for_sprint(p_sprint_id) s
              JOIN view_rank_scores vs
                ON vs.user_strapi_document_id = s.user_strapi_document_id
              JOIN sprints sp ON sp.strapi_document_id = p_sprint_id  -- ← ДОБАВЛЕНО
              LEFT JOIN user_sprint_state uss
                ON uss.user_strapi_document_id = s.user_strapi_document_id
                  AND uss.duel_strapi_document_id = p_duel_id
            WHERE vs.user_duels_total_rank BETWEEN v_left AND v_right
              AND s.user_strapi_document_id <> p_user_id
              AND COALESCE(uss.is_repeats_ok, FALSE) = TRUE  -- волонтёр
              AND vs.stream_strapi_document_id = sp.stream_strapi_document_id  -- ← ДОБАВЛЕНО
              AND vs.sprint_strapi_document_id = CONCAT('total_', sp.stream_strapi_document_id)  -- ← ДОБАВЛЕНО
            )
                SELECT v.*
                  FROM cte_volunteers v
                 WHERE v.team_id <> (
                       SELECT u.team_strapi_document_id
                         FROM users u
                        WHERE u.strapi_document_id = p_user_id
                 )
                   -- Убедимся, что у кандидата <2 оппонентов в ЭТОЙ дуэли
                   AND (
                     SELECT COUNT(DISTINCT dd.rival_strapi_document_id)
                       FROM duel_distributions dd
                      WHERE dd.duel_strapi_document_id = p_duel_id
                        AND dd.is_failed=FALSE
                        AND dd.user_strapi_document_id = v.user_strapi_document_id
                   ) < 2
                 ORDER BY v.sum_match_weight ASC,  -- по уже сыгранным боям (чтобы "наименьший вес")
                          v.weight ASC             -- а при равенстве — берём того, кто "легче" в таблице users
            LOOP
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
                -----------------------------------------------------------------
                -- 4.3) Проверяем, была ли уже схватка => is_repeat
                -----------------------------------------------------------------
                SELECT COUNT(*)
                  INTO v_count_existing
                  FROM duel_distributions dd
                 WHERE dd.duel_strapi_document_id = p_duel_id
                   AND dd.is_failed = FALSE
                   AND (
                     (dd.user_strapi_document_id = p_user_id
                      AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
                     OR
                     (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
                      AND dd.rival_strapi_document_id = p_user_id)
                   );

                v_is_repeat := (v_count_existing > 0);

                -----------------------------------------------------------------
                -- 4.4) can_pair_extended(...) ?
                -----------------------------------------------------------------
                IF can_pair_extended(p_duel_id, p_user_id, v_candidate.user_strapi_document_id) THEN

                    PERFORM log_message(format(
                      '   found volunteer user=%s => create extended. repeat=%s, forced=FALSE, is_late=%s',
                       v_candidate.user_strapi_document_id, v_is_repeat, p_is_late
                    ));

                    -----------------------------------------------------------------
                    -- 4.5) создаём запись в duel_distributions (is_extra=TRUE)
                    -----------------------------------------------------------------
                    PERFORM create_duel_distribution(
                      p_duel_id,
                      p_user_id,
                      v_candidate.user_strapi_document_id,
                      p_sprint_id,
                      v_is_repeat,
                      p_is_late,
                      1/20.0     -- например, "1- 1/60*sprint_number"
                    );

                    v_found := TRUE;
                    EXIT; -- из цикла FOR
                END IF;
            END LOOP;  -- for v_candidate
        END;

        IF NOT v_found THEN
            v_distance := v_distance + 1; -- расширяем distance
        END IF;
    END LOOP;  -- while (v_distance <= v_distance_max) для волонтёров

    -------------------------------------------------------------------------
    -- 5) ЕСЛИ НЕ НАШЛИ ВОЛОНТЁРА, ИЩЕМ СРЕДИ НЕ-ВОЛОНТЁРОВ (ТЗ 2.5)
    -------------------------------------------------------------------------
    IF NOT v_found THEN
        v_distance := v_distance_min; -- снова 10%
    END IF;

    WHILE (v_distance <= v_distance_max) AND (NOT v_found) LOOP

        DECLARE
            v_left INT := v_main_user_rank - v_distance;
            v_right INT := v_main_user_rank + v_distance;
            v_diff INT;
        BEGIN
            IF v_left < 1 THEN
               v_diff := 1 - v_left;
               v_left := 1;
               v_right := v_right + v_diff;
            END IF;
            IF v_right > v_total THEN
               v_diff := v_right - v_total;
               v_right := v_total;
               v_left := v_left - v_diff;
               IF v_left < 1 THEN
                  v_left := 1;
               END IF;
            END IF;

            -----------------------------------------------------------------
            -- 5.1) Цикл по не-волонтёрам
            -----------------------------------------------------------------
            FOR v_candidate IN
                WITH cte_nonvolunteers AS (
                  SELECT 
                    s.user_strapi_document_id,
                    s.team_id,
                    s.weight,
                    vs.user_duels_total_rank,
                    COALESCE((
                      SELECT SUM(dd.weight)
                        FROM duel_distributions dd
                        JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
                      WHERE dd.is_failed = FALSE
                        AND (
                          (dd.user_strapi_document_id = s.user_strapi_document_id
                            AND dd.rival_strapi_document_id = p_user_id)
                          OR
                          (dd.user_strapi_document_id = p_user_id
                            AND dd.rival_strapi_document_id = s.user_strapi_document_id)
                        )
                        AND d.sprint_strapi_document_id = p_sprint_id
                    ),0) AS sum_match_weight
                  FROM filter_users_for_sprint(p_sprint_id) s
                  JOIN view_rank_scores vs
                    ON vs.user_strapi_document_id = s.user_strapi_document_id
                  JOIN sprints sp ON sp.strapi_document_id = p_sprint_id  -- ← ДОБАВЛЕНО
                  LEFT JOIN user_sprint_state uss
                    ON uss.user_strapi_document_id = s.user_strapi_document_id
                      AND uss.duel_strapi_document_id = p_duel_id
                WHERE vs.user_duels_total_rank BETWEEN v_left AND v_right
                  AND s.user_strapi_document_id <> p_user_id
                  AND NOT COALESCE(uss.is_repeats_ok, FALSE)
                  AND vs.stream_strapi_document_id = sp.stream_strapi_document_id  -- ← ДОБАВЛЕНО
                  AND vs.sprint_strapi_document_id = CONCAT('total_', sp.stream_strapi_document_id)  -- ← ДОБАВЛЕНО
                )

                SELECT n.*
                  FROM cte_nonvolunteers n
                 WHERE n.team_id <> (
                       SELECT u.team_strapi_document_id 
                         FROM users u
                        WHERE u.strapi_document_id = p_user_id
                 )
                   AND (
                     SELECT COUNT(DISTINCT dd.rival_strapi_document_id)
                       FROM duel_distributions dd
                      WHERE dd.duel_strapi_document_id = p_duel_id
                        AND dd.is_failed=FALSE
                        AND dd.user_strapi_document_id = n.user_strapi_document_id
                   ) < 2
                 ORDER BY n.sum_match_weight ASC,
                          n.weight ASC
            LOOP
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
                SELECT COUNT(*)
                  INTO v_count_existing
                  FROM duel_distributions dd
                 WHERE dd.duel_strapi_document_id = p_duel_id
                   AND dd.is_failed=FALSE
                   AND (
                     (dd.user_strapi_document_id = p_user_id
                      AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
                     OR
                     (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
                      AND dd.rival_strapi_document_id = p_user_id)
                   );
                v_is_repeat := (v_count_existing>0);

                IF can_pair_extended(p_duel_id, p_user_id, v_candidate.user_strapi_document_id) THEN
                    PERFORM log_message(format(
                      '   found non-volunteer user=%s => create extended. repeat=%s, forced=TRUE, is_late=%s',
                       v_candidate.user_strapi_document_id, v_is_repeat, p_is_late
                    ));

                    PERFORM create_duel_distribution(
                      p_duel_id,
                      p_user_id,
                      v_candidate.user_strapi_document_id,
                      p_sprint_id,
                      v_is_repeat,
                      p_is_late,
                      1/20.0
                    );

                    v_found := TRUE;
                    EXIT; -- из FOR
                END IF;
            END LOOP;  -- for v_candidate

        END;  -- DECLARE block

        IF NOT v_found THEN
            v_distance := v_distance + 1; -- расширяем
        END IF;
    END LOOP;  -- while (v_distance <= v_distance_max) для не-волонтёров

-------------------------------------------------------------------------
-- 6) FALLBACK: Если так и не нашли - берем любого доступного
-------------------------------------------------------------------------
    IF NOT v_found THEN
        PERFORM log_message('try_to_find_opponent_extended: final fallback - taking any available opponent');
        
        FOR v_candidate IN
            WITH cte_any_available AS (
                SELECT s.user_strapi_document_id,
                      s.weight,
                      s.team_id
                FROM filter_users_for_sprint(p_sprint_id) s
                WHERE s.user_strapi_document_id <> p_user_id
                  AND s.team_id <> (
                      SELECT u.team_strapi_document_id 
                      FROM users u 
                      WHERE u.strapi_document_id = p_user_id
                  )
                ORDER BY s.weight ASC  -- берем самого "легкого"
            )
            SELECT * FROM cte_any_available
        LOOP
            PERFORM log_message(format('   extended fallback: trying candidate=%s', v_candidate.user_strapi_document_id));
            
            -- Проверяем, не было ли уже пары в этом спринте
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
                PERFORM log_message(format('   SKIP extended fallback candidate=%s => pair was already in this sprint', 
                                          v_candidate.user_strapi_document_id));
                CONTINUE;
            END IF;
            
            -- Рассчитываем v_is_repeat для этой дуэли
            SELECT COUNT(*)
            INTO v_count_existing
            FROM duel_distributions dd
            WHERE dd.duel_strapi_document_id = p_duel_id
              AND dd.is_failed = FALSE
              AND (
                (dd.user_strapi_document_id = p_user_id 
                AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
                OR
                (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
                AND dd.rival_strapi_document_id = p_user_id)
              );
            v_is_repeat := (v_count_existing > 0);
            
            IF can_pair_extended(p_duel_id, p_user_id, v_candidate.user_strapi_document_id) THEN
                PERFORM log_message(format('   extended fallback SUCCESS: creating pair user=%s vs candidate=%s', 
                                          p_user_id, v_candidate.user_strapi_document_id));
                
                PERFORM create_duel_distribution(
                    p_duel_id,
                    p_user_id,
                    v_candidate.user_strapi_document_id,
                    p_sprint_id,
                    v_is_repeat,
                    p_is_late,
                    1/20.0
                );
                v_found := TRUE;
                EXIT;
            ELSE
                PERFORM log_message(format('   extended fallback: can_pair_extended=FALSE for candidate=%s', 
                                          v_candidate.user_strapi_document_id));
            END IF;
        END LOOP;
    END IF;


    -------------------------------------------------------------------------
    -- 6) Если так и не нашли (v_found=FALSE), пишем в admin_messages
    -------------------------------------------------------------------------
    IF NOT v_found THEN
        PERFORM log_message(format(
          'try_to_find_opponent_extended: no candidates found (distance >= %s) => admin_messages', 
          v_distance_max
        ));

        INSERT INTO admin_messages(message_text, sprint_strapi_document_id, created_at)
        VALUES(
          format(
            'No extended opponent found for user=%s (distance up to 30%% exhausted) in duel=%s', 
            p_user_id,
            p_duel_id
          ),
          p_sprint_id,
          now()
        );
    END IF;

    PERFORM log_message(format(
      'try_to_find_opponent_extended(duel=%s, user=%s) END, v_found=%s',
       p_duel_id, p_user_id, v_found
    ));
END;
