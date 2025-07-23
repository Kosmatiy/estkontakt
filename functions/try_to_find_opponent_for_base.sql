DECLARE
    v_found BOOLEAN := FALSE;            
    v_total INT;                         
    v_main_user_rank INT;                
    v_distance INT;                      
    v_distance_min INT;                  
    v_distance_max INT;                  
    v_candidate RECORD;                  
    v_left_rank INT;                     
    v_right_rank INT;

    -- v_is_repeat BOOLEAN := FALSE;        -- ← ДОБАВИТЬ ЭТУ СТРОКУ
    -- v_count_existing INT;                -- ← И ЭТУ ТОЖЕ

BEGIN
    PERFORM log_message(
        format('try_to_find_opponent_for_base(duel=%s, user=%s) START', 
               p_duel_strapi_id, p_user_id)
    );

    -- 1) Считаем, сколько всего игроков
    WITH cte_all AS (
        SELECT * 
          FROM filter_users_for_sprint(p_sprint_id)
    )
    SELECT COUNT(*) 
      INTO v_total
      FROM cte_all;

    IF v_total <= 1 THEN
        PERFORM log_message('   no or single user in sprint => cannot find opponent => return');
        RETURN;
    END IF;

    -- 2) Определяем ранг пользователя (user_duels_total_rank) через view_rank_scores
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
            '   user=%s not found in view_rank_scores => return', p_user_id
        ));
        RETURN;
    END IF;

    -- 3) distance_min=5%, distance_max=30%
    v_distance_min := GREATEST(1, FLOOR(0.05 * v_total));  -- ~5%
    v_distance_max := GREATEST(1, FLOOR(0.30 * v_total)); -- ~30%
    v_distance := v_distance_min;

    -- 4) Расширяем distance от 5% до 30%
    WHILE (v_distance <= v_distance_max) AND (NOT v_found) LOOP

        -- Берём базовый диапазон:
        v_left_rank  := v_main_user_rank - v_distance;
        v_right_rank := v_main_user_rank + v_distance;

        -- ВАЖНО: Тут вставляем логику "сдвига", если вышли за границы 1..v_total:
        IF v_left_rank < 1 THEN
            DECLARE
                v_diff INT := 1 - v_left_rank;
            BEGIN
                v_left_rank := 1;
                v_right_rank := v_right_rank + v_diff;
            END;
        END IF;

        IF v_right_rank > v_total THEN
            DECLARE
                v_diff INT := v_right_rank - v_total;
            BEGIN
                v_right_rank := v_total;
                v_left_rank := v_left_rank - v_diff;
                IF v_left_rank < 1 THEN
                    v_left_rank := 1;
                END IF;
            END;
        END IF;

        -- Теперь у нас скорректированный диапазон [v_left_rank..v_right_rank]

        FOR v_candidate IN
          WITH cte_candidates AS (
            SELECT s.user_strapi_document_id,
                  s.weight,
                  vs.user_duels_total_rank,
                  s.team_id
              FROM filter_users_for_sprint(p_sprint_id) s
              JOIN view_rank_scores vs 
                ON vs.user_strapi_document_id = s.user_strapi_document_id
              JOIN sprints sp ON sp.strapi_document_id = p_sprint_id  -- JOIN для получения stream
            WHERE vs.user_duels_total_rank BETWEEN v_left_rank AND v_right_rank
              AND s.user_strapi_document_id <> p_user_id
              AND vs.stream_strapi_document_id = sp.stream_strapi_document_id
              AND vs.sprint_strapi_document_id = CONCAT('total_', sp.stream_strapi_document_id)
        )

            SELECT c.*
              FROM cte_candidates c
              WHERE c.team_id <> (
                  SELECT u.team_strapi_document_id 
                    FROM users u 
                   WHERE u.strapi_document_id = p_user_id
              )
              ORDER BY c.weight DESC
        LOOP
            ------------------------------------------------------------------------------
            -- ДОБАВЛЯЕМ: проверку, что пара (p_user_id, v_candidate) не появлялась в ЭТОМ спринте
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
                  '   SKIP candidate=%s => pair was already in this sprint (even if is_failed=TRUE). Continue...',
                  v_candidate.user_strapi_document_id
                ));
                CONTINUE;  -- пропускаем этого кандидата и идём к следующему
            END IF;
            ------------------------------------------------------------------------------
    
            -- Проверяем, подходит ли кандидат для базовой схватки
            IF can_pair_base(
                 p_duel_strapi_id,
                 p_sprint_id,  -- предполагаем, что can_pair_base(...) теперь принимает sprint_id
                 p_user_id,
                 v_candidate.user_strapi_document_id
            ) THEN
                -- Создаём базовую схватку
                PERFORM create_duel_distribution(
                    p_duel_strapi_id,
                    p_user_id,
                    v_candidate.user_strapi_document_id,
                    p_sprint_id,
                    FALSE,      -- p_is_repeat
                    p_is_late,  -- p_is_late
                    1/20.0      -- p_weight_coef
                );
                v_found := TRUE;
                EXIT;
            END IF;
        END LOOP; -- for v_candidate

        IF NOT v_found THEN
            -- Увеличиваем distance и пробуем снова
            v_distance := v_distance + 1;
        END IF;
    END LOOP; -- while

    -- -- ПОСЛЕДНЯЯ ПОПЫТКА: берем любого доступного
    -- IF NOT v_found THEN
    --     PERFORM log_message('Final fallback: taking any available opponent');
        
    --     FOR v_candidate IN
    --         WITH cte_any_available AS (
    --             SELECT s.user_strapi_document_id,
    --                   s.weight,
    --                   s.team_id
    --             FROM filter_users_for_sprint(p_sprint_id) s
    --             WHERE s.user_strapi_document_id <> p_user_id
    --               AND s.team_id <> (
    --                   SELECT u.team_strapi_document_id 
    --                   FROM users u 
    --                   WHERE u.strapi_document_id = p_user_id
    --               )
    --             ORDER BY s.weight ASC  -- берем самого "легкого"
    --         )
    --         SELECT * FROM cte_any_available
    --     LOOP
    --         PERFORM log_message(format('   fallback: trying candidate=%s', v_candidate.user_strapi_document_id));
            
    --         -- Проверяем, не было ли уже пары в этом спринте (как в других функциях)
    --         IF EXISTS (
    --             SELECT 1
    --             FROM duel_distributions dd
    --             JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
    --             WHERE d.sprint_strapi_document_id = p_sprint_id
    --               AND (
    --                   (dd.user_strapi_document_id = p_user_id
    --                     AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
    --                 OR (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
    --                     AND dd.rival_strapi_document_id = p_user_id)
    --               )
    --         ) THEN
    --             PERFORM log_message(format('   SKIP fallback candidate=%s => pair was already in this sprint', 
    --                                       v_candidate.user_strapi_document_id));
    --             CONTINUE;
    --         END IF;
            
    --         -- Рассчитываем v_is_repeat для этой дуэли
    --         SELECT COUNT(*)
    --         INTO v_count_existing
    --         FROM duel_distributions dd
    --         WHERE dd.duel_strapi_document_id = p_duel_strapi_id
    --           AND dd.is_failed = FALSE
    --           AND (
    --             (dd.user_strapi_document_id = p_user_id 
    --             AND dd.rival_strapi_document_id = v_candidate.user_strapi_document_id)
    --             OR
    --             (dd.user_strapi_document_id = v_candidate.user_strapi_document_id
    --             AND dd.rival_strapi_document_id = p_user_id)
    --           );
    --         v_is_repeat := (v_count_existing > 0);
            
    --         PERFORM log_message(format('   fallback: candidate=%s, is_repeat=%s', 
    --                                   v_candidate.user_strapi_document_id, v_is_repeat));
            
    --         IF can_pair_extended(p_duel_strapi_id, p_user_id, v_candidate.user_strapi_document_id) THEN
    --             PERFORM log_message(format('   fallback SUCCESS: creating pair user=%s vs candidate=%s', 
    --                                       p_user_id, v_candidate.user_strapi_document_id));
                
    --             PERFORM create_duel_distribution(
    --                 p_duel_strapi_id, 
    --                 p_user_id, 
    --                 v_candidate.user_strapi_document_id,
    --                 p_sprint_id, 
    --                 v_is_repeat, 
    --                 p_is_late, 
    --                 1/20.0
    --             );
    --             v_found := TRUE;
    --             EXIT;
    --         ELSE
    --             PERFORM log_message(format('   fallback: can_pair_extended=FALSE for candidate=%s', 
    --                                       v_candidate.user_strapi_document_id));
    --         END IF;
    --     END LOOP;
        
    --     IF NOT v_found THEN
    --         PERFORM log_message('   fallback: no suitable candidates found even with relaxed rules');
    --     END IF;
    -- END IF;


    -- Если так и не нашли, вызываем handle_no_opponent_found_base(...) 
    IF NOT v_found THEN
        PERFORM log_message(format(
          '   not found base => handle_no_opponent_found_base(user=%s)', 
          p_user_id
        ));
        PERFORM handle_no_opponent_found_base(
          p_duel_strapi_id, 
          p_user_id, 
          p_sprint_id, 
          p_is_late
        );
    END IF;

    PERFORM log_message(format(
        'try_to_find_opponent_for_base(duel=%s, user=%s) END', 
         p_duel_strapi_id, p_user_id
    ));
END;
