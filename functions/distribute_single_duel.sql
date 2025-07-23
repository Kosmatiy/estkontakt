DECLARE
    v_sprint_id    TEXT;
    v_type         TEXT;
    v_participants TEXT[];
    v_assigned     TEXT[] := ARRAY[]::TEXT[];
    v_mode         TEXT;
    v_needed       INT;
    rec            RECORD;
BEGIN
    -- 1) Найти спринт и тип дуэли
    SELECT d.sprint_strapi_document_id, d.type
      INTO v_sprint_id, v_type
    FROM duels d
    WHERE d.strapi_document_id = p_duel_id;
    IF v_sprint_id IS NULL THEN
        RAISE EXCEPTION 'Duel % not found', p_duel_id;
    END IF;

    -- 2) Собрать всех активных пользователей спринта
    DROP TABLE IF EXISTS tmp_eligible_users;
    CREATE TEMP TABLE tmp_eligible_users AS
    SELECT DISTINCT ua.user_strapi_document_id AS user_id
    FROM user_duel_answers ua
    JOIN duels d2 ON d2.strapi_document_id = ua.duel_strapi_document_id
    WHERE d2.sprint_strapi_document_id = v_sprint_id;

    -- 3) Построить квоты: каждый сыграл N пар → 3×N ревью
    DROP TABLE IF EXISTS tmp_player_quotas;
    CREATE TEMP TABLE tmp_player_quotas AS
    SELECT
      ua.user_strapi_document_id AS user_id,
      COUNT(DISTINCT ua.duel_strapi_document_id || ua.hash) * 3 AS review_quota,
      0 AS assigned_reviews
    FROM user_duel_answers ua
    JOIN duels d3 ON d3.strapi_document_id = ua.duel_strapi_document_id
    WHERE d3.sprint_strapi_document_id = v_sprint_id
    GROUP BY ua.user_strapi_document_id;

    -- 4) Собрать участников конкретной пары по (duel_id, hash)
    SELECT array_agg(DISTINCT ua.user_strapi_document_id)
      INTO v_participants
    FROM user_duel_answers ua
    WHERE ua.duel_strapi_document_id = p_duel_id
      AND ua.hash = p_hash;
    IF cardinality(v_participants) <> 2 THEN
        RAISE EXCEPTION 
          'Pair (% , %) does not have exactly 2 distinct participants', 
           p_duel_id, p_hash;
    END IF;

    -- 5) Уже назначенные на эту пару рецензенты
    SELECT COALESCE(array_agg(DISTINCT udr.reviewer_user_strapi_document_id), ARRAY[]::TEXT[])
      INTO v_assigned
    FROM user_duel_to_review udr
    WHERE udr.duel_strapi_document_id = p_duel_id
      AND udr.hash = p_hash;

    -- 6) Три этапа распределения
    FOREACH v_mode IN ARRAY ARRAY[
        'strict_duel_and_team',
        'strict_duel_any_team',
        'any_player'
    ]
    LOOP
        v_needed := 6 - cardinality(v_assigned);
        EXIT WHEN v_needed <= 0;

        FOR rec IN
            SELECT
              pq.user_id
            FROM tmp_player_quotas pq
            JOIN tmp_eligible_users eu ON eu.user_id = pq.user_id
            WHERE pq.user_id != ALL(v_participants)      -- не участник
              AND pq.assigned_reviews < pq.review_quota   -- есть квота
              AND pq.user_id != ALL(v_assigned)           -- ещё не назначен
              AND (
                  (v_mode IN ('strict_duel_and_team','strict_duel_any_team')
                   AND EXISTS (
                     SELECT 1 FROM user_duel_answers x
                      WHERE x.duel_strapi_document_id = p_duel_id
                        AND x.hash = p_hash
                        AND x.user_strapi_document_id = pq.user_id
                   ))
                  OR v_mode = 'any_player'
              )
            ORDER BY
              CASE 
                WHEN v_mode IN ('strict_duel_and_team','strict_duel_any_team')
                     AND EXISTS (
                       SELECT 1 FROM user_duel_answers x2
                        WHERE x2.duel_strapi_document_id = p_duel_id
                          AND x2.hash = p_hash
                          AND x2.user_strapi_document_id = pq.user_id
                     ) THEN 1
                ELSE 0
              END DESC,
              (pq.review_quota - pq.assigned_reviews) DESC,
              pq.user_id
            LIMIT v_needed
        LOOP
            -- вставляем ревью на обоих участников
            INSERT INTO user_duel_to_review(
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                hash
            )
            SELECT
              rec.user_id,
              p_duel_id,
              UNNEST(v_participants),
              p_hash
            ON CONFLICT DO NOTHING;

            -- обновляем локальные данные
            v_assigned := array_append(v_assigned, rec.user_id);
            UPDATE tmp_player_quotas
               SET assigned_reviews = assigned_reviews + 1
             WHERE user_id = rec.user_id;
        END LOOP;
    END LOOP;

    -- 7) Вернуть отчёт по спринту
    RETURN test_user_duel_to_review(v_sprint_id);
END;
