BEGIN
    -- Собирать все полные пары для спринта
    DROP TABLE IF EXISTS tmp_all_pairs;
    CREATE TEMP TABLE tmp_all_pairs ON COMMIT DROP AS
    SELECT DISTINCT
       uda.duel_strapi_document_id,
       uda.hash,
       d.type AS duel_type,
       ARRAY_AGG(DISTINCT uda.user_strapi_document_id ORDER BY uda.user_strapi_document_id) AS participants
    FROM user_duel_answers uda
    JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
      AND uda.user_strapi_document_id = ANY(p_eligible_users)
    GROUP BY uda.duel_strapi_document_id, uda.hash, d.type
    HAVING COUNT(DISTINCT uda.user_strapi_document_id) = 2;

    -- Вычислить квоты для каждого игрока
    DROP TABLE IF EXISTS tmp_player_quotas;
    CREATE TEMP TABLE tmp_player_quotas ON COMMIT DROP AS
    SELECT user_id,
           COUNT(*) AS total_games,
           COUNT(*) * 3 AS review_quota,
           0             AS assigned_reviews
    FROM (
        SELECT DISTINCT
           uda.user_strapi_document_id AS user_id,
           uda.duel_strapi_document_id,
           uda.hash
        FROM user_duel_answers uda
        JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
          AND uda.user_strapi_document_id = ANY(p_eligible_users)
    ) t
    GROUP BY user_id;

    -- Инициализировать список уже назначенных рецензентов по паре
    DROP TABLE IF EXISTS tmp_pair_reviewers;
    CREATE TEMP TABLE tmp_pair_reviewers ON COMMIT DROP AS
    SELECT p.*,
           ARRAY[]::TEXT[] AS assigned_reviewers
    FROM tmp_all_pairs p;
END;
