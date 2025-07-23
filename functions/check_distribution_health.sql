BEGIN
    RETURN QUERY
    WITH plays_and_reviews AS (
        SELECT 
            u.strapi_document_id as user_id,
            u.telegram_username,
            d.strapi_document_id as duel_id,
            d.duel_number,
            -- Количество сыгранных партий
            COUNT(DISTINCT uda.hash) FILTER (
                WHERE uda.user_strapi_document_id = u.strapi_document_id
            )::INTEGER as played,
            -- Количество назначенных проверок
            COUNT(DISTINCT utr.hash) FILTER (
                WHERE utr.reviewer_user_strapi_document_id = u.strapi_document_id
            )::INTEGER as reviews
        FROM users u
        CROSS JOIN duels d
        LEFT JOIN user_duel_answers uda 
            ON uda.user_strapi_document_id = u.strapi_document_id
            AND uda.duel_strapi_document_id = d.strapi_document_id
            AND uda.hash IS NOT NULL
            AND uda.hash != ''
        LEFT JOIN user_duel_to_review utr 
            ON utr.reviewer_user_strapi_document_id = u.strapi_document_id
            AND utr.duel_strapi_document_id = d.strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
          AND u.dismissed_at IS NULL
          AND EXISTS (
              SELECT 1 FROM sprints s 
              WHERE s.strapi_document_id = p_sprint_id 
              AND s.stream_strapi_document_id = u.stream_strapi_document_id
          )
        GROUP BY u.strapi_document_id, u.telegram_username, d.strapi_document_id, d.duel_number
        HAVING COUNT(DISTINCT uda.hash) > 0 OR COUNT(DISTINCT utr.hash) > 0
    )
    SELECT 
        '@' || pr.telegram_username as user_name,
        pr.duel_number,
        pr.played as played_times,
        (pr.played * 3)::INTEGER as should_review,
        pr.reviews as actually_reviews,
        ((pr.played * 3) - pr.reviews)::INTEGER as deficit,
        CASE 
            WHEN pr.reviews = pr.played * 3 THEN '✓ OK'
            WHEN pr.reviews < pr.played * 3 THEN '⚠ UNDERQUOTA'
            ELSE '❌ OVERQUOTA'
        END as status
    FROM plays_and_reviews pr
    ORDER BY 
        CASE 
            WHEN pr.reviews != pr.played * 3 THEN 0 
            ELSE 1 
        END,
        ABS(pr.reviews - pr.played * 3) DESC,
        pr.telegram_username,
        pr.duel_number;
END;
