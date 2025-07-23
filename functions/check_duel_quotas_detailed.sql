BEGIN
    RETURN QUERY
    WITH user_duel_stats AS (
        SELECT 
            u.strapi_document_id,
            u.telegram_username,
            d.strapi_document_id as duel_id,
            d.duel_number,
            -- Сколько раз сыграл эту конкретную дуэль
            COUNT(DISTINCT uda.hash) FILTER (
                WHERE uda.user_strapi_document_id = u.strapi_document_id
                  AND uda.duel_strapi_document_id = d.strapi_document_id
            ) as played,
            -- Сколько пар этой дуэли назначено на проверку
            COUNT(DISTINCT utr.hash) FILTER (
                WHERE utr.reviewer_user_strapi_document_id = u.strapi_document_id
                  AND utr.duel_strapi_document_id = d.strapi_document_id
            ) as assigned
        FROM users u
        CROSS JOIN duels d
        LEFT JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
            AND uda.duel_strapi_document_id = d.strapi_document_id
            AND uda.hash IS NOT NULL AND uda.hash <> ''
        LEFT JOIN user_duel_to_review utr ON utr.reviewer_user_strapi_document_id = u.strapi_document_id
            AND utr.duel_strapi_document_id = d.strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
          AND u.stream_strapi_document_id = (
              SELECT stream_strapi_document_id FROM sprints WHERE strapi_document_id = p_sprint_id
          )
          AND u.dismissed_at IS NULL
        GROUP BY u.strapi_document_id, u.telegram_username, d.strapi_document_id, d.duel_number
        HAVING COUNT(DISTINCT uda.hash) > 0 OR COUNT(DISTINCT utr.hash) > 0
    )
    SELECT 
        telegram_username,
        duel_number,
        played as played_count,
        played * 3 as expected_reviews,
        assigned as actual_reviews,
        CASE 
            WHEN played = 0 AND assigned > 0 THEN format('Ошибка: не играл, но проверяет %s', assigned)
            WHEN assigned = played * 3 THEN 'OK ✓'
            WHEN assigned > played * 3 THEN format('Перевыполнение: +%s', assigned - played * 3)
            WHEN assigned < played * 3 THEN format('Недовыполнение: -%s', played * 3 - assigned)
        END as quota_status
    FROM user_duel_stats
    WHERE played > 0 OR assigned > 0
    ORDER BY 
        CASE 
            WHEN played = 0 AND assigned > 0 THEN 0  -- Ошибки первыми
            WHEN assigned < played * 3 THEN 1        -- Недовыполнения
            WHEN assigned > played * 3 THEN 2        -- Перевыполнения
            ELSE 3                                    -- OK
        END,
        telegram_username, 
        duel_number;
END;
