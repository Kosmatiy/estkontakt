DECLARE
    v_user_id TEXT;
    v_result JSON;
BEGIN
    -- Найти user_id
    SELECT strapi_document_id INTO v_user_id
    FROM users
    WHERE telegram_username = p_telegram_username;
    
    IF v_user_id IS NULL THEN
        RETURN json_build_object('error', 'User not found');
    END IF;
    
    -- Детальный анализ
    WITH user_duel_stats AS (
        SELECT 
            d.strapi_document_id as duel_id,
            d.duel_number,
            d.type as duel_type,
            COUNT(DISTINCT uda.hash) FILTER (WHERE uda.user_strapi_document_id = v_user_id) as played_count,
            COUNT(DISTINCT utr.hash) FILTER (WHERE utr.reviewer_user_strapi_document_id = v_user_id) as review_count,
            COUNT(DISTINCT uda.hash) as total_pairs_in_duel
        FROM duels d
        LEFT JOIN user_duel_answers uda ON uda.duel_strapi_document_id = d.strapi_document_id
            AND uda.hash IS NOT NULL
        LEFT JOIN user_duel_to_review utr ON utr.duel_strapi_document_id = d.strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
        GROUP BY d.strapi_document_id, d.duel_number, d.type
        HAVING COUNT(DISTINCT uda.hash) FILTER (WHERE uda.user_strapi_document_id = v_user_id) > 0
            OR COUNT(DISTINCT utr.hash) FILTER (WHERE utr.reviewer_user_strapi_document_id = v_user_id) > 0
    ),
    available_for_review AS (
        SELECT 
            uds.duel_id,
            uds.duel_number,
            COUNT(*) as available_pairs
        FROM user_duel_stats uds
        JOIN user_duel_answers uda ON uda.duel_strapi_document_id = uds.duel_id
        WHERE uda.hash IS NOT NULL
            -- Пользователь не участвовал в этой паре
            AND NOT EXISTS (
                SELECT 1 FROM user_duel_answers uda2
                WHERE uda2.duel_strapi_document_id = uda.duel_strapi_document_id
                    AND uda2.hash = uda.hash
                    AND uda2.user_strapi_document_id = v_user_id
            )
            -- Пользователь еще не проверяет эту пару
            AND NOT EXISTS (
                SELECT 1 FROM user_duel_to_review utr
                WHERE utr.duel_strapi_document_id = uda.duel_strapi_document_id
                    AND utr.hash = uda.hash
                    AND utr.reviewer_user_strapi_document_id = v_user_id
            )
            -- В паре нужны еще рецензенты
            AND (
                SELECT COUNT(DISTINCT reviewer_user_strapi_document_id)
                FROM user_duel_to_review utr
                WHERE utr.duel_strapi_document_id = uda.duel_strapi_document_id
                    AND utr.hash = uda.hash
            ) < 6
        GROUP BY uds.duel_id, uds.duel_number
    )
    SELECT json_build_object(
        'user', p_telegram_username,
        'user_id', v_user_id,
        'duel_quotas', (
            SELECT json_agg(json_build_object(
                'duel_number', uds.duel_number,
                'duel_type', uds.duel_type,
                'played_times', uds.played_count,
                'should_review', uds.played_count * 3,
                'actually_reviews', uds.review_count,
                'deficit', uds.played_count * 3 - uds.review_count,
                'total_pairs_in_duel', uds.total_pairs_in_duel,
                'available_to_review', COALESCE(afr.available_pairs, 0),
                'problem', CASE
                    WHEN uds.played_count * 3 = uds.review_count THEN 'None'
                    WHEN COALESCE(afr.available_pairs, 0) = 0 THEN 'No available pairs to review'
                    WHEN COALESCE(afr.available_pairs, 0) < (uds.played_count * 3 - uds.review_count) THEN 'Not enough available pairs'
                    ELSE 'Unknown'
                END
            ) ORDER BY uds.played_count * 3 - uds.review_count DESC)
            FROM user_duel_stats uds
            LEFT JOIN available_for_review afr ON afr.duel_id = uds.duel_id
        ),
        'summary', (
            SELECT json_build_object(
                'total_played', SUM(played_count),
                'total_should_review', SUM(played_count * 3),
                'total_reviews', SUM(review_count),
                'total_deficit', SUM(played_count * 3 - review_count),
                'duels_with_deficit', COUNT(*) FILTER (WHERE played_count * 3 > review_count)
            )
            FROM user_duel_stats
        )
    ) INTO v_result;
    
    RETURN v_result;
END;
