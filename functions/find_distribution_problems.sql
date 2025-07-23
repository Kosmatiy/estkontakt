BEGIN
    -- Пользователи с недовыполненной квотой
    RETURN QUERY
    SELECT 
        'UNDERQUOTA'::TEXT as problem_type,
        '@' || u.telegram_username as user_name,
        chk.duel_number,
        jsonb_build_object(
            'user_id', u.strapi_document_id,
            'played_times', chk.played_times,
            'should_review', chk.should_review,
            'actually_reviews', chk.actually_reviews,
            'deficit', chk.deficit,
            'missing_pairs', (
                -- Находим какие именно пары он мог бы проверить, но не проверяет
                SELECT jsonb_agg(jsonb_build_object(
                    'hash', sub.hash,
                    'participants', sub.participants,
                    'current_reviewers', sub.current_reviewers
                ))
                FROM (
                    SELECT DISTINCT
                        uda.hash,
                        array_agg(DISTINCT uda.user_strapi_document_id) as participants,
                        COUNT(DISTINCT utr.reviewer_user_strapi_document_id) as current_reviewers
                    FROM user_duel_answers uda
                    LEFT JOIN user_duel_to_review utr ON utr.duel_strapi_document_id = uda.duel_strapi_document_id
                        AND utr.hash = uda.hash
                    WHERE uda.duel_strapi_document_id = d.strapi_document_id
                        AND uda.hash IS NOT NULL
                        AND uda.hash != ''
                        -- Пользователь не участвовал в этой паре
                        AND NOT EXISTS (
                            SELECT 1 FROM user_duel_answers uda2
                            WHERE uda2.duel_strapi_document_id = uda.duel_strapi_document_id
                                AND uda2.hash = uda.hash
                                AND uda2.user_strapi_document_id = u.strapi_document_id
                        )
                        -- Пользователь еще не назначен на эту пару
                        AND NOT EXISTS (
                            SELECT 1 FROM user_duel_to_review utr2
                            WHERE utr2.duel_strapi_document_id = uda.duel_strapi_document_id
                                AND utr2.hash = uda.hash
                                AND utr2.reviewer_user_strapi_document_id = u.strapi_document_id
                        )
                    GROUP BY uda.hash
                    HAVING COUNT(DISTINCT uda.user_strapi_document_id) = 2
                        AND COUNT(DISTINCT utr.reviewer_user_strapi_document_id) < 6
                    LIMIT 5
                ) sub
            )
        ) as details
    FROM check_distribution_health(p_sprint_id) chk
    JOIN users u ON '@' || u.telegram_username = chk.user_name
    JOIN duels d ON d.duel_number = chk.duel_number AND d.sprint_strapi_document_id = p_sprint_id
    WHERE chk.status = '⚠ UNDERQUOTA';

    -- Пары с недостаточным количеством рецензентов
    RETURN QUERY
    SELECT 
        'UNDERREVIEWED_PAIR'::TEXT as problem_type,
        d.duel_number as user_name,  -- используем как идентификатор дуэли
        pairs.hash as duel_number,    -- используем как идентификатор пары
        jsonb_build_object(
            'duel_id', d.strapi_document_id,
            'participants', pairs.participants,
            'participant_names', pairs.participant_names,
            'current_reviewers', pairs.reviewer_count,
            'needed_reviewers', 6 - pairs.reviewer_count,
            'assigned_reviewers', pairs.reviewers,
            'potential_reviewers', (
                -- Кто мог бы проверить эту пару
                SELECT jsonb_agg(jsonb_build_object(
                    'user_id', pot.user_id,
                    'username', pot.username,
                    'played_this_duel', pot.played_count,
                    'current_quota_usage', pot.assigned_count,
                    'remaining_quota', pot.remaining_quota
                ))
                FROM (
                    SELECT 
                        u.strapi_document_id as user_id,
                        u.telegram_username as username,
                        COUNT(DISTINCT uda.hash) as played_count,
                        COUNT(DISTINCT utr.hash) as assigned_count,
                        (COUNT(DISTINCT uda.hash) * 3 - COUNT(DISTINCT utr.hash)) as remaining_quota
                    FROM users u
                    LEFT JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
                        AND uda.duel_strapi_document_id = d.strapi_document_id
                    LEFT JOIN user_duel_to_review utr ON utr.reviewer_user_strapi_document_id = u.strapi_document_id
                        AND utr.duel_strapi_document_id = d.strapi_document_id
                    WHERE u.dismissed_at IS NULL
                        -- Пользователь играл эту дуэль
                        AND EXISTS (
                            SELECT 1 FROM user_duel_answers uda2
                            WHERE uda2.user_strapi_document_id = u.strapi_document_id
                                AND uda2.duel_strapi_document_id = d.strapi_document_id
                        )
                        -- Но не участвовал в этой конкретной паре
                        AND u.strapi_document_id NOT IN (
                            SELECT user_strapi_document_id 
                            FROM user_duel_answers 
                            WHERE duel_strapi_document_id = d.strapi_document_id 
                                AND hash = pairs.hash
                        )
                        -- И еще не назначен на эту пару
                        AND NOT EXISTS (
                            SELECT 1 FROM user_duel_to_review utr2
                            WHERE utr2.reviewer_user_strapi_document_id = u.strapi_document_id
                                AND utr2.duel_strapi_document_id = d.strapi_document_id
                                AND utr2.hash = pairs.hash
                        )
                    GROUP BY u.strapi_document_id, u.telegram_username
                    HAVING COUNT(DISTINCT uda.hash) * 3 > COUNT(DISTINCT utr.hash)
                    LIMIT 10
                ) pot
            )
        ) as details
    FROM duels d
    JOIN LATERAL (
        SELECT 
            uda.hash,
            array_agg(DISTINCT uda.user_strapi_document_id ORDER BY uda.user_strapi_document_id) as participants,
            array_agg(DISTINCT u.telegram_username ORDER BY u.telegram_username) as participant_names,
            COUNT(DISTINCT utr.reviewer_user_strapi_document_id) as reviewer_count,
            array_agg(DISTINCT utr_u.telegram_username) FILTER (WHERE utr_u.telegram_username IS NOT NULL) as reviewers
        FROM user_duel_answers uda
        JOIN users u ON u.strapi_document_id = uda.user_strapi_document_id
        LEFT JOIN user_duel_to_review utr ON utr.duel_strapi_document_id = uda.duel_strapi_document_id
            AND utr.hash = uda.hash
        LEFT JOIN users utr_u ON utr_u.strapi_document_id = utr.reviewer_user_strapi_document_id
        WHERE uda.duel_strapi_document_id = d.strapi_document_id
            AND uda.hash IS NOT NULL
            AND uda.hash != ''
        GROUP BY uda.hash
        HAVING COUNT(DISTINCT uda.user_strapi_document_id) = 2
            AND COUNT(DISTINCT utr.reviewer_user_strapi_document_id) < 6
    ) pairs ON true
    WHERE d.sprint_strapi_document_id = p_sprint_id
    LIMIT 20;
END;
