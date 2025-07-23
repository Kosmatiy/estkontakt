SELECT json_build_object(
        'sprint_id', p_sprint_id,
        'total_duels', (
            SELECT COUNT(*) FROM duels WHERE sprint_strapi_document_id = p_sprint_id
        ),
        'total_pairs', (
            SELECT COUNT(DISTINCT hash || '_' || duel_strapi_document_id)
            FROM user_duel_answers uda
            JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
            WHERE d.sprint_strapi_document_id = p_sprint_id
              AND uda.hash IS NOT NULL
        ),
        'total_active_users', (
            SELECT COUNT(DISTINCT u.strapi_document_id)
            FROM users u
            JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
            JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
            WHERE d.sprint_strapi_document_id = p_sprint_id
              AND u.dismissed_at IS NULL
        ),
        'pairs_fully_reviewed', (
            SELECT COUNT(*)
            FROM (
                SELECT duel_strapi_document_id, hash
                FROM user_duel_to_review utr
                JOIN duels d ON d.strapi_document_id = utr.duel_strapi_document_id
                WHERE d.sprint_strapi_document_id = p_sprint_id
                GROUP BY duel_strapi_document_id, hash
                HAVING COUNT(DISTINCT reviewer_user_strapi_document_id) = 6
            ) t
        ),
        'distribution_by_duel', (
            SELECT json_agg(
                json_build_object(
                    'duel_number', duel_number,
                    'total_pairs', total_pairs,
                    'fully_reviewed', fully_reviewed,
                    'partially_reviewed', partially_reviewed,
                    'not_reviewed', not_reviewed
                ) ORDER BY duel_number
            )
            FROM (
                SELECT 
                    d.duel_number,
                    COUNT(DISTINCT uda.hash) as total_pairs,
                    COUNT(DISTINCT uda.hash) FILTER (
                        WHERE sub.reviewer_count = 6
                    ) as fully_reviewed,
                    COUNT(DISTINCT uda.hash) FILTER (
                        WHERE sub.reviewer_count > 0 AND sub.reviewer_count < 6
                    ) as partially_reviewed,
                    COUNT(DISTINCT uda.hash) FILTER (
                        WHERE sub.reviewer_count IS NULL OR sub.reviewer_count = 0
                    ) as not_reviewed
                FROM duels d
                JOIN user_duel_answers uda ON uda.duel_strapi_document_id = d.strapi_document_id
                LEFT JOIN (
                    SELECT 
                        duel_strapi_document_id,
                        hash,
                        COUNT(DISTINCT reviewer_user_strapi_document_id) as reviewer_count
                    FROM user_duel_to_review
                    GROUP BY duel_strapi_document_id, hash
                ) sub ON sub.duel_strapi_document_id = d.strapi_document_id 
                      AND sub.hash = uda.hash
                WHERE d.sprint_strapi_document_id = p_sprint_id
                  AND uda.hash IS NOT NULL
                GROUP BY d.duel_number
            ) t
        ),
        'timestamp', NOW()
    );
