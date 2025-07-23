DECLARE
    v_suggestions JSONB := '[]'::JSONB;
    rec RECORD;
BEGIN
    -- Найти пользователей с дефицитом и доступные пары
    FOR rec IN
        WITH problems AS (
            SELECT 
                u.strapi_document_id as user_id,
                u.telegram_username,
                d.strapi_document_id as duel_id,
                d.duel_number,
                chk.deficit
            FROM check_distribution_health(p_sprint_id) chk
            JOIN users u ON '@' || u.telegram_username = chk.user_name
            JOIN duels d ON d.duel_number = chk.duel_number AND d.sprint_strapi_document_id = p_sprint_id
            WHERE chk.status = '⚠ UNDERQUOTA'
        ),
        available_assignments AS (
            SELECT 
                p.user_id,
                p.telegram_username,
                p.duel_id,
                p.duel_number,
                p.deficit,
                uda.hash,
                array_agg(DISTINCT uda.user_strapi_document_id) as participants,
                COUNT(DISTINCT utr.reviewer_user_strapi_document_id) as current_reviewers
            FROM problems p
            JOIN user_duel_answers uda ON uda.duel_strapi_document_id = p.duel_id
            LEFT JOIN user_duel_to_review utr ON utr.duel_strapi_document_id = uda.duel_strapi_document_id
                AND utr.hash = uda.hash
            WHERE uda.hash IS NOT NULL
                -- Пользователь не участвовал
                AND NOT EXISTS (
                    SELECT 1 FROM user_duel_answers uda2
                    WHERE uda2.duel_strapi_document_id = uda.duel_strapi_document_id
                        AND uda2.hash = uda.hash
                        AND uda2.user_strapi_document_id = p.user_id
                )
                -- Пользователь еще не назначен
                AND NOT EXISTS (
                    SELECT 1 FROM user_duel_to_review utr2
                    WHERE utr2.duel_strapi_document_id = uda.duel_strapi_document_id
                        AND utr2.hash = uda.hash
                        AND utr2.reviewer_user_strapi_document_id = p.user_id
                )
            GROUP BY p.user_id, p.telegram_username, p.duel_id, p.duel_number, p.deficit, uda.hash
            HAVING COUNT(DISTINCT uda.user_strapi_document_id) = 2
                AND COUNT(DISTINCT utr.reviewer_user_strapi_document_id) < 6
        )
        SELECT * FROM available_assignments
        ORDER BY deficit DESC, current_reviewers ASC
        LIMIT 10
    LOOP
        v_suggestions := v_suggestions || jsonb_build_object(
            'action', 'ASSIGN_REVIEWER',
            'user', rec.telegram_username,
            'duel', rec.duel_number,
            'hash', rec.hash,
            'participants', rec.participants,
            'current_reviewers', rec.current_reviewers,
            'sql', format(
                'INSERT INTO user_duel_to_review (reviewer_user_strapi_document_id, duel_strapi_document_id, user_strapi_document_id, hash) VALUES %s;',
                string_agg(
                    format('(''%s'', ''%s'', ''%s'', ''%s'')', 
                        rec.user_id, rec.duel_id, participant, rec.hash),
                    ', '
                )
            )
        )
        FROM unnest(rec.participants) as participant;
    END LOOP;
    
    RETURN json_build_object(
        'problem_summary', (
            SELECT json_build_object(
                'users_with_deficit', COUNT(DISTINCT user_name),
                'total_deficit', SUM(deficit),
                'affected_duels', COUNT(DISTINCT duel_number)
            )
            FROM check_distribution_health(p_sprint_id)
            WHERE status = '⚠ UNDERQUOTA'
        ),
        'suggestions', v_suggestions,
        'manual_fix_available', jsonb_array_length(v_suggestions) > 0
    );
END;
