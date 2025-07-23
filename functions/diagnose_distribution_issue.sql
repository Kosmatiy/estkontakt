DECLARE
    v_result JSON;
    v_user_quotas JSON;
    v_pairs_summary JSON;
    v_can_assign JSON;
BEGIN
    -- Создаем временные таблицы
    CREATE TEMP TABLE temp_quotas (
        user_id TEXT,
        duel_id TEXT,
        duel_number TEXT,
        played_count INTEGER,
        review_quota INTEGER,
        assigned_count INTEGER DEFAULT 0,
        remaining_quota INTEGER
    ) ON COMMIT DROP;
    
    CREATE TEMP TABLE temp_pairs (
        duel_id TEXT,
        duel_number TEXT,
        hash TEXT,
        participant1_id TEXT,
        participant2_id TEXT,
        current_reviewers INTEGER DEFAULT 0,
        needed_reviewers INTEGER DEFAULT 6
    ) ON COMMIT DROP;
    
    -- Заполняем квоты
    INSERT INTO temp_quotas (user_id, duel_id, duel_number, played_count, review_quota, remaining_quota)
    SELECT 
        u.strapi_document_id,
        d.strapi_document_id,
        d.duel_number,
        COUNT(DISTINCT uda.hash)::INTEGER,
        (COUNT(DISTINCT uda.hash) * 3)::INTEGER,
        (COUNT(DISTINCT uda.hash) * 3)::INTEGER
    FROM users u
    INNER JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
    INNER JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_id
      AND u.dismissed_at IS NULL
      AND uda.hash IS NOT NULL
      AND uda.hash != ''
      AND (p_user IS NULL OR u.telegram_username = p_user)
    GROUP BY u.strapi_document_id, d.strapi_document_id, d.duel_number;
    
    -- Учитываем уже назначенные
    UPDATE temp_quotas tq
    SET assigned_count = sub.cnt::INTEGER,
        remaining_quota = GREATEST(0, review_quota - sub.cnt::INTEGER)
    FROM (
        SELECT 
            reviewer_user_strapi_document_id,
            duel_strapi_document_id,
            COUNT(DISTINCT hash) as cnt
        FROM user_duel_to_review
        GROUP BY reviewer_user_strapi_document_id, duel_strapi_document_id
    ) sub
    WHERE tq.user_id = sub.reviewer_user_strapi_document_id
      AND tq.duel_id = sub.duel_strapi_document_id;
    
    -- Заполняем пары
    INSERT INTO temp_pairs (duel_id, duel_number, hash, participant1_id, participant2_id)
    SELECT 
        d.strapi_document_id,
        d.duel_number,
        pairs.hash,
        pairs.user1,
        pairs.user2
    FROM duels d
    INNER JOIN LATERAL (
        SELECT 
            uda.hash,
            MIN(uda.user_strapi_document_id) as user1,
            MAX(uda.user_strapi_document_id) as user2
        FROM user_duel_answers uda
        WHERE uda.duel_strapi_document_id = d.strapi_document_id
          AND uda.hash IS NOT NULL
          AND uda.hash != ''
        GROUP BY uda.hash
        HAVING COUNT(DISTINCT uda.user_strapi_document_id) = 2
    ) pairs ON true
    WHERE d.sprint_strapi_document_id = p_sprint_id;
    
    -- Обновляем счетчики рецензентов
    UPDATE temp_pairs tp
    SET current_reviewers = COALESCE(sub.cnt, 0)::INTEGER,
        needed_reviewers = GREATEST(0, 6 - COALESCE(sub.cnt, 0))::INTEGER
    FROM (
        SELECT 
            duel_strapi_document_id,
            hash,
            COUNT(DISTINCT reviewer_user_strapi_document_id) as cnt
        FROM user_duel_to_review
        GROUP BY duel_strapi_document_id, hash
    ) sub
    WHERE tp.duel_id = sub.duel_strapi_document_id
      AND tp.hash = sub.hash;
    
    -- Собираем результаты по частям
    SELECT json_agg(json_build_object(
        'username', u.telegram_username,
        'duel_number', tq.duel_number,
        'played_count', tq.played_count,
        'review_quota', tq.review_quota,
        'assigned_count', tq.assigned_count,
        'remaining_quota', tq.remaining_quota
    ))
    INTO v_user_quotas
    FROM temp_quotas tq
    JOIN users u ON u.strapi_document_id = tq.user_id
    WHERE p_user IS NULL OR u.telegram_username = p_user;
    
    SELECT json_agg(summary)
    INTO v_pairs_summary
    FROM (
        SELECT json_build_object(
            'duel_number', duel_number,
            'total_pairs', COUNT(*),
            'pairs_needing_reviewers', COUNT(*) FILTER (WHERE needed_reviewers > 0)
        ) as summary
        FROM temp_pairs
        GROUP BY duel_number
    ) t;
    
    SELECT json_agg(assignment_info)
    INTO v_can_assign
    FROM (
        SELECT json_build_object(
            'user', u.telegram_username,
            'duel', tq.duel_number,
            'remaining_quota', tq.remaining_quota,
            'available_pairs', COUNT(tp.hash),
            'sample_hashes', array_agg(LEFT(tp.hash, 8)) FILTER (WHERE tp.hash IS NOT NULL)
        ) as assignment_info
        FROM temp_quotas tq
        JOIN users u ON u.strapi_document_id = tq.user_id
        LEFT JOIN temp_pairs tp ON tp.duel_id = tq.duel_id
            AND tp.participant1_id != tq.user_id
            AND tp.participant2_id != tq.user_id
            AND tp.needed_reviewers > 0
            AND NOT EXISTS (
                SELECT 1 FROM user_duel_to_review utr
                WHERE utr.reviewer_user_strapi_document_id = tq.user_id
                  AND utr.duel_strapi_document_id = tp.duel_id
                  AND utr.hash = tp.hash
            )
        WHERE tq.remaining_quota > 0
          AND (p_user IS NULL OR u.telegram_username = p_user)
        GROUP BY u.telegram_username, tq.duel_number, tq.remaining_quota
    ) t;
    
    v_result := json_build_object(
        'user_quotas', v_user_quotas,
        'available_pairs_per_duel', v_pairs_summary,
        'can_assign', v_can_assign
    );
    
    RETURN v_result;
END;
