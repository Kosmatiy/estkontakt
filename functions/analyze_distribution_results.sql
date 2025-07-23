BEGIN
    -- Анализ по дуэлям
    RETURN QUERY
    SELECT 
        'DUEL_SUMMARY'::TEXT as analysis_type,
        dq.duel_number,
        'total_reviewers'::TEXT as metric,
        COUNT(DISTINCT dq.user_id)::NUMERIC as value
    FROM debug_duel_quotas dq
    WHERE dq.execution_id = p_execution_id
    GROUP BY dq.duel_number
    
    UNION ALL
    
    SELECT 
        'DUEL_SUMMARY'::TEXT,
        dq.duel_number,
        'total_quota'::TEXT,
        SUM(dq.review_quota)::NUMERIC
    FROM debug_duel_quotas dq
    WHERE dq.execution_id = p_execution_id
    GROUP BY dq.duel_number
    
    UNION ALL
    
    SELECT 
        'DUEL_SUMMARY'::TEXT,
        dq.duel_number,
        'unfulfilled_quota'::TEXT,
        SUM(dq.remaining_quota)::NUMERIC
    FROM debug_duel_quotas dq
    WHERE dq.execution_id = p_execution_id
    GROUP BY dq.duel_number
    
    UNION ALL
    
    SELECT 
        'DUEL_PAIRS'::TEXT,
        dtr.duel_number,
        'available_pairs'::TEXT,
        COUNT(DISTINCT dtr.hash)::NUMERIC
    FROM debug_duels_to_review dtr
    WHERE dtr.execution_id = p_execution_id
    GROUP BY dtr.duel_number
    
    UNION ALL
    
    SELECT 
        'DUEL_PAIRS'::TEXT,
        dtr.duel_number,
        'fully_assigned_pairs'::TEXT,
        COUNT(DISTINCT dtr.hash)::NUMERIC
    FROM debug_duels_to_review dtr
    WHERE dtr.execution_id = p_execution_id
      AND dtr.reviewers_assigned = 6
    GROUP BY dtr.duel_number
    
    ORDER BY analysis_type, duel_number, metric;
END;
