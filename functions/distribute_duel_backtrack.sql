DECLARE
    rec RECORD;
    v_deleted INT := 0;
BEGIN
    -- Откатываем последние N рёбер
    FOR rec IN 
        SELECT eh.*, p.pair_hash, p.duel_id
        FROM tmp_edges_history eh
        JOIN tmp_pairs p ON p.pair_id = eh.target_pair_id
        ORDER BY eh.edge_id DESC 
        LIMIT p_count
    LOOP
        -- Удаляем из user_duel_to_review
        DELETE FROM user_duel_to_review
        WHERE reviewer_user_strapi_document_id = rec.reviewer_id
          AND hash = rec.pair_hash
          AND duel_strapi_document_id = rec.duel_id;
        
        GET DIAGNOSTICS v_deleted = ROW_COUNT;
        
        IF v_deleted > 0 THEN
            -- Восстанавливаем счётчики
            UPDATE tmp_pairs SET out_left = out_left + 1, out_done = out_done - 1 
            WHERE pair_id = rec.source_pair_id;
            
            UPDATE tmp_pairs SET in_left = in_left + 1, in_done = in_done - 1 
            WHERE pair_id = rec.target_pair_id;
            
            UPDATE tmp_students SET quota_left = quota_left + 1, quota_used = quota_used - 1 
            WHERE user_id = rec.reviewer_id;
        END IF;
        
        -- Удаляем из истории
        DELETE FROM tmp_edges_history WHERE edge_id = rec.edge_id;
    END LOOP;
    
    INSERT INTO duel_distribution_logs (sprint_id, log_level, step, message, data)
    VALUES (p_sprint_id, 'DEBUG', 'BACKTRACK_DONE', 
            format('Rolled back %s edges', p_count),
            jsonb_build_object('edges_rolled_back', p_count));
END;
