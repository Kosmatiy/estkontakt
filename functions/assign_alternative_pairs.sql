DECLARE
    v_assigned INT := 0;
    v_alternative_offsets INT[] := ARRAY[4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17];
    v_offset INT;
    v_pair_offset INT;
    v_target_positions INT[];
    v_pair_key TEXT;
BEGIN
    -- Пробуем альтернативные смещения
    FOREACH v_offset IN ARRAY v_alternative_offsets LOOP
        EXIT WHEN v_assigned >= p_needed_count;
        
        -- Для каждого смещения пробуем найти пару с подходящим вторым игроком
        FOR v_pair_offset IN 1..p_total_players/2 LOOP
            EXIT WHEN v_assigned >= p_needed_count;
            
            v_target_positions := ARRAY[
                ((p_reviewer_position - 1 + v_offset) % p_total_players) + 1,
                ((p_reviewer_position - 1 + v_offset + v_pair_offset) % p_total_players) + 1
            ];
            
            -- Находим пару
            SELECT pair_key INTO v_pair_key
            FROM tmp_pair_positions
            WHERE (pos1 = v_target_positions[1] AND pos2 = v_target_positions[2])
               OR (pos1 = v_target_positions[2] AND pos2 = v_target_positions[1])
               OR pos1 = v_target_positions[1] 
               OR pos2 = v_target_positions[1]
               OR pos1 = v_target_positions[2]
               OR pos2 = v_target_positions[2]
            LIMIT 1;
            
            -- Проверяем и назначаем
            IF v_pair_key IS NOT NULL 
               AND v_pair_key != ALL(p_already_assigned)
               AND NOT EXISTS (
                   SELECT 1 FROM user_duel_to_review udr
                   JOIN tmp_all_pairs p ON p.pair_key = v_pair_key
                   WHERE udr.reviewer_user_strapi_document_id = p_reviewer_id
                     AND udr.duel_strapi_document_id = p.duel_strapi_document_id
                     AND udr.hash = p.hash
               )
            THEN
                INSERT INTO user_duel_to_review (
                    reviewer_user_strapi_document_id,
                    duel_strapi_document_id,
                    user_strapi_document_id,
                    hash
                )
                SELECT 
                    p_reviewer_id,
                    p.duel_strapi_document_id,
                    unnest(p.participants),
                    p.hash
                FROM tmp_all_pairs p
                WHERE p.pair_key = v_pair_key
                ON CONFLICT DO NOTHING;
                
                v_assigned := v_assigned + 1;
            END IF;
        END LOOP;
    END LOOP;
    
    RETURN v_assigned;
END;
