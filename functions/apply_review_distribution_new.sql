DECLARE
    player_count INTEGER;
    duel_count INTEGER;
    checks_per_run INTEGER := 3;
    player_num INTEGER;
    player_id TEXT;
    match_count INTEGER;
    run_number INTEGER;
    raw_start INTEGER;
    temp_run RECORD;
    check_offset INTEGER;
    pair_index INTEGER;
    selected_hash TEXT;
    participant1_id TEXT;
    participant2_id TEXT;
BEGIN
    -- Получаем количество игроков и схваток
    SELECT MAX(key::INTEGER) INTO player_count FROM jsonb_object_keys(players_json) AS key;
    SELECT MAX(key::INTEGER) INTO duel_count FROM jsonb_object_keys(duels_json) AS key;

    -- Очищаем временную таблицу
    DELETE FROM temp_runs;

    -- Создаем записи runs для каждого игрока
    FOR player_num IN 1..player_count LOOP
        player_id := players_json->>player_num::text;
        
        -- Определяем количество матчей для данного игрока
        SELECT COUNT(DISTINCT uda.hash) INTO match_count
        FROM user_duel_answers uda
        WHERE uda.duel_strapi_document_id = target_duel_id
        AND (uda.user_strapi_document_id = player_id OR uda.rival_user_strapi_document_id = player_id);
        
        -- Создаем runs для данного игрока
        FOR run_number IN 1..match_count LOOP
            raw_start := player_num + checks_per_run * (run_number - 1) + 1;
            
            INSERT INTO temp_runs (run_data, raw_start_val, reviewer_val, corrected_start_val)
            VALUES (
                jsonb_build_object(
                    'reviewer', player_num,
                    'player_id', player_id,
                    'run_number', run_number
                ),
                raw_start,
                player_num,
                raw_start
            );
        END LOOP;
    END LOOP;

    -- Корректируем start значения в правильном порядке
    WITH corrected AS (
        SELECT 
            id,
            CASE 
                WHEN raw_start_val <= lag(raw_start_val, 1, 0) OVER (ORDER BY raw_start_val, reviewer_val)
                THEN lag(raw_start_val, 1, 0) OVER (ORDER BY raw_start_val, reviewer_val) + 1
                ELSE raw_start_val
            END as new_corrected_start
        FROM temp_runs
        ORDER BY raw_start_val, reviewer_val
    )
    UPDATE temp_runs 
    SET corrected_start_val = corrected.new_corrected_start
    FROM corrected
    WHERE temp_runs.id = corrected.id;

    -- Генерируем задания для каждого run
    FOR temp_run IN 
        SELECT * FROM temp_runs ORDER BY corrected_start_val
    LOOP
        player_id := temp_run.run_data->>'player_id';

        -- Назначаем 3 проверки для данного run
        FOR check_offset IN 0..checks_per_run-1 LOOP
            pair_index := ((temp_run.corrected_start_val + check_offset - 1) % duel_count) + 1;
            selected_hash := duels_json->>pair_index::text;
            
            -- Пропускаем, если hash не найден
            CONTINUE WHEN selected_hash IS NULL;
            
            -- Получаем участников схватки
            SELECT 
                uda.user_strapi_document_id, 
                uda.rival_user_strapi_document_id
            INTO participant1_id, participant2_id
            FROM user_duel_answers uda
            WHERE uda.duel_strapi_document_id = target_duel_id
            AND uda.hash = selected_hash
            LIMIT 1;

            -- Пропускаем, если участники не найдены
            CONTINUE WHEN participant1_id IS NULL OR participant2_id IS NULL;

            -- Вставляем записи для обоих участников схватки
            INSERT INTO user_duel_to_review (
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                hash,
                created_at
            ) VALUES 
            (player_id, target_duel_id, participant1_id, selected_hash, NOW()),
            (player_id, target_duel_id, participant2_id, selected_hash, NOW())
            ON CONFLICT DO NOTHING;  -- Избегаем дублирования записей
            
        END LOOP;
    END LOOP;

    -- Очищаем временную таблицу после использования
    DELETE FROM temp_runs;

END;
