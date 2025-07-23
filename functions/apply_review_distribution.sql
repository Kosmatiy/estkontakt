DECLARE
    player_count INTEGER;
    duel_count INTEGER;
    checks_per_run INTEGER := 3;
    player_num INTEGER;
    player_id TEXT;
    match_count INTEGER;
    run_number INTEGER;
    raw_start INTEGER;
    corrected_start INTEGER;
    last_start INTEGER := 0;
    check_offset INTEGER;
    pair_index INTEGER;
    selected_hash TEXT;
    participant1_id TEXT;
    participant2_id TEXT;
    runs_array JSONB[] := ARRAY[]::JSONB[];
    run_data JSONB;
    current_run JSONB;
    temp_runs JSONB[] := ARRAY[]::JSONB[];
    i INTEGER;
BEGIN
    -- Получаем количество игроков и схваток
    SELECT MAX(key::INTEGER) INTO player_count FROM jsonb_object_keys(players_json) key;
    SELECT MAX(key::INTEGER) INTO duel_count FROM jsonb_object_keys(duels_json) key;

    -- Создаем массив runs для каждого игрока
    FOR player_num IN 1..player_count LOOP
        player_id := players_json->>player_num::text;
        
        -- Определяем количество матчей для данного игрока (количество уникальных hash)
        SELECT COUNT(DISTINCT uda.hash) INTO match_count
        FROM user_duel_answers uda
        WHERE uda.duel_strapi_document_id = target_duel_id
        AND (uda.user_strapi_document_id = player_id OR uda.rival_user_strapi_document_id = player_id);
        
        -- Создаем runs для данного игрока
        FOR run_number IN 1..match_count LOOP
            raw_start := player_num + checks_per_run * (run_number - 1);
            
            run_data := jsonb_build_object(
                'reviewer', player_num,
                'player_id', player_id,
                'raw_start', raw_start,
                'run_number', run_number
            );
            
            runs_array := runs_array || run_data;
        END LOOP;
    END LOOP;

    -- ИСПРАВЛЕНИЕ: Используем другой алиас для устранения конфликта имен
    SELECT array_agg(run_item ORDER BY (run_item->>'raw_start')::INTEGER, (run_item->>'reviewer')::INTEGER)
    INTO temp_runs
    FROM unnest(runs_array) AS run_item;
    
    runs_array := temp_runs;

    -- Корректируем raw_start для избежания пересечений
    last_start := 0;
    FOR i IN 1..array_length(runs_array, 1) LOOP
        current_run := runs_array[i];
        corrected_start := (current_run->>'raw_start')::INTEGER;
        
        IF corrected_start <= last_start THEN
            corrected_start := last_start + 1;
        END IF;
        
        -- Обновляем run с исправленным start
        runs_array[i] := jsonb_set(current_run, '{corrected_start}', to_jsonb(corrected_start));
        last_start := corrected_start;
    END LOOP;

    -- Генерируем задания для каждого run
    FOR i IN 1..array_length(runs_array, 1) LOOP
        current_run := runs_array[i];
        player_id := current_run->>'player_id';
        corrected_start := (current_run->>'corrected_start')::INTEGER;

        -- Назначаем 3 проверки для данного run
        FOR check_offset IN 1..checks_per_run LOOP
            pair_index := ((corrected_start + check_offset - 1) % duel_count) + 1;
            selected_hash := duels_json->>pair_index::text;
            
            -- Пропускаем, если hash не найден
            IF selected_hash IS NULL THEN
                CONTINUE;
            END IF;
            
            -- Получаем участников схватки
            SELECT 
                uda.user_strapi_document_id, 
                uda.rival_user_strapi_document_id
            INTO participant1_id, participant2_id
            FROM user_duel_answers uda
            WHERE uda.duel_strapi_document_id = target_duel_id
            AND uda.hash = selected_hash
            LIMIT 1;

            -- Вставляем записи только если участники найдены
            IF participant1_id IS NOT NULL AND participant2_id IS NOT NULL THEN
                INSERT INTO user_duel_to_review (
                    reviewer_user_strapi_document_id,
                    duel_strapi_document_id,
                    user_strapi_document_id,
                    hash,
                    created_at
                ) VALUES 
                (player_id, target_duel_id, participant1_id, selected_hash, NOW()),
                (player_id, target_duel_id, participant2_id, selected_hash, NOW());
            END IF;
            
        END LOOP;
    END LOOP;

END;
