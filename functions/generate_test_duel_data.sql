DECLARE
    v_stream_id TEXT := 'test_stream_' || gen_random_uuid();
    v_sprint_id TEXT := 'test_sprint_' || gen_random_uuid();
    v_team_ids TEXT[];
    v_user_ids TEXT[];
    v_duel_ids TEXT[];
    i INT;
    j INT;
    v_pair_count INT := 0;
    v_game_count INT := 0;
BEGIN
    -- Создаем поток
    INSERT INTO streams (strapi_document_id, name)
    VALUES (v_stream_id, 'Test Stream');

    -- Создаем спринт
    INSERT INTO sprints (strapi_document_id, name, stream_strapi_document_id)
    VALUES (v_sprint_id, 'Test Sprint', v_stream_id);

    -- Создаем команды
    FOR i IN 1..p_teams_count LOOP
        v_team_ids := array_append(v_team_ids, 'test_team_' || i || '_' || gen_random_uuid());
        INSERT INTO teams (strapi_document_id, name)
        VALUES (v_team_ids[i], 'Team ' || i);
    END LOOP;

    -- Создаем пользователей
    FOR i IN 1..p_users_count LOOP
        v_user_ids := array_append(v_user_ids, 'test_user_' || i || '_' || gen_random_uuid());
        INSERT INTO users (strapi_document_id, name, team_strapi_document_id, stream_strapi_document_id)
        VALUES (
            v_user_ids[i], 
            'User ' || i,
            v_team_ids[((i-1) % p_teams_count) + 1],  -- Равномерно распределяем по командам
            v_stream_id
        );
    END LOOP;

    -- Создаем дуэли
    FOR i IN 1..p_duels_count LOOP
        v_duel_ids := array_append(v_duel_ids, 'test_duel_' || i || '_' || gen_random_uuid());
        INSERT INTO duels (strapi_document_id, name, type, sprint_strapi_document_id)
        VALUES (
            v_duel_ids[i],
            'Duel ' || i,
            CASE WHEN i <= 2 THEN 'FULL-CONTACT' ELSE 'TRAINING' END,
            v_sprint_id
        );
    END LOOP;

    -- Создаем ответы для дуэлей
    -- Каждый пользователь играет по 1 игре в первых двух дуэлях
    FOR i IN 1..p_users_count LOOP
        FOR j IN 1..2 LOOP
            IF j <= p_duels_count THEN
                -- Находим соперника из другой команды
                DECLARE
                    v_rival_id TEXT;
                    v_hash TEXT;
                    v_user_team TEXT;
                BEGIN
                    SELECT team_strapi_document_id INTO v_user_team
                      FROM users WHERE strapi_document_id = v_user_ids[i];
                    
                    -- Ищем соперника из другой команды, с которым еще не играли
                    SELECT u.strapi_document_id INTO v_rival_id
                      FROM users u
                     WHERE u.strapi_document_id != v_user_ids[i]
                       AND u.team_strapi_document_id != v_user_team
                       AND NOT EXISTS (
                           SELECT 1 FROM user_duel_answers
                            WHERE duel_strapi_document_id = v_duel_ids[j]
                              AND ((user_strapi_document_id = v_user_ids[i] 
                                    AND rival_user_strapi_document_id = u.strapi_document_id)
                                OR (user_strapi_document_id = u.strapi_document_id 
                                    AND rival_user_strapi_document_id = v_user_ids[i]))
                       )
                     ORDER BY random()
                     LIMIT 1;
                    
                    IF v_rival_id IS NOT NULL THEN
                        -- Создаем hash для пары
                        IF v_user_ids[i] < v_rival_id THEN
                            v_hash := v_user_ids[i] || '_' || v_rival_id;
                        ELSE
                            v_hash := v_rival_id || '_' || v_user_ids[i];
                        END IF;
                        
                        -- Вставляем ответы для обоих игроков
                        INSERT INTO user_duel_answers (
                            user_strapi_document_id,
                            rival_user_strapi_document_id,
                            duel_strapi_document_id,
                            hash,
                            created_at
                        ) VALUES
                        (v_user_ids[i], v_rival_id, v_duel_ids[j], v_hash, NOW()),
                        (v_rival_id, v_user_ids[i], v_duel_ids[j], v_hash, NOW());
                        
                        v_pair_count := v_pair_count + 1;
                        v_game_count := v_game_count + 2;
                    END IF;
                END;
            END IF;
        END LOOP;
    END LOOP;

    -- Добавляем дополнительные игры для некоторых пользователей
    FOR i IN 1..p_extra_games_users LOOP
        -- Третья дуэль для первых 4 пользователей
        IF p_duels_count >= 3 THEN
            DECLARE
                v_rival_id TEXT;
                v_hash TEXT;
            BEGIN
                -- Находим соперника
                SELECT strapi_document_id INTO v_rival_id
                  FROM users
                 WHERE strapi_document_id != v_user_ids[i]
                   AND strapi_document_id != ALL(
                       SELECT rival_user_strapi_document_id 
                         FROM user_duel_answers 
                        WHERE user_strapi_document_id = v_user_ids[i]
                          AND duel_strapi_document_id = v_duel_ids[3]
                   )
                 ORDER BY random()
                 LIMIT 1;
                
                IF v_rival_id IS NOT NULL THEN
                    IF v_user_ids[i] < v_rival_id THEN
                        v_hash := v_user_ids[i] || '_' || v_rival_id;
                    ELSE
                        v_hash := v_rival_id || '_' || v_user_ids[i];
                    END IF;
                    
                    INSERT INTO user_duel_answers (
                        user_strapi_document_id,
                        rival_user_strapi_document_id,
                        duel_strapi_document_id,
                        hash,
                        created_at
                    ) VALUES
                    (v_user_ids[i], v_rival_id, v_duel_ids[3], v_hash, NOW()),
                    (v_rival_id, v_user_ids[i], v_duel_ids[3], v_hash, NOW());
                    
                    v_pair_count := v_pair_count + 1;
                    v_game_count := v_game_count + 2;
                END IF;
            END;
        END IF;
    END LOOP;

    RETURN json_build_object(
        'result', 'success',
        'sprint_id', v_sprint_id,
        'statistics', json_build_object(
            'users_created', p_users_count,
            'teams_created', p_teams_count,
            'duels_created', p_duels_count,
            'pairs_created', v_pair_count,
            'games_created', v_game_count
        )
    );
END;
