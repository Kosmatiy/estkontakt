DECLARE
    v_result distribution_result;
    v_sprint RECORD;
    v_start_time TIMESTAMP;
    v_temp_count INTEGER;
    
    -- Курсоры для обработки
    cur_quota CURSOR FOR 
        SELECT * FROM temp_quotas 
        WHERE remaining_quota > 0 
        ORDER BY remaining_quota DESC, duel_id, user_id
        FOR UPDATE;
    
    rec_quota RECORD;
    rec_pair RECORD;
BEGIN
    -- Инициализация
    v_start_time := clock_timestamp();
    v_result.execution_id := 'dist_' || to_char(NOW(), 'YYYYMMDDHH24MISS') || '_' || substr(md5(random()::text), 1, 8);
    v_result.status := 'PROCESSING';
    v_result.warnings := ARRAY[]::TEXT[];
    v_result.errors := ARRAY[]::TEXT[];
    v_result.cleaned_rows := 0;
    v_result.inserted_rows := 0;
    
    -- Валидация входных параметров
    IF p_mode NOT IN ('CLEANSLATE', 'APPEND') THEN
        v_result.status := 'ERROR';
        v_result.message := 'Invalid mode. Must be CLEANSLATE or APPEND';
        v_result.errors := array_append(v_result.errors, 'Invalid mode parameter');
        RETURN v_result;
    END IF;
    
    -- Проверка существования спринта
    SELECT s.*, st.strapi_document_id as stream_id
    INTO v_sprint
    FROM sprints s
    JOIN streams st ON st.strapi_document_id = s.stream_strapi_document_id
    WHERE s.strapi_document_id = p_sprint_strapi_document_id;
    
    IF NOT FOUND THEN
        v_result.status := 'ERROR';
        v_result.message := format('Sprint %s not found', p_sprint_strapi_document_id);
        v_result.errors := array_append(v_result.errors, 'Sprint not found');
        RETURN v_result;
    END IF;
    
    -- Начинаем транзакцию
    BEGIN
        -- 1. Очистка данных при CLEANSLATE
        IF p_mode = 'CLEANSLATE' THEN
            DELETE FROM user_duel_to_review utr
            USING duels d
            WHERE utr.duel_strapi_document_id = d.strapi_document_id
              AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
            
            GET DIAGNOSTICS v_result.cleaned_rows = ROW_COUNT;
        END IF;
        
        -- 2. Создание временных таблиц для обработки
        CREATE TEMP TABLE temp_quotas (
            user_id TEXT,
            duel_id TEXT,
            duel_number TEXT,
            played_count INTEGER,
            review_quota INTEGER,
            assigned_count INTEGER DEFAULT 0,
            remaining_quota INTEGER,
            PRIMARY KEY (user_id, duel_id)
        ) ON COMMIT DROP;
        
        CREATE TEMP TABLE temp_pairs (
            duel_id TEXT,
            duel_number TEXT,
            hash TEXT,
            participant1_id TEXT,
            participant2_id TEXT,
            current_reviewers INTEGER DEFAULT 0,
            needed_reviewers INTEGER DEFAULT 6,
            PRIMARY KEY (duel_id, hash)
        ) ON COMMIT DROP;
        
        -- 3. Заполнение таблицы квот
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
        WHERE u.stream_strapi_document_id = v_sprint.stream_id
          AND u.dismissed_at IS NULL
          AND d.sprint_strapi_document_id = p_sprint_strapi_document_id
          AND uda.hash IS NOT NULL
          AND uda.hash != ''
        GROUP BY u.strapi_document_id, d.strapi_document_id, d.duel_number;
        
        GET DIAGNOSTICS v_temp_count = ROW_COUNT;
        
        IF v_temp_count = 0 THEN
            v_result.status := 'WARNING';
            v_result.message := 'No duels found to distribute';
            v_result.warnings := array_append(v_result.warnings, 'No user quotas generated');
            RETURN v_result;
        END IF;
        
        -- 4. Учет уже назначенных проверок при APPEND
        IF p_mode = 'APPEND' THEN
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
        END IF;
        
        -- 5. Заполнение таблицы доступных пар
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
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id;
        
        -- Обновление счетчиков уже назначенных рецензентов
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
        
        -- 6. Подсчет общей квоты
        SELECT 
            SUM(review_quota)::INTEGER,
            SUM(assigned_count)::INTEGER
        INTO 
            v_result.total_quota,
            v_result.fulfilled_quota
        FROM temp_quotas;
        
        -- 7. Основной цикл распределения
        OPEN cur_quota;
        LOOP
            FETCH cur_quota INTO rec_quota;
            EXIT WHEN NOT FOUND;
            
            -- Для каждой квоты находим подходящие пары
            FOR rec_pair IN
                SELECT tp.*
                FROM temp_pairs tp
                WHERE tp.duel_id = rec_quota.duel_id
                  AND tp.needed_reviewers > 0
                  AND tp.participant1_id != rec_quota.user_id
                  AND tp.participant2_id != rec_quota.user_id
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM user_duel_to_review utr
                      WHERE utr.reviewer_user_strapi_document_id = rec_quota.user_id
                        AND utr.duel_strapi_document_id = tp.duel_id
                        AND utr.hash = tp.hash
                  )
                ORDER BY tp.needed_reviewers DESC, tp.hash
                LIMIT rec_quota.remaining_quota
            LOOP
                BEGIN
                    -- Вставка назначений для обоих участников
                    INSERT INTO user_duel_to_review (
                        reviewer_user_strapi_document_id,
                        duel_strapi_document_id,
                        user_strapi_document_id,
                        hash
                    )
                    VALUES
                        (rec_quota.user_id, rec_pair.duel_id, rec_pair.participant1_id, rec_pair.hash),
                        (rec_quota.user_id, rec_pair.duel_id, rec_pair.participant2_id, rec_pair.hash)
                    ON CONFLICT DO NOTHING;
                    
                    GET DIAGNOSTICS v_temp_count = ROW_COUNT;
                    
                    IF v_temp_count > 0 THEN
                        v_result.inserted_rows := v_result.inserted_rows + v_temp_count;
                        
                        -- Обновление счетчиков
                        UPDATE temp_pairs
                        SET current_reviewers = current_reviewers + 1,
                            needed_reviewers = needed_reviewers - 1
                        WHERE duel_id = rec_pair.duel_id 
                          AND hash = rec_pair.hash;
                        
                        UPDATE temp_quotas
                        SET assigned_count = assigned_count + 1,
                            remaining_quota = remaining_quota - 1
                        WHERE user_id = rec_quota.user_id 
                          AND duel_id = rec_quota.duel_id;
                        
                        v_result.fulfilled_quota := v_result.fulfilled_quota + 1;
                    END IF;
                    
                EXCEPTION WHEN OTHERS THEN
                    v_result.errors := array_append(v_result.errors, 
                        format('Assignment error: %s', SQLERRM));
                END;
            END LOOP;
        END LOOP;
        CLOSE cur_quota;
        
        -- 8. Сбор предупреждений о проблемах
        -- Пары с недостаточным количеством рецензентов
        FOR rec_pair IN
            SELECT duel_number, COUNT(*) as cnt
            FROM temp_pairs
            WHERE current_reviewers < 6
            GROUP BY duel_number
            ORDER BY duel_number
            LIMIT 10
        LOOP
            v_result.warnings := array_append(v_result.warnings,
                format('Duel %s: %s pairs lack reviewers', 
                       rec_pair.duel_number, rec_pair.cnt));
        END LOOP;
        
        -- Пользователи с невыполненными квотами
        FOR rec_quota IN
            SELECT 
                u.telegram_username,
                tq.duel_number,
                tq.remaining_quota
            FROM temp_quotas tq
            JOIN users u ON u.strapi_document_id = tq.user_id
            WHERE tq.remaining_quota > 0
            ORDER BY tq.remaining_quota DESC
            LIMIT 10
        LOOP
            v_result.warnings := array_append(v_result.warnings,
                format('User @%s: %s reviews missing for duel %s', 
                       rec_quota.telegram_username, 
                       rec_quota.remaining_quota, 
                       rec_quota.duel_number));
        END LOOP;
        
        -- Финализация результата
        v_result.fulfillment_percent := CASE 
            WHEN v_result.total_quota > 0 
            THEN ROUND((v_result.fulfilled_quota::NUMERIC / v_result.total_quota) * 100, 2)
            ELSE 0 
        END;
        
        v_result.execution_time_ms := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time)::INTEGER;
        
        IF array_length(v_result.errors, 1) > 0 THEN
            v_result.status := 'ERROR';
            v_result.message := 'Distribution completed with errors';
        ELSIF array_length(v_result.warnings, 1) > 0 THEN
            v_result.status := 'WARNING';
            v_result.message := 'Distribution completed with warnings';
        ELSE
            v_result.status := 'SUCCESS';
            v_result.message := 'Distribution completed successfully';
        END IF;
        
        RETURN v_result;
        
    EXCEPTION WHEN OTHERS THEN
        -- Откат транзакции происходит автоматически
        v_result.status := 'ERROR';
        v_result.message := format('Critical error: %s', SQLERRM);
        v_result.errors := array_append(v_result.errors, SQLERRM);
        RETURN v_result;
    END;
END;
