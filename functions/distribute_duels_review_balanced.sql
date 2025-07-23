DECLARE
    v_sprint           sprints%ROWTYPE;
    v_execution_id     TEXT := 'exec_' || extract(epoch from now())::bigint || '_' || random()::text;
    v_cleaned_rows     INT := 0;
    v_inserted_rows    INT := 0;
    v_total_duels      INT := 0;
    v_processed_duels  INT := 0;
    
    rec_duel           RECORD;
    rec_reviewer       RECORD;
    v_hash             TEXT;
    v_participants     TEXT[];
    v_assigned_count   INT;
    v_reviewer_offset  INT := 0;
    
    -- Для отладки
    v_debug_info       JSONB := '[]'::JSONB;
BEGIN
    -- 1) Проверяем, что спринт существует
    SELECT * INTO v_sprint
    FROM sprints
    WHERE strapi_document_id = p_sprint_strapi_document_id;
    
    IF NOT FOUND THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Спринт %s не найден', p_sprint_strapi_document_id)
        );
    END IF;

    -- 2) Удаляем предыдущие назначения
    DELETE FROM user_duel_to_review AS utdr
    USING duels AS d
    WHERE utdr.duel_strapi_document_id = d.strapi_document_id
      AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
    GET DIAGNOSTICS v_cleaned_rows = ROW_COUNT;

    -- 3) Создаем временную таблицу с балансировкой нагрузки
    DROP TABLE IF EXISTS tmp_reviewer_load;
    CREATE TEMP TABLE tmp_reviewer_load (
        user_id TEXT PRIMARY KEY,
        telegram_username TEXT,
        assigned_count INT DEFAULT 0,
        last_assigned_at INT DEFAULT 0
    ) ON COMMIT DROP;
    
    -- Заполняем всех активных пользователей
    INSERT INTO tmp_reviewer_load (user_id, telegram_username)
    SELECT u.strapi_document_id, u.telegram_username
    FROM users u
    WHERE u.stream_strapi_document_id = v_sprint.stream_strapi_document_id
      AND u.dismissed_at IS NULL
    ORDER BY u.strapi_document_id;  -- Для предсказуемости
    
    -- Проверяем, что есть достаточно пользователей
    IF (SELECT COUNT(*) FROM tmp_reviewer_load) < 7 THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', 'Недостаточно активных пользователей (минимум 7)'
        );
    END IF;

    -- 4) Создаем временную таблицу для дуэлей с участниками
    DROP TABLE IF EXISTS tmp_duel_pairs;
    CREATE TEMP TABLE tmp_duel_pairs (
        duel_id TEXT,
        duel_number TEXT,
        hash TEXT,
        participant1 TEXT,
        participant2 TEXT,
        processed BOOLEAN DEFAULT FALSE
    ) ON COMMIT DROP;
    
    -- Заполняем информацию о парах
    INSERT INTO tmp_duel_pairs (duel_id, duel_number, hash, participant1, participant2)
    SELECT 
        d.strapi_document_id,
        d.duel_number,
        uda.hash,
        MIN(uda.user_strapi_document_id),
        MAX(uda.user_strapi_document_id)
    FROM duels d
    JOIN user_duel_answers uda ON uda.duel_strapi_document_id = d.strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
      AND uda.hash IS NOT NULL 
      AND uda.hash <> ''
    GROUP BY d.strapi_document_id, d.duel_number, uda.hash
    HAVING COUNT(DISTINCT uda.user_strapi_document_id) = 2;
    
    SELECT COUNT(*) INTO v_total_duels FROM tmp_duel_pairs;

    -- 5) Основной цикл распределения
    FOR rec_duel IN 
        SELECT * FROM tmp_duel_pairs 
        ORDER BY duel_number
    LOOP
        v_participants := ARRAY[rec_duel.participant1, rec_duel.participant2];
        v_assigned_count := 0;
        
        -- Назначаем 6 рецензентов для этой пары
        FOR rec_reviewer IN 
            SELECT user_id, telegram_username
            FROM tmp_reviewer_load
            WHERE user_id <> ALL(v_participants)  -- Исключаем участников
            ORDER BY 
                assigned_count ASC,      -- Сначала те, у кого меньше назначений
                last_assigned_at ASC,    -- Из них - кто давно не назначался
                user_id                  -- Для стабильности
            LIMIT 6
        LOOP
            -- Вставляем назначения для обоих участников
            INSERT INTO user_duel_to_review (
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                hash
            )
            VALUES
                (rec_reviewer.user_id, rec_duel.duel_id, rec_duel.participant1, rec_duel.hash),
                (rec_reviewer.user_id, rec_duel.duel_id, rec_duel.participant2, rec_duel.hash)
            ON CONFLICT DO NOTHING;
            
            GET DIAGNOSTICS v_assigned_count = ROW_COUNT;
            v_inserted_rows := v_inserted_rows + v_assigned_count;
            
            -- Обновляем счетчики для рецензента
            UPDATE tmp_reviewer_load 
            SET assigned_count = assigned_count + 1,
                last_assigned_at = v_processed_duels
            WHERE user_id = rec_reviewer.user_id;
            
            v_assigned_count := v_assigned_count + 1;
        END LOOP;
        
        -- Отмечаем дуэль как обработанную
        UPDATE tmp_duel_pairs SET processed = TRUE WHERE duel_id = rec_duel.duel_id;
        v_processed_duels := v_processed_duels + 1;
        
        -- Добавляем отладочную информацию
        v_debug_info := v_debug_info || jsonb_build_object(
            'duel_number', rec_duel.duel_number,
            'assigned_reviewers', v_assigned_count
        );
    END LOOP;

    -- 6) Собираем статистику распределения
    DROP TABLE IF EXISTS tmp_distribution_stats;
    CREATE TEMP TABLE tmp_distribution_stats AS
    SELECT 
        rl.telegram_username,
        rl.assigned_count,
        COUNT(DISTINCT utr.hash || '_' || utr.duel_strapi_document_id) as actual_duels
    FROM tmp_reviewer_load rl
    LEFT JOIN user_duel_to_review utr ON utr.reviewer_user_strapi_document_id = rl.user_id
    LEFT JOIN duels d ON d.strapi_document_id = utr.duel_strapi_document_id 
        AND d.sprint_strapi_document_id = p_sprint_strapi_document_id
    GROUP BY rl.telegram_username, rl.assigned_count
    ORDER BY rl.assigned_count DESC, rl.telegram_username;

    -- 7) Формируем итоговый отчет
    RETURN json_build_object(
        'execution_id', v_execution_id,
        'status', 'SUCCESS',
        'message', format(
            'Спринт %s: сбалансированное распределение. Удалено %s записей, добавлено %s.',
            p_sprint_strapi_document_id, v_cleaned_rows, v_inserted_rows
        ),
        'stats', json_build_object(
            'total_duels', v_total_duels,
            'processed_duels', v_processed_duels,
            'cleaned_rows', v_cleaned_rows,
            'inserted_rows', v_inserted_rows,
            'reviewers_count', (SELECT COUNT(*) FROM tmp_reviewer_load),
            'distribution', (
                SELECT json_agg(json_build_object(
                    'reviewer', telegram_username,
                    'duels_assigned', actual_duels
                ) ORDER BY actual_duels DESC, telegram_username)
                FROM tmp_distribution_stats
            ),
            'load_variance', (
                SELECT ROUND(STDDEV(actual_duels)::numeric, 2)
                FROM tmp_distribution_stats
            ),
            'min_load', (SELECT MIN(actual_duels) FROM tmp_distribution_stats),
            'max_load', (SELECT MAX(actual_duels) FROM tmp_distribution_stats)
        )
    );

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Ошибка выполнения: %s', SQLERRM),
            'details', json_build_object(
                'error_detail', SQLERRM,
                'error_state', SQLSTATE,
                'error_hint', SQLERRM
            )
        );
END;
