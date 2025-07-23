DECLARE
    v_sprint           sprints%ROWTYPE;
    v_execution_id     TEXT := 'exec_' || extract(epoch from now())::bigint || '_' || random()::text;
    v_cleaned_rows     INT := 0;
    v_inserted_rows    INT := 0;
    v_skipped_rows     INT := 0;
    
    rec_reviewer       RECORD;
    rec_duel           RECORD;
    v_assignments_made INT;
    v_total_quota      INT;
    v_fulfilled_quota  INT;
    
    -- Для отчета
    v_messages         TEXT[] := '{}';
    v_warnings         TEXT[] := '{}';
    v_errors           TEXT[] := '{}';
BEGIN
    -- 1) Проверяем режим
    IF p_mode NOT IN ('CLEANSLATE', 'APPEND') THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', 'Режим должен быть CLEANSLATE или APPEND'
        );
    END IF;

    -- 2) Проверяем спринт
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

    v_messages := array_append(v_messages, 
        format('Начало распределения для спринта "%s" (ID: %s)', 
               v_sprint.sprint_name, p_sprint_strapi_document_id));

    -- 3) Очистка при CLEANSLATE
    IF p_mode = 'CLEANSLATE' THEN
        DELETE FROM user_duel_to_review AS utdr
        USING duels AS d
        WHERE utdr.duel_strapi_document_id = d.strapi_document_id
          AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
        GET DIAGNOSTICS v_cleaned_rows = ROW_COUNT;
        
        v_messages := array_append(v_messages, 
            format('Очищено %s старых назначений', v_cleaned_rows));
    END IF;

    -- 4) Создаем таблицу с детальными квотами по каждому типу дуэли
    DROP TABLE IF EXISTS tmp_user_duel_quotas;
    CREATE TEMP TABLE tmp_user_duel_quotas (
        user_id TEXT,
        telegram_username TEXT,
        duel_id TEXT,                    -- strapi_document_id дуэли
        duel_number TEXT,
        played_count INT DEFAULT 0,      -- Сколько раз сыграл эту дуэль
        review_quota INT DEFAULT 0,      -- Сколько должен проверить (played_count * 3)
        assigned_reviews INT DEFAULT 0,  -- Сколько уже назначено
        remaining_quota INT DEFAULT 0,   -- Сколько осталось назначить
        PRIMARY KEY (user_id, duel_id)
    ) ON COMMIT DROP;

    -- Заполняем квоты по каждой дуэли для каждого пользователя
    INSERT INTO tmp_user_duel_quotas (user_id, telegram_username, duel_id, duel_number, played_count, review_quota, remaining_quota)
    SELECT 
        u.strapi_document_id,
        u.telegram_username,
        d.strapi_document_id,
        d.duel_number,
        COUNT(DISTINCT uda.hash) as played_count,
        COUNT(DISTINCT uda.hash) * 3 as review_quota,
        COUNT(DISTINCT uda.hash) * 3 as remaining_quota
    FROM users u
    JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
    JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE u.stream_strapi_document_id = v_sprint.stream_strapi_document_id
      AND u.dismissed_at IS NULL
      AND d.sprint_strapi_document_id = p_sprint_strapi_document_id
      AND uda.hash IS NOT NULL
      AND uda.hash <> ''
    GROUP BY u.strapi_document_id, u.telegram_username, d.strapi_document_id, d.duel_number;

    -- При режиме APPEND учитываем уже назначенные проверки по каждой дуэли
    IF p_mode = 'APPEND' THEN
        UPDATE tmp_user_duel_quotas udq
        SET assigned_reviews = sub.assigned_count,
            remaining_quota = GREATEST(0, review_quota - sub.assigned_count)
        FROM (
            SELECT 
                utr.reviewer_user_strapi_document_id,
                utr.duel_strapi_document_id,
                COUNT(DISTINCT utr.hash) as assigned_count
            FROM user_duel_to_review utr
            GROUP BY utr.reviewer_user_strapi_document_id, utr.duel_strapi_document_id
        ) sub
        WHERE udq.user_id = sub.reviewer_user_strapi_document_id
          AND udq.duel_id = sub.duel_strapi_document_id;
    END IF;

    -- Создаем сводную таблицу по пользователям для отчета
    DROP TABLE IF EXISTS tmp_user_quotas;
    CREATE TEMP TABLE tmp_user_quotas AS
    SELECT 
        user_id,
        telegram_username,
        SUM(played_count) as played_duels,
        SUM(review_quota) as review_quota,
        SUM(assigned_reviews) as assigned_reviews,
        SUM(remaining_quota) as remaining_quota
    FROM tmp_user_duel_quotas
    GROUP BY user_id, telegram_username;

    v_messages := array_append(v_messages, 
        format('Обработано %s пользователей, из них %s играли дуэли', 
               (SELECT COUNT(*) FROM tmp_user_quotas),
               (SELECT COUNT(*) FROM tmp_user_quotas WHERE played_duels > 0)));

    -- 5) Создаем таблицу дуэлей для распределения
    DROP TABLE IF EXISTS tmp_duels_to_review;
    CREATE TEMP TABLE tmp_duels_to_review (
        duel_id TEXT,
        duel_number TEXT,
        duel_type TEXT,
        hash TEXT,
        participant1 TEXT,
        participant2 TEXT,
        reviewers_needed INT DEFAULT 6,
        reviewers_assigned INT DEFAULT 0,
        priority INT DEFAULT 0
    ) ON COMMIT DROP;

    -- Заполняем дуэли с участниками
    INSERT INTO tmp_duels_to_review (duel_id, duel_number, duel_type, hash, participant1, participant2)
    SELECT 
        d.strapi_document_id,
        d.duel_number,
        d.type,
        sub.hash,
        sub.participant1,
        sub.participant2
    FROM duels d
    JOIN (
        SELECT 
            duel_strapi_document_id,
            hash,
            MIN(user_strapi_document_id) as participant1,
            MAX(user_strapi_document_id) as participant2
        FROM user_duel_answers
        WHERE hash IS NOT NULL AND hash <> ''
        GROUP BY duel_strapi_document_id, hash
        HAVING COUNT(DISTINCT user_strapi_document_id) = 2
    ) sub ON sub.duel_strapi_document_id = d.strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id;

    -- При APPEND режиме учитываем уже назначенных рецензентов
    IF p_mode = 'APPEND' THEN
        UPDATE tmp_duels_to_review dtr
        SET reviewers_assigned = sub.reviewer_count,
            reviewers_needed = GREATEST(0, 6 - sub.reviewer_count)
        FROM (
            SELECT 
                duel_strapi_document_id,
                hash,
                COUNT(DISTINCT reviewer_user_strapi_document_id) as reviewer_count
            FROM user_duel_to_review
            GROUP BY duel_strapi_document_id, hash
        ) sub
        WHERE dtr.duel_id = sub.duel_strapi_document_id
          AND dtr.hash = sub.hash;
    END IF;

    -- Устанавливаем приоритет: боевые дуэли важнее тренировочных
    UPDATE tmp_duels_to_review
    SET priority = CASE 
        WHEN duel_type = 'FULL-CONTACT' THEN 2
        WHEN duel_type = 'TRAINING' THEN 1
        ELSE 0
    END;

    v_messages := array_append(v_messages, 
        format('Найдено %s дуэлей для распределения', 
               (SELECT COUNT(*) FROM tmp_duels_to_review)));

    -- 6) Основной цикл распределения
    -- Проходим по всем квотам пользователь-дуэль
    FOR rec_reviewer IN
        SELECT * FROM tmp_user_duel_quotas
        WHERE remaining_quota > 0
        ORDER BY 
            remaining_quota DESC,  -- Сначала те, кому больше нужно
            duel_id,              -- Группируем по дуэлям
            user_id
    LOOP
        v_assignments_made := 0;
        
        -- Для каждой квоты выбираем подходящие пары ТОЛЬКО ТОЙ ЖЕ ДУЭЛИ
        FOR rec_duel IN
            SELECT dtr.*
            FROM tmp_duels_to_review dtr
            WHERE dtr.duel_id = rec_reviewer.duel_id  -- ВАЖНО: только та же дуэль!
              AND dtr.reviewers_needed > 0
              -- Рецензент не должен быть участником
              AND dtr.participant1 <> rec_reviewer.user_id
              AND dtr.participant2 <> rec_reviewer.user_id
              -- Рецензент еще не назначен на эту пару
              AND NOT EXISTS (
                  SELECT 1 FROM user_duel_to_review utr
                  WHERE utr.reviewer_user_strapi_document_id = rec_reviewer.user_id
                    AND utr.duel_strapi_document_id = dtr.duel_id
                    AND utr.hash = dtr.hash
              )
            ORDER BY 
                dtr.reviewers_needed DESC,   -- Сначала те, где мало рецензентов
                dtr.hash                     -- Для стабильности
            LIMIT rec_reviewer.remaining_quota
        LOOP
            -- Вставляем назначения для обоих участников
            BEGIN
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
                
                GET DIAGNOSTICS v_skipped_rows = ROW_COUNT;
                v_inserted_rows := v_inserted_rows + v_skipped_rows;
                
                -- Обновляем счетчики
                UPDATE tmp_duels_to_review
                SET reviewers_assigned = reviewers_assigned + 1,
                    reviewers_needed = reviewers_needed - 1
                WHERE duel_id = rec_duel.duel_id AND hash = rec_duel.hash;
                
                v_assignments_made := v_assignments_made + 1;
                
            EXCEPTION WHEN OTHERS THEN
                v_warnings := array_append(v_warnings,
                    format('Не удалось назначить %s на дуэль %s (hash %s): %s',
                           rec_reviewer.telegram_username, rec_reviewer.duel_number, rec_duel.hash, SQLERRM));
            END;
        END LOOP;
        
        -- Обновляем оставшуюся квоту для конкретной дуэли
        UPDATE tmp_user_duel_quotas
        SET assigned_reviews = assigned_reviews + v_assignments_made,
            remaining_quota = remaining_quota - v_assignments_made
        WHERE user_id = rec_reviewer.user_id
          AND duel_id = rec_reviewer.duel_id;
    END LOOP;

    -- 7) Проверка результатов
    -- Проверяем дуэли с недостаточным количеством рецензентов
    FOR rec_duel IN
        SELECT * FROM tmp_duels_to_review
        WHERE reviewers_assigned < 6
        ORDER BY duel_number
    LOOP
        v_warnings := array_append(v_warnings,
            format('Дуэль %s: назначено только %s рецензентов из 6',
                   rec_duel.duel_number, rec_duel.reviewers_assigned));
    END LOOP;

    -- Проверяем пользователей с невыполненной квотой по дуэлям
    FOR rec_reviewer IN
        SELECT * FROM tmp_user_duel_quotas
        WHERE remaining_quota > 0
        ORDER BY telegram_username, duel_number
    LOOP
        v_warnings := array_append(v_warnings,
            format('Пользователь %s: не хватило пар для дуэли %s (осталось %s из %s)',
                   rec_reviewer.telegram_username, rec_reviewer.duel_number, 
                   rec_reviewer.remaining_quota, rec_reviewer.review_quota));
    END LOOP;

    -- 8) Формируем финальную статистику
    SELECT SUM(review_quota), SUM(review_quota - remaining_quota)
    INTO v_total_quota, v_fulfilled_quota
    FROM tmp_user_quotas;

    -- 9) Возвращаем результат
    RETURN json_build_object(
        'execution_id', v_execution_id,
        'status', CASE 
            WHEN array_length(v_errors, 1) > 0 THEN 'ERROR'
            WHEN array_length(v_warnings, 1) > 0 THEN 'WARNING'
            ELSE 'SUCCESS'
        END,
        'message', format('Распределение завершено. Режим: %s', p_mode),
        'stats', json_build_object(
            'cleaned_rows', v_cleaned_rows,
            'inserted_rows', v_inserted_rows,
            'total_quota', v_total_quota,
            'fulfilled_quota', v_fulfilled_quota,
            'fulfillment_percent', ROUND((v_fulfilled_quota::numeric / NULLIF(v_total_quota, 0)) * 100, 2),
            'users_with_quota', (SELECT COUNT(*) FROM tmp_user_quotas WHERE review_quota > 0),
            'duels_processed', (SELECT COUNT(*) FROM tmp_duels_to_review),
            'duels_fully_assigned', (SELECT COUNT(*) FROM tmp_duels_to_review WHERE reviewers_assigned = 6)
        ),
        'messages', v_messages,
        'warnings', v_warnings,
        'errors', v_errors,
        'quota_details', (
            SELECT json_agg(json_build_object(
                'user', telegram_username,
                'played', played_duels,
                'quota', review_quota,
                'assigned', assigned_reviews,
                'remaining', remaining_quota
            ) ORDER BY remaining_quota DESC, telegram_username)
            FROM tmp_user_quotas
            WHERE review_quota > 0
        ),
        'quota_by_duel', (
            SELECT json_agg(json_build_object(
                'user', telegram_username,
                'duel', duel_number,
                'played_count', played_count,
                'quota', review_quota,
                'assigned', assigned_reviews,
                'remaining', remaining_quota
            ) ORDER BY user_id, duel_number)
            FROM tmp_user_duel_quotas
            WHERE review_quota > 0 AND remaining_quota > 0
        )
    );

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Критическая ошибка: %s', SQLERRM),
            'error_detail', SQLERRM,
            'error_hint', SQLERRM
        );
END;
