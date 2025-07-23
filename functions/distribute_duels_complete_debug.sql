DECLARE
    v_sprint           sprints%ROWTYPE;
    v_execution_id     TEXT := 'debug_' || extract(epoch from now())::bigint || '_' || random()::text;
    v_cleaned_rows     INT := 0;
    v_inserted_rows    INT := 0;
    v_temp_rows        INT := 0;
    
    rec_reviewer       RECORD;
    rec_duel           RECORD;
    v_assignments_made INT;
    v_total_quota      INT;
    v_fulfilled_quota  INT;
    
    -- Для анализа
    v_total_pairs      INT;
    v_total_reviewers  INT;
    v_avg_quota        NUMERIC;
    
    -- Для отчета
    v_messages         TEXT[] := '{}';
    v_warnings         TEXT[] := '{}';
    v_errors           TEXT[] := '{}';
    v_step             INT := 0;
BEGIN
    -- Очистка старых данных диагностики (старше 7 дней)
    DELETE FROM debug_duel_distribution_log WHERE created_at < NOW() - INTERVAL '7 days';
    DELETE FROM debug_duel_quotas WHERE created_at < NOW() - INTERVAL '7 days';
    DELETE FROM debug_duels_to_review WHERE created_at < NOW() - INTERVAL '7 days';
    
    v_step := v_step + 1;
    PERFORM log_debug_step(v_execution_id, v_step, 'START', 
        format('Начало распределения для спринта %s, режим %s', p_sprint_strapi_document_id, p_mode));

    -- 1) Проверяем спринт
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

    -- 2) Очистка при CLEANSLATE
    IF p_mode = 'CLEANSLATE' THEN
        v_step := v_step + 1;
        DELETE FROM user_duel_to_review AS utdr
        USING duels AS d
        WHERE utdr.duel_strapi_document_id = d.strapi_document_id
          AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
        GET DIAGNOSTICS v_cleaned_rows = ROW_COUNT;
        
        PERFORM log_debug_step(v_execution_id, v_step, 'CLEANUP', 
            format('Очищено %s старых назначений', v_cleaned_rows));
    END IF;

    -- 3) Анализируем исходные данные
    v_step := v_step + 1;
    -- Сколько всего уникальных пар играло
    SELECT COUNT(DISTINCT uda.hash || '_' || uda.duel_strapi_document_id)
    INTO v_total_pairs
    FROM user_duel_answers uda
    JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
      AND uda.hash IS NOT NULL AND uda.hash <> '';
    
    PERFORM log_debug_step(v_execution_id, v_step, 'ANALYSIS', 
        format('Всего сыграно пар: %s', v_total_pairs));

    -- 4) Создаем и заполняем таблицу квот
    v_step := v_step + 1;
    DROP TABLE IF EXISTS tmp_user_duel_quotas;
    CREATE TEMP TABLE tmp_user_duel_quotas (
        user_id TEXT,
        telegram_username TEXT,
        duel_id TEXT,
        duel_number TEXT,
        played_count INT DEFAULT 0,
        review_quota INT DEFAULT 0,
        assigned_reviews INT DEFAULT 0,
        remaining_quota INT DEFAULT 0,
        PRIMARY KEY (user_id, duel_id)
    ) ON COMMIT DROP;

    -- Заполняем квоты
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
    
    -- Сохраняем в постоянную таблицу для анализа
    INSERT INTO debug_duel_quotas (execution_id, user_id, telegram_username, duel_id, duel_number, 
                                   played_count, review_quota, assigned_reviews, remaining_quota)
    SELECT v_execution_id, user_id, telegram_username, duel_id, duel_number, 
           played_count, review_quota, assigned_reviews, remaining_quota
    FROM tmp_user_duel_quotas;
    
    -- Анализ квот
    SELECT COUNT(DISTINCT user_id), AVG(review_quota)
    INTO v_total_reviewers, v_avg_quota
    FROM tmp_user_duel_quotas
    WHERE review_quota > 0;
    
    PERFORM log_debug_step(v_execution_id, v_step, 'QUOTAS_ANALYSIS', 
        format('Создано квот: %s рецензентов, средняя квота: %s', v_total_reviewers, ROUND(v_avg_quota, 2)));

    -- 5) Создаем таблицу дуэлей для распределения
    v_step := v_step + 1;
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
    
    -- Сохраняем в постоянную таблицу
    INSERT INTO debug_duels_to_review (execution_id, duel_id, duel_number, duel_type, hash, 
                                      participant1, participant2, reviewers_needed, reviewers_assigned)
    SELECT v_execution_id, duel_id, duel_number, duel_type, hash, 
           participant1, participant2, reviewers_needed, reviewers_assigned
    FROM tmp_duels_to_review;

    -- 6) Анализ до распределения
    v_step := v_step + 1;
    WITH duel_analysis AS (
        SELECT 
            dtr.duel_number,
            COUNT(DISTINCT dtr.hash) as available_pairs,
            COUNT(DISTINCT udq.user_id) as potential_reviewers,
            SUM(udq.review_quota) as total_reviews_needed,
            COUNT(DISTINCT dtr.hash) * 6 as max_possible_reviews
        FROM tmp_duels_to_review dtr
        LEFT JOIN tmp_user_duel_quotas udq ON udq.duel_id = dtr.duel_id
        GROUP BY dtr.duel_number
    )
    SELECT COUNT(*)
    INTO v_temp_rows
    FROM duel_analysis
    WHERE total_reviews_needed > max_possible_reviews;
    
    IF v_temp_rows > 0 THEN
        PERFORM log_debug_step(v_execution_id, v_step, 'WARNING', 
            format('ВНИМАНИЕ: %s дуэлей имеют недостаточно пар для выполнения всех квот', v_temp_rows));
    END IF;

    -- 7) Основной цикл распределения
    v_step := v_step + 1;
    FOR rec_reviewer IN
        SELECT * FROM tmp_user_duel_quotas
        WHERE remaining_quota > 0
        ORDER BY remaining_quota DESC, duel_id, user_id
    LOOP
        v_assignments_made := 0;
        
        FOR rec_duel IN
            SELECT dtr.*
            FROM tmp_duels_to_review dtr
            WHERE dtr.duel_id = rec_reviewer.duel_id
              AND dtr.reviewers_needed > 0
              AND dtr.participant1 <> rec_reviewer.user_id
              AND dtr.participant2 <> rec_reviewer.user_id
              AND NOT EXISTS (
                  SELECT 1 FROM user_duel_to_review utr
                  WHERE utr.reviewer_user_strapi_document_id = rec_reviewer.user_id
                    AND utr.duel_strapi_document_id = dtr.duel_id
                    AND utr.hash = dtr.hash
              )
            ORDER BY dtr.reviewers_needed DESC, dtr.hash
            LIMIT rec_reviewer.remaining_quota
        LOOP
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
                
                GET DIAGNOSTICS v_temp_rows = ROW_COUNT;
                v_inserted_rows := v_inserted_rows + v_temp_rows;
                
                IF v_temp_rows > 0 THEN
                    UPDATE tmp_duels_to_review
                    SET reviewers_assigned = reviewers_assigned + 1,
                        reviewers_needed = reviewers_needed - 1
                    WHERE duel_id = rec_duel.duel_id AND hash = rec_duel.hash;
                    
                    v_assignments_made := v_assignments_made + 1;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                v_errors := array_append(v_errors, SQLERRM);
            END;
        END LOOP;
        
        UPDATE tmp_user_duel_quotas
        SET assigned_reviews = assigned_reviews + v_assignments_made,
            remaining_quota = remaining_quota - v_assignments_made
        WHERE user_id = rec_reviewer.user_id
          AND duel_id = rec_reviewer.duel_id;
    END LOOP;

    -- 8) Обновляем постоянные таблицы после распределения
    UPDATE debug_duel_quotas dq
    SET assigned_reviews = udq.assigned_reviews,
        remaining_quota = udq.remaining_quota
    FROM tmp_user_duel_quotas udq
    WHERE dq.execution_id = v_execution_id
      AND dq.user_id = udq.user_id
      AND dq.duel_id = udq.duel_id;
    
    UPDATE debug_duels_to_review ddr
    SET reviewers_assigned = dtr.reviewers_assigned,
        reviewers_needed = dtr.reviewers_needed
    FROM tmp_duels_to_review dtr
    WHERE ddr.execution_id = v_execution_id
      AND ddr.duel_id = dtr.duel_id
      AND ddr.hash = dtr.hash;

    -- 9) Финальная статистика
    SELECT SUM(review_quota), SUM(review_quota - remaining_quota)
    INTO v_total_quota, v_fulfilled_quota
    FROM tmp_user_duel_quotas;

    -- 10) Анализ проблем
    -- Проверяем дуэли с недостаточным количеством рецензентов
    FOR rec_duel IN
        SELECT * FROM tmp_duels_to_review
        WHERE reviewers_assigned < 6
        ORDER BY duel_number
    LOOP
        v_warnings := array_append(v_warnings,
            format('Дуэль %s (hash %s): только %s из 6 рецензентов', 
                   rec_duel.duel_number, rec_duel.hash, rec_duel.reviewers_assigned));
    END LOOP;

    -- Проверяем пользователей с невыполненной квотой
    FOR rec_reviewer IN
        SELECT * FROM tmp_user_duel_quotas
        WHERE remaining_quota > 0
        ORDER BY remaining_quota DESC
        LIMIT 10
    LOOP
        v_warnings := array_append(v_warnings,
            format('Пользователь %s: не выполнена квота для дуэли %s (осталось %s из %s)',
                   rec_reviewer.telegram_username, rec_reviewer.duel_number, 
                   rec_reviewer.remaining_quota, rec_reviewer.review_quota));
    END LOOP;

    PERFORM log_debug_step(v_execution_id, 9999, 'COMPLETE', 
        format('Распределение завершено. Вставлено %s строк, выполнено %s из %s квоты', 
               v_inserted_rows, v_fulfilled_quota, v_total_quota));

    -- Возвращаем результат
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
            'total_pairs', v_total_pairs,
            'total_reviewers', v_total_reviewers,
            'avg_quota_per_reviewer', ROUND(v_avg_quota, 2)
        ),
        'warnings', v_warnings,
        'errors', v_errors,
        'analysis_queries', json_build_array(
            format('-- Анализ квот по дуэлям:
SELECT duel_number, COUNT(*) as reviewers, SUM(played_count) as total_plays, 
       SUM(review_quota) as total_quota, SUM(remaining_quota) as unfulfilled
FROM debug_duel_quotas WHERE execution_id = ''%s''
GROUP BY duel_number ORDER BY duel_number', v_execution_id),
            
            format('-- Пользователи с проблемами:
SELECT telegram_username, duel_number, played_count, review_quota, 
       assigned_reviews, remaining_quota
FROM debug_duel_quotas WHERE execution_id = ''%s'' AND remaining_quota > 0
ORDER BY remaining_quota DESC', v_execution_id),
            
            format('-- Дуэли с недостатком рецензентов:
SELECT duel_number, hash, participant1, participant2, reviewers_assigned
FROM debug_duels_to_review WHERE execution_id = ''%s'' AND reviewers_assigned < 6
ORDER BY duel_number, hash', v_execution_id)
        )
    );

EXCEPTION
    WHEN OTHERS THEN
        PERFORM log_debug_step(v_execution_id, 99999, 'CRITICAL_ERROR', SQLERRM);
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Критическая ошибка: %s', SQLERRM),
            'error_detail', SQLERRM
        );
END;
