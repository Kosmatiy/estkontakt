DECLARE
    user_record RECORD;
    task_record RECORD;
    missing_reviews TEXT := '';
    strike_comment TEXT;
    strike_exists BOOLEAN;
    strikes_assigned INTEGER := 0;
    A INTEGER := 0; -- Переменная для логирования заданий
    found_tasks TEXT;
    task_count INTEGER := 0;
BEGIN
    -- Логирование начала обработки события типа 'task_review'
    PERFORM log_message(format('process_task_review_event: START for sprint=%s, event=%s', p_sprint_id, p_event_id));

    -- Если A = 0, логируем найденные задания
    IF A = 0 THEN
        SELECT string_agg(t.task_number::text, ', '), COUNT(t.task_number)
        INTO found_tasks, task_count
        FROM tasks t
        JOIN lectures l ON t.lecture_strapi_document_id = l.strapi_document_id
        WHERE l.sprint_strapi_document_id = p_sprint_id;

        PERFORM log_message(format('process_task_review_event: Found %s tasks for sprint %s. Task Numbers: %s', 
                                   COALESCE(task_count, 0), p_sprint_id, COALESCE(found_tasks, 'None')));
        A := 1;
    END IF;

    -- Получение всех пользователей для данного стрима и спринта, которые не dismissed и не имеют страйков
    FOR user_record IN
        SELECT u.*
        FROM users u
        WHERE u.stream_strapi_document_id IN (
            SELECT s.stream_strapi_document_id
            FROM sprints s
            WHERE s.strapi_document_id = p_sprint_id
        )
          AND u.dismissed_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM strikes st
              WHERE st.user_strapi_document_id = u.strapi_document_id
                AND st.sprint_strapi_document_id = p_sprint_id
          )
    LOOP
        -- Получение всех заданий, которые должны быть проверены
        FOR task_record IN
            SELECT t.*
            FROM tasks t
            WHERE t.lecture_strapi_document_id IN (
                SELECT l.strapi_document_id
                FROM lectures l
                WHERE l.sprint_strapi_document_id = p_sprint_id
            )
        LOOP
            -- Проверка, было ли задание проверено
            IF NOT EXISTS (
                SELECT 1
                FROM user_task_reviewed utr
                WHERE utr.reviewee_user_strapi_document_id = user_record.strapi_document_id
                  AND utr.task_strapi_document_id = task_record.strapi_document_id
            ) THEN
                missing_reviews := missing_reviews || 'Задание ' || task_record.task_number || ', ';
            END IF;
        END LOOP;
        
        -- Если есть непроверенные задания, назначаем страйк
        IF missing_reviews <> '' THEN
            -- Удаление последней запятой и пробела
            missing_reviews := TRIM(TRAILING ', ' FROM missing_reviews);
            
            -- Формирование комментария
            strike_comment := '';
            IF LENGTH(missing_reviews) - LENGTH(REPLACE(missing_reviews, ',', '')) > 0 THEN
                strike_comment := 'Не были проверены следующие задания: ' || missing_reviews || '.';
            ELSE
                strike_comment := 'Не было проверено задание: ' || missing_reviews || '.';
            END IF;
            
            -- Проверка, есть ли уже страйк для пользователя и спринта
            SELECT EXISTS (
                SELECT 1
                FROM strikes st
                WHERE st.user_strapi_document_id = user_record.strapi_document_id
                  AND st.sprint_strapi_document_id = p_sprint_id
            ) INTO strike_exists;
            
            IF NOT strike_exists THEN
                -- Вставка нового страйка
                INSERT INTO strikes (type, comment, user_strapi_document_id, sprint_strapi_document_id)
                VALUES ('TASK_REVIEW', strike_comment, user_record.strapi_document_id, p_sprint_id);
                
                strikes_assigned := strikes_assigned + 1;
                
                -- Логирование назначения страйка
                PERFORM log_message(format('process_task_review_event: Страйк назначен пользователю %s %s за спринт %s. Комментарий: %s', 
                                           user_record.name, user_record.surname, p_sprint_id, strike_comment));
            END IF;
        END IF;
        
        -- Сброс переменных для следующего пользователя
        missing_reviews := '';
    END LOOP;
    
    -- Логирование результатов обработки
    PERFORM log_message(format('process_task_review_event: Completed for sprint=%s, event=%s. Strikes assigned: %s', 
                              p_sprint_id, p_event_id, strikes_assigned));
    
    -- Обновление статуса события на 'COMPLETED'
    UPDATE events
    SET event_status = 'COMPLETED',
        updated_at = NOW()
    WHERE strapi_document_id = p_event_id;
    
    RAISE NOTICE 'Обработка события типа task_review завершена для спринта %s', p_sprint_id;
END;
