DECLARE
    user_record RECORD;
    lecture_record RECORD;
    task_record RECORD;
    test_record RECORD;
    missing_tasks TEXT := '';
    missing_tests TEXT := '';
    strike_comment TEXT;
    strike_exists BOOLEAN;
    strikes_assigned INTEGER := 0;
    A INTEGER := 0; -- Переменная для логирования заданий
    B INTEGER := 0; -- Переменная для логирования тестов
    found_tasks TEXT;
    found_tests TEXT;
    task_count INTEGER := 0;
    test_count INTEGER := 0;
BEGIN
    -- Логирование начала обработки события типа 'ttt'
    PERFORM log_message(format('process_ttt_event: START for sprint=%s, event=%s', p_sprint_id, p_event_id));

    -- Если A = 0, логируем найденные задания
    IF A = 0 THEN
        SELECT string_agg(t.task_number::text, ', '), COUNT(t.task_number)
        INTO found_tasks, task_count
        FROM tasks t
        JOIN lectures l ON t.lecture_strapi_document_id = l.strapi_document_id
        WHERE l.sprint_strapi_document_id = p_sprint_id;

        PERFORM log_message(format('process_ttt_event: Found %s tasks for sprint %s. Task Numbers: %s', 
                                   COALESCE(task_count, 0), p_sprint_id, COALESCE(found_tasks, 'None')));
        A := 1;
    END IF;

    -- Если B = 0, логируем найденные тесты
    IF B = 0 THEN
        SELECT string_agg(tst.test_number::text, ', '), COUNT(tst.test_number)
        INTO found_tests, test_count
        FROM tests tst
        JOIN lectures l ON tst.lecture_strapi_document_id = l.strapi_document_id
        WHERE l.sprint_strapi_document_id = p_sprint_id;

        PERFORM log_message(format('process_ttt_event: Found %s tests for sprint %s. Test Numbers: %s', 
                                   COALESCE(test_count, 0), p_sprint_id, COALESCE(found_tests, 'None')));
        B := 1;
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
        -- Получение всех лекций для спринта
        FOR lecture_record IN
            SELECT l.*
            FROM lectures l
            WHERE l.sprint_strapi_document_id = p_sprint_id
        LOOP
            -- Проверка заданий
            FOR task_record IN
                SELECT t.*
                FROM tasks t
                WHERE t.lecture_strapi_document_id = lecture_record.strapi_document_id
            LOOP
                IF NOT EXISTS (
                    SELECT 1
                    FROM user_task_answers uta
                    WHERE uta.user_strapi_document_id = user_record.strapi_document_id
                      AND uta.task_strapi_document_id = task_record.strapi_document_id
                ) THEN
                    missing_tasks := missing_tasks || 'Задание ' || task_record.task_number || ', ';
                END IF;
            END LOOP;
            
            -- Проверка тестов
            FOR test_record IN
                SELECT tst.*
                FROM tests tst
                WHERE tst.lecture_strapi_document_id = lecture_record.strapi_document_id
            LOOP
                IF NOT EXISTS (
                    SELECT 1
                    FROM user_test_answers uta
                    WHERE uta.user_strapi_document_id = user_record.strapi_document_id
                      AND uta.test_strapi_document_id = test_record.strapi_document_id
                ) THEN
                    missing_tests := missing_tests || 'Тест ' || test_record.test_number || ', ';
                END IF;
            END LOOP;
        END LOOP;
        
        -- Если есть пропущенные задания или тесты, назначаем страйк
        IF missing_tasks <> '' OR missing_tests <> '' THEN
            -- Удаление последней запятой и пробела
            missing_tasks := TRIM(TRAILING ', ' FROM missing_tasks);
            missing_tests := TRIM(TRAILING ', ' FROM missing_tests);
            
            -- Формирование комментария
            strike_comment := '';
            IF missing_tasks <> '' THEN
                IF LENGTH(missing_tasks) - LENGTH(REPLACE(missing_tasks, ',', '')) > 0 THEN
                    strike_comment := strike_comment || 'Не были выполнены следующие задания: ' || missing_tasks || '. ';
                ELSE
                    strike_comment := strike_comment || 'Не было выполнено задание: ' || missing_tasks || '. ';
                END IF;
            END IF;
            IF missing_tests <> '' THEN
                IF LENGTH(missing_tests) - LENGTH(REPLACE(missing_tests, ',', '')) > 0 THEN
                    strike_comment := strike_comment || 'Не были выполнены следующие тесты: ' || missing_tests || '.';
                ELSE
                    strike_comment := strike_comment || 'Не был выполнен тест: ' || missing_tests || '.';
                END IF;
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
                VALUES ('TTT', strike_comment, user_record.strapi_document_id, p_sprint_id);
                
                strikes_assigned := strikes_assigned + 1;
                
                -- Логирование назначения страйка
                PERFORM log_message(format('process_ttt_event: Страйк назначен пользователю %s %s за спринт %s. Комментарий: %s', 
                                           user_record.name, user_record.surname, p_sprint_id, strike_comment));
            END IF;
        END IF;
        
        -- Сброс переменных для следующего пользователя
        missing_tasks := '';
        missing_tests := '';
    END LOOP;
    
    -- Логирование результатов обработки
    PERFORM log_message(format('process_ttt_event: Completed for sprint=%s, event=%s. Strikes assigned: %s', 
                              p_sprint_id, p_event_id, strikes_assigned));
    
    -- Обновление статуса события на 'COMPLETED'
    UPDATE events
    SET event_status = 'COMPLETED',
        updated_at = NOW()
    WHERE strapi_document_id = p_event_id;
    
    RAISE NOTICE 'Обработка события типа ttt завершена для спринта %s', p_sprint_id;
END;
