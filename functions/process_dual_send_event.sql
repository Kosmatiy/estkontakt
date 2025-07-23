DECLARE
    user_record RECORD;
    duel_distribution RECORD;
    strike_comment TEXT;
    strike_exists BOOLEAN;
    strikes_assigned INTEGER := 0;
    A INTEGER := 0; -- Переменная для логирования дуэлей
    found_duels TEXT;
    duel_count INTEGER := 0;
BEGIN
    -- Логирование начала обработки события типа 'dual_send'
    PERFORM log_message(format('process_dual_send_event: START for sprint=%s, event=%s', p_sprint_id, p_event_id));

    -- Если A = 0, логируем найденные дуэли
    IF A = 0 THEN
        SELECT string_agg(d.duel_number::text, ', '), COUNT(d.duel_number)
        INTO found_duels, duel_count
        FROM duels d
        WHERE d.sprint_strapi_document_id = p_sprint_id;

        PERFORM log_message(format('process_dual_send_event: Found %s duels for sprint %s. Duel Numbers: %s', 
                                   COALESCE(duel_count, 0), p_sprint_id, COALESCE(found_duels, 'None')));
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
        -- Поиск дуэлей, которые должны были быть сыграны и не были проиграны
        FOR duel_distribution IN
            SELECT dd.*, d.duel_number  -- Добавляем duel_number из таблицы duels
            FROM duel_distributions dd
            JOIN duels d ON dd.duel_strapi_document_id = d.strapi_document_id  -- Соединение с таблицей duels
            WHERE dd.duel_strapi_document_id IN (
                SELECT d.strapi_document_id
                FROM duels d
                WHERE d.sprint_strapi_document_id = p_sprint_id
            )
              AND dd.user_strapi_document_id = user_record.strapi_document_id
              AND dd.is_failed = FALSE
        LOOP
            -- Проверка, была ли сыграна дуэль
            IF NOT EXISTS (
                SELECT 1
                FROM user_duel_answers uda
                WHERE uda.user_strapi_document_id = user_record.strapi_document_id
                  AND uda.duel_strapi_document_id = duel_distribution.duel_strapi_document_id
            ) THEN
                -- Формирование комментария
                strike_comment := 'Дуэль ' || duel_distribution.duel_number || ' не была сыграна.';
                
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
                    VALUES ('DUEL_SEND', strike_comment, user_record.strapi_document_id, p_sprint_id);
                    
                    strikes_assigned := strikes_assigned + 1;
                    
                    -- Логирование назначения страйка
                    PERFORM log_message(format('process_dual_send_event: Страйк назначен пользователю %s %s за спринт %s. Комментарий: %s', 
                                               user_record.name, user_record.surname, p_sprint_id, strike_comment));
                END IF;
            END IF;
        END LOOP;
    END LOOP;
    
    -- Логирование результатов обработки
    PERFORM log_message(format('process_dual_send_event: Completed for sprint=%s, event=%s. Strikes assigned: %s', 
                              p_sprint_id, p_event_id, strikes_assigned));
    
    -- Обновление статуса события на 'COMPLETED'
    UPDATE events
    SET event_status = 'COMPLETED',
        updated_at = NOW()
    WHERE strapi_document_id = p_event_id;
    
    RAISE NOTICE 'Обработка события типа dual_send завершена для спринта %s', p_sprint_id;
END;
