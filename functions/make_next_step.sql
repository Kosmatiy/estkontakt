DECLARE
    current_ts TIMESTAMP WITH TIME ZONE := NOW(); -- Текущее время с часовым поясом
    stream_record RECORD;
    active_sprint RECORD;
    active_event RECORD;
    event_age INTERVAL;
    stream_count INTEGER := 0;
BEGIN
    -- Логирование начала работы функции
    PERFORM log_message(format('make_next_step: START at %s', current_ts));
    
    -- Подсчёт количества стримов
    SELECT COUNT(*) INTO stream_count FROM streams;
    PERFORM log_message(format('make_next_step: Found %s streams', stream_count));
    
    -- 1. Проход по всем стримам
    FOR stream_record IN
        SELECT *
        FROM streams
    LOOP
        -- Логирование обработки текущего стрима
        PERFORM log_message(format('make_next_step: Processing stream "%s" (strapi_document_id=%s)', 
                                    stream_record.name, stream_record.strapi_document_id));
        
        -- 2. Нахождение активного спринта для текущего стрима
        SELECT *
        INTO active_sprint
        FROM sprints
        WHERE stream_strapi_document_id = stream_record.strapi_document_id
          AND date_start <= current_ts
          AND date_end >= current_ts
        ORDER BY date_start DESC
        LIMIT 1;
        
        -- Если активный спринт найден
        IF FOUND THEN
            PERFORM log_message(format('make_next_step: Found active sprint "%s" (strapi_document_id=%s) for stream "%s"', 
                                        active_sprint.sprint_name, active_sprint.strapi_document_id, stream_record.name));
            
            -- 3. Нахождение актуального события для спринта
            SELECT *
            INTO active_event
            FROM events
            WHERE sprint_strapi_document_id = active_sprint.strapi_document_id
              AND (event_status = 'AWAITING' OR event_status = 'FAILED')
              AND datetime_start <= current_ts
              AND datetime_end >= current_ts
            ORDER BY strapi_document_id DESC
            LIMIT 1;
            

            -- Если актуальное событие найдено
            IF FOUND THEN
                PERFORM log_message(format('make_next_step: Processing event "%s" (strapi_document_id=%s) for sprint "%s"', 
                                            active_event.name, active_event.strapi_document_id, active_sprint.sprint_name));
                event_age := current_ts - active_event.updated_at;
                IF event_age > INTERVAL '15 minutes' and active_event.event_status = 'IN PROGRESS' THEN
                    -- Обновление статуса события на 'FAILED'
                    UPDATE events
                    SET event_status = 'FAILED',
                        updated_at = current_ts
                    WHERE id = active_event.id;
                    PERFORM log_message(format('make_next_step: Event "%s" (strapi_document_id=%s) set to FAILED due to timeout.', 
                                              active_event.name, active_event.strapi_document_id));
                    PERFORM insert_admin_message(format('make_next_step: Event "%s" (strapi_document_id=%s) set to FAILED due to timeout.', 
                                          active_event.name, active_event.strapi_document_id));
                END IF;

                -- Проверка статуса события
                IF active_event.event_status = 'AWAITING' OR active_event.event_status = 'FAILED' THEN

                    UPDATE events
                    SET event_status = 'IN PROGRESS',
                        updated_at = current_ts
                    WHERE id = active_event.id;

                    -- SELECT pg_sleep(60);

                    PERFORM log_message(format('make_next_step: Event "%s" (strapi_document_id=%s) set to FAILED due to timeout.', 
                                              active_event.name, active_event.strapi_document_id));
                    PERFORM insert_admin_message(format('make_next_step: Event "%s" (strapi_document_id=%s) set to FAILED due to timeout.', 
                                          active_event.name, active_event.strapi_document_id));

                    -- Обработка события в зависимости от его типа
                    CASE active_event.event_type
                        WHEN 'TASK_REVIEW' THEN
                            -- Обработка события типа 'TASK_REVIEW'
                            PERFORM process_ttt_event(active_sprint.strapi_document_id, active_event.strapi_document_id);
                            PERFORM distribute_tasks_circular_with_limits(active_sprint.strapi_document_id);
                            PERFORM distribute_tasks_among_experts(stream_record.strapi_document_id);
                            
                        WHEN 'DUEL_SEND' THEN
                            -- Обработка события типа 'DUEL_SEND'
                            PERFORM process_task_review_event(active_sprint.strapi_document_id, active_event.strapi_document_id);
                            PERFORM distribute_all_sprint_matches(active_sprint.strapi_document_id);
                            
                        WHEN 'DUEL_REVIEW' THEN
                            -- Обработка события типа 'DUEL_REVIEW'
                            PERFORM process_dual_send_event(active_sprint.strapi_document_id, active_event.strapi_document_id);
                            PERFORM distribute_duel_reviews_for_sprint(active_sprint.strapi_document_id);
                            PERFORM distribute_duels_among_experts(stream_record.strapi_document_id);
                            

    
                        WHEN 'TTT' THEN
                            ------------------------------------------------------------------
                            -- 1. Посчитать предыдущий спринт для того же стрима
                            ------------------------------------------------------------------
                            DECLARE
                                prev_sprint_id TEXT;
                            BEGIN
                                SELECT strapi_document_id
                                INTO   prev_sprint_id
                                FROM   sprints
                                WHERE  stream_strapi_document_id = stream_record.strapi_document_id
                                AND  sprint_number             = active_sprint.sprint_number - 1
                                LIMIT  1;

                                ------------------------------------------------------------------
                                -- 2. Если это не первый спринт → выполняем DUEL_REVIEW для прошлого
                                ------------------------------------------------------------------
                                IF prev_sprint_id IS NOT NULL THEN
                                    PERFORM log_message(format(
                                        'make_next_step: TTT-trigger – запускаю process_dual_review_event для спринта %s',
                                        prev_sprint_id));

                                    PERFORM process_dual_review_event(prev_sprint_id,
                                                                    active_event.strapi_document_id);
                                END IF;

                                ------------------------------------------------------------------
                                -- 3. Само событие TTT помечаем как COMPLETED (делает called-функция)
                                ------------------------------------------------------------------
                                -- никаких отдельных UPDATE здесь больше не нужно
                            END;

                        ELSE
                            PERFORM log_message(format('make_next_step: Unknown event type "%s" for event "%s"', 
                                                      active_event.event_type, active_event.name));
                    END CASE;
                    
                    PERFORM insert_admin_message(format('make_next_step: I work with "%s", type:"%s" from sprint "%s", id:"%s".', 
                            active_event.name, active_event.event_type, active_sprint.sprint_name, active_sprint.strapi_document_id));
                    
                ELSE
                    PERFORM log_message(format('make_next_step: No awaiting or failed events found for sprint "%s"', active_sprint.sprint_name));
                    PERFORM insert_admin_message(format('make_next_step: No awaiting or failed events found for sprint "%s"', active_sprint.sprint_name));
                END IF;
            ELSE
                PERFORM log_message(format('make_next_step: No awaiting or failed events found for sprint "%s"', active_sprint.sprint_name));
                PERFORM insert_admin_message(format('make_next_step: No awaiting or failed events found for sprint "%s"', active_sprint.sprint_name));
            END IF;
        ELSE
            PERFORM log_message(format('make_next_step: No active sprint found for stream "%s"', stream_record.name));
            PERFORM insert_admin_message(format('make_next_step: No active sprint found for stream "%s"', stream_record.name));
        END IF;
    END LOOP;
    
    -- Логирование завершения работы функции
    PERFORM log_message(format('make_next_step: COMPLETED at %s', NOW()));
END;
