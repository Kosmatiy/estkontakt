DECLARE
    stream_id TEXT;
    user_rec RECORD;
    duel_rec RECORD;
    missing_duels TEXT := '';
    strike_comment TEXT;
    strike_exists BOOLEAN;
    strikes_assigned INTEGER := 0;
    duel_count INTEGER;
    user_count INTEGER;
    rival_id TEXT;
    rival_username TEXT;
    missing_count INTEGER;
BEGIN
    -- Логирование начала обработки
    PERFORM log_message(format('process_dual_review_event: START for sprint=%s, event=%s', p_sprint_id, p_event_id));
    RAISE NOTICE 'Starting process_dual_review_event for sprint=% and event=%', p_sprint_id, p_event_id;

    -- Получение stream_strapi_document_id для данного спринта
    SELECT stream_strapi_document_id INTO stream_id
    FROM sprints
    WHERE strapi_document_id = p_sprint_id;

    IF stream_id IS NULL THEN
        RAISE NOTICE 'Stream not found for sprint %', p_sprint_id;
        PERFORM log_message(format('process_dual_review_event: Stream not found for sprint %s', p_sprint_id));
        RETURN;
    END IF;

    -- Логирование найденного стрима
    RAISE NOTICE 'Found stream_id=% for sprint=%', stream_id, p_sprint_id;
    PERFORM log_message(format('process_dual_review_event: Found stream_id=%s for sprint=%s', stream_id, p_sprint_id));

    -- Подсчёт общего количества дуэлей в спринте
    SELECT COUNT(*) INTO duel_count
    FROM duels
    WHERE sprint_strapi_document_id = p_sprint_id;

    RAISE NOTICE 'Found % duels in sprint %', duel_count, p_sprint_id;
    PERFORM log_message(format('process_dual_review_event: Found %s duels in sprint %s', duel_count, p_sprint_id));

    -- Подсчёт общего количества пользователей в стриме
    SELECT COUNT(*) INTO user_count
    FROM users
    WHERE stream_strapi_document_id = stream_id;

    RAISE NOTICE 'Found % users in stream %', user_count, stream_id;
    PERFORM log_message(format('process_dual_review_event: Found %s users in stream %s', user_count, stream_id));

    -- Итерация по каждому пользователю в стриме
    FOR user_rec IN
        SELECT *
        FROM users
        WHERE stream_strapi_document_id = stream_id
    LOOP
        RAISE NOTICE 'Processing user: % % (strapi_document_id=%)', user_rec.name, user_rec.surname, user_rec.strapi_document_id;
        PERFORM log_message(format('process_dual_review_event: Processing user: %s %s (strapi_document_id=%s)', user_rec.name, user_rec.surname, user_rec.strapi_document_id));

        -- Инициализация списка незаполненных дуэлей для пользователя
        missing_duels := '';

        -- Итерация по дуэлям, которые пользователь должен проверить
        FOR duel_rec IN
            SELECT d.duel_number, d.strapi_document_id
            FROM duels d
            JOIN user_duel_to_review utdr ON d.strapi_document_id = utdr.duel_strapi_document_id
            WHERE d.sprint_strapi_document_id = p_sprint_id
              AND utdr.reviewer_user_strapi_document_id = user_rec.strapi_document_id
        LOOP
            RAISE NOTICE 'Processing duel: % (strapi_document_id=%)', duel_rec.duel_number, duel_rec.strapi_document_id;
            PERFORM log_message(format('process_dual_review_event: Processing duel: %s (strapi_document_id=%s)', duel_rec.duel_number, duel_rec.strapi_document_id));

            -- Проверка, была ли уже выполнена проверка дуэли
            IF NOT EXISTS (
                SELECT 1
                FROM user_duel_reviewed udr
                WHERE udr.reviewer_user_strapi_document_id = user_rec.strapi_document_id
                  AND udr.duel_strapi_document_id = duel_rec.strapi_document_id
                  AND udr.hash = (
                      SELECT utdr_inner.hash
                      FROM user_duel_to_review utdr_inner
                      WHERE utdr_inner.reviewer_user_strapi_document_id = user_rec.strapi_document_id
                        AND utdr_inner.duel_strapi_document_id = duel_rec.strapi_document_id
                      LIMIT 1
                  )
            ) THEN
                -- Получение rival_strapi_document_id из duel_distributions
                SELECT rival_strapi_document_id INTO rival_id
                FROM duel_distributions
                WHERE duel_strapi_document_id = duel_rec.strapi_document_id
                  AND user_strapi_document_id = user_rec.strapi_document_id
                LIMIT 1;

                -- Получение telegram_username соперника
                IF rival_id IS NOT NULL THEN
                    SELECT telegram_username INTO rival_username
                    FROM users
                    WHERE strapi_document_id = rival_id
                    LIMIT 1;
                ELSE
                    rival_username := 'Unknown';
                END IF;

                IF rival_username IS NULL THEN
                    rival_username := 'Unknown';
                END IF;

                -- Добавление информации о незаполненной дуэли в комментарий к страйку
                missing_duels := missing_duels || format('Не была проверена дуэль %s между @%s и @%s.', duel_rec.duel_number, user_rec.telegram_username, rival_username) || E'\n';

                -- Логирование отсутствующей проверки
                RAISE NOTICE 'Missing review for duel % between @%s и @%s for user %s %s', duel_rec.duel_number, user_rec.telegram_username, rival_username, user_rec.name, user_rec.surname;
                PERFORM log_message(format('process_dual_review_event: Missing review for duel %s between @%s и @%s for user %s %s', duel_rec.duel_number, user_rec.telegram_username, rival_username, user_rec.name, user_rec.surname));
            ELSE
                RAISE NOTICE 'Review already exists for duel % by user %', duel_rec.duel_number, user_rec.telegram_username;
                PERFORM log_message(format('process_dual_review_event: Review already exists for duel %s by user %s', duel_rec.duel_number, user_rec.telegram_username));
            END IF;
        END LOOP;

        -- Проверка наличия незаполненных дуэлей для пользователя
        IF missing_duels <> '' THEN
            -- Подсчёт количества незаполненных дуэлей для пользователя
            SELECT COUNT(*) INTO missing_count
            FROM user_duel_to_review utdr
            WHERE utdr.reviewer_user_strapi_document_id = user_rec.strapi_document_id
              AND utdr.duel_strapi_document_id IN (
                  SELECT d.strapi_document_id
                  FROM duels d
                  WHERE d.sprint_strapi_document_id = p_sprint_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM user_duel_reviewed udr
                  WHERE udr.reviewer_user_strapi_document_id = utdr.reviewer_user_strapi_document_id
                    AND udr.duel_strapi_document_id = utdr.duel_strapi_document_id
                    AND udr.hash = utdr.hash
              );

            -- Логирование количества незаполненных дуэлей
            RAISE NOTICE 'User % % has % missing duel reviews.', user_rec.name, user_rec.surname, missing_count;
            PERFORM log_message(format('process_dual_review_event: User %s %s has %s missing duel reviews.', user_rec.name, user_rec.surname, missing_count));

            -- Проверка, существует ли уже страйк для ревьюера и спринта
            SELECT EXISTS (
                SELECT 1
                FROM strikes st
                WHERE st.user_strapi_document_id = user_rec.strapi_document_id
                  AND st.sprint_strapi_document_id = p_sprint_id
            ) INTO strike_exists;

            IF NOT strike_exists THEN
                -- Формирование комментария к страйку
                strike_comment := missing_duels;

                -- Вставка нового страйка
                INSERT INTO strikes (type, comment, user_strapi_document_id, sprint_strapi_document_id)
                VALUES ('DUEL_REVIEW', strike_comment, user_rec.strapi_document_id, p_sprint_id);

                strikes_assigned := strikes_assigned + 1;

                -- Логирование назначения страйка
                RAISE NOTICE 'Assigned DUEL_REVIEW strike to % % for sprint %. Comment: %', user_rec.name, user_rec.surname, p_sprint_id, strike_comment;
                PERFORM log_message(format('process_dual_review_event: Assigned DUEL_REVIEW strike to %s %s for sprint %s. Comment: %s', user_rec.name, user_rec.surname, p_sprint_id, strike_comment));
            ELSE
                -- Логирование, если страйк уже существует
                RAISE NOTICE 'Strike already exists for user % % in sprint %', user_rec.name, user_rec.surname, p_sprint_id;
                PERFORM log_message(format('process_dual_review_event: Strike already exists for user %s %s in sprint %s', user_rec.name, user_rec.surname, p_sprint_id));
            END IF;
        ELSE
            -- Логирование, если нет незаполненных дуэлей для пользователя
            RAISE NOTICE 'No missing reviews for user % %', user_rec.name, user_rec.surname;
            PERFORM log_message(format('process_dual_review_event: No missing reviews for user %s %s', user_rec.name, user_rec.surname));
        END IF;
    END LOOP;

    -- Логирование общего количества назначенных страйков
    RAISE NOTICE 'process_dual_review_event: Completed for sprint=% and event=%. Strikes assigned: %', p_sprint_id, p_event_id, strikes_assigned;
    PERFORM log_message(format('process_dual_review_event: Completed for sprint=%s and event=%s. Strikes assigned: %s', p_sprint_id, p_event_id, strikes_assigned));

    -- Обновление статуса события на 'COMPLETED'
    UPDATE events
    SET event_status = 'COMPLETED',
        updated_at = NOW()
    WHERE strapi_document_id = p_event_id;

    RAISE NOTICE 'Updated event % status to COMPLETED', p_event_id;
    PERFORM log_message(format('process_dual_review_event: Updated event %s status to COMPLETED', p_event_id));

    -- Логирование завершения функции
    RAISE NOTICE 'process_dual_review_event: Finished processing sprint=% and event=%', p_sprint_id, p_event_id;
    PERFORM log_message(format('process_dual_review_event: Finished processing sprint=%s and event=%s', p_sprint_id, p_event_id));

END;
