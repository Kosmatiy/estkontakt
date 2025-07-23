DECLARE
    rec_reviewer RECORD;
    rec_duel RECORD;
    missing_list TEXT;
    userA_id TEXT;
    userB_id TEXT;
    userA_name TEXT;
    userB_name TEXT;
    has_strike BOOLEAN;
BEGIN
    -- Логируем старт
    PERFORM log_message(
        format('process_duel_review_event: START for sprint=%s, event=%s', 
               p_sprint_id, p_event_id)
    );

    -- Идём по уникальным ревьюерам для конкретного спринта
    FOR rec_reviewer IN
        SELECT DISTINCT utdr.reviewer_user_strapi_document_id
        FROM user_duel_to_review utdr
        JOIN duels d ON d.strapi_document_id = utdr.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
        ORDER BY utdr.reviewer_user_strapi_document_id
    LOOP
        -- Сбрасываем накопитель для каждого ревьюера
        missing_list := '';

        -- Проверяем, что ревьюер не уволен
        IF NOT EXISTS (
            SELECT 1
            FROM users
            WHERE strapi_document_id = rec_reviewer.reviewer_user_strapi_document_id
              AND dismissed_at IS NULL
        ) THEN
            PERFORM log_message(
                format('Reviewer %s is dismissed or not found => skip', 
                       rec_reviewer.reviewer_user_strapi_document_id)
            );
            CONTINUE;
        END IF;

        -- Проверяем, есть ли уже страйк в этом спринте
        SELECT EXISTS (
            SELECT 1
            FROM strikes st
            WHERE st.user_strapi_document_id = rec_reviewer.reviewer_user_strapi_document_id
              AND st.sprint_strapi_document_id = p_sprint_id
        ) INTO has_strike;

        -- Итерируемся по дуэлям для данного ревьюера
        FOR rec_duel IN
            SELECT DISTINCT utdr.duel_strapi_document_id,
                   utdr.hash,
                   d.duel_number
            FROM user_duel_to_review utdr
            JOIN duels d ON d.strapi_document_id = utdr.duel_strapi_document_id
            WHERE utdr.reviewer_user_strapi_document_id = rec_reviewer.reviewer_user_strapi_document_id
              AND d.sprint_strapi_document_id = p_sprint_id
            ORDER BY d.duel_number
        LOOP
            -- Проверяем на NULL/пустой hash
            IF rec_duel.hash IS NULL OR rec_duel.hash = '' THEN
                PERFORM log_message(
                    format('Empty hash for duel %s, skipping', rec_duel.duel_strapi_document_id)
                );
                CONTINUE;
            END IF;

            -- Проверяем, была ли дуэль проверена
            IF NOT EXISTS (
                SELECT 1
                FROM user_duel_reviewed udr
                WHERE udr.reviewer_user_strapi_document_id = rec_reviewer.reviewer_user_strapi_document_id
                  AND udr.duel_strapi_document_id = rec_duel.duel_strapi_document_id
                  AND udr.hash = rec_duel.hash
            ) THEN
                -- Дуэль не была проверена - парсим hash
                userA_id := split_part(rec_duel.hash, '_', 1);
                userB_id := split_part(rec_duel.hash, '_', 2);

                -- Проверяем корректность парсинга
                IF userA_id = '' OR userB_id = '' THEN
                    PERFORM log_message(
                        format('Invalid hash format "%s" for duel %s', 
                               rec_duel.hash, rec_duel.duel_strapi_document_id)
                    );
                    userA_name := 'Unknown';
                    userB_name := 'Unknown';
                ELSE
                    -- Получаем имена пользователей
                    SELECT COALESCE(u.telegram_username, 'UnknownA')
                    INTO userA_name
                    FROM users u
                    WHERE u.strapi_document_id = userA_id
                    LIMIT 1;

                    IF userA_name IS NULL THEN
                        userA_name := 'UnknownA';
                    END IF;

                    SELECT COALESCE(u.telegram_username, 'UnknownB')
                    INTO userB_name
                    FROM users u
                    WHERE u.strapi_document_id = userB_id
                    LIMIT 1;

                    IF userB_name IS NULL THEN
                        userB_name := 'UnknownB';
                    END IF;
                END IF;

                -- Добавляем в список пропущенных
                missing_list := missing_list || format(
                    'Не была проверена дуэль %s между %s и %s.' || chr(10),
                    COALESCE(rec_duel.duel_number, 'N/A'),
                    userA_name,
                    userB_name
                );
            END IF;
        END LOOP;

        -- Обрабатываем результат для ревьюера
        IF missing_list <> '' THEN
            PERFORM log_message(
                format(
                    'У ревьюера [%s] (sprint=%s) пропущены дуэли:' || chr(10) || '%s',
                    rec_reviewer.reviewer_user_strapi_document_id,
                    p_sprint_id,
                    missing_list
                )
            );

            -- Назначаем страйк если его еще нет
            IF has_strike = FALSE THEN
                INSERT INTO strikes(type, comment, user_strapi_document_id, sprint_strapi_document_id, created_at)
                VALUES(
                    'DUEL_REVIEW',
                    format('Пропущены дуэли в спринте %s:' || chr(10) || '%s', p_sprint_id, missing_list),
                    rec_reviewer.reviewer_user_strapi_document_id,
                    p_sprint_id,
                    NOW()
                );
                PERFORM log_message(format(
                    'Assigned DUEL_REVIEW strike => user=%s, sprint=%s',
                    rec_reviewer.reviewer_user_strapi_document_id, p_sprint_id
                ));
            ELSE
                PERFORM log_message(format(
                    'Strike already exists for user=%s, sprint=%s - not assigning new one',
                    rec_reviewer.reviewer_user_strapi_document_id, p_sprint_id
                ));
            END IF;
        ELSE
            PERFORM log_message(
                format('Reviewer %s => no missing duels in sprint=%s',
                       rec_reviewer.reviewer_user_strapi_document_id,
                       p_sprint_id)
            );
        END IF;
    END LOOP;

    -- Завершаем событие
    UPDATE events
    SET event_status = 'COMPLETED',
        updated_at = NOW()
    WHERE strapi_document_id = p_event_id;

    PERFORM log_message(
        format('process_duel_review_event: COMPLETED => sprint=%s, event=%s', 
               p_sprint_id, p_event_id)
    );

EXCEPTION WHEN OTHERS THEN
    PERFORM log_message(format(
        'process_duel_review_event: ERROR => sprint=%s, event=%s, err=%s',
        p_sprint_id, p_event_id, SQLERRM
    ));
    PERFORM insert_admin_message(
        format('Ошибка process_duel_review_event(sprint=%s, event=%s): %s',
               p_sprint_id, p_event_id, SQLERRM),
        p_sprint_id
    );
    RAISE;
END;
