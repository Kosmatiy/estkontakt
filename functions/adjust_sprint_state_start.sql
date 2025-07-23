DECLARE
    v_stream_strapi_document_id text;
    v_current_sprint_number integer;
    v_previous_sprint_strapi_document_id text;
    v_users_with_repeats_ok text[];
    v_current_duels text[];
    v_user_id text;
    v_duel_id text;
    v_inserted_count integer := 0;
    v_updated_count integer := 0;
BEGIN
    -- 1) Получаем stream_strapi_document_id и sprint_number для текущего спринта
    SELECT 
        stream_strapi_document_id, 
        sprint_number
    INTO 
        v_stream_strapi_document_id, 
        v_current_sprint_number
    FROM public.sprints 
    WHERE strapi_document_id = p_sprint_strapi_document_id;

    -- Проверяем, что спринт найден
    IF v_stream_strapi_document_id IS NULL THEN
        RAISE EXCEPTION 'Sprint not found: %', p_sprint_strapi_document_id;
    END IF;

    -- 2) Находим предыдущий спринт (не BREAK) с меньшим sprint_number
    SELECT strapi_document_id
    INTO v_previous_sprint_strapi_document_id
    FROM public.sprints
    WHERE stream_strapi_document_id = v_stream_strapi_document_id
      AND sprint_number < v_current_sprint_number
      AND sprint_type != 'BREAK'
    ORDER BY sprint_number DESC
    LIMIT 1;

    -- Если предыдущего спринта нет, завершаем функцию
    IF v_previous_sprint_strapi_document_id IS NULL THEN
        RAISE NOTICE 'No previous non-BREAK sprint found for sprint %. Nothing to adjust.', p_sprint_strapi_document_id;
        RETURN;
    END IF;

    -- 3) Получаем всех пользователей, у которых в предыдущем спринте is_repeats_ok = TRUE
    SELECT array_agg(DISTINCT uss.user_strapi_document_id)
    INTO v_users_with_repeats_ok
    FROM public.user_sprint_state uss
    JOIN public.duels d ON d.strapi_document_id = uss.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = v_previous_sprint_strapi_document_id
      AND uss.is_repeats_ok = TRUE;

    -- Если нет пользователей с is_repeats_ok = TRUE в предыдущем спринте, завершаем
    IF v_users_with_repeats_ok IS NULL OR array_length(v_users_with_repeats_ok, 1) = 0 THEN
        RAISE NOTICE 'No users with is_repeats_ok = TRUE found in previous sprint %. Nothing to adjust.', v_previous_sprint_strapi_document_id;
        RETURN;
    END IF;

    -- 4) Получаем все дуэли текущего спринта
    SELECT array_agg(strapi_document_id)
    INTO v_current_duels
    FROM public.duels
    WHERE sprint_strapi_document_id = p_sprint_strapi_document_id;

    -- Если в текущем спринте нет дуэлей, завершаем
    IF v_current_duels IS NULL OR array_length(v_current_duels, 1) = 0 THEN
        RAISE NOTICE 'No duels found in current sprint %. Nothing to adjust.', p_sprint_strapi_document_id;
        RETURN;
    END IF;

    -- 5) Для каждого пользователя и каждой дуэли устанавливаем is_repeats_ok = TRUE
    FOREACH v_user_id IN ARRAY v_users_with_repeats_ok
    LOOP
        FOREACH v_duel_id IN ARRAY v_current_duels
        LOOP
            -- Используем UPSERT (INSERT ... ON CONFLICT)
            INSERT INTO public.user_sprint_state (
                user_strapi_document_id,
                duel_strapi_document_id,
                is_repeats_ok,
                created_at
            )
            VALUES (
                v_user_id,
                v_duel_id,
                TRUE,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (user_strapi_document_id, duel_strapi_document_id) 
            DO UPDATE SET
                is_repeats_ok = TRUE,
                created_at = CURRENT_TIMESTAMP;

            -- Проверяем, была ли запись вставлена или обновлена
            IF FOUND THEN
                v_updated_count := v_updated_count + 1;
            ELSE
                v_inserted_count := v_inserted_count + 1;
            END IF;
        END LOOP;
    END LOOP;

    -- Логируем результат
    RAISE NOTICE 'Successfully adjusted sprint state for sprint: %. Previous sprint: %. Users: %. Duels: %. Records processed: %', 
                 p_sprint_strapi_document_id, 
                 v_previous_sprint_strapi_document_id,
                 array_length(v_users_with_repeats_ok, 1),
                 array_length(v_current_duels, 1),
                 v_inserted_count + v_updated_count;

END;
