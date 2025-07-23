DECLARE
    v_stream_strapi_document_id text;
    v_current_sprint_number integer;
    v_max_sprint_number integer;
    v_is_last_sprint boolean := false;
BEGIN
    -- 1) Получаем stream_strapi_document_id и sprint_number для данного спринта
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

    -- 2) Определяем максимальный sprint_number в этом стриме
    SELECT MAX(sprint_number)
    INTO v_max_sprint_number
    FROM public.sprints
    WHERE stream_strapi_document_id = v_stream_strapi_document_id;

    -- 3) Проверяем, является ли текущий спринт последним в стриме
    IF v_current_sprint_number = v_max_sprint_number THEN
        v_is_last_sprint := true;
    END IF;

    -- 4) Удаляем все существующие ачивки для данного спринта
    DELETE FROM public.user_achievements 
    WHERE sprint_strapi_document_id = p_sprint_strapi_document_id;

    -- 5) Если это последний спринт, удаляем также все STREAM ачивки для данного стрима
    IF v_is_last_sprint THEN
        DELETE FROM public.user_achievements ua
        WHERE ua.sprint_strapi_document_id IS NULL
          AND EXISTS (
            SELECT 1 
            FROM public.achievements a 
            WHERE a.strapi_document_id = ua.achievement_strapi_document_id 
              AND a.stream_strapi_document_id = v_stream_strapi_document_id
          );
    END IF;

    -- 6) Вставляем SPRINT ачивки для данного спринта из view_all_achievements_unified
    INSERT INTO public.user_achievements (
        created_at,
        user_strapi_document_id,
        achievement_strapi_document_id,
        sprint_strapi_document_id
    )
    SELECT 
        CURRENT_TIMESTAMP as created_at,
        vau.user_strapi_document_id,
        vau.achievement_strapi_document_id,
        vau.sprint_strapi_document_id
    FROM public.view_all_achievements_unified vau
    WHERE vau.sprint_strapi_document_id = p_sprint_strapi_document_id
      AND vau.type = 'SPRINT';

    -- 7) Если это последний спринт, добавляем STREAM ачивки для данного стрима
    IF v_is_last_sprint THEN
        INSERT INTO public.user_achievements (
            created_at,
            user_strapi_document_id,
            achievement_strapi_document_id,
            sprint_strapi_document_id
        )
        SELECT 
            CURRENT_TIMESTAMP as created_at,
            vau.user_strapi_document_id,
            vau.achievement_strapi_document_id,
            NULL as sprint_strapi_document_id  -- STREAM ачивки не привязаны к спринту
        FROM public.view_all_achievements_unified vau
        WHERE vau.stream_strapi_document_id = v_stream_strapi_document_id
          AND vau.type = 'STREAM';
    END IF;

    -- Логируем результат
    RAISE NOTICE 'Successfully updated achievements for sprint: %. Stream: %. Is last sprint: %', 
                 p_sprint_strapi_document_id, v_stream_strapi_document_id, v_is_last_sprint;

END;
