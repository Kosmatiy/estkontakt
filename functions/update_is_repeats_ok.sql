DECLARE
    affected_rows INTEGER;
    duel_id TEXT;
BEGIN
    -- Перебираем все подходящие дуэли из спринта
    FOR duel_id IN
        SELECT strapi_document_id
        FROM duels
        WHERE sprint_strapi_document_id = p_sprint_strapi_document_id
    LOOP
        -- Обновляем, если запись есть
        UPDATE user_sprint_state
        SET is_repeats_ok = p_is_repeats_ok
        WHERE user_strapi_document_id = p_user_strapi_document_id
          AND duel_strapi_document_id = duel_id;

        GET DIAGNOSTICS affected_rows = ROW_COUNT;

        -- Если не обновилась — вставляем новую
        IF affected_rows = 0 THEN
            INSERT INTO user_sprint_state (user_strapi_document_id, duel_strapi_document_id, is_repeats_ok)
            VALUES (p_user_strapi_document_id, duel_id, p_is_repeats_ok);
        END IF;
    END LOOP;

    RETURN json_build_object('status', 'ok');
EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object('status', 'fail', 'message', SQLERRM);
END;
