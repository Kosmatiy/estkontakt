BEGIN
    -- Вставляем новое сообщение в таблицу admin_messages
    INSERT INTO admin_messages (message_text, sprint_strapi_document_id, created_at)
    VALUES (p_message_text, p_sprint_strapi_id, NOW());

    -- Логируем действие в таблицу distribution_logs
    PERFORM log_message(format(
        'Admin message inserted: "%s" for sprint=%s',
        p_message_text,
        p_sprint_strapi_id
    ));
EXCEPTION WHEN OTHERS THEN
    -- В случае ошибки логируем её и пробрасываем дальше
    PERFORM log_message(format(
        'Error inserting admin message: "%s" for sprint=%s',
        p_message_text,
        p_sprint_strapi_id
    ));
    RAISE;
END;
