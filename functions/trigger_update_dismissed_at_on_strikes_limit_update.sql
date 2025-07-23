BEGIN
    -- Вызов функции обновления dismissed_at для соответствующего пользователя
    PERFORM update_dismissed_at(NEW.strapi_document_id);
    RETURN NULL; -- Триггер AFTER не требует возврата новой строки
END;
