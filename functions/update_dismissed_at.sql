DECLARE
    strike_count BIGINT;
    limit_val INTEGER;
BEGIN
    -- Подсчёт количества страйков для пользователя
    SELECT COUNT(*) INTO strike_count
    FROM strikes
    WHERE user_strapi_document_id = user_doc_id;

    -- Получение текущего значения strikes_limit для пользователя
    SELECT strikes_limit INTO limit_val
    FROM users
    WHERE strapi_document_id = user_doc_id;

    -- Установка dismissed_at, если количество страйков >= strikes_limit и dismissed_at ещё не установлено
    IF strike_count >= limit_val AND NOT EXISTS (
        SELECT 1 FROM users
        WHERE strapi_document_id = user_doc_id
          AND dismissed_at IS NOT NULL
    ) THEN
        UPDATE users
        SET dismissed_at = NOW()
        WHERE strapi_document_id = user_doc_id
          AND dismissed_at IS NULL; -- Дополнительная проверка для безопасности

    -- Очистка dismissed_at, если количество страйков < strikes_limit и dismissed_at установлено
    ELSIF strike_count < limit_val AND EXISTS (
        SELECT 1 FROM users
        WHERE strapi_document_id = user_doc_id
          AND dismissed_at IS NOT NULL
    ) THEN
        UPDATE users
        SET dismissed_at = NULL
        WHERE strapi_document_id = user_doc_id
          AND dismissed_at IS NOT NULL; -- Дополнительная проверка для безопасности
    END IF;
END;
