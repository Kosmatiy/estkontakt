BEGIN
    IF (TG_OP = 'INSERT') THEN
        -- При вставке новой дуэли устанавливаем is_chosen в TRUE для обоих пользователей
        UPDATE user_sprint_state
        SET is_chosen = TRUE
        WHERE user_strapi_document_id IN (NEW.user_strapi_document_id, NEW.rival_strapi_document_id)
          AND duel_strapi_document_id = NEW.duel_strapi_document_id;
          
    ELSIF (TG_OP = 'DELETE') THEN
        -- При удалении дуэли проверяем, участвуют ли пользователи в других дуэлях
        -- Если нет, устанавливаем is_chosen в FALSE
        UPDATE user_sprint_state
        SET is_chosen = FALSE
        WHERE user_strapi_document_id IN (OLD.user_strapi_document_id, OLD.rival_strapi_document_id)
          AND duel_strapi_document_id = OLD.duel_strapi_document_id
          AND NOT EXISTS (
              SELECT 1
              FROM duel_distributions dd
              WHERE dd.user_strapi_document_id = OLD.user_strapi_document_id
                AND dd.duel_strapi_document_id = OLD.duel_strapi_document_id
                AND dd.is_failed = FALSE
          );
          
    ELSIF (TG_OP = 'UPDATE') THEN
        -- Обработка обновлений, например, при изменении is_failed
        -- Если дуэль помечена как failed, устанавливаем is_chosen в FALSE
        IF NEW.is_failed = TRUE AND OLD.is_failed = FALSE THEN
            UPDATE user_sprint_state
            SET is_chosen = FALSE
            WHERE user_strapi_document_id IN (NEW.user_strapi_document_id, NEW.rival_strapi_document_id)
              AND duel_strapi_document_id = NEW.duel_strapi_document_id;
        END IF;
    END IF;
    
    RETURN NULL; -- Для триггеров AFTER
END;
