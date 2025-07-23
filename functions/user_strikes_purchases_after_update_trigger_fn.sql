DECLARE
    v_current_limit integer;
BEGIN
    -- Если is_active поменялось с false->true, добавляем страйки
    IF OLD.is_active = false AND NEW.is_active = true THEN
        SELECT strikes_limit 
          INTO v_current_limit
          FROM public.users
         WHERE strapi_document_id = NEW.user_strapi_document_id
         FOR UPDATE; -- блокируем строку, чтобы избежать гонок

        IF FOUND THEN
            UPDATE public.users
               SET strikes_limit = v_current_limit + NEW.added_strikes
             WHERE strapi_document_id = NEW.user_strapi_document_id;
        END IF;
    END IF;

    -- Если is_active поменялось с true->false, вычитаем страйки
    IF OLD.is_active = true AND NEW.is_active = false THEN
        SELECT strikes_limit 
          INTO v_current_limit
          FROM public.users
         WHERE strapi_document_id = NEW.user_strapi_document_id
         FOR UPDATE;

        IF FOUND THEN
            UPDATE public.users
               SET strikes_limit = v_current_limit - NEW.added_strikes
             WHERE strapi_document_id = NEW.user_strapi_document_id;
        END IF;
    END IF;

    RETURN NEW;
END;
