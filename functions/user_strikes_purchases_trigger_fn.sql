DECLARE
    v_current_limit integer;
BEGIN
    -- Лог для отладки (по желанию)
    PERFORM log_payment_message(
        'DEBUG',
        'user_strikes_purchases_trigger',
        'Trigger fired: OP='||TG_OP
         ||' old.is_active='||COALESCE(OLD.is_active::text,'<null>')
         ||' new.is_active='||COALESCE(NEW.is_active::text,'<null>'),
        NEW.payment_id,
        NEW.user_strapi_document_id,
        NULL
    );

    -- При простом UPDATE, если is_active вообще не менялось, выходим
    IF TG_OP = 'UPDATE' AND OLD.is_active = NEW.is_active THEN
        RETURN NEW;
    END IF;

    -- Обработка INSERT, когда is_active=true
    IF TG_OP = 'INSERT' AND NEW.is_active = true THEN
        SELECT strikes_limit
          INTO v_current_limit
          FROM public.users
         WHERE strapi_document_id = NEW.user_strapi_document_id
         FOR UPDATE;

        IF FOUND THEN
            UPDATE public.users
               SET strikes_limit = v_current_limit + NEW.added_strikes
             WHERE strapi_document_id = NEW.user_strapi_document_id;
        END IF;
    END IF;

    -- Обработка UPDATE false->true
    IF TG_OP = 'UPDATE'
       AND OLD.is_active = false
       AND NEW.is_active = true THEN
        SELECT strikes_limit
          INTO v_current_limit
          FROM public.users
         WHERE strapi_document_id = NEW.user_strapi_document_id
         FOR UPDATE;

        IF FOUND THEN
            UPDATE public.users
               SET strikes_limit = v_current_limit + NEW.added_strikes
             WHERE strapi_document_id = NEW.user_strapi_document_id;
        END IF;
    END IF;

    -- Обработка UPDATE true->false
    IF TG_OP = 'UPDATE'
       AND OLD.is_active = true
       AND NEW.is_active = false THEN
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
