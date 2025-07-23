DECLARE
    v_user_id text;
    v_lead_id text;
    v_product_id text;
    v_amount double precision;
    v_entity_type text;
    v_entity_id text;
    v_strikes_to_add integer;
BEGIN
    PERFORM log_payment_message(
        'DEBUG',
        'handle_payment_event',
        'Start handle_payment_event(p_old_status='||p_old_status||', p_new_status='||p_new_status||')',
        p_payment_id,
        NULL,
        NULL
    );

    SELECT user_strapi_document_id,
           product_strapi_document_id,
           amount,
           lead_supabase_id
      INTO v_user_id,
           v_product_id,
           v_amount,
           v_lead_id
      FROM public.payments
     WHERE supabase_id = p_payment_id;

    IF NOT FOUND THEN
        PERFORM log_payment_message(
            'ERROR',
            'handle_payment_event',
            'No payment record found for p_payment_id='||p_payment_id::text,
            p_payment_id,
            NULL,
            NULL
        );
        RETURN;
    END IF;

    PERFORM log_payment_message(
        'DEBUG',
        'handle_payment_event',
        'Payment data: user='||COALESCE(v_user_id,'<NULL>')
         ||', product_id='||COALESCE(v_product_id,'<NULL>')
         ||', amount='||COALESCE(v_amount::text,'<NULL>')
         ||', lead='||COALESCE(v_lead_id,'<NULL>'),
        p_payment_id,
        v_user_id,
        v_lead_id
    );

    SELECT entity_type,
           entity_strapi_document_id
      INTO v_entity_type,
           v_entity_id
      FROM public.products
     WHERE strapi_document_id = v_product_id;

    PERFORM log_payment_message(
        'DEBUG',
        'handle_payment_event',
        'Product: entity_type='||COALESCE(v_entity_type,'<NULL>')
         ||', entity_id='||COALESCE(v_entity_id,'<NULL>'),
        p_payment_id,
        v_user_id,
        v_lead_id
    );

    IF p_new_status = 'CONFIRMED' THEN
        PERFORM log_payment_message(
            'DEBUG',
            'handle_payment_event',
            'CONFIRMED => activate resources',
            p_payment_id,
            v_user_id,
            v_lead_id
        );

    IF v_entity_type = 'LIVE_PACK' THEN

    -- 1. Пытаемся получить реальное количество из lifepacks
    SELECT live_count
      INTO v_strikes_to_add
    FROM public.live_packs
    WHERE strapi_document_id = v_entity_id;

    -- 2. Если не нашли — возвращаем значение по умолчанию
    IF NOT FOUND OR v_strikes_to_add IS NULL THEN
        v_strikes_to_add := 1;
        PERFORM log_payment_message(
            'WARNING',
            'handle_payment_event',
            'lifepacks record not found for strapi_document_id='||v_entity_id
             ||'; defaulting v_strikes_to_add to 1',
            p_payment_id,
            v_user_id,
            v_lead_id
        );
    ELSE
        PERFORM log_payment_message(
            'DEBUG',
            'handle_payment_event',
            'Loaded life_count='||v_strikes_to_add
             ||' for strapi_document_id='||v_entity_id,
            p_payment_id,
            v_user_id,
            v_lead_id
        );
    END IF;

    -- 3. Вставка (или обновление) с реальным количеством
    INSERT INTO public.user_strikes_purchases(
        user_strapi_document_id,
        payment_id,
        added_strikes,
        comment
    ) VALUES (
        v_user_id,
        p_payment_id,
        v_strikes_to_add,
        'LIVE_PACK purchase'
    )
    ON CONFLICT (payment_id, user_strapi_document_id)
    DO UPDATE SET
        is_active      = true,
        added_strikes  = EXCLUDED.added_strikes;

        ELSIF v_entity_type = 'STREAM_PASS' THEN
            INSERT INTO public.user_stream_access(
                user_strapi_document_id,
                stream_strapi_document_id
            ) VALUES (
                v_user_id,
                v_entity_id
            )
            ON CONFLICT (user_strapi_document_id, stream_strapi_document_id)
            DO UPDATE SET is_active = true;

        ELSIF v_entity_type = 'MEETING_PASS' THEN
            INSERT INTO public.user_meeting_access(
                user_strapi_document_id,
                meeting_strapi_document_id
            ) VALUES (
                v_user_id,
                v_entity_id
            )
            ON CONFLICT (user_strapi_document_id, meeting_strapi_document_id)
            DO UPDATE SET is_active = true;

        ELSE
            PERFORM log_payment_message(
                'WARNING',
                'handle_payment_event',
                'Unknown entity_type='||COALESCE(v_entity_type,'<NULL>')||' for CONFIRMED',
                p_payment_id,
                v_user_id,
                v_lead_id
            );
        END IF;

        PERFORM log_payment_message(
            'INFO',
            'handle_payment_event',
            'Payment confirmed and resources activated',
            p_payment_id,
            v_user_id,
            v_lead_id
        );

    ELSIF p_new_status IN (
        'REFUNDED',
        'PARTIAL_REFUNDED',
        'REVERSED',
        'PARTIAL_REVERSED',
        'CANCELED'
    ) THEN
        PERFORM log_payment_message(
            'DEBUG',
            'handle_payment_event',
            'Refund/Cancel => deactivate resources',
            p_payment_id,
            v_user_id,
            v_lead_id
        );

        IF v_entity_type = 'LIVE_PACK' THEN
            UPDATE public.user_strikes_purchases
               SET is_active = false
             WHERE payment_id = p_payment_id
               AND user_strapi_document_id = v_user_id
               AND is_active = true;

        ELSIF v_entity_type = 'STREAM_PASS' THEN
            UPDATE public.user_stream_access
               SET is_active = false
             WHERE user_strapi_document_id = v_user_id
               AND stream_strapi_document_id = v_entity_id
               AND is_active = true;

        ELSIF v_entity_type = 'MEETING_PASS' THEN
            UPDATE public.user_meeting_access
               SET is_active = false
             WHERE user_strapi_document_id = v_user_id
               AND meeting_strapi_document_id = v_entity_id
               AND is_active = true;

        ELSE
            PERFORM log_payment_message(
                'WARNING',
                'handle_payment_event',
                'Unknown entity_type='||COALESCE(v_entity_type,'<NULL>')||' for REFUND/REVERSE',
                p_payment_id,
                v_user_id,
                v_lead_id
            );
        END IF;

        PERFORM log_payment_message(
            'INFO',
            'handle_payment_event',
            'Payment refunded/canceled, resources deactivated',
            p_payment_id,
            v_user_id,
            v_lead_id
        );

    ELSIF p_new_status IN (
       'NEW','FORM_SHOWED','AUTHORIZING','3DS_CHECKING','3DS_CHECKED',
       'AUTHORIZED','CONFIRMING'
    ) THEN
        PERFORM log_payment_message(
            'DEBUG',
            'handle_payment_event',
            'Intermediate status: '||p_new_status,
            p_payment_id,
            v_user_id,
            v_lead_id
        );

    ELSIF p_new_status IN (
       'DEADLINE_EXPIRED','REJECTED','AUTH_FAIL'
    ) THEN
        PERFORM log_payment_message(
            'DEBUG',
            'handle_payment_event',
            'Unsuccessful status: '||p_new_status,
            p_payment_id,
            v_user_id,
            v_lead_id
        );

    ELSE
        PERFORM log_payment_message(
            'WARNING',
            'handle_payment_event',
            'Unknown status='||p_new_status||'. No logic applied',
            p_payment_id,
            v_user_id,
            v_lead_id
        );
    END IF;

    PERFORM log_payment_message(
        'INFO',
        'handle_payment_event',
        'Done handle_payment_event for payment_id='||p_payment_id,
        p_payment_id,
        v_user_id,
        v_lead_id
    );

EXCEPTION
    WHEN OTHERS THEN
        PERFORM log_payment_message(
            'ERROR',
            'handle_payment_event',
            'Exception: '||SQLERRM,
            p_payment_id,
            v_user_id,
            v_lead_id
        );
        RAISE;
END;
