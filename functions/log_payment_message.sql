DECLARE
    v_enable_logs boolean;
BEGIN
    SELECT (value = 'true')::boolean
      INTO v_enable_logs
      FROM public.global_vars
     WHERE key = 'enable_payment_logs'
     LIMIT 1;

    IF NOT FOUND THEN
        v_enable_logs := true;
    END IF;

    IF v_enable_logs THEN
        INSERT INTO public.payment_logs(
            level,
            context,
            message,
            payment_id,
            user_strapi_document_id,
            lead_supabase_id
        ) VALUES (
            p_level,
            p_context,
            p_message,
            p_payment_id,
            p_user_strapi_document_id,
            p_lead_supabase_id
        );
    END IF;
END;
