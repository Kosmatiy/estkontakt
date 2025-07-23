BEGIN
    INSERT INTO public.test_logs(level, context, message)
    VALUES(p_level, p_context, p_message);

    -- Дополнительно выводим через RAISE NOTICE (наглядно при ручном запуске):
    RAISE NOTICE '% [%] %', p_level, p_context, p_message;
END;
