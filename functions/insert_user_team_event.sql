DECLARE
    v_now       timestamptz := clock_timestamp();
    v_error     text        := NULL;

    v_user_rec  users%ROWTYPE;
    v_event_rec team_events%ROWTYPE;
BEGIN
/* ---------- 1. Пользователь ----------------------------------------- */
    SELECT * INTO v_user_rec
    FROM users
    WHERE strapi_document_id = v_user_strapi_document_id;

    IF NOT FOUND THEN
        v_error := format('пользователь %s не найден',
                          v_user_strapi_document_id);
    ELSIF v_user_rec.dismissed_at IS NOT NULL THEN
        v_error := format('пользователь %s отчислен с курса и не может отправить ответ',
                          v_user_strapi_document_id);
    END IF;

/* ---------- 2. Событие ---------------------------------------------- */
    IF v_error IS NULL OR v_mode = 'TEST' THEN
        SELECT * INTO v_event_rec
        FROM team_events
        WHERE strapi_document_id = v_team_event_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error || '; ', '') ||
                       format('событие %s не найдено',
                              v_team_event_strapi_document_id);
        END IF;
    END IF;

/* ---------- 3. Проверка дедлайна (только REGULAR) ------------------- */
    IF (v_error IS NULL OR v_mode = 'TEST')
       AND v_mode = 'REGULAR' THEN
        IF v_now > v_event_rec.rsvp_deadline_datetime THEN
            v_error := coalesce(v_error || '; ', '') ||
                       'дедлайн RSVP для события уже прошёл';
        END IF;
    END IF;

/* ---------- 4. Запись RSVP ------------------------------------------ */
    IF (v_mode = 'TEST')
       OR (v_mode = 'REGULAR' AND v_error IS NULL) THEN

        INSERT INTO user_team_events (
            user_strapi_document_id,
            team_event_strapi_document_id,
            will_attend )
        VALUES (
            v_user_strapi_document_id,
            v_team_event_strapi_document_id,
            v_will_attend )
        ON CONFLICT (user_strapi_document_id, team_event_strapi_document_id)
        DO UPDATE
           SET will_attend = EXCLUDED.will_attend;
    END IF;

/* ---------- 5. Итог -------------------------------------------------- */
    IF v_error IS NULL THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', format('RSVP успешно записан (will_attend = %s)',
                                   v_will_attend)
               );

    ELSIF v_mode = 'TEST' THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', 'RSVP записан с предупреждениями: ' || v_error
               );

    ELSE
        RETURN json_build_object(
                 'result',  'error',
                 'message', v_error
               );
    END IF;
END;
