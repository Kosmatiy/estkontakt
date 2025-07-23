DECLARE
    v_now        timestamptz := now();
    v_error      text        := NULL;

    v_user_rec   users%ROWTYPE;
    v_duel_rec   duels%ROWTYPE;
    v_sprint_rec sprints%ROWTYPE;
    v_event_rec  events%ROWTYPE;
BEGIN
/* ---------- 1. Проверка пользователя (автора ответа) ---------- */
    SELECT * INTO v_user_rec
    FROM users
    WHERE strapi_document_id = v_user_strapi_document_id;

    IF NOT FOUND THEN
        v_error := 'пользователь со strapi_document_id='
                   || v_user_strapi_document_id || ' не найден';
    ELSIF v_user_rec.dismissed_at IS NOT NULL THEN
        v_error := 'пользователь '
                   || v_user_strapi_document_id
                   || ' отчислен с курса и не может отправить ответ';
    END IF;

/* ---------- 2. Проверка дуэли ---------- */
    IF v_error IS NULL OR v_mode = 'TEST' THEN
        SELECT * INTO v_duel_rec
        FROM duels
        WHERE strapi_document_id = v_duel_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error || '; ', '')
                       || 'дуэль со strapi_document_id='
                       || v_duel_strapi_document_id || ' не найдена';
        END IF;
    END IF;

/* ---------- 3. Проверка sprint-event (REGULAR) ---------- */
    IF (v_error IS NULL OR v_mode = 'TEST')
       AND v_mode = 'REGULAR' THEN

        /* sprint */
        SELECT * INTO v_sprint_rec
        FROM sprints
        WHERE strapi_document_id = v_duel_rec.sprint_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error || '; ', '')
                       || 'спринт со strapi_document_id='
                       || v_duel_rec.sprint_strapi_document_id || ' не найден';
        END IF;

        /* event sprint_phase = 3 */
        IF FOUND THEN
            SELECT * INTO v_event_rec
            FROM events
            WHERE sprint_strapi_document_id = v_sprint_rec.strapi_document_id
              AND sprint_phase = 3
            LIMIT 1;

            IF NOT FOUND THEN
                v_error := coalesce(v_error || '; ', '')
                           || 'не найдено событие sprint_phase=3 для указанного спринта';
            ELSIF v_now < v_event_rec.datetime_start THEN
                v_error := coalesce(v_error || '; ', '')
                           || 'событие, на котором сдаётся дуэльный ответ, ещё не началось';
            ELSIF v_now > v_event_rec.datetime_end THEN
                v_error := coalesce(v_error || '; ', '')
                           || 'событие, на котором сдаётся дуэльный ответ, уже закончилось';
            END IF;
        END IF;
    END IF;

/* ---------- 4. Запись ответов ---------- */
    IF v_mode = 'TEST'
       OR (v_mode = 'REGULAR' AND v_error IS NULL) THEN

        INSERT INTO user_duel_answers (
                 user_strapi_document_id,
                 rival_user_strapi_document_id,
                 duel_strapi_document_id,
                 video_url,
                 comment,
                 pair_id,
                 hash,
                 status,
                 answer_part)
        VALUES
          (v_user_strapi_document_id,
           v_rival_user_strapi_document_id,
           v_duel_strapi_document_id,
           v_video_url,
           v_comment,
           v_pair_id,
           v_hash,
           v_status,
           1),

          (v_rival_user_strapi_document_id,
           v_user_strapi_document_id,
           v_duel_strapi_document_id,
           v_video_url,
           v_comment,
           v_pair_id,
           v_hash,
           v_status,
           2);
    END IF;

/* ---------- 5. Итог ---------- */
    IF v_error IS NULL THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', 'ответы успешно записаны'
               );
    ELSIF v_mode = 'TEST' THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', 'ответы записаны с предупреждениями: ' || v_error
               );
    ELSE
        RETURN json_build_object(
                 'result',  'error',
                 'message', v_error
               );
    END IF;
END;
