DECLARE
    v_now            timestamptz := now();
    v_error          text        := NULL;
    v_user_rec       users%ROWTYPE;
    v_task_rec       tasks%ROWTYPE;
    v_lecture_rec    lectures%ROWTYPE;
    v_sprint_rec     sprints%ROWTYPE;
    v_event_rec      events%ROWTYPE;
BEGIN
/* ---------- 1. Проверка ревьюера ---------- */
    SELECT * INTO v_user_rec
    FROM users
    WHERE strapi_document_id = v_reviewer_user_strapi_document_id;

    IF NOT FOUND THEN
        v_error := 'пользователь-ревьюер со strapi_document_id='
                   || v_reviewer_user_strapi_document_id || ' не найден';
    ELSIF v_user_rec.dismissed_at IS NOT NULL THEN
        v_error := 'пользователь-ревьюер '
                   || v_reviewer_user_strapi_document_id
                   || ' отчислен с курса и не может отправить отзыв';
    END IF;

/* ---------- 2. Проверка задания ---------- */
    IF v_error IS NULL OR v_mode = 'TEST' THEN
        SELECT * INTO v_task_rec
        FROM tasks
        WHERE strapi_document_id = v_task_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error || '; ', '')
                       || 'задание со strapi_document_id='
                       || v_task_strapi_document_id || ' не найдено';
        END IF;
    END IF;

/* ---------- 3. Проверка lecture-sprint-event (только REGULAR) ---------- */
    IF (v_error IS NULL OR v_mode = 'TEST')
       AND v_mode = 'REGULAR' THEN

        /* lecture */
        SELECT * INTO v_lecture_rec
        FROM lectures
        WHERE strapi_document_id = v_task_rec.lecture_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error || '; ', '')
                       || 'лекция со strapi_document_id='
                       || v_task_rec.lecture_strapi_document_id || ' не найдена';
        END IF;

        /* sprint */
        IF FOUND THEN
            SELECT * INTO v_sprint_rec
            FROM sprints
            WHERE strapi_document_id = v_lecture_rec.sprint_strapi_document_id;

            IF NOT FOUND THEN
                v_error := coalesce(v_error || '; ', '')
                           || 'спринт со strapi_document_id='
                           || v_lecture_rec.sprint_strapi_document_id || ' не найден';
            END IF;
        END IF;

        /* event: sprint_phase = 1 */
        IF FOUND THEN
            SELECT * INTO v_event_rec
            FROM events
            WHERE sprint_strapi_document_id = v_sprint_rec.strapi_document_id
              AND sprint_phase = 2
            LIMIT 1;

            IF NOT FOUND THEN
                v_error := coalesce(v_error || '; ', '')
                           || 'не найдено событие sprint_phase=1 для указанного спринта';
            ELSIF v_now < v_event_rec.datetime_start THEN
                v_error := coalesce(v_error || '; ', '')
                           || 'событие, на котором сдаётся отзыв, ещё не началось';
            ELSIF v_now > v_event_rec.datetime_end THEN
                v_error := coalesce(v_error || '; ', '')
                           || 'событие, на котором сдаётся отзыв, уже закончилось';
            END IF;
        END IF;
    END IF;

/* ---------- 4. Запись отзывов ---------- */
    IF v_mode = 'TEST'
       OR (v_mode = 'REGULAR' AND v_error IS NULL) THEN

        INSERT INTO user_task_reviewed (
                 reviewer_user_strapi_document_id,
                 task_strapi_document_id,
                 reviewee_user_strapi_document_id,
                 mark,
                 number_in_batch,
                 is_valid)
        VALUES
          (v_reviewer_user_strapi_document_id, v_task_strapi_document_id,
           v_reviewee_user_strapi_document_id1,
           CASE WHEN v_is_valid1 THEN v_mark1 ELSE 0 END,
           v_number_in_batch1,
           v_is_valid1),

          (v_reviewer_user_strapi_document_id, v_task_strapi_document_id,
           v_reviewee_user_strapi_document_id2,
           CASE WHEN v_is_valid2 THEN v_mark2 ELSE 0 END,
           v_number_in_batch2,
           v_is_valid2),

          (v_reviewer_user_strapi_document_id, v_task_strapi_document_id,
           v_reviewee_user_strapi_document_id3,
           CASE WHEN v_is_valid3 THEN v_mark3 ELSE 0 END,
           v_number_in_batch3,
           v_is_valid3);
    END IF;

/* ---------- 5. Результат ---------- */
    IF v_error IS NULL THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', 'отзывы успешно записаны'
               );
    ELSIF v_mode = 'TEST' THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', 'отзывы записаны с предупреждениями: ' || v_error
               );
    ELSE
        RETURN json_build_object(
                 'result',  'error',
                 'message', v_error
               );
    END IF;
END;
