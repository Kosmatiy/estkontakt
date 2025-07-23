DECLARE
    /* служебные -------------------------------------------------------- */
    v_now               timestamptz := clock_timestamp();
    v_error             text        := NULL;

    v_user_rec          users%ROWTYPE;
    v_question_rec      questions%ROWTYPE;
    v_test_rec          tests%ROWTYPE;
    v_lecture_rec       lectures%ROWTYPE;
    v_sprint_rec        sprints%ROWTYPE;
    v_event_rec         events%ROWTYPE;

    v_attempt           int;
    v_score             int;
    v_questions_total   int;
    v_answered_now      int;
    v_user_score        int;

    m_first_zero        boolean;
    m_second_zero       boolean;
    m_strike_exists     boolean;
BEGIN
/* ---------- 1. Пользователь ----------------------------------------- */
    SELECT * INTO v_user_rec
    FROM users
    WHERE strapi_document_id = v_user_strapi_document_id;

    IF NOT FOUND THEN
        v_error := format('пользователь %s не найден', v_user_strapi_document_id);
    ELSIF v_user_rec.dismissed_at IS NOT NULL THEN
        v_error := format('пользователь %s отчислен и не может сдавать',
                          v_user_strapi_document_id);
    END IF;

/* ---------- 2. Вопрос ------------------------------------------------ */
    IF v_error IS NULL OR v_mode = 'TEST' THEN
        SELECT * INTO v_question_rec
        FROM questions
        WHERE strapi_document_id = v_question_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error||'; ','')||
                       format('вопрос %s не найден', v_question_strapi_document_id);
        END IF;
    END IF;

/* ---------- 3. Проверки цепочки (REGULAR) --------------------------- */
    IF (v_error IS NULL OR v_mode = 'TEST') AND v_mode = 'REGULAR' THEN
        /* test → lecture → sprint */
        SELECT * INTO v_test_rec
        FROM tests
        WHERE strapi_document_id = v_question_rec.test_strapi_document_id;

        SELECT * INTO v_lecture_rec
        FROM lectures
        WHERE strapi_document_id = v_test_rec.lecture_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error||'; ','')||
                       format('лекция %s не найдена',
                              v_test_rec.lecture_strapi_document_id);
        END IF;

        IF FOUND THEN
            SELECT * INTO v_sprint_rec
            FROM sprints
            WHERE strapi_document_id = v_lecture_rec.sprint_strapi_document_id;

            IF NOT FOUND THEN
                v_error := coalesce(v_error||'; ','')||
                           format('спринт %s не найден',
                                  v_lecture_rec.sprint_strapi_document_id);
            END IF;
        END IF;

        /* окно сдачи: sprint_phase = 1 */
        IF FOUND THEN
            SELECT * INTO v_event_rec
            FROM events
            WHERE sprint_strapi_document_id = v_sprint_rec.strapi_document_id
              AND sprint_phase               = 1
            LIMIT 1;

            IF NOT FOUND THEN
                v_error := coalesce(v_error||'; ','')||
                           'окно сдачи (phase=1) не найдено';
            ELSIF v_now < v_event_rec.datetime_start THEN
                v_error := coalesce(v_error||'; ','')||
                           'окно сдачи ещё не открылось';
            ELSIF v_now > v_event_rec.datetime_end THEN
                v_error := coalesce(v_error||'; ','')||
                           'окно сдачи уже закрыто';
            END IF;
        END IF;

    ELSE  -- TEST-режим: получаем test/lecture/sprint «вручную»
        SELECT * INTO v_test_rec
        FROM tests
        WHERE strapi_document_id = v_question_rec.test_strapi_document_id;

        SELECT * INTO v_lecture_rec
        FROM lectures
        WHERE strapi_document_id = v_test_rec.lecture_strapi_document_id;

        SELECT * INTO v_sprint_rec
        FROM sprints
        WHERE strapi_document_id = v_lecture_rec.sprint_strapi_document_id;
    END IF;

/* ---------- 4. Определяем attempt ----------------------------------- */
    IF v_error IS NULL OR v_mode = 'TEST' THEN
        SELECT COALESCE(max(attempt),0)+1
        INTO   v_attempt
        FROM   user_question_answers
        WHERE  user_strapi_document_id     = v_user_strapi_document_id
          AND  question_strapi_document_id = v_question_strapi_document_id;

        IF v_attempt > 2 THEN
            v_error := coalesce(v_error||'; ','')||'уже было две попытки';
        END IF;
    END IF;

/* ---------- 5. Записываем ответ ------------------------------------- */
    IF (v_mode = 'TEST'  AND v_attempt IS NOT NULL)
       OR (v_mode = 'REGULAR' AND v_error IS NULL) THEN

        v_score := CASE WHEN v_user_answer = v_question_rec.variant_right THEN 1 ELSE 0 END;

        INSERT INTO user_question_answers
              (user_strapi_document_id, user_answer, right_answer,
               attempt, score, question_strapi_document_id, test_strapi_document_id)
        VALUES(v_user_strapi_document_id, v_user_answer, v_question_rec.variant_right,
               v_attempt, v_score, v_question_strapi_document_id,
               v_question_rec.test_strapi_document_id);

        /* ---------- 6. Закрываем попытку, если отвечен последний вопрос */
        SELECT count(*) INTO v_questions_total
        FROM questions
        WHERE test_strapi_document_id = v_question_rec.test_strapi_document_id;

        SELECT count(*) INTO v_answered_now
        FROM user_question_answers
        WHERE user_strapi_document_id = v_user_strapi_document_id
          AND test_strapi_document_id = v_question_rec.test_strapi_document_id
          AND attempt                 = v_attempt;

        IF v_answered_now = v_questions_total THEN
            SELECT COALESCE(sum(score),0)
            INTO   v_user_score
            FROM   user_question_answers
            WHERE  user_strapi_document_id = v_user_strapi_document_id
              AND  test_strapi_document_id = v_question_rec.test_strapi_document_id
              AND  attempt                 = v_attempt;

            INSERT INTO user_test_answers
                   (user_strapi_document_id, attempt, user_score,
                    test_strapi_document_id, max_score)
            VALUES (v_user_strapi_document_id, v_attempt, v_user_score,
                    v_question_rec.test_strapi_document_id, v_questions_total)
            ON CONFLICT DO NOTHING;
        END IF;

        /* ---------- 7. Страйк: обе попытки по 0 ----------------------- */
        IF v_attempt = 2 AND v_answered_now = v_questions_total THEN

            SELECT user_score = 0
            INTO   m_second_zero
            FROM   user_test_answers
            WHERE  user_strapi_document_id = v_user_strapi_document_id
              AND  test_strapi_document_id = v_question_rec.test_strapi_document_id
              AND  attempt = 2;

            SELECT user_score = 0
            INTO   m_first_zero
            FROM   user_test_answers
            WHERE  user_strapi_document_id = v_user_strapi_document_id
              AND  test_strapi_document_id = v_question_rec.test_strapi_document_id
              AND  attempt = 1;

            IF m_first_zero AND m_second_zero THEN
                /* страйк уже есть? */
                SELECT EXISTS (
                    SELECT 1
                    FROM strikes
                    WHERE user_strapi_document_id   = v_user_strapi_document_id
                      AND sprint_strapi_document_id = v_sprint_rec.strapi_document_id
                      AND type = 'TTT'
                      AND comment LIKE '%'||v_test_rec.test_number||'%'
                )
                INTO m_strike_exists;

                IF NOT m_strike_exists THEN
                    INSERT INTO strikes(type, comment,
                                        user_strapi_document_id,
                                        sprint_strapi_document_id)
                    VALUES ('TTT',
                            format('Тест №%s — «%s»: две попытки по 0 баллов',
                                   v_test_rec.test_number, v_test_rec.title),
                            v_user_strapi_document_id,
                            v_sprint_rec.strapi_document_id);
                END IF;
            END IF;
        END IF;
    END IF;

/* ---------- 8. Итог -------------------------------------------------- */
    IF v_error IS NULL THEN
        RETURN json_build_object('result','success','message','ответ успешно записан');
    ELSIF v_mode = 'TEST' THEN
        RETURN json_build_object('result','success',
                                 'message','ответ записан с предупреждениями: '||v_error);
    ELSE
        RETURN json_build_object('result','error','message',v_error);
    END IF;
END;
