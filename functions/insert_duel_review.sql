DECLARE
    /* 0. «Нормализованные» оценки: обнуляем, если отзыв невалидный */
    m_result_mark1     int := CASE WHEN v_is_valid THEN v_result_mark1   ELSE 0 END;
    m_image_mark1      int := CASE WHEN v_is_valid THEN v_image_mark1    ELSE 0 END;
    m_attention_mark1  int := CASE WHEN v_is_valid THEN v_attention_mark1 ELSE 0 END;
    m_skill1_mark1     int := CASE WHEN v_is_valid THEN v_skill1_mark1   ELSE 0 END;
    m_skill2_mark1     int := CASE WHEN v_is_valid THEN v_skill2_mark1   ELSE 0 END;

    m_result_mark2     int := CASE WHEN v_is_valid THEN v_result_mark2   ELSE 0 END;
    m_image_mark2      int := CASE WHEN v_is_valid THEN v_image_mark2    ELSE 0 END;
    m_attention_mark2  int := CASE WHEN v_is_valid THEN v_attention_mark2 ELSE 0 END;
    m_skill1_mark2     int := CASE WHEN v_is_valid THEN v_skill1_mark2   ELSE 0 END;
    m_skill2_mark2     int := CASE WHEN v_is_valid THEN v_skill2_mark2   ELSE 0 END;
    m_comment text := nullif( trim(both '" ' from v_comment), '' );
    /* служебные переменные */
    v_now        timestamptz := now();
    v_error      text        := NULL;

    v_user_rec   users%ROWTYPE;
    v_duel_rec   duels%ROWTYPE;
    v_sprint_rec sprints%ROWTYPE;
    v_event_rec  events%ROWTYPE;
BEGIN
/* ---------- 1. Проверяем пользователя --------------------------------*/
    SELECT * INTO v_user_rec
    FROM users
    WHERE strapi_document_id = v_user_strapi_document_id;

    IF NOT FOUND THEN
        v_error := format('пользователь %s не найден', v_user_strapi_document_id);
    ELSIF v_user_rec.dismissed_at IS NOT NULL THEN
        v_error := format('пользователь %s отчислен и не может получить оценку',
                          v_user_strapi_document_id);
    END IF;

/* ---------- 2. Проверяем дуэль ---------------------------------------*/
    IF v_error IS NULL OR v_mode = 'TEST' THEN
        SELECT * INTO v_duel_rec
        FROM duels
        WHERE strapi_document_id = v_duel_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error||'; ', '') ||
                       format('дуэль %s не найдена', v_duel_strapi_document_id);
        END IF;
    END IF;

/* ---------- 3. Проверяем окно сдачи (только REGULAR) -----------------*/
    IF (v_error IS NULL OR v_mode = 'TEST') AND v_mode = 'REGULAR' THEN
        SELECT * INTO v_sprint_rec
        FROM sprints
        WHERE strapi_document_id = v_duel_rec.sprint_strapi_document_id;

        IF NOT FOUND THEN
            v_error := coalesce(v_error||'; ', '') ||
                       format('спринт %s не найден',
                              v_duel_rec.sprint_strapi_document_id);
        ELSE
            SELECT * INTO v_event_rec
            FROM events
            WHERE sprint_strapi_document_id = v_sprint_rec.strapi_document_id
              AND sprint_phase               = 4
            LIMIT 1;

            IF NOT FOUND THEN
                v_error := coalesce(v_error||'; ', '') ||
                           'событие sprint_phase=4 не найдено';
            ELSIF v_now < v_event_rec.datetime_start THEN
                v_error := coalesce(v_error||'; ', '') ||
                           'событие ещё не началось';
            ELSIF v_now > v_event_rec.datetime_end THEN
                v_error := coalesce(v_error||'; ', '') ||
                           'событие уже закончилось';
            END IF;
        END IF;
    END IF;

/* ---------- 4. Записываем отзывы -------------------------------------*/
    IF (v_mode = 'TEST') OR (v_mode = 'REGULAR' AND v_error IS NULL) THEN
        IF v_duel_type = 'FULL-CONTACT' THEN
            INSERT INTO user_duel_reviewed (
                     reviewer_user_strapi_document_id,
                     duel_strapi_document_id,
                     user_strapi_document_id,
                     comment,
                     hash,
                     is_valid,
                     result_mark,
                     image_mark,
                     attention_mark)
            VALUES
              (v_reviewer_user_strapi_document_id, v_duel_strapi_document_id,
               v_user_strapi_document_id,
               m_comment, v_hash, v_is_valid,
               m_result_mark1, m_image_mark1, m_attention_mark1),

              (v_reviewer_user_strapi_document_id, v_duel_strapi_document_id,
               v_rival_user_strapi_document_id,
               m_comment, v_hash, v_is_valid,
               m_result_mark2, m_image_mark2, m_attention_mark2);

        ELSE /* TRAINING -------------------------------------------------*/
            INSERT INTO user_duel_reviewed (
                     reviewer_user_strapi_document_id,
                     duel_strapi_document_id,
                     user_strapi_document_id,
                     comment,
                     hash,
                     is_valid,
                     skill1_mark,
                     skill2_mark,
                     skill1_strapi_document_id,
                     skill2_strapi_document_id)
            VALUES
              (v_reviewer_user_strapi_document_id, v_duel_strapi_document_id,
               v_user_strapi_document_id,
               m_comment, v_hash, v_is_valid,
               m_skill1_mark1, m_skill2_mark1,
               v_skill1_strapi_document_id, v_skill2_strapi_document_id),

              (v_reviewer_user_strapi_document_id, v_duel_strapi_document_id,
               v_rival_user_strapi_document_id,
               m_comment, v_hash, v_is_valid,
               m_skill1_mark2, m_skill2_mark2,
               v_skill1_strapi_document_id, v_skill2_strapi_document_id);
        END IF;
    END IF;

/* ---------- 5. Итог ---------------------------------------------------*/
    IF v_error IS NULL THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', CASE
                              WHEN NOT v_is_valid
                                THEN 'отзыв записан, но is_valid = false: оценки обнулены'
                              ELSE 'отзыв успешно записан'
                            END);
    ELSIF v_mode = 'TEST' THEN
        RETURN json_build_object(
                 'result',  'success',
                 'message', 'отзыв записан с предупреждениями: '||v_error);
    ELSE
        RETURN json_build_object(
                 'result',  'error',
                 'message', v_error);
    END IF;
END;
