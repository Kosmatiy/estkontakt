DECLARE
    /* ── добавлено для проверки времени события ─────────────── */
    v_now      TIMESTAMPTZ := clock_timestamp();
    v_event    RECORD;

    /* ── существующие переменные (не изменялись) ────────────── */
    v_count_active INT := 0;
    v_count_failed INT := 0;
    v_user1_had_base   BOOLEAN := FALSE;
    v_user2_had_base   BOOLEAN := FALSE;
    v_has_base_user1   BOOLEAN := FALSE;
    v_has_base_user2   BOOLEAN := FALSE;
    v_user1_ref_count  INT := 0;
    v_user2_ref_count  INT := 0;
    v_sprint_number    INT := 0;
    v_message          TEXT;
    v_strike_message_user1 TEXT := '';
    v_strike_message_user2 TEXT := '';
    v_user1_new_strike BOOLEAN := FALSE;
    v_user2_new_strike BOOLEAN := FALSE;
    rec  RECORD;
    rec2 RECORD;
BEGIN
    /*── 0. Проверка «окна события» (только REGULAR) ────────────*/
    IF p_mode <> 'TEST' THEN
        SELECT *
          INTO v_event
          FROM events
         WHERE sprint_strapi_document_id = p_sprint_id
           AND sprint_phase = 3
         LIMIT 1;

        IF v_event IS NULL THEN
            RETURN jsonb_build_object(
              'status','error',
              'message','Событие sprint_phase = 3 не найдено.'
            );
        END IF;

        IF v_now < v_event.datetime_start THEN
            RETURN jsonb_build_object(
              'status','error',
              'message','Событие ещё не началось — отказ невозможен.'
            );
        ELSIF v_event.datetime_end - v_now < INTERVAL '6 hours' THEN
            RETURN jsonb_build_object(
              'status','error',
              'message','До конца события осталось менее шести часов — отказ запрещён.'
            );
        END IF;
    END IF;
    -------------------------------------------------------------------------
    -- 1) Определяем номер спринта (для текстов сообщений)
    -------------------------------------------------------------------------
    SELECT s.sprint_number
      INTO v_sprint_number
      FROM sprints s
     WHERE s.strapi_document_id = p_sprint_id
     LIMIT 1;

    IF v_sprint_number IS NULL THEN
       v_sprint_number := 0;
    END IF;

    PERFORM log_message(
        format(
            'handle_refusal: start. sprint_id=%s(#%s), duel=%s, user1=%s, user2=%s, initiator=%s, skip_increments=%s, mode=%s',
            p_sprint_id, v_sprint_number, p_duel_id, p_user1, p_user2, p_initiator, p_skip_refusal_increments, p_mode
        )
    );

    -------------------------------------------------------------------------
    -- 2) Проверяем, есть ли «живая» (is_failed=FALSE) запись в duel_distributions
    --    для пары (p_user1, p_user2) в p_duel_id.
    -------------------------------------------------------------------------
    SELECT COUNT(*)
      INTO v_count_active
      FROM duel_distributions dd
     WHERE dd.duel_strapi_document_id = p_duel_id
       AND dd.is_failed = FALSE
       AND (
           (dd.user_strapi_document_id = p_user1 AND dd.rival_strapi_document_id = p_user2)
        OR (dd.user_strapi_document_id = p_user2 AND dd.rival_strapi_document_id = p_user1)
       );

    IF v_count_active = 0 THEN
        -- Может, она уже is_failed=TRUE
        SELECT COUNT(*)
          INTO v_count_failed
          FROM duel_distributions dd
         WHERE dd.duel_strapi_document_id = p_duel_id
           AND dd.is_failed = TRUE
           AND (
               (dd.user_strapi_document_id = p_user1 AND dd.rival_strapi_document_id = p_user2)
            OR (dd.user_strapi_document_id = p_user2 AND dd.rival_strapi_document_id = p_user1)
           );

        IF v_count_failed > 0 THEN
            PERFORM log_message('handle_refusal => уже была отменена');
            RETURN jsonb_build_object(
                'status', 'error',
                'message', 'Схватка уже была отменена.'
            );
        ELSE
            PERFORM log_message('handle_refusal => схватка не найдена => return');
            RETURN jsonb_build_object(
                'status', 'error',
                'message', 'Схватка не найдена или не существует. Свяжитесь с организаторами.'
            );
        END IF;
    END IF;

    -------------------------------------------------------------------------
    -- 3) Ставим is_failed=TRUE, fail_initiator=p_initiator
    -------------------------------------------------------------------------
    UPDATE duel_distributions
       SET is_failed      = TRUE,
           fail_initiator = p_initiator
     WHERE duel_strapi_document_id = p_duel_id
       AND is_failed      = FALSE
       AND (
            (user_strapi_document_id = p_user1 AND rival_strapi_document_id = p_user2)
         OR (user_strapi_document_id = p_user2 AND rival_strapi_document_id = p_user1)
       );

    PERFORM log_message('handle_refusal => перевели схватку в is_failed=TRUE');

    -------------------------------------------------------------------------
    -- 4) Проверяем, была ли base (is_extra=FALSE) => сбрасываем is_chosen=FALSE
    -------------------------------------------------------------------------
    DROP TABLE IF EXISTS tmp_base_rows;
    CREATE TEMP TABLE tmp_base_rows AS
    SELECT user_strapi_document_id AS uid,
           rival_strapi_document_id AS rid
      FROM duel_distributions
     WHERE duel_strapi_document_id = p_duel_id
       AND is_failed = TRUE
       AND is_extra = FALSE
       AND (
           (user_strapi_document_id = p_user1 AND rival_strapi_document_id = p_user2)
        OR (user_strapi_document_id = p_user2 AND rival_strapi_document_id = p_user1)
       );

    IF EXISTS (SELECT 1 FROM tmp_base_rows WHERE uid = p_user1) THEN
        v_user1_had_base := TRUE;
        PERFORM set_is_chosen(p_duel_id, p_user1, FALSE);
        PERFORM log_message('   user1 had base => set_is_chosen=FALSE');
    END IF;
    IF EXISTS (SELECT 1 FROM tmp_base_rows WHERE uid = p_user2) THEN
        v_user2_had_base := TRUE;
        PERFORM set_is_chosen(p_duel_id, p_user2, FALSE);
        PERFORM log_message('   user2 had base => set_is_chosen=FALSE');
    END IF;

    -------------------------------------------------------------------------
    -- 5) Проверяем, остались ли живые (не-failed) базовые схватки у user1 / user2
    -------------------------------------------------------------------------
    SELECT EXISTS(
      SELECT 1
        FROM duel_distributions dd
       WHERE dd.duel_strapi_document_id = p_duel_id
         AND dd.is_failed=FALSE
         AND dd.is_extra=FALSE
         AND dd.user_strapi_document_id = p_user1
    )
    INTO v_has_base_user1;

    SELECT EXISTS(
      SELECT 1
        FROM duel_distributions dd
       WHERE dd.duel_strapi_document_id = p_duel_id
         AND dd.is_failed=FALSE
         AND dd.is_extra=FALSE
         AND dd.user_strapi_document_id = p_user2
    )
    INTO v_has_base_user2;

    -------------------------------------------------------------------------
    -- 6) Если нет base, но есть extra => переводим extra->base
    -------------------------------------------------------------------------
    IF NOT v_has_base_user1 THEN
        IF EXISTS (
            SELECT 1
              FROM duel_distributions dd
             WHERE dd.duel_strapi_document_id = p_duel_id
               AND dd.is_failed=FALSE
               AND dd.is_extra=TRUE
               AND dd.user_strapi_document_id = p_user1
        ) THEN
            UPDATE duel_distributions
               SET is_extra = FALSE
             WHERE duel_strapi_document_id = p_duel_id
               AND is_failed=FALSE
               AND is_extra=TRUE
               AND user_strapi_document_id = p_user1;
            v_has_base_user1 := TRUE;
            PERFORM log_message('   user1 extra->base');
        END IF;
    END IF;

    IF NOT v_has_base_user2 THEN
        IF EXISTS (
            SELECT 1
              FROM duel_distributions dd
             WHERE dd.duel_strapi_document_id = p_duel_id
               AND dd.is_failed=FALSE
               AND dd.is_extra=TRUE
               AND dd.user_strapi_document_id = p_user2
        ) THEN
            UPDATE duel_distributions
               SET is_extra = FALSE
             WHERE duel_strapi_document_id = p_duel_id
               AND is_failed=FALSE
               AND is_extra=TRUE
               AND user_strapi_document_id = p_user2;
            v_has_base_user2 := TRUE;
            PERFORM log_message('   user2 extra->base');
        END IF;
    END IF;

    -------------------------------------------------------------------------
    -- 7) Инкрементируем счётчики, если skip_increments=FALSE. Иначе просто читаем тек.значение.
    -------------------------------------------------------------------------
    IF NOT p_skip_refusal_increments THEN
        /* user1 */
        WITH upd AS (
            UPDATE user_duels_refusal_count
               SET refusal_count = refusal_count + 1,
                   updated_at    = now()
             WHERE sprint_strapi_document_id = p_sprint_id
               AND user_strapi_document_id   = p_user1
             RETURNING refusal_count
        )
        INSERT INTO user_duels_refusal_count(
            sprint_strapi_document_id, user_strapi_document_id, refusal_count
        )
        SELECT p_sprint_id, p_user1, 1
         WHERE NOT EXISTS (SELECT 1 FROM upd);

        SELECT refusal_count
          INTO v_user1_ref_count
          FROM user_duels_refusal_count
         WHERE sprint_strapi_document_id = p_sprint_id
           AND user_strapi_document_id   = p_user1
         LIMIT 1;

        IF v_user1_ref_count = 5 THEN
            v_user1_new_strike := TRUE;
            PERFORM log_message(
              format('user1=%s, sprint=%s(#%s) достиг 5 отказов => strike', p_user1, p_sprint_id, v_sprint_number)
            );
            INSERT INTO strikes(type, comment, user_strapi_document_id, sprint_strapi_document_id, created_at)
            SELECT 'REFUSAL',
                   format('Было совершено 5 отказов на спринте №%s', v_sprint_number),
                   p_user1,
                   p_sprint_id,
                   now()
             WHERE NOT EXISTS(
                SELECT 1
                  FROM strikes s
                 WHERE s.user_strapi_document_id   = p_user1
                   AND s.sprint_strapi_document_id = p_sprint_id
                   AND s.type = 'REFUSAL'
             );
            v_strike_message_user1 := format(
              'Вы совершили 5 отказов на спринте №%s => Вам выписан Страйк, вы больше не участвуете в распределении.',
              v_sprint_number
            );
        ELSIF v_user1_ref_count > 5 THEN
            v_strike_message_user1 := format(
              'Вы превысили лимит (≥5) отказов на спринте №%s => Страйк уже в силе, распределение для вас недоступно.',
              v_sprint_number
            );
        END IF;

        /* user2 */
        WITH upd AS (
            UPDATE user_duels_refusal_count
               SET refusal_count = refusal_count + 1,
                   updated_at    = now()
             WHERE sprint_strapi_document_id = p_sprint_id
               AND user_strapi_document_id   = p_user2
             RETURNING refusal_count
        )
        INSERT INTO user_duels_refusal_count(
            sprint_strapi_document_id, user_strapi_document_id, refusal_count
        )
        SELECT p_sprint_id, p_user2, 1
         WHERE NOT EXISTS (SELECT 1 FROM upd);

        SELECT refusal_count
          INTO v_user2_ref_count
          FROM user_duels_refusal_count
         WHERE sprint_strapi_document_id = p_sprint_id
           AND user_strapi_document_id   = p_user2
         LIMIT 1;

        IF v_user2_ref_count = 5 THEN
            v_user2_new_strike := TRUE;
            PERFORM log_message(
              format('user2=%s, sprint=%s(#%s) достиг 5 отказов => strike', p_user2, p_sprint_id, v_sprint_number)
            );
            INSERT INTO strikes(type, comment, user_strapi_document_id, sprint_strapi_document_id, created_at)
            SELECT 'REFUSAL',
                   format('Было совершено 5 отказов на спринте №%s', v_sprint_number),
                   p_user2,
                   p_sprint_id,
                   now()
             WHERE NOT EXISTS(
                SELECT 1
                  FROM strikes s
                 WHERE s.user_strapi_document_id   = p_user2
                   AND s.sprint_strapi_document_id = p_sprint_id
                   AND s.type = 'REFUSAL'
             );
            v_strike_message_user2 := format(
              'Вы совершили 5 отказов на спринте №%s => Вам выписан Страйк, вы больше не участвуете в распределении.',
              v_sprint_number
            );
        ELSIF v_user2_ref_count > 5 THEN
            v_strike_message_user2 := format(
              'Вы превысили лимит (≥5) отказов на спринте №%s => Страйк уже в силе, распределение для вас недоступно.',
              v_sprint_number
            );
        END IF;
    ELSE
        -- skip_increments=TRUE => просто считываем текущее
        SELECT refusal_count
          INTO v_user1_ref_count
          FROM user_duels_refusal_count
         WHERE sprint_strapi_document_id = p_sprint_id
           AND user_strapi_document_id   = p_user1
         LIMIT 1;
        IF v_user1_ref_count IS NULL THEN
           v_user1_ref_count := 0;
        END IF;
        IF v_user1_ref_count >= 5 THEN
           v_strike_message_user1 := format(
             'У вас уже ≥5 отказов на спринте №%s => Страйк действует, распределение недоступно.',
             v_sprint_number
           );
        END IF;

        SELECT refusal_count
          INTO v_user2_ref_count
          FROM user_duels_refusal_count
         WHERE sprint_strapi_document_id = p_sprint_id
           AND user_strapi_document_id   = p_user2
         LIMIT 1;
        IF v_user2_ref_count IS NULL THEN
           v_user2_ref_count := 0;
        END IF;
        IF v_user2_ref_count >= 5 THEN
           v_strike_message_user2 := format(
             'У вас уже ≥5 отказов на спринте №%s => Страйк действует, распределение недоступно.',
             v_sprint_number
           );
        END IF;
    END IF;

    -------------------------------------------------------------------------
    -- 8) Если userX_new_strike => выбиваем его из оставшихся схваток +
    --    (дополнительно фиксируем несыгранные дуэли, как в process_dual_send_event)
    -------------------------------------------------------------------------
    IF v_user1_new_strike THEN
        PERFORM log_message(format('STRIKE => kick user1=%s from all active in sprint=%s', p_user1, p_sprint_id));

        /* 8а) Обработка активных дуэлей */
        FOR rec IN 
            SELECT dd.duel_strapi_document_id AS d_id,
                   dd.user_strapi_document_id  AS u1,
                   dd.rival_strapi_document_id AS u2
              FROM duel_distributions dd
              JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
             WHERE d.sprint_strapi_document_id = p_sprint_id
               AND dd.is_failed=FALSE
               AND (dd.user_strapi_document_id = p_user1 OR dd.rival_strapi_document_id = p_user1)
        LOOP
            UPDATE duel_distributions
               SET is_failed=TRUE,
                   fail_initiator='SYSTEM_STRIKE'
             WHERE duel_strapi_document_id = rec.d_id
               AND is_failed=FALSE
               AND (
                  (user_strapi_document_id = rec.u1 AND rival_strapi_document_id = rec.u2)
               OR (user_strapi_document_id = rec.u2 AND rival_strapi_document_id = rec.u1)
               );

            IF rec.u1 = p_user1 THEN
                PERFORM handle_refusal(
                  p_sprint_id,
                  rec.d_id,
                  rec.u1,
                  rec.u2,
                  'SYSTEM_STRIKE',
                  TRUE
                );
            ELSE
                PERFORM handle_refusal(
                  p_sprint_id,
                  rec.d_id,
                  rec.u2,
                  rec.u1,
                  'SYSTEM_STRIKE',
                  TRUE
                );
            END IF;
        END LOOP;

        /* 8б) Обработка дуэлей, где ответа нет – фиксируем несыгранную дуэль */
        FOR rec2 IN
            SELECT dd.duel_strapi_document_id AS d_id,
                   dd.user_strapi_document_id  AS a_user,
                   dd.rival_strapi_document_id AS a_rival,
                   d.duel_number
              FROM duel_distributions dd
              JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
              LEFT JOIN user_duel_answers uda 
                     ON uda.duel_strapi_document_id = dd.duel_strapi_document_id
                     AND uda.user_strapi_document_id = dd.user_strapi_document_id
                     AND uda.rival_user_strapi_document_id = dd.rival_strapi_document_id
             WHERE d.sprint_strapi_document_id = p_sprint_id
               AND dd.is_failed=FALSE
               AND (dd.user_strapi_document_id = p_user1 OR dd.rival_strapi_document_id = p_user1)
               AND uda.duel_strapi_document_id IS NULL
        LOOP
            UPDATE duel_distributions
               SET is_failed=TRUE,
                   fail_initiator='SYSTEM_STRIKE'
             WHERE duel_strapi_document_id = rec2.d_id
               AND is_failed=FALSE
               AND (
                  (user_strapi_document_id = rec2.a_user AND rival_strapi_document_id = rec2.a_rival)
               OR (user_strapi_document_id = rec2.a_rival AND rival_strapi_document_id = rec2.a_user)
               );

            INSERT INTO strikes(type, comment, user_strapi_document_id, sprint_strapi_document_id, created_at)
            SELECT 'DUEL_SEND',
                   format('Дуэль %s не была сыграна.', rec2.duel_number),
                   rec2.a_user,
                   p_sprint_id,
                   now()
             WHERE NOT EXISTS(
                SELECT 1
                  FROM strikes s
                 WHERE s.user_strapi_document_id = rec2.a_user
                   AND s.sprint_strapi_document_id = p_sprint_id
                   AND s.type = 'DUEL_SEND'
                   AND s.comment = format('Дуэль %s не была сыграна.', rec2.duel_number)
             );
        END LOOP;
    END IF;  -- v_user1_new_strike

    IF v_user2_new_strike THEN
        PERFORM log_message(format('STRIKE => kick user2=%s from all active in sprint=%s', p_user2, p_sprint_id));

        /* 8а) Обработка активных дуэлей */
        FOR rec IN
            SELECT dd.duel_strapi_document_id AS d_id,
                   dd.user_strapi_document_id  AS u1,
                   dd.rival_strapi_document_id AS u2
              FROM duel_distributions dd
              JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
             WHERE d.sprint_strapi_document_id = p_sprint_id
               AND dd.is_failed=FALSE
               AND (dd.user_strapi_document_id = p_user2 OR dd.rival_strapi_document_id = p_user2)
        LOOP
            UPDATE duel_distributions
               SET is_failed=TRUE,
                   fail_initiator='SYSTEM_STRIKE'
             WHERE duel_strapi_document_id = rec.d_id
               AND is_failed=FALSE
               AND (
                  (user_strapi_document_id = rec.u1 AND rival_strapi_document_id = rec.u2)
               OR (user_strapi_document_id = rec.u2 AND rival_strapi_document_id = rec.u1)
               );

            IF rec.u1 = p_user2 THEN
                PERFORM handle_refusal(
                  p_sprint_id,
                  rec.d_id,
                  rec.u1,
                  rec.u2,
                  'SYSTEM_STRIKE',
                  TRUE
                );
            ELSE
                PERFORM handle_refusal(
                  p_sprint_id,
                  rec.d_id,
                  rec.u2,
                  rec.u1,
                  'SYSTEM_STRIKE',
                  TRUE
                );
            END IF;
        END LOOP;

        /* 8б) Обработка дуэлей без ответа */
        FOR rec2 IN
            SELECT dd.duel_strapi_document_id AS d_id,
                   dd.user_strapi_document_id  AS a_user,
                   dd.rival_strapi_document_id AS a_rival,
                   d.duel_number
              FROM duel_distributions dd
              JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
              LEFT JOIN user_duel_answers uda 
                     ON uda.duel_strapi_document_id = dd.duel_strapi_document_id
                     AND uda.user_strapi_document_id = dd.user_strapi_document_id
                     AND uda.rival_user_strapi_document_id = dd.rival_strapi_document_id
             WHERE d.sprint_strapi_document_id = p_sprint_id
               AND dd.is_failed=FALSE
               AND (dd.user_strapi_document_id = p_user2 OR dd.rival_strapi_document_id = p_user2)
               AND uda.duel_strapi_document_id IS NULL
        LOOP
            UPDATE duel_distributions
               SET is_failed=TRUE,
                   fail_initiator='SYSTEM_STRIKE'
             WHERE duel_strapi_document_id = rec2.d_id
               AND is_failed=FALSE
               AND (
                  (user_strapi_document_id = rec2.a_user AND rival_strapi_document_id = rec2.a_rival)
               OR (user_strapi_document_id = rec2.a_rival AND rival_strapi_document_id = rec2.a_user)
               );

            INSERT INTO strikes(type, comment, user_strapi_document_id, sprint_strapi_document_id, created_at)
            SELECT 'DUEL_SEND',
                   format('Дуэль %s не была сыграна.', rec2.duel_number),
                   rec2.a_user,
                   p_sprint_id,
                   now()
             WHERE NOT EXISTS(
                SELECT 1
                  FROM strikes s
                 WHERE s.user_strapi_document_id = rec2.a_user
                   AND s.sprint_strapi_document_id = p_sprint_id
                   AND s.type = 'DUEL_SEND'
                   AND s.comment = format('Дуэль %s не была сыграна.', rec2.duel_number)
             );
        END LOOP;
    END IF;  -- v_user2_new_strike

    -------------------------------------------------------------------------
    -- 9) Если (NOT p_skip_refusal_increments), пользователь <5 отказов, потерял base => try_to_find_opponent_for_base(late=TRUE).
    -------------------------------------------------------------------------
    IF NOT p_skip_refusal_increments THEN
        IF v_user1_had_base
           AND (NOT v_has_base_user1)
           AND (v_user1_ref_count < 5)
           AND (NOT v_user1_new_strike)
        THEN
            PERFORM try_to_find_opponent_for_base(
              p_duel_id,
              p_user1,
              p_sprint_id,
              TRUE
            );
        END IF;

        IF v_user2_had_base
           AND (NOT v_has_base_user2)
           AND (v_user2_ref_count < 5)
           AND (NOT v_user2_new_strike)
        THEN
            PERFORM try_to_find_opponent_for_base(
              p_duel_id,
              p_user2,
              p_sprint_id,
              TRUE
            );
        END IF;
    END IF;

    -------------------------------------------------------------------------
    -- 10) Формируем ответ
    -------------------------------------------------------------------------
    v_message := format(
      'Отмена схватки прошла успешно на спринте №%s. (skip_increments=%s).',
      v_sprint_number,
      p_skip_refusal_increments
    );

    RETURN jsonb_build_object(
        'status', 'success',
        'message', v_message,
        'user1_refusals', jsonb_build_object(
            'count', v_user1_ref_count,
            'left_before_strike', CASE
                                     WHEN v_user1_ref_count >= 5 THEN 0
                                     ELSE 5 - v_user1_ref_count
                                  END,
            'strike_message', v_strike_message_user1
        ),
        'user2_refusals', jsonb_build_object(
            'count', v_user2_ref_count,
            'left_before_strike', CASE
                                     WHEN v_user2_ref_count >= 5 THEN 0
                                     ELSE 5 - v_user2_ref_count
                                  END,
            'strike_message', v_strike_message_user2
        )
    );
END;
