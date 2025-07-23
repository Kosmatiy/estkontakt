DECLARE
    /* ── спринт ─────────────────────────────────────────────── */
    v_sprint        sprints%ROWTYPE;
    v_stream_id     TEXT;

    /* ── счётчики ───────────────────────────────────────────── */
    v_total_rows    INT;
    v_skip_rows     INT;
    v_rows_done     INT := 0;
    v_batch_cnt     INT;

    /* ── список пропущенных для отчёта ─────────────────────── */
    v_skipped_ids   TEXT;

    /* ── рабочие переменные для оценок ─────────────────────── */
    new_result      INT;
    new_image       INT;
    new_attention   INT;
    new_skill1      INT;
    new_skill2      INT;

    /* ── курсор review-строк ───────────────────────────────── */
    rec             RECORD;
    duel_rec        RECORD;
    comp_rec        RECORD;      -- комплементарные оценки FC

    /* ── статический текст комментария ─────────────────────── */
    v_comment       TEXT;
BEGIN
    /* 0. ищем спринт */
    SELECT * INTO v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object(
          'result' ,'error',
          'message', format('Спринт %s не найден', in_sprint_document_id)
        );
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* 1. очистка при CLEANSLATE */
    IF mode = 'CLEANSLATE' THEN
        DELETE FROM user_duel_reviewed ur
        USING duels d
        WHERE ur.duel_strapi_document_id = d.strapi_document_id
          AND d.sprint_strapi_document_id = in_sprint_document_id;
    END IF;

    /* 2. собираем строки user_duel_to_review этого спринта
          и reviewer-ов нужного stream */
    CREATE TEMP TABLE _reviews ON COMMIT DROP AS
    SELECT t.*
    FROM   user_duel_to_review t
    JOIN   duels  d ON d.strapi_document_id = t.duel_strapi_document_id
    JOIN   users  u ON u.strapi_document_id = t.reviewer_user_strapi_document_id
    WHERE  d.sprint_strapi_document_id = in_sprint_document_id
      AND  u.stream_strapi_document_id = v_stream_id;

    SELECT COUNT(*) INTO v_total_rows FROM _reviews;

    IF v_total_rows = 0 THEN
        RETURN json_build_object(
          'result' ,'error',
          'message','Нет строк user_duel_to_review для этого спринта/потока'
        );
    END IF;

    /* 3. определяем, сколько пропускаем */
    v_skip_rows := CEIL(v_total_rows * in_percent / 100.0)::INT;

    CREATE TEMP TABLE _skip(id BIGINT PRIMARY KEY) ON COMMIT DROP;
    INSERT INTO _skip(id)
    SELECT id FROM _reviews ORDER BY random() LIMIT v_skip_rows;

    SELECT string_agg(id::text, ', ') INTO v_skipped_ids FROM _skip;

    /* 4. основной цикл */
    FOR rec IN
        SELECT * FROM _reviews ORDER BY random()
    LOOP
        /* пропуск? */
        IF EXISTS (SELECT 1 FROM _skip WHERE id = rec.id) THEN
            CONTINUE;
        END IF;

        /* GOON: запись уже есть? */
        IF mode = 'GOON' THEN
            PERFORM 1
            FROM   user_duel_reviewed r
            WHERE  r.reviewer_user_strapi_document_id = rec.reviewer_user_strapi_document_id
              AND  r.user_strapi_document_id         = rec.user_strapi_document_id
              AND  r.hash                            = rec.hash
              AND  r.duel_strapi_document_id         = rec.duel_strapi_document_id;
            IF FOUND THEN CONTINUE; END IF;
        END IF;

        /* берём дуэль */
        SELECT * INTO duel_rec
        FROM   duels
        WHERE  strapi_document_id = rec.duel_strapi_document_id
        LIMIT  1;

        IF NOT FOUND THEN CONTINUE; END IF;

        /* TRAINING или FULL-CONTACT */
        IF duel_rec.type = 'TRAINING' THEN
            new_skill1 := floor(random()*3)::INT;  -- 0-2
            new_skill2 := floor(random()*3)::INT;

            v_comment := format(
               'Auto TRAINING review. skill1=%s skill2=%s',
               new_skill1::TEXT, new_skill2::TEXT);

            INSERT INTO user_duel_reviewed(
                created_at, reviewer_user_strapi_document_id,
                duel_strapi_document_id, user_strapi_document_id,
                comment, is_valid, hash,
                result_mark, image_mark, attention_mark,
                skill1_mark, skill2_mark,
                skill1_strapi_document_id, skill2_strapi_document_id)
            VALUES(
                NOW(), rec.reviewer_user_strapi_document_id,
                rec.duel_strapi_document_id, rec.user_strapi_document_id,
                v_comment, TRUE, rec.hash,
                NULL,NULL,NULL,
                new_skill1, new_skill2,
                duel_rec.skill1_strapi_document_id,
                duel_rec.skill2_strapi_document_id)
            ON CONFLICT DO NOTHING;

        ELSIF duel_rec.type = 'FULL-CONTACT' THEN
            /* ищем уже существующие оценки по этому hash/duel */
            SELECT result_mark, image_mark, attention_mark
              INTO comp_rec
              FROM user_duel_reviewed
             WHERE duel_strapi_document_id = rec.duel_strapi_document_id
               AND hash = rec.hash
             LIMIT 1;

            IF FOUND THEN
                new_result    := 1 - COALESCE(comp_rec.result_mark,0);
                new_image     := 1 - COALESCE(comp_rec.image_mark,0);
                new_attention := 1 - COALESCE(comp_rec.attention_mark,0);
            ELSE
                new_result    := floor(random()*2)::INT; -- 0/1
                new_image     := floor(random()*2)::INT;
                new_attention := floor(random()*2)::INT;
            END IF;

            v_comment := format(
               'Auto FULL-CONTACT review. R=%s I=%s A=%s',
               new_result::TEXT, new_image::TEXT, new_attention::TEXT);

            INSERT INTO user_duel_reviewed(
                created_at, reviewer_user_strapi_document_id,
                duel_strapi_document_id, user_strapi_document_id,
                comment, is_valid, hash,
                result_mark, image_mark, attention_mark,
                skill1_mark, skill2_mark,
                skill1_strapi_document_id, skill2_strapi_document_id)
            VALUES(
                NOW(), rec.reviewer_user_strapi_document_id,
                rec.duel_strapi_document_id, rec.user_strapi_document_id,
                v_comment, TRUE, rec.hash,
                new_result, new_image, new_attention,
                NULL,NULL,NULL,NULL)
            ON CONFLICT DO NOTHING;
        END IF;

        GET DIAGNOSTICS v_batch_cnt = ROW_COUNT;
        v_rows_done := v_rows_done + v_batch_cnt;
    END LOOP;

    /* 5. JSON-ответ */
    RETURN json_build_object(
        'result'           , 'success',
        'generated_records', v_rows_done,
        'skipped_reviews'  , COALESCE(v_skipped_ids,'нет'),
        'message'          , format(
           'Студенческие ревью дуэлей: спринт «%s». Всего строк %s, пропущено %s (%s%%), создано %s, режим %s.',
           v_sprint.sprint_name,
           v_total_rows,
           v_skip_rows,
           in_percent::TEXT,
           v_rows_done,
           mode
        )
    );

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
        'result' ,'error',
        'message', SQLERRM
    );
END;
