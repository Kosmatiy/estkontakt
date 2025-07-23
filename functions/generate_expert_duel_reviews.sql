DECLARE
    /* ── спринт / stream ────────────────────────────────────── */
    v_sprint        sprints%ROWTYPE;
    v_stream_id     TEXT;

    /* ── эксперты и веса ────────────────────────────────────── */
    total_weight    NUMERIC;
    v_expert_id     TEXT;
    rnd_val         NUMERIC;

    /* ── счётчик вставленных строк ──────────────────────────── */
    v_rows_inserted INT := 0;
    v_batch_cnt     INT;

    /* ── рабочие записи ─────────────────────────────────────── */
    rec_group       RECORD;
    duel_type       TEXT;

    /* ── случайные оценки (FULL-CONTACT) ───────────────────── */
    res_a INT; res_b INT;
    img_a INT; img_b INT;
    att_a INT; att_b INT;

    /* ── случайные оценки (TRAINING) ───────────────────────── */
    s1_a  INT; s2_a  INT;
    s1_b  INT; s2_b  INT;
BEGIN
    /* 0. Проверяем режим */
    mode := UPPER(COALESCE(mode,'CLEANSLATE'));
    IF mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN json_build_object('result','error',
                                 'message','mode = CLEANSLATE | GOON');
    END IF;

    /* 1. Спринт и stream */
    SELECT * INTO v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                                 'message',format('Спринт %s не найден',in_sprint_document_id));
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* 2. Эксперты выбранного stream */
    DROP TABLE IF EXISTS _experts;
    CREATE TEMP TABLE _experts ON COMMIT DROP AS
    SELECT
        expert_strapi_document_id AS expert_id,
        duel_workload::NUMERIC    AS weight
    FROM   expert_workload
    WHERE  stream_strapi_document_id = v_stream_id
      AND  duel_workload > 0;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                                 'message',format('Для stream %s нет экспертов с duel_workload > 0',v_stream_id));
    END IF;

    /* 2.1. Кумулятивные веса для взвешенного random */
    ALTER TABLE _experts ADD COLUMN cum_weight NUMERIC;
    UPDATE _experts e
    SET    cum_weight = sub.cum
    FROM  (
        SELECT expert_id,
               SUM(weight) OVER (ORDER BY expert_id) AS cum
        FROM   _experts
    ) sub
    WHERE sub.expert_id = e.expert_id;

    SELECT MAX(cum_weight) INTO total_weight FROM _experts;

    /* 3. Очистка при CLEANSLATE */
    IF mode = 'CLEANSLATE' THEN
        DELETE FROM expert_duel_reviewed er
        USING duels d
        WHERE d.sprint_strapi_document_id = in_sprint_document_id
          AND er.duel_strapi_document_id  = d.strapi_document_id;

        DELETE FROM expert_duel_to_review et
        USING duels d
        WHERE d.sprint_strapi_document_id = in_sprint_document_id
          AND et.duel_strapi_document_id  = d.strapi_document_id;
    END IF;

    /* 4. Формируем список пар пользователей (2 строки на дуэль) */
    DROP TABLE IF EXISTS _pairs;
    CREATE TEMP TABLE _pairs ON COMMIT DROP AS
    SELECT
        uda.duel_strapi_document_id      AS duel_id,
        uda.hash,
        d.type                            AS duel_type,
        MIN(uda.user_strapi_document_id)  AS user_a,
        MAX(uda.user_strapi_document_id)  AS user_b
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY user_strapi_document_id, hash, answer_part
            ORDER BY created_at DESC) AS rn
        FROM user_duel_answers
        WHERE COALESCE(status,'ok') = 'ok'
    ) uda
    JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE rn = 1
      AND d.sprint_strapi_document_id = in_sprint_document_id
    GROUP BY uda.duel_strapi_document_id, uda.hash, d.type
    HAVING COUNT(*) = 2;

    /* 5. Основной цикл по дуэлям */
    FOR rec_group IN
        SELECT * FROM _pairs
    LOOP
        duel_type := rec_group.duel_type;

        /* 5.1. Взвешенно выбираем эксперта */
        rnd_val := random() * total_weight;
        SELECT expert_id INTO v_expert_id
        FROM   _experts
        WHERE  rnd_val < cum_weight
        ORDER  BY cum_weight
        LIMIT  1;

        /* 5.2. При GOON пропускаем, если этот эксперт уже оценил пару */
        IF mode = 'GOON' THEN
            SELECT COUNT(*) INTO v_batch_cnt
            FROM   expert_duel_reviewed
            WHERE  reviewer_user_strapi_document_id = v_expert_id
              AND  duel_strapi_document_id          = rec_group.duel_id
              AND  hash                             = rec_group.hash;

            IF v_batch_cnt >= 2 THEN
                CONTINUE;
            END IF;
        END IF;

        /* 5.3. Генерируем оценки */
        IF duel_type = 'FULL-CONTACT' THEN
            IF floor(random()*2)::INT = 0 THEN res_a := 1; res_b := 0; ELSE res_a := 0; res_b := 1; END IF;
            IF floor(random()*2)::INT = 0 THEN img_a := 1; img_b := 0; ELSE img_a := 0; img_b := 1; END IF;
            IF floor(random()*2)::INT = 0 THEN att_a := 1; att_b := 0; ELSE att_a := 0; att_b := 1; END IF;

            INSERT INTO expert_duel_reviewed(
                reviewer_user_strapi_document_id, duel_strapi_document_id,
                user_strapi_document_id, comment, is_valid, hash,
                result_mark, image_mark, attention_mark)
            VALUES
            (v_expert_id, rec_group.duel_id, rec_group.user_a,
             'Expert auto comment (FULL-CONTACT)', TRUE, rec_group.hash,
             res_a, img_a, att_a),
            (v_expert_id, rec_group.duel_id, rec_group.user_b,
             'Expert auto comment (FULL-CONTACT)', TRUE, rec_group.hash,
             res_b, img_b, att_b)
            ON CONFLICT DO NOTHING;

        ELSE  -- TRAINING
            s1_a := floor(random()*3)::INT;
            s2_a := floor(random()*3)::INT;
            s1_b := floor(random()*3)::INT;
            s2_b := floor(random()*3)::INT;

            INSERT INTO expert_duel_reviewed(
                reviewer_user_strapi_document_id, duel_strapi_document_id,
                user_strapi_document_id, comment, is_valid, hash,
                skill1_mark, skill2_mark,
                skill1_strapi_document_id, skill2_strapi_document_id)
            VALUES
            (v_expert_id, rec_group.duel_id, rec_group.user_a,
             'Expert auto comment (TRAINING)', TRUE, rec_group.hash,
             s1_a, s2_a,
             'x32dfbgykyu92o6wb92nm50i','zk0vk04tbl4n7pe9s0w1py78'),
            (v_expert_id, rec_group.duel_id, rec_group.user_b,
             'Expert auto comment (TRAINING)', TRUE, rec_group.hash,
             s1_b, s2_b,
             'x32dfbgykyu92o6wb92nm50i','zk0vk04tbl4n7pe9s0w1py78')
            ON CONFLICT DO NOTHING;
        END IF;

        /* 5.4. Для интерфейса */
        INSERT INTO expert_duel_to_review(
            reviewer_user_strapi_document_id, duel_strapi_document_id,
            hash, user_strapi_document_id)
        VALUES
        (v_expert_id, rec_group.duel_id, rec_group.hash, rec_group.user_a),
        (v_expert_id, rec_group.duel_id, rec_group.hash, rec_group.user_b)
        ON CONFLICT DO NOTHING;

        GET DIAGNOSTICS v_batch_cnt = ROW_COUNT;
        v_rows_inserted := v_rows_inserted + v_batch_cnt;
    END LOOP;

    /* 6. Итог */
    RETURN json_build_object(
        'result'           , 'success',
        'generated_records', v_rows_inserted,
        'message'          , format(
            'Stream %s: экспертные оценки распределены по %s дуэль-записям (режим %s).',
            v_stream_id, v_rows_inserted, mode)
    );

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
        'result','error',
        'message', SQLERRM
    );
END;
