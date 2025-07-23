DECLARE
    /* ── лог ─────────────────────────────────────────────────── */
    a_logs           TEXT[] := ARRAY[]::TEXT[];

    /* ── спринт / stream ─────────────────────────────────────── */
    v_sprint         sprints%ROWTYPE;
    v_stream_id      TEXT;

    /* ── счётчики ────────────────────────────────────────────── */
    v_rows_ok        INT := 0;
    v_rows_conflict  INT := 0;

    /* «фейлеры» ------------------------------------------------ */
    v_total_reviewers INT;
    v_fail_cnt        INT;
    v_failed_list     TEXT;

    /* вспомогательные переменные ------------------------------ */
    v_fail_left INT;
    v_marks     INT[];

    /* курсор по UTR                                            */
    rec_utr     RECORD;

    v_deleted   BIGINT;
BEGIN
/* ─── 0. Проверка аргументов ────────────────────────────────── */
    IF in_fail_percent < 0 OR in_fail_percent > 100 THEN
        RETURN json_build_object('result','error',
                                 'message','fail_percent должен быть 0-100');
    END IF;

    in_mode := UPPER(COALESCE(in_mode,'CLEANSLATE'));
    IF in_mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN json_build_object('result','error',
                                 'message','mode = CLEANSLATE | GOON');
    END IF;

    a_logs := array_append(a_logs,
              format('start: sprint=%s mode=%s fail%%=%s',
                     in_sprint_strapi_document_id,in_mode,in_fail_percent));

/* ─── 1. Спринт и stream ────────────────────────────────────── */
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = in_sprint_strapi_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object(
                'result' ,'error',
                'message',format('Спринт %s не найден',in_sprint_strapi_document_id),
                'logs'   ,a_logs);
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    a_logs := array_append(a_logs,
              format('sprint «%s», stream=%s',
                     COALESCE(v_sprint.sprint_name,'?'),v_stream_id));

/* ─── 2. Очистка (CLEANSLATE) ───────────────────────────────── */
    IF in_mode = 'CLEANSLATE' THEN
        DELETE FROM user_task_reviewed ur
         USING tasks t
         JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
        WHERE ur.task_strapi_document_id = t.strapi_document_id
          AND l.sprint_strapi_document_id = in_sprint_strapi_document_id;

        GET DIAGNOSTICS v_deleted = ROW_COUNT;
        a_logs := array_append(a_logs,
                  format('cleanslate: deleted %s old rows',v_deleted));
    END IF;

/* ─── 3. Загружаем user_task_to_review спринта ---------------- */
    DROP TABLE IF EXISTS _utr;
    CREATE TEMP TABLE _utr ON COMMIT DROP AS
    SELECT utr.*
      FROM user_task_to_review utr
      JOIN users   u ON u.strapi_document_id = utr.reviewer_user_strapi_document_id
      JOIN tasks   t ON t.strapi_document_id = utr.task_strapi_document_id
      JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
     WHERE l.sprint_strapi_document_id = in_sprint_strapi_document_id
       AND u.stream_strapi_document_id = v_stream_id;

    SELECT COUNT(*) ,
           COUNT(DISTINCT reviewer_user_strapi_document_id)
      INTO v_deleted, v_total_reviewers        -- v_deleted здесь = кол-во строк UTR
      FROM _utr;

    a_logs := array_append(a_logs,
              format('utr rows=%s reviewers=%s',v_deleted,v_total_reviewers));

    IF v_total_reviewers = 0 THEN
        RETURN json_build_object('result','error',
                                 'message','Нет ревьюеров нужного stream',
                                 'logs',a_logs);
    END IF;

/* ─── 4. «Несдавшие» ревьюеры --------------------------------- */
    v_fail_cnt := CEIL(v_total_reviewers * in_fail_percent / 100.0);

    DROP TABLE IF EXISTS _fail_reviewers;
    CREATE TEMP TABLE _fail_reviewers ON COMMIT DROP AS
    SELECT reviewer_id,
           CASE WHEN random()<0.5 THEN 1 ELSE 2 END AS fails_left
    FROM (
            SELECT reviewer_user_strapi_document_id AS reviewer_id,
                   random() AS rnd
            FROM   _utr
            GROUP  BY reviewer_user_strapi_document_id
         ) s
    ORDER BY rnd
    LIMIT v_fail_cnt;

    a_logs := array_append(a_logs,
              format('fail reviewers picked=%s',v_fail_cnt));

/* ─── 5. Основной цикл по UTR --------------------------------- */
    FOR rec_utr IN
        SELECT * FROM _utr ORDER BY random()
    LOOP
        /* 5.1. Проверка на «фейлера» */
        SELECT fails_left
          INTO v_fail_left
          FROM _fail_reviewers
         WHERE reviewer_id = rec_utr.reviewer_user_strapi_document_id;

        IF FOUND AND v_fail_left > 0 THEN
            UPDATE _fail_reviewers
               SET fails_left = fails_left-1
             WHERE reviewer_id = rec_utr.reviewer_user_strapi_document_id;
            CONTINUE;
        END IF;

        /* 5.2. Три уникальные оценки */
        SELECT ARRAY(
               SELECT val FROM unnest('{1,2,3,4}'::INT[]) val
               ORDER BY random() LIMIT 3)
          INTO v_marks;

        /* 5.3. Вставка оценки */
        INSERT INTO user_task_reviewed(
            created_at,
            reviewer_user_strapi_document_id,
            reviewee_user_strapi_document_id,
            task_strapi_document_id,
            mark,
            number_in_batch)
        VALUES( now(),
                rec_utr.reviewer_user_strapi_document_id,
                rec_utr.reviewee_user_strapi_document_id,
                rec_utr.task_strapi_document_id,
                v_marks[rec_utr.number_in_batch],
                rec_utr.number_in_batch )
        ON CONFLICT DO NOTHING;

        IF FOUND THEN
            v_rows_ok := v_rows_ok + 1;
        ELSE
            v_rows_conflict := v_rows_conflict + 1;
        END IF;
    END LOOP;

/* ─── 6. Человекочитаемый список «фейлеров» ------------------- */
    SELECT string_agg(
             format('%s %s (@%s)',
                    COALESCE(name,''),COALESCE(surname,''),
                    COALESCE(telegram_username,'')), ', ')
      INTO v_failed_list
      FROM users
     WHERE strapi_document_id IN (SELECT reviewer_id FROM _fail_reviewers);

    v_failed_list := COALESCE(v_failed_list,'нет');

/* ─── 7. Финальный ответ -------------------------------------- */
    a_logs := array_append(a_logs,
              format('done: inserted=%s conflicts=%s',
                     v_rows_ok,v_rows_conflict));

    RETURN json_build_object(
        'result'                 ,'success',
        'generated_records'      ,v_rows_ok,
        'skipped_due_to_conflict',v_rows_conflict,
        'failed_groups'          ,v_failed_list,
        'logs'                   ,a_logs,
        'message'                ,format(
            'Спринт %s: добавлено %s строк, %s дублей, «фейлеров» %s, режим %s.',
            in_sprint_strapi_document_id,
            v_rows_ok,
            v_rows_conflict,
            v_fail_cnt,
            in_mode)
    );

EXCEPTION
    WHEN OTHERS THEN
        a_logs := array_append(a_logs, SQLERRM);
        RETURN json_build_object('result','error',
                                 'message',SQLERRM,
                                 'logs',a_logs);
END;
