DECLARE
    /* ── спринт / stream ─────────────────────────────────────── */
    v_sprint         sprints%ROWTYPE;
    v_stream_id      TEXT;

    /* ── режим (нельзя присваивать IN‑параметру) ─────────────── */
    v_mode           TEXT;

    /* ── счётчики результата ────────────────────────────────── */
    v_rows_inserted  INT := 0;   -- реально вставлено
    v_rows_conflict  INT := 0;   -- конфликтов ON CONFLICT
    v_empty_tasks    INT := 0;   -- заданий без «сдавших»

    /* ── курсоры ─────────────────────────────────────────────── */
    rec_task         RECORD;

    /* ── служебные ───────────────────────────────────────────── */
    v_eligible_students TEXT[];
    v_expected_rows     INT;
    v_batch_rows        INT;
BEGIN
/* ───────── 0. Валидация аргументов ───────────────────────────── */
    v_mode := COALESCE(upper(p_mode),'CLEANSLATE');
    IF v_mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN json_build_object('result','error',
                                 'message','mode = CLEANSLATE | GOON');
    END IF;

/* ───────── 1. Получаем спринт и stream ──────────────────────── */
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = p_sprint_strapi_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                                 'message',format('Спринт %s не найден',
                                                  p_sprint_strapi_document_id));
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

/* ───────── 2. eligible‑студенты (без страйков + не отчислены) ─ */
    DROP TABLE IF EXISTS tmp_strikes;
    CREATE TEMP TABLE tmp_strikes ON COMMIT DROP AS
       SELECT DISTINCT user_strapi_document_id
         FROM strikes
        WHERE sprint_strapi_document_id = p_sprint_strapi_document_id;

    DROP TABLE IF EXISTS tmp_students;
    CREATE TEMP TABLE tmp_students ON COMMIT DROP AS
       SELECT u.strapi_document_id AS user_id
         FROM users u
        WHERE u.stream_strapi_document_id = v_stream_id
          AND u.dismissed_at IS NULL
          AND NOT EXISTS (SELECT 1
                            FROM tmp_strikes s
                           WHERE s.user_strapi_document_id = u.strapi_document_id);

    SELECT array_agg(user_id)
      INTO v_eligible_students
      FROM tmp_students;

    IF v_eligible_students IS NULL THEN
        RETURN json_build_object('result','error',
                                 'message','Нет подходящих студентов в этом стриме');
    END IF;

/* ───────── 3. CLEANSLATE: очищаем старое распределение ──────── */
    IF v_mode = 'CLEANSLATE' THEN
        DELETE FROM user_task_to_review utr
        USING tasks t
        JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
        WHERE utr.task_strapi_document_id = t.strapi_document_id
          AND l.sprint_strapi_document_id = p_sprint_strapi_document_id;
    END IF;

/* ───────── 4. tmp_latest_answers: последние сдачи задач ─────── */
    DROP TABLE IF EXISTS tmp_latest_answers;
    CREATE TEMP TABLE tmp_latest_answers ON COMMIT DROP AS
    WITH ranked AS (
        SELECT a.*,
               ROW_NUMBER() OVER (
                 PARTITION BY a.user_strapi_document_id, a.task_strapi_document_id
                 ORDER BY a.created_at DESC) AS rn
          FROM user_task_answers a
          JOIN tasks    t ON t.strapi_document_id = a.task_strapi_document_id
          JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
         WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id
    )
    SELECT *
      FROM ranked
     WHERE rn = 1
       AND user_strapi_document_id = ANY(v_eligible_students);

/* ───────── offsets (1‑3) делаем ОДИН раз ─────────────────────── */
    DROP TABLE IF EXISTS _offsets;
    CREATE TEMP TABLE _offsets ON COMMIT DROP AS
        SELECT 1 AS offset UNION ALL SELECT 2 UNION ALL SELECT 3;

/* ───────── 5. Цикл по заданиям спринта ───────────────────────── */
    FOR rec_task IN
        SELECT t.strapi_document_id
          FROM tasks t
          JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
         WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id
    LOOP
        /* ——— сбор «сдавших» конкретное задание ————— */
        DROP TABLE IF EXISTS _task_students;
        CREATE TEMP TABLE _task_students ON COMMIT DROP AS
        SELECT la.user_strapi_document_id,
               ROW_NUMBER() OVER (ORDER BY la.user_strapi_document_id) AS rn,
               COUNT(*) OVER ()                                        AS n
          FROM tmp_latest_answers la
         WHERE la.task_strapi_document_id = rec_task.strapi_document_id;

        SELECT COUNT(*) INTO v_expected_rows FROM _task_students;
        IF v_expected_rows = 0 THEN
            v_empty_tasks := v_empty_tasks + 1;
            CONTINUE;
        END IF;

        /* ——— вставка распределения для задания ————— */
        INSERT INTO user_task_to_review(
             reviewer_user_strapi_document_id,
             reviewee_user_strapi_document_id,
             task_strapi_document_id,
             number_in_batch,
             control)
        SELECT
            s2.user_strapi_document_id,        -- reviewer
            s1.user_strapi_document_id,        -- reviewee
            rec_task.strapi_document_id,
            o.offset,
            s2.user_strapi_document_id || '_' ||
            rec_task.strapi_document_id || '_' ||
            o.offset                           AS control
        FROM _task_students s1
        CROSS JOIN _offsets o
        JOIN _task_students s2
          ON s2.rn = ((s1.rn - 1 + o.offset) % s1.n) + 1
        WHERE s1.user_strapi_document_id <> s2.user_strapi_document_id
        ON CONFLICT (control) DO NOTHING;

        GET DIAGNOSTICS v_batch_rows = ROW_COUNT;
        v_rows_inserted := v_rows_inserted + v_batch_rows;
        v_rows_conflict := v_rows_conflict + (3 * v_expected_rows - v_batch_rows);
    END LOOP;

/* ───────── 6. Итоговый JSON‑ответ ────────────────────────────── */
    RETURN json_build_object(
        'result'          ,'success',
        'inserted_rows'   ,v_rows_inserted,
        'skipped_conflict',v_rows_conflict,
        'empty_tasks'     ,v_empty_tasks,
        'message'         ,format(
           'Спринт %s: вставлено %s строк, %s конфликтов (уже были), пустых задач %s, режим %s.',
            p_sprint_strapi_document_id,
            v_rows_inserted,
            v_rows_conflict,
            v_empty_tasks,
            v_mode)
    );

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object('result','error','message',SQLERRM);
END;
