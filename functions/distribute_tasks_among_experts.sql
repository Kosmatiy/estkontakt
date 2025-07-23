DECLARE
    /* ── сведения о спринте/стриме ───────────────────────────── */
    v_sprint         sprints%ROWTYPE;
    v_stream_id      TEXT;

    /* ── счётчики результата ─────────────────────────────────── */
    v_rows_inserted  INT := 0;
    v_rows_conflict  INT := 0;
    v_total_tasks    INT := 0;
    v_total_experts  INT := 0;

    /* ── массивы экспертов ───────────────────────────────────── */
    a_exp_ids   TEXT[]  := '{}';   -- id экспертов
    a_exp_load  FLOAT[] := '{}';   -- workload
    a_exp_quota INT[]   := '{}';   -- итоговая квота

    /* ── курсор по задачам ───────────────────────────────────── */
    c_tasks  REFCURSOR;
    r_task   RECORD;

    /* ── рабочие ─────────────────────────────────────────────── */
    sum_workload FLOAT := 0;
    v_leftover   INT;
    idx          INT;
BEGIN
/* 0. режим ------------------------------------------------------ */
    in_mode := UPPER(COALESCE(in_mode,'CLEANSLATE'));
    IF in_mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN json_build_object('result','error',
                                 'message','mode = CLEANSLATE | GOON');
    END IF;

/* 1. спринт и его stream --------------------------------------- */
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = in_sprint_strapi_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                'message',format('Спринт %s не найден',
                                 in_sprint_strapi_document_id));
    END IF;

    v_stream_id := v_sprint.stream_strapi_document_id;

/* 2. задачи спринта -------------------------------------------- */
    DROP TABLE IF EXISTS _tasks;
    CREATE TEMP TABLE _tasks ON COMMIT DROP AS
    SELECT DISTINCT
           uta.answer_id,
           uta.user_strapi_document_id AS reviewee_id,
           uta.task_strapi_document_id AS task_id
      FROM user_task_answers uta
      JOIN tasks    t  ON t.strapi_document_id = uta.task_strapi_document_id
      JOIN lectures l  ON l.strapi_document_id = t.lecture_strapi_document_id
     WHERE l.sprint_strapi_document_id = in_sprint_strapi_document_id;

    SELECT COUNT(*) INTO v_total_tasks FROM _tasks;
    IF v_total_tasks = 0 THEN
        RETURN json_build_object('result','error',
                                 'message','Нет ответов в этом спринте');
    END IF;

/* 3. эксперты текущего stream ---------------------------------- */
    DROP TABLE IF EXISTS _experts;
    CREATE TEMP TABLE _experts ON COMMIT DROP AS
    SELECT e.strapi_document_id             AS expert_id,
           COALESCE(ew.task_workload,0)::FLOAT AS workload
      FROM expert_workload ew
      JOIN experts e ON e.strapi_document_id = ew.expert_strapi_document_id
     WHERE ew.stream_strapi_document_id = v_stream_id
       AND e.dismissed_at IS NULL
       AND COALESCE(ew.task_workload,0) > 0;

    SELECT COUNT(*) INTO v_total_experts FROM _experts;
    IF v_total_experts = 0 THEN
        RETURN json_build_object('result','error',
               'message','В стриме нет активных экспертов с workload>0');
    END IF;

/* 4. массивы + суммы ------------------------------------------- */
    SELECT ARRAY_AGG(expert_id  ORDER BY expert_id),
           ARRAY_AGG(workload   ORDER BY expert_id),
           SUM(workload)
      INTO a_exp_ids,
           a_exp_load,
           sum_workload
      FROM _experts;

/* 5. целые квоты ------------------------------------------------ */
    a_exp_quota := ARRAY[]::INT[];
    FOR idx IN 1..v_total_experts LOOP
        a_exp_quota := a_exp_quota ||
            FLOOR(v_total_tasks * (a_exp_load[idx]/sum_workload))::INT;
    END LOOP;

    /* распределяем остаток */
    SELECT v_total_tasks - SUM(q) INTO v_leftover
      FROM UNNEST(a_exp_quota) q;

    IF v_leftover > 0 THEN
        DROP TABLE IF EXISTS _frac;
        CREATE TEMP TABLE _frac(idx INT, frac FLOAT);

        FOR idx IN 1..v_total_experts LOOP
            INSERT INTO _frac VALUES(
                idx,
                v_total_tasks*(a_exp_load[idx]/sum_workload) - a_exp_quota[idx]);
        END LOOP;

        FOR idx IN
            SELECT idx
              FROM _frac
             ORDER BY frac DESC, idx
             LIMIT v_leftover
        LOOP
            a_exp_quota[idx] := a_exp_quota[idx] + 1;
        END LOOP;
    END IF;

/* 6. очистка ---------------------------------------------------- */
    IF in_mode = 'CLEANSLATE' THEN
        DELETE FROM expert_task_to_review etr
         USING tasks t
         JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
        WHERE etr.task_strapi_document_id = t.strapi_document_id
          AND l.sprint_strapi_document_id = in_sprint_strapi_document_id;
    END IF;

/* 7. распределение --------------------------------------------- */
    OPEN c_tasks FOR SELECT * FROM _tasks ORDER BY answer_id;

    FOR idx IN 1..v_total_experts LOOP
        DECLARE
            need INT := a_exp_quota[idx];
            exp  TEXT := a_exp_ids[idx];
            i    INT;
        BEGIN
            FOR i IN 1..need LOOP
                FETCH c_tasks INTO r_task;
                EXIT WHEN NOT FOUND;

                INSERT INTO expert_task_to_review(
                    reviewer_user_strapi_document_id,
                    reviewee_user_strapi_document_id,
                    task_strapi_document_id,
                    control)
                VALUES(
                    exp,
                    r_task.reviewee_id,
                    r_task.task_id,
                    exp || '_' || r_task.task_id || '_' || i)
                ON CONFLICT DO NOTHING;

                IF FOUND THEN
                    v_rows_inserted := v_rows_inserted + 1;
                ELSE
                    v_rows_conflict := v_rows_conflict + 1;
                END IF;
            END LOOP;
        END;
    END LOOP;

    CLOSE c_tasks;

/* 8. итог ------------------------------------------------------- */
    RETURN json_build_object(
        'result'                ,'success',
        'inserted_rows'         ,v_rows_inserted,
        'skipped_due_to_conflict',v_rows_conflict,
        'total_tasks'           ,v_total_tasks,
        'total_experts'         ,v_total_experts,
        'message'               ,format(
           'Sprint %s: распределено %s/%s задач между %s экспертами (режим %s).',
            in_sprint_strapi_document_id,
            v_rows_inserted,
            v_total_tasks,
            v_total_experts,
            in_mode)
    );

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object('result','error','message',SQLERRM);
END;
