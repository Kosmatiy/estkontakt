DECLARE
    a_logs           TEXT[] := ARRAY[]::TEXT[];
    v_sprint         sprints%ROWTYPE;
    v_rows_ok        INT := 0;
    v_rows_conflict  INT := 0;
    v_deleted        BIGINT;
    rec_etr          RECORD;
    v_mark           INT;
BEGIN
    -- 0. Проверка режима
    in_mode := UPPER(COALESCE(in_mode,'CLEANSLATE'));
    IF in_mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN json_build_object('result','error','message','mode = CLEANSLATE | GOON');
    END IF;

    a_logs := array_append(a_logs,
              format('start: sprint=%s mode=%s',
                     in_sprint_strapi_document_id,in_mode));

    -- 1. Спринт
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = in_sprint_strapi_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object('result','error',
                                 'message',format('Спринт %s не найден',in_sprint_strapi_document_id),
                                 'logs',a_logs);
    END IF;

    a_logs := array_append(a_logs,
              format('sprint «%s»',
                     COALESCE(v_sprint.sprint_name,'?')));

    -- 2. Очистка (CLEANSLATE)
    IF in_mode = 'CLEANSLATE' THEN
        DELETE FROM expert_task_reviewed er
         USING tasks t
         JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
        WHERE er.task_strapi_document_id = t.strapi_document_id
          AND l.sprint_strapi_document_id = in_sprint_strapi_document_id;

        GET DIAGNOSTICS v_deleted = ROW_COUNT;
        a_logs := array_append(a_logs,
                  format('cleanslate: deleted %s old rows',v_deleted));
    END IF;

    -- 3. Загружаем expert_task_to_review
    DROP TABLE IF EXISTS _etr;
    CREATE TEMP TABLE _etr ON COMMIT DROP AS
    SELECT etr.*
      FROM expert_task_to_review etr
      JOIN tasks   t ON t.strapi_document_id = etr.task_strapi_document_id
      JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
     WHERE l.sprint_strapi_document_id = in_sprint_strapi_document_id;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    a_logs := array_append(a_logs,
              format('etr rows=%s',v_deleted));

    IF v_deleted = 0 THEN
        RETURN json_build_object('result','error',
                                 'message','Нет заданий на экспертную проверку',
                                 'logs',a_logs);
    END IF;

    -- 4. Основной цикл
    FOR rec_etr IN
        SELECT * FROM _etr ORDER BY random()
    LOOP
        -- 4.1. Случайная оценка: 1 и 4 по 10%; 2 и 3 по 40%
        SELECT CASE
                 WHEN rnd < 0.10 THEN 1
                 WHEN rnd < 0.50 THEN 2
                 WHEN rnd < 0.90 THEN 3
                 ELSE 4
               END
          INTO v_mark
          FROM (SELECT random() AS rnd) r;

        -- 4.2. Вставка
        INSERT INTO expert_task_reviewed(
            created_at,
            reviewer_user_strapi_document_id,
            reviewee_user_strapi_document_id,
            task_strapi_document_id,
            mark)
        VALUES( now(),
                rec_etr.reviewer_user_strapi_document_id,
                rec_etr.reviewee_user_strapi_document_id,
                rec_etr.task_strapi_document_id,
                v_mark )
        ON CONFLICT DO NOTHING;

        IF FOUND THEN
            v_rows_ok := v_rows_ok + 1;
        ELSE
            v_rows_conflict := v_rows_conflict + 1;
        END IF;
    END LOOP;

    -- 5. Результат
    a_logs := array_append(a_logs,
              format('done: inserted=%s conflicts=%s',
                     v_rows_ok,v_rows_conflict));

    RETURN json_build_object(
        'result'                 ,'success',
        'generated_records'      ,v_rows_ok,
        'skipped_due_to_conflict',v_rows_conflict,
        'logs'                   ,a_logs,
        'message'                ,format(
            'Спринт %s: добавлено %s строк, %s дублей, режим %s.',
            in_sprint_strapi_document_id,
            v_rows_ok,
            v_rows_conflict,
            in_mode)
    );

EXCEPTION
    WHEN OTHERS THEN
        a_logs := array_append(a_logs, SQLERRM);
        RETURN json_build_object('result','error',
                                 'message',SQLERRM,
                                 'logs',a_logs);
END;
