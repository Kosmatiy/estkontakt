DECLARE
  v_count_students INT;
  v_count_tasks    INT;
  v_count_answers  INT;
BEGIN
  /* 1) Лог: старт, очистка логов */
  PERFORM clear_distribution_logs();
  PERFORM log_message('   logs cleared');

  /* Удаляем старые записи user_task_to_review для этого спринта */
  DELETE FROM user_task_to_review utr
   USING tasks t
   JOIN lectures l
     ON l.strapi_document_id = t.lecture_strapi_document_id
   WHERE utr.task_strapi_document_id = t.strapi_document_id
     AND l.sprint_strapi_document_id = p_sprint_strapi_document_id;
  PERFORM log_message('   old user_task_to_review records removed for this sprint');


  /*****************************************************************************
   ШАГ 1: Собираем всех студентов
  *****************************************************************************/
  CREATE TEMP TABLE tmp_students AS
  WITH this_sprint AS (
    SELECT s.*
      FROM sprints s
     WHERE s.strapi_document_id = p_sprint_strapi_document_id
     LIMIT 1
  )
  SELECT u.*
    FROM users u
    JOIN this_sprint sp
      ON sp.stream_strapi_document_id = u.stream_strapi_document_id
    WHERE u.dismissed_at IS NULL
      AND NOT EXISTS (
        SELECT 1
          FROM strikes st
         WHERE st.user_strapi_document_id = u.strapi_document_id
           AND st.sprint_strapi_document_id = p_sprint_strapi_document_id
      );

  SELECT COUNT(*) INTO v_count_students FROM tmp_students;
  PERFORM log_message(format('   found %s eligible students for sprint', v_count_students));

  IF v_count_students=0 THEN
    PERFORM log_message('   no students => stop distribution');
    RETURN;
  END IF;


  /*****************************************************************************
   ШАГ 2: Собираем все задачи (через lectures)
  *****************************************************************************/
  CREATE TEMP TABLE tmp_tasks AS
  SELECT t.*
    FROM tasks t
    JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
   WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id;

  SELECT COUNT(*) INTO v_count_tasks FROM tmp_tasks;
  PERFORM log_message(format('   found %s tasks for sprint', v_count_tasks));

  IF v_count_tasks=0 THEN
    PERFORM log_message('   no tasks => stop distribution');
    RETURN;
  END IF;


  /*****************************************************************************
   ШАГ 3: Собираем user_task_answers
  *****************************************************************************/
  CREATE TEMP TABLE tmp_user_solutions AS
  SELECT ans.*
    FROM user_task_answers ans
    JOIN tmp_tasks tk
      ON tk.strapi_document_id = ans.task_strapi_document_id;

  SELECT COUNT(*) INTO v_count_answers FROM tmp_user_solutions;
  PERFORM log_message(format(
    '   found %s user_task_answers relevant to tasks in this sprint', 
    v_count_answers
  ));


  /*****************************************************************************
   ШАГ 4: Распределяем: (каждый студент) x (каждая задача) => 3 (или меньше) авторов
  *****************************************************************************/

  CREATE TEMP TABLE tmp_review_assignments(
    reviewer_user_strapi_document_id TEXT,
    reviewee_user_strapi_document_id TEXT,
    task_strapi_document_id TEXT,
    number_in_batch INT,
    control TEXT
  );

  DECLARE
    rec_stu   RECORD;  -- текущий студент
    rec_task  RECORD;  -- текущая задача
    rec_auth  RECORD;  -- автор решения
    i         INT;     -- batchIndex 1..3
    v_control TEXT;
    c_authors INT;
  BEGIN
    FOR rec_stu IN (SELECT * FROM tmp_students) LOOP
      FOR rec_task IN (SELECT * FROM tmp_tasks) LOOP
        /* Сколько авторов, кроме самого rec_stu? */
        SELECT COUNT(*) 
          INTO c_authors
          FROM tmp_user_solutions sol
         WHERE sol.task_strapi_document_id = rec_task.strapi_document_id
           AND sol.user_strapi_document_id <> rec_stu.strapi_document_id;

        IF c_authors=0 THEN
          /* Никто, кроме этого студента, задачу не сдавал => пропуск */
          CONTINUE;
        END IF;

        /* Берём 3 случайных автора (или меньше, если авторов <3) */
        i := 1;
        FOR rec_auth IN (
          SELECT sol.user_strapi_document_id AS author_id
            FROM tmp_user_solutions sol
           WHERE sol.task_strapi_document_id = rec_task.strapi_document_id
             AND sol.user_strapi_document_id <> rec_stu.strapi_document_id
           ORDER BY random()
           LIMIT 3
        )
        LOOP
          v_control := rec_stu.strapi_document_id
                       || '_' || rec_task.strapi_document_id
                       || '_batch' || i::text;

          INSERT INTO tmp_review_assignments(
            reviewer_user_strapi_document_id,
            reviewee_user_strapi_document_id,
            task_strapi_document_id,
            number_in_batch,
            control
          )
          VALUES(
            rec_stu.strapi_document_id,
            rec_auth.author_id,
            rec_task.strapi_document_id,
            i,
            v_control
          );
          i := i + 1;
        END LOOP;  -- rec_auth
      END LOOP;  -- rec_task
    END LOOP;  -- rec_stu
  END;

  /*****************************************************************************
   ШАГ 5: Вставляем (UPSERT) в user_task_to_review
  *****************************************************************************/
  PERFORM log_message(format(
    '   total new assignments to insert: %s',
    (SELECT COUNT(*) FROM tmp_review_assignments)
  ));

  IF (SELECT COUNT(*) FROM tmp_review_assignments) > 0 THEN
    INSERT INTO user_task_to_review(
      reviewer_user_strapi_document_id,
      reviewee_user_strapi_document_id,
      task_strapi_document_id,
      number_in_batch,
      control
    )
    SELECT
      reviewer_user_strapi_document_id,
      reviewee_user_strapi_document_id,
      task_strapi_document_id,
      number_in_batch,
      control
    FROM tmp_review_assignments
    ON CONFLICT (control) DO UPDATE
      SET
        reviewer_user_strapi_document_id = EXCLUDED.reviewer_user_strapi_document_id,
        reviewee_user_strapi_document_id = EXCLUDED.reviewee_user_strapi_document_id,
        task_strapi_document_id          = EXCLUDED.task_strapi_document_id,
        number_in_batch                  = EXCLUDED.number_in_batch;

    PERFORM log_message('   all assignments inserted (UPSERT)');
  ELSE
    PERFORM log_message('   no assignments to insert');
  END IF;

  /*****************************************************************************
   ШАГ 6: Итоговый лог
  *****************************************************************************/
  PERFORM log_message(format(
    '=== distribute_all_tasks_reviews_for_sprint DONE for sprint=%s ===',
    p_sprint_strapi_document_id
  ));

END;
