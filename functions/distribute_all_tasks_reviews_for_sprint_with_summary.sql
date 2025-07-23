DECLARE
  v_count_students INT;
  v_count_tasks    INT;
  v_count_answers  INT;
  v_reviewer_violations INT;
  v_answer_violations   INT;
BEGIN
  /*****************************************************************************
   ШАГ A: ОЧИСТКА ЛОГОВ, УДАЛЕНИЕ ПРЕЖНИХ user_task_to_review
  *****************************************************************************/
  PERFORM clear_distribution_logs();
  PERFORM log_message('logs cleared');

  DELETE FROM user_task_to_review utr
   USING tasks t
   JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
   WHERE utr.task_strapi_document_id = t.strapi_document_id
     AND l.sprint_strapi_document_id = p_sprint_strapi_document_id;
  PERFORM log_message('old user_task_to_review records removed for this sprint');


  /*****************************************************************************
   ШАГ B: СОБИРАЕМ СТУДЕНТОВ (не отчислены, без страйков, stream совпадает)
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
  PERFORM log_message(format('found %s eligible students for sprint', v_count_students));

  IF v_count_students=0 THEN
    PERFORM log_message('no students => stop distribution');
    RETURN;
  END IF;


  /*****************************************************************************
   ШАГ C: ЗАДАЧИ (lecture -> sprint)
  *****************************************************************************/
  CREATE TEMP TABLE tmp_tasks AS
  SELECT t.*
    FROM tasks t
    JOIN lectures l
      ON l.strapi_document_id = t.lecture_strapi_document_id
   WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id;

  SELECT COUNT(*) INTO v_count_tasks FROM tmp_tasks;
  PERFORM log_message(format('found %s tasks for sprint', v_count_tasks));

  IF v_count_tasks=0 THEN
    PERFORM log_message('no tasks => stop distribution');
    RETURN;
  END IF;


  /*****************************************************************************
   ШАГ D: user_task_answers
  *****************************************************************************/
  CREATE TEMP TABLE tmp_user_solutions AS
  SELECT ans.*
    FROM user_task_answers ans
    JOIN tmp_tasks tk
      ON tk.strapi_document_id = ans.task_strapi_document_id;

  SELECT COUNT(*) INTO v_count_answers FROM tmp_user_solutions;
  PERFORM log_message(format(
    'found %s user_task_answers relevant to tasks in this sprint',
    v_count_answers
  ));


  /*****************************************************************************
   ШАГ E: РАСПРЕДЕЛЕНИЕ (каждый студент × каждая задача => берём 3 авторов)
  *****************************************************************************/
  CREATE TEMP TABLE tmp_review_assignments(
    reviewer_user_strapi_document_id TEXT,
    reviewee_user_strapi_document_id TEXT,
    task_strapi_document_id TEXT,
    number_in_batch INT,
    control TEXT
  );

  DECLARE
    rec_stu   RECORD;  -- студент
    rec_task  RECORD;  -- задача
    rec_auth  RECORD;  -- автор решения
    i         INT;
    v_control TEXT;
    c_authors INT;
  BEGIN
    FOR rec_stu IN (SELECT * FROM tmp_students) LOOP
      FOR rec_task IN (SELECT * FROM tmp_tasks) LOOP
        /* Сколько есть авторов у этой задачи (кроме rec_stu) */
        SELECT COUNT(*)
          INTO c_authors
          FROM tmp_user_solutions sol
         WHERE sol.task_strapi_document_id = rec_task.strapi_document_id
           AND sol.user_strapi_document_id <> rec_stu.strapi_document_id;

        IF c_authors=0 THEN
          -- Никто, кроме проверяющего, не сдал => пропуск
          CONTINUE;
        END IF;

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
        END LOOP;
      END LOOP;
    END LOOP;
  END;


  /*****************************************************************************
   ШАГ F: ВСТАВКА (UPSERT) В user_task_to_review
  *****************************************************************************/
  PERFORM log_message(format(
    'total new assignments to insert: %s',
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

    PERFORM log_message('all assignments inserted (UPSERT)');
  ELSE
    PERFORM log_message('no assignments to insert');
  END IF;


  /*****************************************************************************
   ШАГ G1: ПРОВЕРКА (REVIEWER-BASED) — У КАЖДОГО (reviewer, task) ДОЛЖНО БЫТЬ РОВНО 3
  *****************************************************************************/
  CREATE TEMP TABLE tmp_reviewer_violations AS
  SELECT
    r.reviewer_user_strapi_document_id AS reviewer,
    r.task_strapi_document_id          AS task,
    COUNT(*) AS real_count
  FROM user_task_to_review r
    JOIN tasks t
      ON t.strapi_document_id = r.task_strapi_document_id
    JOIN lectures l
      ON l.strapi_document_id = t.lecture_strapi_document_id
  WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id
  GROUP BY r.reviewer_user_strapi_document_id, r.task_strapi_document_id
  HAVING COUNT(*) <> 3;

  SELECT COUNT(*) INTO v_reviewer_violations FROM tmp_reviewer_violations;

  IF v_reviewer_violations = 0 THEN
    PERFORM log_message('no order violations (reviewer-based) found => all good');
  ELSE
    PERFORM log_message(format(
      'found %s order violations => details below',
      v_reviewer_violations
    ));

    DECLARE
      rec_violation RECORD;
    BEGIN
      FOR rec_violation IN (
        SELECT *
        FROM tmp_reviewer_violations
        ORDER BY reviewer, task
      )
      LOOP
        PERFORM log_message(format(
          'Reviewer=%s, Task=%s => has %s checks (expected=3)',
          rec_violation.reviewer,
          rec_violation.task,
          rec_violation.real_count
        ));
      END LOOP;
    END;
  END IF;


  /*****************************************************************************
   ШАГ G2: ПРОВЕРКА (ANSWER-BASED) — КАЖДЫЙ (author, task) ДОЛЖЕН БЫТЬ ПРОВЕРЕН 3 РАЗА
  *****************************************************************************/
  /* Взятие всех user_task_answers (из tmp_user_solutions),
     и сравнение с user_task_to_review (reviewee= author). */
  CREATE TEMP TABLE tmp_answer_violations AS
  SELECT
    a.user_strapi_document_id AS answer_author,
    a.task_strapi_document_id AS answer_task,
    COUNT(r.*) AS real_count
  FROM tmp_user_solutions a
  LEFT JOIN user_task_to_review r
         ON r.reviewee_user_strapi_document_id = a.user_strapi_document_id
        AND r.task_strapi_document_id = a.task_strapi_document_id
  GROUP BY
    a.user_strapi_document_id,
    a.task_strapi_document_id
  HAVING COUNT(r.*) <> 3;

  SELECT COUNT(*) INTO v_answer_violations FROM tmp_answer_violations;

  IF v_answer_violations = 0 THEN
    PERFORM log_message('no coverage violations (answer-based) found => all good');
  ELSE
    PERFORM log_message(format(
      'found %s coverage violations => details below',
      v_answer_violations
    ));

    DECLARE
      rec_ansvio RECORD;
    BEGIN
      FOR rec_ansvio IN (
        SELECT *
        FROM tmp_answer_violations
        ORDER BY answer_author, answer_task
      )
      LOOP
        PERFORM log_message(format(
          'AnswerAuthor=%s, Task=%s => has %s checks (expected=3)',
          rec_ansvio.answer_author,
          rec_ansvio.answer_task,
          rec_ansvio.real_count
        ));
      END LOOP;
    END;
  END IF;


  /*****************************************************************************
   ШАГ H: ИТОГОВОЕ СООБЩЕНИЕ
  *****************************************************************************/
  PERFORM log_message(format(
    '=== distribute_all_tasks_reviews_for_sprint_with_summary DONE for sprint=%s ===',
    p_sprint_strapi_document_id
  ));
END;
