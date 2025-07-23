DECLARE
    v_sprint_id  TEXT;
    v_stream_id  TEXT;
    v_students_count INT;
    v_tasks_count INT;
    v_n INT;  -- число студентов
    v_total_inserted INT; 
BEGIN
    /*****************************************************************************
     A) Логируем старт
    *****************************************************************************/
    PERFORM log_message('=== distribute_tasks_for_sprint_circular: START for sprint='||p_sprint_strapi_document_id);

    /*****************************************************************************
     B) Ищем спринт + его поток
    *****************************************************************************/
    SELECT s.strapi_document_id,
           s.stream_strapi_document_id
      INTO v_sprint_id, v_stream_id
      FROM sprints s
     WHERE s.strapi_document_id = p_sprint_strapi_document_id
     LIMIT 1;

    IF v_sprint_id IS NULL THEN
       PERFORM log_message('No sprint found => STOP');
       RETURN;
    END IF;

    /*****************************************************************************
     C) Удаляем старые записи user_task_to_review (по задачам этого спринта)
        Чтобы не было дублей
    *****************************************************************************/
    DELETE FROM user_task_to_review utr
    USING tasks t
    JOIN lectures l
      ON l.strapi_document_id = t.lecture_strapi_document_id
    WHERE utr.task_strapi_document_id = t.strapi_document_id
      AND l.sprint_strapi_document_id = p_sprint_strapi_document_id;

    PERFORM log_message('Old user_task_to_review records removed for sprint='||p_sprint_strapi_document_id);

    /*****************************************************************************
     D) Фильтруем студентов: stream=v_stream_id, dismissed_at IS NULL
        (Доп. условия можно добавить, если надо исключать страйки и т.п.)
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_students;
    CREATE TEMP TABLE tmp_students ON COMMIT DROP AS
    SELECT u.*
      FROM users u
     WHERE u.stream_strapi_document_id = v_stream_id
       AND u.dismissed_at IS NULL;

    SELECT COUNT(*) INTO v_students_count FROM tmp_students;
    PERFORM log_message('Found '||v_students_count||' active students in stream='||v_stream_id);

    /*****************************************************************************
     E) Упорядочим этих студентов => row_number() => tmp_students_sorted
        Это нужно для «кругового» метода (idx=1..N).
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_students_sorted;
    CREATE TEMP TABLE tmp_students_sorted ON COMMIT DROP AS
    SELECT 
      u.*,
      ROW_NUMBER() OVER (ORDER BY u.strapi_document_id) AS idx  -- упорядочили
    FROM tmp_students u;

    SELECT COUNT(*) INTO v_n FROM tmp_students_sorted;  -- N
    IF v_n<1 THEN
       PERFORM log_message('No eligible students => STOP');
       RETURN;
    END IF;

    /*****************************************************************************
     F) Находим tasks в данном спринте 
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_lectures;
    CREATE TEMP TABLE tmp_lectures ON COMMIT DROP AS
    SELECT l.*
      FROM lectures l
     WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id;

    DROP TABLE IF EXISTS tmp_tasks;
    CREATE TEMP TABLE tmp_tasks ON COMMIT DROP AS
    SELECT t.*
      FROM tmp_lectures lec
      JOIN tasks t ON t.lecture_strapi_document_id = lec.strapi_document_id;

    SELECT COUNT(*) INTO v_tasks_count FROM tmp_tasks;
    PERFORM log_message('Found '||v_tasks_count||' tasks in this sprint='||p_sprint_strapi_document_id);

    /*****************************************************************************
     G) Выбрать ПОСЛЕДНИЕ user_task_answers (по (user,task)) => tmp_latest_user_task_answers
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_usertaskanswers_raw;
    CREATE TEMP TABLE tmp_usertaskanswers_raw ON COMMIT DROP AS
    SELECT *
      FROM user_task_answers uta
     WHERE uta.task_strapi_document_id IN (SELECT strapi_document_id FROM tmp_tasks);

    DROP TABLE IF EXISTS tmp_latest_user_task_answers;
    CREATE TEMP TABLE tmp_latest_user_task_answers ON COMMIT DROP AS
    WITH cte_rn AS (
       SELECT uta.*,
              ROW_NUMBER() OVER(
                PARTITION BY uta.user_strapi_document_id, uta.task_strapi_document_id
                ORDER BY uta.created_at DESC
              ) as rn
         FROM tmp_usertaskanswers_raw uta
    )
    SELECT *
      FROM cte_rn
     WHERE rn=1;

    PERFORM log_message('Selected latest user_task_answers per (user,task)');

    /*****************************************************************************
     H) Для кругового распределения нам нужно (author, task).
        Проверяем только студентов, которые в tmp_students_sorted.
        Создадим tmp_author_tasks (author, task, idx_author).
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_author_tasks;
    CREATE TEMP TABLE tmp_author_tasks ON COMMIT DROP AS
    WITH cte_auth AS (
      SELECT DISTINCT 
        la.user_strapi_document_id AS author, 
        la.task_strapi_document_id AS task
      FROM tmp_latest_user_task_answers la
    ),
    cte_join AS (
      SELECT 
        cte_auth.author,
        cte_auth.task,
        s.idx AS idx_author
      FROM cte_auth
      JOIN tmp_students_sorted s 
        ON s.strapi_document_id = cte_auth.author
      -- тут implicitly отсекаем тех авторов, кто не в tmp_students_sorted
    )
    SELECT * FROM cte_join;

    /*****************************************************************************
     I) Вставим (author,task) => 3 reviewers (shift=1..3) => круговой
        idx_reviewer = ((idx_author + shift -1) mod v_n) +1
        Исключая случай reviewer=author, 
        хотя при shift=1..3 (и n>3) такое совпадение не возникнет,
        но на всякий случай проверим strapi_document_id.
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_circular_assignments;
    CREATE TEMP TABLE tmp_circular_assignments ON COMMIT DROP AS
    WITH shifts AS (
      SELECT 1 AS shift
      UNION ALL SELECT 2
      UNION ALL SELECT 3
    ),
    cte_assign AS (
      SELECT 
        a.author,
        a.task,
        a.idx_author,
        s.shift,
        /* Цикличный индекс: mod N */
        ((a.idx_author + s.shift - 1) % v_n) + 1 AS idx_reviewer
      FROM tmp_author_tasks a
      CROSS JOIN shifts s
    ),
    cte_join2 AS (
      SELECT 
        c.author,
        c.task,
        c.shift AS number_in_batch,
        r.strapi_document_id AS reviewer_user_strapi_document_id,
        c.author AS reviewee_user_strapi_document_id
      FROM cte_assign c
      JOIN tmp_students_sorted r 
        ON r.idx = c.idx_reviewer
      WHERE r.strapi_document_id <> c.author
    )
    SELECT 
      reviewer_user_strapi_document_id,
      reviewee_user_strapi_document_id,
      task AS task_strapi_document_id,
      number_in_batch,
      -- control: уникальный ключ
      reviewer_user_strapi_document_id || '_' ||
      reviewee_user_strapi_document_id || '_' ||
      task || '_batch' || number_in_batch AS control
    FROM cte_join2;

    /*****************************************************************************
     J) Заносим в user_task_to_review (UPSERT по control)
    *****************************************************************************/
    DELETE FROM tmp_circular_assignments WHERE control IS NULL;

    SELECT COUNT(*) INTO v_total_inserted FROM tmp_circular_assignments;
    PERFORM log_message('total new circular assignments='||v_total_inserted);

    IF v_total_inserted>0 THEN
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
      FROM tmp_circular_assignments
      ON CONFLICT (control) DO UPDATE
        SET
          reviewer_user_strapi_document_id = EXCLUDED.reviewer_user_strapi_document_id,
          reviewee_user_strapi_document_id = EXCLUDED.reviewee_user_strapi_document_id,
          task_strapi_document_id          = EXCLUDED.task_strapi_document_id,
          number_in_batch                  = EXCLUDED.number_in_batch;

      PERFORM log_message('Upserted '||v_total_inserted||' rows into user_task_to_review');
    ELSE
      PERFORM log_message('No assignments => no distribution done');
    END IF;

    /*****************************************************************************
     K) Финальное сообщение
    *****************************************************************************/
    PERFORM log_message(format(
      '=== distribute_tasks_for_sprint_circular DONE for sprint=%s => total=%s ===',
      p_sprint_strapi_document_id,
      v_total_inserted
    ));
END;
