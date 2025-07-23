DECLARE
    v_sprint_id   TEXT;
    v_stream_id   TEXT;

    -- Счётчики для логирования
    v_students_count INT;
    v_lectures_count INT;
    v_tasks_count INT;
    v_tests_count INT;
BEGIN
    /*****************************************************************************
     A) ЛОГИРУЕМ НАЧАЛО / ОЧИСТИМ distribution_logs ПРИ ЖЕЛАНИИ
    *****************************************************************************/
    -- Если хотите очищать логи каждый раз -- раскомментируйте строку:
    -- TRUNCATE TABLE distribution_logs RESTART IDENTITY;
    PERFORM log_message('=== distribute_all_for_sprint: start for sprint='||p_sprint_strapi_document_id);

    /*****************************************************************************
     B) СЧИТЫВАЕМ СПРИНТ
    *****************************************************************************/
    SELECT s.strapi_document_id,
           s.stream_strapi_document_id
      INTO v_sprint_id, v_stream_id
      FROM sprints s
     WHERE s.strapi_document_id = p_sprint_strapi_document_id
     LIMIT 1;

    IF v_sprint_id IS NULL THEN
       PERFORM log_message('No sprint found => stop');
       RETURN;
    END IF;

    /*****************************************************************************
     C) УДАЛЯЕМ СТАРЫЕ user_task_to_review (по этому спринту),
        ЧТОБЫ НЕ БЫЛО ДУБЛЕЙ
    *****************************************************************************/
    DELETE FROM user_task_to_review utr
    USING tasks t
    JOIN lectures l
      ON l.strapi_document_id = t.lecture_strapi_document_id
    WHERE utr.task_strapi_document_id = t.strapi_document_id
      AND l.sprint_strapi_document_id = p_sprint_strapi_document_id;

    PERFORM log_message('old user_task_to_review records removed for this sprint');


    /*****************************************************************************
     D) ФИЛЬТРУЕМ СТУДЕНТОВ:
        - stream_strapi_document_id=v_stream_id
        - dismissed_at IS NULL
        - (опционально: нет страйка за sprint=p_sprint_strapi_document_id)
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_students;
    CREATE TEMP TABLE tmp_students ON COMMIT DROP AS
    SELECT u.*
      FROM users u
     WHERE u.stream_strapi_document_id = v_stream_id
       AND u.dismissed_at IS NULL;


    SELECT COUNT(*) INTO v_students_count FROM tmp_students;
    PERFORM log_message('found '||v_students_count||' eligible students for sprint');


    /*****************************************************************************
     E) НАХОДИМ ЛЕКЦИИ, ЗАДАЧИ, ТЕСТЫ (по этому спринту)
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_lectures;
    CREATE TEMP TABLE tmp_lectures ON COMMIT DROP AS
    SELECT l.*
      FROM lectures l
     WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id;

    SELECT COUNT(*) INTO v_lectures_count FROM tmp_lectures;

    DROP TABLE IF EXISTS tmp_tasks;
    CREATE TEMP TABLE tmp_tasks ON COMMIT DROP AS
    SELECT t.*
      FROM tmp_lectures L
      JOIN tasks t
        ON t.lecture_strapi_document_id = l.strapi_document_id;

    SELECT COUNT(*) INTO v_tasks_count FROM tmp_tasks;

    DROP TABLE IF EXISTS tmp_tests;
    CREATE TEMP TABLE tmp_tests ON COMMIT DROP AS
    SELECT tst.*
      FROM tmp_lectures L
      JOIN tests tst
        ON tst.lecture_strapi_document_id = l.strapi_document_id;

    SELECT COUNT(*) INTO v_tests_count FROM tmp_tests;

    PERFORM log_message(format('found %s lectures, %s tasks, %s tests for sprint',
      v_lectures_count, v_tasks_count, v_tests_count));

    /*****************************************************************************
     F) ОЧИСТКА ДУБЛЕЙ user_task_answers => берём самую ПОЗДНЮЮ версию
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

    PERFORM log_message(format('user_task_answers duplicates cleaned => kept only latest version'));

    /*****************************************************************************
     G) ОЧИСТКА ДУБЛЕЙ user_test_answers => (если тоже нужно)
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_usertestanswers_raw;
    CREATE TEMP TABLE tmp_usertestanswers_raw ON COMMIT DROP AS
    SELECT uta.*
      FROM user_test_answers uta
     WHERE uta.test_strapi_document_id IN (SELECT strapi_document_id FROM tmp_tests);

    DROP TABLE IF EXISTS tmp_latest_user_test_answers;
    CREATE TEMP TABLE tmp_latest_user_test_answers ON COMMIT DROP AS
    WITH cte_rn AS (
       SELECT uta.*,
              ROW_NUMBER() OVER(
                PARTITION BY uta.user_strapi_document_id, uta.test_strapi_document_id
                ORDER BY uta.created_at DESC
              ) as rn
         FROM tmp_usertestanswers_raw uta
    )
    SELECT *
      FROM cte_rn
     WHERE rn=1;

    /*****************************************************************************
     H) Выдача страйков: если студент из tmp_students НЕ выполнил
        какое-то задание (из tmp_tasks) или тест (из tmp_tests).
        (В ТЗ: «Если нет хотя бы одного, пишем strike»)
    *****************************************************************************/
    DECLARE
      rec_stu RECORD;
      v_incomplete_tasks TEXT[];
      v_incomplete_tests TEXT[];
    BEGIN
      FOR rec_stu IN (SELECT * FROM tmp_students) LOOP

         /* 1) Проверим, какие tasks студент выполнил */
         DECLARE
           v_all_task_ids TEXT[];
         BEGIN
           SELECT array_agg(strapi_document_id::text)
             INTO v_all_task_ids
             FROM tmp_tasks;

           -- Удаляем, если уже запускали
           DROP TABLE IF EXISTS tmp_single_user_tasks;
           CREATE TEMP TABLE tmp_single_user_tasks ON COMMIT DROP AS
           SELECT DISTINCT la.task_strapi_document_id
             FROM tmp_latest_user_task_answers la
            WHERE la.user_strapi_document_id = rec_stu.strapi_document_id;

           DROP TABLE IF EXISTS tmp_missing_t;
           CREATE TEMP TABLE tmp_missing_t ON COMMIT DROP AS
           SELECT unnest(v_all_task_ids) AS tid
            EXCEPT
           SELECT task_strapi_document_id FROM tmp_single_user_tasks;

           SELECT array_agg(tid) INTO v_incomplete_tasks
             FROM tmp_missing_t;
           IF v_incomplete_tasks IS NULL THEN
             v_incomplete_tasks := '{}';
           END IF;
         END;

         /* 2) Аналогично для tests */
         DECLARE
           v_all_test_ids TEXT[];
         BEGIN
           SELECT array_agg(strapi_document_id::text)
             INTO v_all_test_ids
             FROM tmp_tests;

           DROP TABLE IF EXISTS tmp_single_user_tests;
           CREATE TEMP TABLE tmp_single_user_tests ON COMMIT DROP AS
           SELECT DISTINCT lta.test_strapi_document_id
             FROM tmp_latest_user_test_answers lta
            WHERE lta.user_strapi_document_id = rec_stu.strapi_document_id
              AND lta.user_score>0;  -- условие «сдал тест» (балл>0)

           DROP TABLE IF EXISTS tmp_missing_te;
           CREATE TEMP TABLE tmp_missing_te ON COMMIT DROP AS
           SELECT unnest(v_all_test_ids) AS testid
            EXCEPT
           SELECT test_strapi_document_id FROM tmp_single_user_tests;

           SELECT array_agg(testid) INTO v_incomplete_tests
             FROM tmp_missing_te;
           IF v_incomplete_tests IS NULL THEN
             v_incomplete_tests := '{}';
           END IF;
         END;

         /* 3) Если есть хоть одно пропущенное задание/тест => выдаём страйк */
         IF cardinality(v_incomplete_tasks)>0 OR cardinality(v_incomplete_tests)>0 THEN
           DECLARE
             v_comment TEXT := '';
             v_task_comment TEXT := '';
             v_test_comment TEXT := '';
           BEGIN
             IF cardinality(v_incomplete_tasks)>0 THEN
               v_task_comment := format('не выполнены задания: %s',
                 array_to_string(v_incomplete_tasks, ', ')
               );
             END IF;
             IF cardinality(v_incomplete_tests)>0 THEN
               v_test_comment := format('не сданы тесты: %s',
                 array_to_string(v_incomplete_tests, ', ')
               );
             END IF;

             IF v_task_comment<>'' AND v_test_comment<>'' THEN
               v_comment := v_task_comment || ' и ' || v_test_comment;
             ELSIF v_task_comment<>'' THEN
               v_comment := v_task_comment;
             ELSIF v_test_comment<>'' THEN
               v_comment := v_test_comment;
             END IF;

             v_comment := v_comment||format(' (sprint=%s)', p_sprint_strapi_document_id);

             INSERT INTO strikes(
               user_strapi_document_id,
               sprint_strapi_document_id,
               type,
               comment,
               created_at
             )
             VALUES(
               rec_stu.strapi_document_id,
               p_sprint_strapi_document_id,
               'missed_task_or_test',
               v_comment,
               now()
             );

             PERFORM log_message(format('Strike => user=%s => %s', rec_stu.strapi_document_id, v_comment));
           END;
         END IF;
      END LOOP; -- rec_stu
    END;
    PERFORM log_message('Strikes assigned for incomplete tasks/tests');

    /*****************************************************************************
     I) РАСПРЕДЕЛЕНИЕ (author,task) => 3 reviewer
        reviewer => из tmp_students, 
                    выполнил тот же task,
                    (reviewer != author),
                    (reviewer, task).counter < 3
    *****************************************************************************/
    DROP TABLE IF EXISTS tmp_review_assignments;
    CREATE TEMP TABLE tmp_review_assignments ON COMMIT DROP AS
    SELECT 'stub'::TEXT AS reviewer_user_strapi_document_id
           , 'stub'::TEXT AS reviewee_user_strapi_document_id
           , 'stub'::TEXT AS task_strapi_document_id
           , 0 AS number_in_batch
           , 'stub'::TEXT AS control
    LIMIT 0;

    DROP TABLE IF EXISTS tmp_reviewer_task_counter;
    CREATE TEMP TABLE tmp_reviewer_task_counter ON COMMIT DROP AS
    SELECT 'stub' AS reviewer, 'stub' AS task, 0 AS counter
    LIMIT 0;

    DECLARE
      rec_auth RECORD;   -- (author, task)
      assignedReviewers TEXT[];
      i INT;
      v_control TEXT;
      v_needed INT := 3;
    BEGIN
      FOR rec_auth IN (
        SELECT DISTINCT
          la.user_strapi_document_id AS author,
          la.task_strapi_document_id AS task
        FROM tmp_latest_user_task_answers la
      )
      LOOP
        assignedReviewers := '{}';
        i := 0;

        DECLARE
          v_try_count INT := 0;
          v_max_tries INT := 100;
        BEGIN
          LOOP
            v_try_count := v_try_count + 1;
            EXIT WHEN v_try_count > v_max_tries OR i >= v_needed;

            DECLARE
              v_candidate RECORD;
              v_found BOOLEAN := FALSE;
            BEGIN
              -- Найдём случайного reviewer (см. пункты 1–5)
              SELECT st.strapi_document_id AS reviewer
                INTO v_candidate
                FROM tmp_students st
                JOIN tmp_latest_user_task_answers la2
                  ON la2.user_strapi_document_id = st.strapi_document_id
                 AND la2.task_strapi_document_id = rec_auth.task
                WHERE st.strapi_document_id <> rec_auth.author
                  AND st.strapi_document_id <> ALL(assignedReviewers)
                  AND (
                    (
                      SELECT counter
                        FROM tmp_reviewer_task_counter
                       WHERE reviewer = st.strapi_document_id
                         AND task     = rec_auth.task
                    ) < 3
                    OR NOT EXISTS (
                      SELECT 1
                        FROM tmp_reviewer_task_counter
                       WHERE reviewer = st.strapi_document_id
                         AND task     = rec_auth.task
                    )
                  )
                ORDER BY random()
                LIMIT 1;

              IF NOT FOUND THEN
                CONTINUE;
              ELSE
                v_found := TRUE;
              END IF;

              IF v_found THEN
                i := i + 1;
                v_control := v_candidate.reviewer||'_'||rec_auth.task||'_batch'||i::text;

                INSERT INTO tmp_review_assignments(
                  reviewer_user_strapi_document_id,
                  reviewee_user_strapi_document_id,
                  task_strapi_document_id,
                  number_in_batch,
                  control
                )
                VALUES(
                  v_candidate.reviewer,
                  rec_auth.author,
                  rec_auth.task,
                  i,
                  v_control
                );

                DECLARE
                  v_cc INT;
                BEGIN
                  SELECT counter
                    INTO v_cc
                    FROM tmp_reviewer_task_counter
                   WHERE reviewer = v_candidate.reviewer
                     AND task     = rec_auth.task
                   LIMIT 1;
                  IF v_cc IS NULL THEN
                    INSERT INTO tmp_reviewer_task_counter(reviewer, task, counter)
                    VALUES(v_candidate.reviewer, rec_auth.task,1);
                  ELSE
                    UPDATE tmp_reviewer_task_counter
                       SET counter = counter + 1
                     WHERE reviewer = v_candidate.reviewer
                       AND task     = rec_auth.task;
                  END IF;
                END;

                assignedReviewers := array_append(assignedReviewers, v_candidate.reviewer);
              END IF;
            END; -- DECLARE v_candidate

            EXIT WHEN i >= v_needed;
          END LOOP;

          IF i<3 THEN
            PERFORM log_message(format(
              'cannot gather 3 reviewers for (author=%s,task=%s) => got only %s',
              rec_auth.author, rec_auth.task, i
            ));
          END IF;
        END; -- DECLARE
      END LOOP; -- rec_auth
    END;

    PERFORM log_message(format(
      'total new assignments to insert: %s',
      (SELECT COUNT(*) FROM tmp_review_assignments WHERE control<>'stub')
    ));

    DELETE FROM tmp_review_assignments WHERE control='stub';

    IF (SELECT COUNT(*) FROM tmp_review_assignments)>0 THEN
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
          reviewer_user_strapi_document_id=EXCLUDED.reviewer_user_strapi_document_id,
          reviewee_user_strapi_document_id=EXCLUDED.reviewee_user_strapi_document_id,
          task_strapi_document_id=EXCLUDED.task_strapi_document_id,
          number_in_batch=EXCLUDED.number_in_batch;

      PERFORM log_message('all assignments inserted (UPSERT)');
    ELSE
      PERFORM log_message('no assignments to insert => no distribution was done');
    END IF;

    PERFORM log_message(format(
      '=== distribute_all_for_sprint DONE for sprint=%s ===',
      p_sprint_strapi_document_id
    ));
END;
