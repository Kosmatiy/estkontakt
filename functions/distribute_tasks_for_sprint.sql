DECLARE
    v_sprint_id   TEXT;
    v_stream_id   TEXT;
    v_students_count INT;   
    v_tasks_count    INT;   
    v_total_assignments INT;
    v_shift1 INT;
    v_shift2 INT;
    v_shift3 INT;
BEGIN
    -- Log start
    PERFORM log_message('=== distribute_tasks_for_sprint: START for sprint='||p_sprint_strapi_document_id);

    -- Find sprint and stream
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

    -- Clear old review assignments
    DELETE FROM user_task_to_review utr
    USING tasks t
    JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
    WHERE utr.task_strapi_document_id = t.strapi_document_id
      AND l.sprint_strapi_document_id = p_sprint_strapi_document_id;

    -- Get active students without strikes and assign them sequential numbers
    DROP TABLE IF EXISTS tmp_students;
    CREATE TEMP TABLE tmp_students AS
    WITH eligible_students AS (
        SELECT u.*
        FROM users u
        WHERE u.stream_strapi_document_id = v_stream_id
          AND u.dismissed_at IS NULL
          AND NOT EXISTS (
              SELECT 1 
              FROM strikes s 
              WHERE s.user_strapi_document_id = u.strapi_document_id
                AND s.sprint_strapi_document_id = p_sprint_strapi_document_id
          )
    )
    SELECT 
        u.*,
        ROW_NUMBER() OVER (ORDER BY u.strapi_document_id) - 1 as student_index
    FROM eligible_students u;

    SELECT COUNT(*) INTO v_students_count FROM tmp_students;
    
    IF v_students_count = 0 THEN
        PERFORM log_message('No eligible students found => STOP');
        RETURN;
    END IF;

    -- Generate random shifts (1 to students_count-1)
    SELECT 
        1 + floor(random() * (v_students_count - 1))::int,
        1 + floor(random() * (v_students_count - 1))::int,
        1 + floor(random() * (v_students_count - 1))::int
    INTO v_shift1, v_shift2, v_shift3;

    PERFORM log_message('Using shifts: '||v_shift1||', '||v_shift2||', '||v_shift3);

    -- Get tasks with their answers
    DROP TABLE IF EXISTS tmp_tasks_answers;
    CREATE TEMP TABLE tmp_tasks_answers AS
    WITH latest_answers AS (
        SELECT DISTINCT ON (user_strapi_document_id, task_strapi_document_id)
            uta.user_strapi_document_id,
            uta.task_strapi_document_id
        FROM user_task_answers uta
        JOIN tasks t ON t.strapi_document_id = uta.task_strapi_document_id
        JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
        WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id
        ORDER BY user_strapi_document_id, task_strapi_document_id, uta.created_at DESC
    )
    SELECT 
        la.*,
        s.student_index
    FROM latest_answers la
    JOIN tmp_students s ON s.strapi_document_id = la.user_strapi_document_id;

    -- Create review assignments
    INSERT INTO user_task_to_review (
        reviewer_user_strapi_document_id,
        reviewee_user_strapi_document_id,
        task_strapi_document_id,
        number_in_batch,
        control
    )
    WITH assignments AS (
        -- First shift
        SELECT 
            r.strapi_document_id as reviewer_id,
            ta.user_strapi_document_id as reviewee_id,
            ta.task_strapi_document_id as task_id,
            1 as batch_number
        FROM tmp_tasks_answers ta
        JOIN tmp_students s ON s.strapi_document_id = ta.user_strapi_document_id
        JOIN tmp_students r ON r.student_index = MOD(s.student_index + v_shift1, v_students_count)
        WHERE r.strapi_document_id != ta.user_strapi_document_id
        
        UNION ALL
        
        -- Second shift
        SELECT 
            r.strapi_document_id as reviewer_id,
            ta.user_strapi_document_id as reviewee_id,
            ta.task_strapi_document_id as task_id,
            2 as batch_number
        FROM tmp_tasks_answers ta
        JOIN tmp_students s ON s.strapi_document_id = ta.user_strapi_document_id
        JOIN tmp_students r ON r.student_index = MOD(s.student_index + v_shift2, v_students_count)
        WHERE r.strapi_document_id != ta.user_strapi_document_id
        
        UNION ALL
        
        -- Third shift
        SELECT 
            r.strapi_document_id as reviewer_id,
            ta.user_strapi_document_id as reviewee_id,
            ta.task_strapi_document_id as task_id,
            3 as batch_number
        FROM tmp_tasks_answers ta
        JOIN tmp_students s ON s.strapi_document_id = ta.user_strapi_document_id
        JOIN tmp_students r ON r.student_index = MOD(s.student_index + v_shift3, v_students_count)
        WHERE r.strapi_document_id != ta.user_strapi_document_id
    )
    SELECT 
        reviewer_id,
        reviewee_id,
        task_id,
        batch_number,
        reviewer_id || '_' || reviewee_id || '_' || task_id || '_batch' || batch_number
    FROM assignments
    ON CONFLICT (control) DO UPDATE SET
        reviewer_user_strapi_document_id = EXCLUDED.reviewer_user_strapi_document_id,
        reviewee_user_strapi_document_id = EXCLUDED.reviewee_user_strapi_document_id,
        task_strapi_document_id = EXCLUDED.task_strapi_document_id,
        number_in_batch = EXCLUDED.number_in_batch;

    -- Get total assignments
    SELECT COUNT(*) INTO v_total_assignments 
    FROM user_task_to_review utr
    JOIN tasks t ON t.strapi_document_id = utr.task_strapi_document_id
    JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
    WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id;

    -- Log results
    PERFORM log_message(format(
        'Completed distribution with shifts %s, %s, %s',
        v_shift1, v_shift2, v_shift3
    ));
    PERFORM log_message('Total assignments created: '||v_total_assignments);
    PERFORM log_message(format(
        '=== distribute_tasks_for_sprint DONE for sprint=%s => total=%s ===',
        p_sprint_strapi_document_id,
        v_total_assignments
    ));
END;
