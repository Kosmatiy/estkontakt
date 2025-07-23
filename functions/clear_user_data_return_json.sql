DECLARE
    deleted_count INTEGER := 0;
    count1 INTEGER := 0;
    count2 INTEGER := 0;
    user_name TEXT;
BEGIN
    -- Получаем имя пользователя (имя и фамилия)
    SELECT name || ' ' || surname 
      INTO user_name
      FROM users
      WHERE strapi_document_id = in_user_strapi_document_id;
      
    IF in_mode = 'TESTS' THEN
        WITH del1 AS (
            DELETE FROM user_question_answers
            USING tests t, lectures l
            WHERE user_question_answers.user_strapi_document_id = in_user_strapi_document_id
              AND user_question_answers.test_strapi_document_id = t.strapi_document_id
              AND t.lecture_strapi_document_id = l.strapi_document_id
              AND l.sprint_strapi_document_id = in_sprint_strapi_document_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO count1 FROM del1;
        
        WITH del2 AS (
            DELETE FROM user_test_answers
            USING tests t, lectures l
            WHERE user_test_answers.user_strapi_document_id = in_user_strapi_document_id
              AND user_test_answers.test_strapi_document_id = t.strapi_document_id
              AND t.lecture_strapi_document_id = l.strapi_document_id
              AND l.sprint_strapi_document_id = in_sprint_strapi_document_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO count2 FROM del2;
        
        deleted_count := count1 + count2;
        
    ELSIF in_mode = 'TASKS' THEN
        WITH del AS (
            DELETE FROM user_task_answers
            USING tasks t, lectures l
            WHERE user_task_answers.user_strapi_document_id = in_user_strapi_document_id
              AND user_task_answers.task_strapi_document_id = t.strapi_document_id
              AND t.lecture_strapi_document_id = l.strapi_document_id
              AND l.sprint_strapi_document_id = in_sprint_strapi_document_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO deleted_count FROM del;
        
    ELSIF in_mode = 'TASK_REVIEWES' THEN
        WITH del AS (
            DELETE FROM user_task_reviewed
            USING tasks t, lectures l
            WHERE user_task_reviewed.reviewer_user_strapi_document_id = in_user_strapi_document_id
              AND user_task_reviewed.task_strapi_document_id = t.strapi_document_id
              AND t.lecture_strapi_document_id = l.strapi_document_id
              AND l.sprint_strapi_document_id = in_sprint_strapi_document_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO deleted_count FROM del;
        
    ELSIF in_mode = 'DUELS' THEN
        WITH del AS (
            DELETE FROM user_duel_answers
            USING duels d
            WHERE user_duel_answers.user_strapi_document_id = in_user_strapi_document_id
              AND user_duel_answers.duel_strapi_document_id = d.strapi_document_id
              AND d.sprint_strapi_document_id = in_sprint_strapi_document_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO deleted_count FROM del;
        
    ELSIF in_mode = 'DUEL_REVIEWES' THEN
        WITH del AS (
            DELETE FROM user_duel_reviewed
            USING duels d
            WHERE user_duel_reviewed.reviewer_user_strapi_document_id = in_user_strapi_document_id
              AND user_duel_reviewed.duel_strapi_document_id = d.strapi_document_id
              AND d.sprint_strapi_document_id = in_sprint_strapi_document_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO deleted_count FROM del;
        
    ELSE
        RAISE EXCEPTION 'Invalid mode: %', in_mode;
    END IF;
    
    RETURN json_build_object(
        'user', user_name,
        'mode', in_mode,
        'deleted_records', deleted_count
    );
END;
