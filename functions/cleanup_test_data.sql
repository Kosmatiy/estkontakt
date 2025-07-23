DECLARE
    v_stream_id TEXT;
    v_deleted_count INT := 0;
BEGIN
    -- Получаем stream_id
    SELECT stream_strapi_document_id INTO v_stream_id
      FROM sprints WHERE strapi_document_id = p_sprint_id;

    -- Удаляем в правильном порядке
    DELETE FROM user_duel_to_review 
     WHERE duel_strapi_document_id IN (
         SELECT strapi_document_id FROM duels 
          WHERE sprint_strapi_document_id = p_sprint_id
     );
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    DELETE FROM user_duel_answers
     WHERE duel_strapi_document_id IN (
         SELECT strapi_document_id FROM duels 
          WHERE sprint_strapi_document_id = p_sprint_id
     );

    DELETE FROM duels WHERE sprint_strapi_document_id = p_sprint_id;
    DELETE FROM users WHERE stream_strapi_document_id = v_stream_id;
    DELETE FROM teams WHERE strapi_document_id IN (
        SELECT DISTINCT team_strapi_document_id 
          FROM users WHERE stream_strapi_document_id = v_stream_id
    );
    DELETE FROM sprints WHERE strapi_document_id = p_sprint_id;
    DELETE FROM streams WHERE strapi_document_id = v_stream_id;

    RETURN json_build_object(
        'result', 'success',
        'deleted_assignments', v_deleted_count
    );
END;
