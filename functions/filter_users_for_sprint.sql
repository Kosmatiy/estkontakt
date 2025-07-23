BEGIN
   PERFORM log_message('filter_users_for_sprint: start for sprint=' || p_sprint_id);

   RETURN QUERY
   SELECT u.strapi_document_id,
          u.weight,
          u.team_strapi_document_id
     FROM users u
     JOIN sprints s
       ON s.strapi_document_id = p_sprint_id
    WHERE u.dismissed_at IS NULL
      AND u.stream_strapi_document_id = s.stream_strapi_document_id
      AND NOT EXISTS (
        SELECT 1
          FROM strikes st
         WHERE st.sprint_strapi_document_id = p_sprint_id
           AND st.user_strapi_document_id   = u.strapi_document_id
      );

   PERFORM log_message('filter_users_for_sprint: done for sprint=' || p_sprint_id);
END;
