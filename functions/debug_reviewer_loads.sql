BEGIN
    RETURN QUERY
    WITH active_users AS (
        SELECT u.strapi_document_id AS user_id
        FROM users u
        JOIN user_stream_links usl ON usl.user_strapi_document_id = u.strapi_document_id
        JOIN sprints s ON s.strapi_document_id = p_sprint_id
        WHERE u.dismissed_at IS NULL
          AND usl.is_active
          AND usl.stream_strapi_document_id = s.stream_strapi_document_id
    ),
    quotas AS (
        SELECT
          au.user_id,
          (COUNT(uda.duel_answer_id) * 3)::INT AS quota
        FROM active_users au
        LEFT JOIN user_duel_answers uda
          ON uda.user_strapi_document_id = au.user_id
        JOIN duels d
          ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
        GROUP BY au.user_id
    )
    SELECT
      q.user_id,
      q.quota,
      0 AS assigned
    FROM quotas q
    ORDER BY q.user_id;
END;
