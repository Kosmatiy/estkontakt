SELECT
      ua.user_strapi_document_id,
      COUNT(*) * 3
    FROM user_duel_answers ua
    JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_id
      AND ua.user_strapi_document_id IN (
        SELECT u.strapi_document_id FROM users u WHERE u.dismissed_at IS NULL
      )
    GROUP BY ua.user_strapi_document_id;
