SELECT
      ROW_NUMBER() OVER (ORDER BY ua.duel_strapi_document_id, ua.hash, ua.user_strapi_document_id) AS unit_index,
      ua.duel_strapi_document_id,
      ua.hash,
      ua.user_strapi_document_id
    FROM user_duel_answers ua
    JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_id
      AND ua.user_strapi_document_id IN (
        SELECT u.strapi_document_id FROM users u
         WHERE u.dismissed_at IS NULL
           AND EXISTS (
             SELECT 1 FROM user_duel_answers x
              JOIN duels dd ON dd.strapi_document_id = x.duel_strapi_document_id
             WHERE dd.sprint_strapi_document_id = p_sprint_id
               AND x.user_strapi_document_id = u.strapi_document_id
           )
      );
