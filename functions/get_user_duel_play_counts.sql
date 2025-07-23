SELECT
        uda.user_strapi_document_id AS user_id,
        uda.duel_strapi_document_id AS duel_id,
        COUNT(*)                   AS play_count
    FROM user_duel_answers uda
    JOIN duels d
      ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_id
    GROUP BY 1,2
    ORDER BY 1,2;
