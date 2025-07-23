BEGIN
    RETURN QUERY
    WITH player_positions AS (
        SELECT 
            uda.user_strapi_document_id AS player_id,
            ROW_NUMBER() OVER (ORDER BY uda.user_strapi_document_id) AS position,
            COUNT(DISTINCT uda.duel_strapi_document_id || '_' || uda.hash) AS games
        FROM user_duel_answers uda
        JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        GROUP BY uda.user_strapi_document_id
    ),
    review_assignments AS (
        SELECT 
            udr.reviewer_user_strapi_document_id AS reviewer_id,
            array_agg(DISTINCT udr.user_strapi_document_id ORDER BY udr.user_strapi_document_id) AS reviewed_players,
            COUNT(DISTINCT udr.duel_strapi_document_id || '_' || udr.hash) AS review_count
        FROM user_duel_to_review udr
        JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        GROUP BY udr.reviewer_user_strapi_document_id
    )
    SELECT 
        pp.position::INT,
        pp.player_id,
        pp.games::INT,
        COALESCE(ra.review_count, 0)::INT,
        COALESCE(
            (SELECT string_agg(pp2.position::TEXT, ', ' ORDER BY pp2.position)
             FROM unnest(ra.reviewed_players) rp(player_id)
             JOIN player_positions pp2 ON pp2.player_id = rp.player_id),
            ''
        ) AS reviewed_positions,
        CASE 
            WHEN pp.games >= 2 THEN 'Multiple games player'
            ELSE 'Single game player'
        END AS pattern_match
    FROM player_positions pp
    LEFT JOIN review_assignments ra ON ra.reviewer_id = pp.player_id
    ORDER BY pp.position;
END;
