BEGIN
    -- 1. Проверка структуры данных
    RETURN QUERY
    SELECT 
        'data_structure'::TEXT,
        jsonb_build_object(
            'players', (
                SELECT COUNT(DISTINCT user_strapi_document_id) 
                FROM user_duel_answers uda
                JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
                WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
            ),
            'pairs', (
                SELECT COUNT(DISTINCT duel_strapi_document_id || '_' || hash)
                FROM user_duel_answers uda
                JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
                WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
            ) / 2,
            'duels', (
                SELECT COUNT(*)
                FROM duels
                WHERE sprint_strapi_document_id = p_sprint_strapi_document_id
            )
        );

    -- 2. Распределение игр по игрокам
    RETURN QUERY
    WITH game_distribution AS (
        SELECT 
            COUNT(DISTINCT uda.duel_strapi_document_id || '_' || uda.hash) AS games_count,
            COUNT(DISTINCT uda.user_strapi_document_id) AS player_count
        FROM user_duel_answers uda
        JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        GROUP BY uda.user_strapi_document_id
    )
    SELECT 
        'game_distribution'::TEXT,
        jsonb_object_agg(
            games_count || '_games',
            player_count
        )
    FROM (
        SELECT games_count, COUNT(*) AS player_count
        FROM game_distribution
        GROUP BY games_count
    ) gd;

    -- 3. Текущее покрытие пар
    RETURN QUERY
    WITH pair_coverage AS (
        SELECT 
            COUNT(DISTINCT reviewer_user_strapi_document_id) AS reviewer_count,
            COUNT(*) AS pair_count
        FROM (
            SELECT DISTINCT duel_strapi_document_id, hash
            FROM user_duel_answers uda
            JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
            WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        ) pairs
        LEFT JOIN user_duel_to_review udr USING (duel_strapi_document_id, hash)
        GROUP BY duel_strapi_document_id, hash
    )
    SELECT 
        'pair_coverage'::TEXT,
        jsonb_object_agg(
            reviewer_count || '_reviewers',
            pair_count
        )
    FROM (
        SELECT reviewer_count, COUNT(*) AS pair_count
        FROM pair_coverage
        GROUP BY reviewer_count
    ) pc;

    -- 4. Проблемные игроки
    RETURN QUERY
    WITH player_issues AS (
        SELECT 
            u.strapi_document_id,
            u.name,
            COUNT(DISTINCT uda.duel_strapi_document_id || '_' || uda.hash) AS games_played,
            COUNT(DISTINCT uda.duel_strapi_document_id || '_' || uda.hash) * 3 AS expected_reviews,
            COUNT(DISTINCT udr.duel_strapi_document_id || '_' || udr.hash) AS actual_reviews
        FROM users u
        LEFT JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
        LEFT JOIN user_duel_to_review udr ON udr.reviewer_user_strapi_document_id = u.strapi_document_id
        LEFT JOIN duels d1 ON d1.strapi_document_id = uda.duel_strapi_document_id
        LEFT JOIN duels d2 ON d2.strapi_document_id = udr.duel_strapi_document_id
        WHERE EXISTS (
            SELECT 1 FROM user_duel_answers uda2
            JOIN duels d ON d.strapi_document_id = uda2.duel_strapi_document_id
            WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
              AND uda2.user_strapi_document_id = u.strapi_document_id
        )
        AND (d1.sprint_strapi_document_id = p_sprint_strapi_document_id OR d1.sprint_strapi_document_id IS NULL)
        AND (d2.sprint_strapi_document_id = p_sprint_strapi_document_id OR d2.sprint_strapi_document_id IS NULL)
        GROUP BY u.strapi_document_id, u.name
        HAVING COUNT(DISTINCT uda.duel_strapi_document_id || '_' || uda.hash) * 3 != COUNT(DISTINCT udr.duel_strapi_document_id || '_' || udr.hash)
    )
    SELECT 
        'player_issues'::TEXT,
        jsonb_agg(
            jsonb_build_object(
                'player', name,
                'games', games_played,
                'expected', expected_reviews,
                'actual', actual_reviews,
                'difference', expected_reviews - actual_reviews
            )
        )
    FROM player_issues
    LIMIT 10;
END;
