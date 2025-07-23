BEGIN
    RETURN QUERY
    WITH stats AS (
        SELECT 
            u.telegram_username,
            COUNT(DISTINCT utr.hash || '_' || utr.duel_strapi_document_id) as duels_count
        FROM users u
        JOIN sprints s ON s.stream_strapi_document_id = u.stream_strapi_document_id
        LEFT JOIN user_duel_to_review utr ON utr.reviewer_user_strapi_document_id = u.strapi_document_id
        LEFT JOIN duels d ON d.strapi_document_id = utr.duel_strapi_document_id 
            AND d.sprint_strapi_document_id = p_sprint_id
        WHERE s.strapi_document_id = p_sprint_id
          AND u.dismissed_at IS NULL
        GROUP BY u.telegram_username
    )
    SELECT 'Всего рецензентов'::TEXT, COUNT(*)::TEXT FROM stats
    UNION ALL
    SELECT 'Рецензентов с заданиями', COUNT(*)::TEXT FROM stats WHERE duels_count > 0
    UNION ALL
    SELECT 'Рецензентов без заданий', COUNT(*)::TEXT FROM stats WHERE duels_count = 0
    UNION ALL
    SELECT 'Минимальная нагрузка', MIN(duels_count)::TEXT FROM stats
    UNION ALL
    SELECT 'Максимальная нагрузка', MAX(duels_count)::TEXT FROM stats
    UNION ALL
    SELECT 'Средняя нагрузка', ROUND(AVG(duels_count), 2)::TEXT FROM stats
    UNION ALL
    SELECT 'Стандартное отклонение', ROUND(STDDEV(duels_count), 2)::TEXT FROM stats
    UNION ALL
    SELECT 'Коэффициент вариации (%)', 
           ROUND(STDDEV(duels_count) / NULLIF(AVG(duels_count), 0) * 100, 2)::TEXT 
    FROM stats;
END;
