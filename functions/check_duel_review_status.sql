BEGIN
   -- Проверка 1: Все ли пары имеют 6 рецензентов
   RETURN QUERY
   WITH pair_stats AS (
       SELECT udr.hash,
              udr.duel_strapi_document_id,
              COUNT(DISTINCT udr.reviewer_user_strapi_document_id) AS reviewers
         FROM user_duel_to_review udr
         JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        GROUP BY udr.hash, udr.duel_strapi_document_id
   ),
   all_pairs AS (
       SELECT DISTINCT hash, duel_strapi_document_id
         FROM user_duel_answers uda
         JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
   )
   SELECT 'Pairs with 6 reviewers'::TEXT,
          CASE WHEN COUNT(*) FILTER (WHERE COALESCE(ps.reviewers, 0) != 6) = 0 
               THEN 'OK' 
               ELSE 'FAILED' 
          END,
          format('%s из %s пар имеют неправильное количество рецензентов',
                 COUNT(*) FILTER (WHERE COALESCE(ps.reviewers, 0) != 6),
                 COUNT(*))
     FROM all_pairs ap
     LEFT JOIN pair_stats ps 
       ON ps.hash = ap.hash 
      AND ps.duel_strapi_document_id = ap.duel_strapi_document_id;

   -- Проверка 2: Все ли игроки выполнили квоту
   RETURN QUERY
   WITH player_stats AS (
       SELECT uda.user_strapi_document_id,
              COUNT(DISTINCT uda.hash) AS games_played,
              COUNT(DISTINCT uda.hash) * 3 AS expected_reviews
         FROM user_duel_answers uda
         JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        GROUP BY uda.user_strapi_document_id
   ),
   review_stats AS (
       SELECT reviewer_user_strapi_document_id,
              COUNT(DISTINCT hash) AS actual_reviews
         FROM user_duel_to_review udr
         JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        GROUP BY reviewer_user_strapi_document_id
   )
   SELECT 'Player quotas'::TEXT,
          CASE WHEN COUNT(*) FILTER (WHERE ps.expected_reviews != COALESCE(rs.actual_reviews, 0)) = 0
               THEN 'OK'
               ELSE 'FAILED'
          END,
          format('%s из %s игроков имеют неправильное количество назначенных проверок',
                 COUNT(*) FILTER (WHERE ps.expected_reviews != COALESCE(rs.actual_reviews, 0)),
                 COUNT(*))
     FROM player_stats ps
     LEFT JOIN review_stats rs ON rs.reviewer_user_strapi_document_id = ps.user_strapi_document_id;

   -- Проверка 3: Нет ли дубликатов
   RETURN QUERY
   SELECT 'Duplicates'::TEXT,
          CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FAILED' END,
          format('Найдено %s дубликатов', COUNT(*))
     FROM (
         SELECT 1
           FROM user_duel_to_review udr
           JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
          WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
          GROUP BY reviewer_user_strapi_document_id,
                   duel_strapi_document_id,
                   user_strapi_document_id,
                   hash
         HAVING COUNT(*) > 1
     ) dups;

END;
