BEGIN
    RETURN QUERY
    WITH candidate_reviewers AS (
        SELECT pq.user_id,
               pq.review_quota,
               pq.assigned_reviews,
               pq.review_quota - pq.assigned_reviews AS available_slots,
               eu.team_id,
               -- Проверяем, играл ли кандидат в этой дуэли
               EXISTS (
                   SELECT 1 FROM user_duel_answers uda
                    WHERE uda.user_strapi_document_id = pq.user_id
                      AND uda.duel_strapi_document_id = p_duel_id
               ) AS played_this_duel,
               -- Получаем команды участников пары
               (SELECT array_agg(DISTINCT team_id) 
                  FROM tmp_eligible_users 
                 WHERE user_id = ANY(p_participants)) AS participant_teams
          FROM tmp_player_quotas pq
          JOIN tmp_eligible_users eu ON eu.user_id = pq.user_id
         WHERE pq.user_id != ALL(p_participants)  -- Не участник пары
           AND (p_already_assigned IS NULL OR pq.user_id != ALL(p_already_assigned))  -- Еще не назначен
           AND pq.review_quota > pq.assigned_reviews  -- Есть свободная квота
    )
    SELECT cr.user_id,
           -- Приоритет в зависимости от режима прохода
           CASE 
               WHEN p_mode = 'strict_duel_and_team' THEN
                   CASE
                       WHEN cr.played_this_duel AND p_duel_type = 'FULL-CONTACT' 
                            AND cr.team_id != ALL(cr.participant_teams) THEN 100
                       WHEN cr.played_this_duel AND p_duel_type = 'TRAINING' THEN 90
                       ELSE 0
                   END
               WHEN p_mode = 'strict_duel_any_team' THEN
                   CASE
                       WHEN cr.played_this_duel THEN 80
                       ELSE 0
                   END
               WHEN p_mode = 'any_player' THEN
                   -- Приоритет тем, у кого больше свободных слотов нужно заполнить
                   CASE
                       WHEN cr.available_slots > 0 THEN 50 + cr.available_slots
                       ELSE 0
                   END
           END AS priority,
           cr.available_slots
      FROM candidate_reviewers cr
     WHERE CASE 
               WHEN p_mode IN ('strict_duel_and_team', 'strict_duel_any_team') 
                    THEN cr.played_this_duel
               ELSE true
           END
     ORDER BY priority DESC, 
              available_slots ASC,  -- Сначала те, у кого меньше свободных слотов
              cr.user_id  -- Детерминированность
     LIMIT p_limit;
END;
