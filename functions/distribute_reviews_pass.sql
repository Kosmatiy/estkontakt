DECLARE
    rec_pair RECORD;
    rec_reviewer RECORD;
    v_needed_reviewers INT;
    v_assigned_count INT := 0;
BEGIN
    -- Обрабатываем пары, начиная с тех, где участники играли больше всего
    FOR rec_pair IN
        SELECT pr.*,
               -- Считаем суммарное количество игр участников
               (SELECT SUM(total_games) 
                  FROM tmp_player_quotas pq 
                 WHERE pq.user_id = ANY(pr.participants)) AS participants_total_games
          FROM tmp_pair_reviewers pr
         WHERE array_length(pr.assigned_reviewers, 1) < 6 
            OR pr.assigned_reviewers IS NULL
         ORDER BY participants_total_games DESC NULLS LAST,
                  pr.duel_strapi_document_id, pr.hash
    LOOP
        v_needed_reviewers := 6 - COALESCE(array_length(rec_pair.assigned_reviewers, 1), 0);
        
        IF v_needed_reviewers <= 0 THEN
            CONTINUE;
        END IF;

        -- Находим подходящих рецензентов в зависимости от режима
        FOR rec_reviewer IN
            SELECT * FROM find_reviewers_for_pair(
                rec_pair.duel_strapi_document_id,
                rec_pair.hash,
                rec_pair.duel_type,
                rec_pair.participants,
                rec_pair.assigned_reviewers,
                p_mode,
                v_needed_reviewers
            )
        LOOP
            -- Вставляем назначения (по 2 строки на рецензента)
            INSERT INTO user_duel_to_review (
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                hash
            )
            SELECT rec_reviewer.user_id,
                   rec_pair.duel_strapi_document_id,
                   unnest(rec_pair.participants),
                   rec_pair.hash
            ON CONFLICT DO NOTHING;

            -- Обновляем список назначенных рецензентов
            UPDATE tmp_pair_reviewers
               SET assigned_reviewers = array_append(assigned_reviewers, rec_reviewer.user_id)
             WHERE duel_strapi_document_id = rec_pair.duel_strapi_document_id
               AND hash = rec_pair.hash;

            -- Обновляем счетчик назначенных проверок
            UPDATE tmp_player_quotas
               SET assigned_reviews = assigned_reviews + 1
             WHERE user_id = rec_reviewer.user_id;

            v_assigned_count := v_assigned_count + 1;
        END LOOP;
    END LOOP;
END;
