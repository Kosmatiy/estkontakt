DECLARE
    rec_reviewer RECORD;
    v_assigned_count INT := 0;
    v_needed_reviewers CONSTANT INT := 6;
BEGIN
    -- Получаем подходящих ревьюеров
    FOR rec_reviewer IN (
        WITH reviewer_stats AS (
            -- Считаем количество партий и назначенных проверок для каждого потенциального ревьюера
            SELECT 
                u.strapi_document_id as reviewer_id,
                COUNT(DISTINCT uda.duel_answer_id) as played_count,
                COUNT(DISTINCT udr.id) as review_count
            FROM users u
            LEFT JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
                AND uda.duel_strapi_document_id = p_duel_id
            LEFT JOIN user_duel_to_review udr ON udr.reviewer_user_strapi_document_id = u.strapi_document_id
                AND udr.duel_strapi_document_id = p_duel_id
            WHERE u.dismissed_at IS NULL
            AND u.strapi_document_id NOT IN (p_owner_a, p_owner_b)
            -- Проверяем отсутствие страйков
            AND NOT EXISTS (
                SELECT 1 FROM strikes s
                WHERE s.user_strapi_document_id = u.strapi_document_id
                AND s.sprint_strapi_document_id = (
                    SELECT sprint_strapi_document_id 
                    FROM duels 
                    WHERE strapi_document_id = p_duel_id
                )
            )
            GROUP BY u.strapi_document_id
        )
        SELECT 
            reviewer_id
        FROM reviewer_stats
        WHERE played_count > 0  -- Должен иметь хотя бы одну сыгранную партию
        AND review_count < (played_count * 3)  -- Не превышен лимит проверок
        AND reviewer_id NOT IN (  -- Не назначен ранее на эту партию
            SELECT reviewer_user_strapi_document_id
            FROM user_duel_to_review
            WHERE duel_strapi_document_id = p_duel_id
            AND (
                user_strapi_document_id = p_owner_a
                OR user_strapi_document_id = p_owner_b
            )
        )
        ORDER BY RANDOM()  -- Случайный порядок
        LIMIT (v_needed_reviewers - v_assigned_count)
    ) LOOP
        -- Назначаем проверку
        INSERT INTO user_duel_to_review (
            reviewer_user_strapi_document_id,
            user_strapi_document_id,
            duel_strapi_document_id,
            created_at
        ) VALUES (
            rec_reviewer.reviewer_id,
            p_owner_a,  -- Основной владелец
            p_duel_id,
            NOW()
        );

        v_assigned_count := v_assigned_count + 1;
        
        -- Также добавляем запись для второго участника
        INSERT INTO user_duel_to_review (
            reviewer_user_strapi_document_id,
            user_strapi_document_id,
            duel_strapi_document_id,
            created_at
        ) VALUES (
            rec_reviewer.reviewer_id,
            p_owner_b,  -- Второй участник
            p_duel_id,
            NOW()
        );
    END LOOP;

    -- Проверяем, что назначили всех необходимых проверяющих
    IF v_assigned_count < v_needed_reviewers THEN
        RAISE EXCEPTION 'Could not assign enough reviewers (got %, need %)',
            v_assigned_count, v_needed_reviewers;
    END IF;
END;
