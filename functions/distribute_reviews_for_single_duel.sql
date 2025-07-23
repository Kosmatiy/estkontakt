DECLARE
    rec_party RECORD;
    v_assigned_count INT;
BEGIN
    -- Получаем все сыгранные партии этой дуэли
    FOR rec_party IN (
        SELECT DISTINCT 
            uda.user_strapi_document_id as owner_a,
            uda.rival_user_strapi_document_id as owner_b,
            uda.duel_strapi_document_id
        FROM user_duel_answers uda
        WHERE uda.duel_strapi_document_id = p_duel_id
        AND uda.created_at = (
            -- Берем самый свежий ответ для каждой пары
            SELECT MAX(created_at)
            FROM user_duel_answers uda2
            WHERE uda2.user_strapi_document_id = uda.user_strapi_document_id
            AND uda2.rival_user_strapi_document_id = uda.rival_user_strapi_document_id
            AND uda2.duel_strapi_document_id = uda.duel_strapi_document_id
        )
    ) LOOP
        -- Распределяем проверяющих для каждой партии
        PERFORM assign_reviewers_for_party(
            rec_party.owner_a,
            rec_party.owner_b,
            rec_party.duel_strapi_document_id
        );
    END LOOP;
END;
