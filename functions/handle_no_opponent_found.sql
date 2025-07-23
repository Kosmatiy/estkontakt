DECLARE
    v_volunteer_id TEXT;
    v_any_id       TEXT;
    v_is_repeat    BOOLEAN;
    v_count_existing INT;
BEGIN
    /*******************************************************************************************************
      1) Добровольцы (is_repeats_ok=TRUE), can_make_pair(...),
         берём самого тяжёлого (или как в ТЗ «самого тяжёлого»)
    *******************************************************************************************************/
    WITH cte_volunteers AS (
        SELECT u.strapi_document_id AS user_id,
               u.weight
          FROM users u
          JOIN user_sprint_state uss ON uss.user_strapi_document_id = u.strapi_document_id
         WHERE uss.is_repeats_ok = TRUE
           AND u.dismissed_at IS NULL
           AND u.strapi_document_id <> p_user_id
    )
    SELECT v.user_id
      INTO v_volunteer_id
      FROM cte_volunteers v
     WHERE can_make_pair(p_sprint_id, p_user_id, v.user_id)
     ORDER BY v.weight DESC
     LIMIT 1;

    IF v_volunteer_id IS NOT NULL THEN
        SELECT COUNT(*)
          INTO v_count_existing
          FROM duel_distributions dd
          JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
         WHERE d.sprint_strapi_document_id = p_sprint_id
           AND dd.is_failed = FALSE
           AND (
             (dd.user_strapi_document_id = p_user_id AND dd.rival_strapi_document_id = v_volunteer_id)
             OR
             (dd.user_strapi_document_id = v_volunteer_id AND dd.rival_strapi_document_id = p_user_id)
           );
        v_is_repeat := (v_count_existing>0);

        PERFORM create_duel_distribution(
            p_sprint_id,
            p_user_id,
            v_volunteer_id,
            p_duel_type,
            TRUE,
            v_is_repeat,
            p_is_late,
            p_weight_coef
        );
        RETURN;
    END IF;

    /*******************************************************************************************************
      2) Обычные пользователи (не is_repeats_ok)
    *******************************************************************************************************/
    WITH cte_all AS (
        SELECT u.strapi_document_id AS user_id,
               u.weight
          FROM users u
         WHERE u.dismissed_at IS NULL
           AND u.strapi_document_id <> p_user_id
    )
    SELECT a.user_id
      INTO v_any_id
      FROM cte_all a
     WHERE can_make_pair(p_sprint_id, p_user_id, a.user_id)
     ORDER BY a.weight DESC
     LIMIT 1;

    IF v_any_id IS NOT NULL THEN
        SELECT COUNT(*) INTO v_count_existing
          FROM duel_distributions dd
          JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
         WHERE d.sprint_strapi_document_id = p_sprint_id
           AND dd.is_failed = FALSE
           AND (
             (dd.user_strapi_document_id = p_user_id AND dd.rival_strapi_document_id = v_any_id)
             OR
             (dd.user_strapi_document_id = v_any_id AND dd.rival_strapi_document_id = p_user_id)
           );
        v_is_repeat := (v_count_existing>0);

        PERFORM create_duel_distribution(
            p_sprint_id,
            p_user_id,
            v_any_id,
            p_duel_type,
            TRUE,
            v_is_repeat,
            p_is_late,
            p_weight_coef
        );
        RETURN;
    END IF;

    /*******************************************************************************************************
      3) Никого нет => пишем в admin_messages
    *******************************************************************************************************/
    INSERT INTO admin_messages(message_text, sprint_strapi_document_id, created_at)
    VALUES(
        format('No opponent found for user=%, sprint=%, type=%', p_user_id, p_sprint_id, p_duel_type),
        p_sprint_id,
        NOW()
    );
END;
