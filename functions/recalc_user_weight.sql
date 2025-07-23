DECLARE
    v_new_weight NUMERIC;
BEGIN
    -- Суммируем weight из duel_distributions, 
    -- JOIN duels, чтобы фильтровать по sprint_strapi_document_id
    SELECT COALESCE(SUM(dd.weight), 0)
      INTO v_new_weight
      FROM duel_distributions dd
      JOIN duels d 
        ON d.strapi_document_id = dd.duel_strapi_document_id
     WHERE d.sprint_strapi_document_id = p_sprint_id
       AND dd.is_failed = FALSE
       AND (
           dd.user_strapi_document_id  = p_user_id
           OR dd.rival_strapi_document_id = p_user_id
       );

    -- Обновляем поле weight в таблице users
    UPDATE users
       SET weight = v_new_weight
     WHERE strapi_document_id = p_user_id;

    -- Запишем в лог
    PERFORM log_message(
      format('recalc_user_weight: user=%s, new_weight=%s', 
              p_user_id, 
              v_new_weight)
    );
END;
