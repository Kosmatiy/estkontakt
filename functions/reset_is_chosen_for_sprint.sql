BEGIN
    PERFORM log_message(format('reset_is_chosen_for_sprint: START for sprint=%s', p_sprint_id));

    -- Обновляем user_sprint_state для всех пользователей, связанных с дуэлями этого спринта
    UPDATE user_sprint_state
       SET is_chosen = FALSE
      WHERE duel_strapi_document_id IN (
          SELECT d.strapi_document_id
            FROM duels d
           WHERE d.sprint_strapi_document_id = p_sprint_id
      );

    PERFORM log_message(format('reset_is_chosen_for_sprint: FINISHED for sprint=%s', p_sprint_id));
END;
