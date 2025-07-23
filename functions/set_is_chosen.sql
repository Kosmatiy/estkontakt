BEGIN
   IF EXISTS (
      SELECT 1 FROM user_sprint_state uss
       WHERE uss.duel_strapi_document_id=p_duel_strapi_id
         AND uss.user_strapi_document_id=p_user_id
   ) THEN
       UPDATE user_sprint_state
          SET is_chosen = p_value
        WHERE duel_strapi_document_id=p_duel_strapi_id
          AND user_strapi_document_id=p_user_id;
   ELSE
       INSERT INTO user_sprint_state(
           duel_strapi_document_id,
           user_strapi_document_id,
           is_chosen,
           is_repeats_ok,
           created_at
       )
       VALUES(
           p_duel_strapi_id,
           p_user_id,
           p_value,
           FALSE,
           NOW()
       );
   END IF;

   PERFORM log_message(format('set_is_chosen(duel=%s, user=%s, val=%s)',
                              p_duel_strapi_id, p_user_id, p_value));
END;
