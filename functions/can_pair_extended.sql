DECLARE
   v_count_user INT;
   v_count_pair INT;
BEGIN
   PERFORM log_message(format('can_pair_extended(duel=%s, user=%s, opp=%s)', 
                              p_duel_strapi_id, p_user_id, p_opponent_id));

   IF p_user_id = p_opponent_id THEN
       PERFORM log_message('   same user => FALSE');
       RETURN FALSE;
   END IF;

   /* dismissed? */
   IF EXISTS (
       SELECT 1 FROM users u
        WHERE u.strapi_document_id = p_opponent_id
          AND u.dismissed_at IS NOT NULL
   ) THEN
       PERFORM log_message('   opp dismissed => FALSE');
       RETURN FALSE;
   END IF;

   /* команда */
   IF EXISTS (
       SELECT 1
         FROM users u1
         JOIN users u2 ON (u2.strapi_document_id = p_opponent_id)
        WHERE u1.strapi_document_id = p_user_id
          AND u1.team_strapi_document_id = u2.team_strapi_document_id
   ) THEN
       PERFORM log_message('   same team => FALSE');
       RETURN FALSE;
   END IF;

   /*
     Лимит 2 оппонентов (шаг 3.5.2.2):
   */
   SELECT COUNT(DISTINCT dd.rival_strapi_document_id)
     INTO v_count_user
     FROM duel_distributions dd
    WHERE dd.duel_strapi_document_id = p_duel_strapi_id
      AND dd.is_failed = FALSE
      AND dd.user_strapi_document_id = p_user_id;

   IF v_count_user >= 2 THEN
       PERFORM log_message('   user already has 2 opponents => FALSE');
       RETURN FALSE;
   END IF;

   /* Уже встречались? => is_repeat=TRUE */
   SELECT COUNT(*)
     INTO v_count_pair
     FROM duel_distributions dd
    WHERE dd.duel_strapi_document_id = p_duel_strapi_id
      AND dd.is_failed=FALSE
      AND (
        (dd.user_strapi_document_id = p_user_id 
         AND dd.rival_strapi_document_id = p_opponent_id)
        OR
        (dd.user_strapi_document_id = p_opponent_id 
         AND dd.rival_strapi_document_id = p_user_id)
      );

   PERFORM log_message(format('   can_pair_extended => user vs opp => already %s matches', v_count_pair));

   RETURN TRUE;
END;
