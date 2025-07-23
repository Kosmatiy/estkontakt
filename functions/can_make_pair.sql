DECLARE
   v_same_team BOOLEAN;
   v_opponent_dismissed BOOLEAN;
   v_strike_count INT;
   v_active_count INT;
BEGIN
   PERFORM log_message(format('can_make_pair(sprint=%s, user=%s, opp=%s) called', 
                              p_sprint_id, p_user_id, p_opponent_id));

   IF p_user_id = p_opponent_id THEN
       RETURN FALSE;
   END IF;

   /* Проверка команд */
   SELECT (u1.team_strapi_document_id = u2.team_strapi_document_id)
     INTO v_same_team
     FROM users u1
     JOIN users u2 ON (u2.strapi_document_id = p_opponent_id)
    WHERE u1.strapi_document_id = p_user_id;
   IF v_same_team THEN
       PERFORM log_message('   same team => FALSE');
       RETURN FALSE;
   END IF;

   /* dismissed? */
   SELECT (u.dismissed_at IS NOT NULL)
     INTO v_opponent_dismissed
     FROM users u
    WHERE u.strapi_document_id = p_opponent_id;
   IF v_opponent_dismissed THEN
       PERFORM log_message('   opponent dismissed => FALSE');
       RETURN FALSE;
   END IF;

   /* strikes */
   SELECT COUNT(*)
     INTO v_strike_count
     FROM strikes s
    WHERE s.user_strapi_document_id = p_opponent_id
      AND s.sprint_strapi_document_id = p_sprint_id;
   IF v_strike_count>0 THEN
       PERFORM log_message('   strike_count>0 => FALSE');
       RETURN FALSE;
   END IF;

   PERFORM log_message('   can_make_pair => TRUE');
   RETURN TRUE;
END;
