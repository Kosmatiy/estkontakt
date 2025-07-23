DECLARE
   v_same_team BOOLEAN;
   v_user_base_exists BOOLEAN;
   v_active_count INT;
BEGIN
   PERFORM log_message(format('can_pair_base(duel=%s, user=%s, opp=%s)', 
                              p_duel_strapi_id, p_user_id, p_opponent_id));

   IF p_user_id = p_opponent_id THEN
       PERFORM log_message('   same user => FALSE');
       RETURN FALSE;
   END IF;

   /* команда */
   SELECT (u1.team_strapi_document_id = u2.team_strapi_document_id)
     INTO v_same_team
     FROM users u1
     JOIN users u2 ON (u2.strapi_document_id = p_opponent_id)
    WHERE u1.strapi_document_id = p_user_id;
   IF v_same_team THEN
       PERFORM log_message('   same team => FALSE');
       RETURN FALSE;
   END IF;

   /* dismissed */
   IF EXISTS (
       SELECT 1 FROM users u
        WHERE u.strapi_document_id = p_opponent_id
          AND u.dismissed_at IS NOT NULL
   ) THEN
       PERFORM log_message('   opp dismissed => FALSE');
       RETURN FALSE;
   END IF;

   /* уже есть базовая схватка? */
   SELECT EXISTS (
     SELECT 1
       FROM duel_distributions dd
      WHERE dd.duel_strapi_document_id = p_duel_strapi_id
        AND dd.is_extra = FALSE
        AND dd.is_failed=FALSE
        AND (
           dd.user_strapi_document_id = p_user_id
           OR dd.user_strapi_document_id = p_opponent_id
        )
   ) INTO v_user_base_exists;
   IF v_user_base_exists THEN
       PERFORM log_message('   user or opp already has base => FALSE');
       RETURN FALSE;
   END IF;

   /* Проверяем, не было ли уже схватки (is_failed=FALSE) => тогда это repeat */
   IF EXISTS (
       SELECT 1
         FROM duel_distributions dd
        WHERE dd.duel_strapi_document_id = p_duel_strapi_id
          AND dd.is_failed=FALSE
          AND (
            (dd.user_strapi_document_id = p_user_id 
             AND dd.rival_strapi_document_id = p_opponent_id)
            OR
            (dd.user_strapi_document_id = p_opponent_id 
             AND dd.rival_strapi_document_id = p_user_id)
          )
   ) THEN
       PERFORM log_message('   already had match => this is repeat => FALSE for base');
       RETURN FALSE;
   END IF;

   /* Новый фрагмент: лимит 2 схваток (между user & opponent) в рамках спринта */
   SELECT COUNT(*)
     INTO v_active_count
     FROM duel_distributions dd
     JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_id
      AND dd.is_failed = FALSE
      AND (
        (dd.user_strapi_document_id = p_user_id 
         AND dd.rival_strapi_document_id = p_opponent_id)
        OR
        (dd.user_strapi_document_id = p_opponent_id 
         AND dd.rival_strapi_document_id = p_user_id)
      );

   IF v_active_count >= 1 THEN
       PERFORM log_message('   user & opponent already have >=2 matches in sprint => FALSE for base');
       RETURN FALSE;
   END IF;


   PERFORM log_message('   can_pair_base => TRUE');
   RETURN TRUE;
END;
