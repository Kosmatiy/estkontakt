DECLARE
   rec_user RECORD;
   v_users_count INT;
   v_leftover INT;
   v_lone_user TEXT;
BEGIN
   PERFORM log_message(format('distribute_matches_for_one_duel(duel=%s, sprint=%s)', 
                              p_duel_strapi_id, p_sprint_id));
   PERFORM log_message(format('Deleting old distributions for duel=%s', p_duel_strapi_id));
   DELETE FROM duel_distributions
    WHERE duel_strapi_document_id = p_duel_strapi_id;

   DROP TABLE IF EXISTS tmp_candidates_duel;
   CREATE TEMP TABLE tmp_candidates_duel AS
   SELECT f.user_strapi_document_id AS user_id,
          f.weight,
          f.team_id
     FROM filter_users_for_sprint(p_sprint_id) f;

   SELECT COUNT(*) INTO v_users_count FROM tmp_candidates_duel;
   IF v_users_count=0 THEN
       PERFORM log_message('   no candidates => return');
       RETURN;
   END IF;

   /* Перебираем каждого пользователя, вызываем try_to_find_opponent_for_base(...) */
   FOR rec_user IN
       SELECT * FROM tmp_candidates_duel
        ORDER BY weight DESC
   LOOP
       IF EXISTS (
           SELECT 1
             FROM user_sprint_state uss
            WHERE uss.duel_strapi_document_id = p_duel_strapi_id
              AND uss.user_strapi_document_id = rec_user.user_id
              AND uss.is_chosen=TRUE
       ) THEN
           PERFORM log_message(format(
             '   user=%s already chosen => skip', 
              rec_user.user_id
           ));
           CONTINUE;
       END IF;

       PERFORM log_message(format(
         '   user=%s => try_to_find_opponent_for_base', 
          rec_user.user_id
       ));
       PERFORM try_to_find_opponent_for_base(
         p_duel_strapi_id, 
         rec_user.user_id, 
         p_sprint_id, 
         FALSE
       );
   END LOOP;

   /* 
     Шаг 6: считаем, сколько пользователей осталось без пары
     (chosen_users = те, у кого is_chosen=TRUE)
   */
   SELECT sub.cnt
     INTO v_leftover
   FROM (
     WITH chosen_users AS (
       SELECT DISTINCT uss.user_strapi_document_id
         FROM user_sprint_state uss
        WHERE uss.duel_strapi_document_id = p_duel_strapi_id
          AND uss.is_chosen = TRUE
     )
     SELECT COUNT(*) AS cnt
       FROM tmp_candidates_duel t
      WHERE t.user_id NOT IN (SELECT user_strapi_document_id 
                                FROM chosen_users)
   ) sub;

   IF v_leftover = 1 THEN
       /* Находим lone_user */
       SELECT sub.user_id
         INTO v_lone_user
       FROM (
         WITH chosen_users AS (
           SELECT DISTINCT uss.user_strapi_document_id
             FROM user_sprint_state uss
            WHERE uss.duel_strapi_document_id = p_duel_strapi_id
              AND uss.is_chosen = TRUE
         )
         SELECT t.user_id
           FROM tmp_candidates_duel t
          WHERE t.user_id NOT IN (
             SELECT user_strapi_document_id 
               FROM chosen_users
          )
          LIMIT 1
       ) sub;

       IF v_lone_user IS NOT NULL THEN
           PERFORM log_message(format(
             '   leftover=1 => handle_no_opponent_found_base(user=%s)', 
              v_lone_user
           ));
           PERFORM handle_no_opponent_found_base(
             p_duel_strapi_id, 
             v_lone_user, 
             p_sprint_id, 
             FALSE
           );
       END IF;

   ELSIF v_leftover > 1 THEN
       INSERT INTO admin_messages(message_text, sprint_strapi_document_id, created_at)
       VALUES(
           format('Leftover %s players on duel=%s', v_leftover, p_duel_strapi_id),
           p_sprint_id,
           now()
       );
       PERFORM log_message(format(
         '   leftover=%s => wrote admin_messages', 
          v_leftover
       ));
   END IF;

   PERFORM log_message(format(
     'distribute_matches_for_one_duel: DONE duel=%s', 
      p_duel_strapi_id
   ));
END;
