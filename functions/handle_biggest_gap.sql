DECLARE
    -- Нужно для цикла итераций
    v_iteration INT := 0;
    

    -- «Очки» (score) лучшего распределения и текущего
    v_best_score NUMERIC;
    v_current_score NUMERIC;

    -- Пара пользователей (user1, user2), у которых максимальный разрыв rank-gap
    v_user1 TEXT;
    v_user2 TEXT;
    v_rank_gap INT := -1;

    -- Сохраняем их «старые» веса (чтобы откатить, если не улучшилось)
    v_user1_old_weight NUMERIC;
    v_user2_old_weight NUMERIC;

    -- Флаг улучшения
    v_improved BOOLEAN;

    -- Для пересчёта веса у всех пользователей (нужно RECORD):
    rec_user RECORD; 
BEGIN
    PERFORM log_message(format('handle_biggest_gap: START iterative improvement for sprint=%s', p_sprint_id));

    /*
      1) Сначала делаем начальное распределение (distribute_all_sprint_matches),
         считаем score (calculate_distribution_score),
         сохраняем в temp_best_distribution.
    */
    PERFORM distribute_all_sprint_matches(p_sprint_id);
    v_best_score := calculate_distribution_score(p_sprint_id);

    PERFORM log_message(format('   initial distribution => score=%s', v_best_score));

    -- Создадим (или пересоздадим) таблицу temp_best_distribution
    EXECUTE 'DROP TABLE IF EXISTS temp_best_distribution';
    EXECUTE 'CREATE TABLE temp_best_distribution AS SELECT * FROM duel_distributions';

    /*
      2) Цикл до max_steps (в нашем случае 100)
         На каждой итерации:
           - Находим пару с максимальным rank-gap
           - +100 им к весу
           - Снова распределяем
           - Если улучшилось — запоминаем
           - Если нет — откатываем
    */
    WHILE v_iteration < v_max_steps LOOP
        v_iteration := v_iteration + 1;
        v_improved := FALSE;

        /* 2.1) Находим пару (u1,u2) с максимальным rank-gap */
        DROP TABLE IF EXISTS tmp_pairs_for_gap;
        CREATE TEMP TABLE tmp_pairs_for_gap AS
        SELECT DISTINCT 
               LEAST(dd.user_strapi_document_id, dd.rival_strapi_document_id) AS u1,
               GREATEST(dd.user_strapi_document_id, dd.rival_strapi_document_id) AS u2
          FROM duel_distributions dd
          JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
         WHERE d.sprint_strapi_document_id = p_sprint_id
           AND dd.is_failed=FALSE;

        DROP TABLE IF EXISTS tmp_gap_calc;
        CREATE TEMP TABLE tmp_gap_calc AS
        SELECT p.u1, p.u2,
               ABS(vs1.user_duels_total_rank - vs2.user_duels_total_rank) AS rank_gap
          FROM tmp_pairs_for_gap p
          JOIN view_rank_scores vs1 ON vs1.user_strapi_document_id = p.u1
          JOIN view_rank_scores vs2 ON vs2.user_strapi_document_id = p.u2
        ORDER BY rank_gap DESC;

        SELECT u1, u2, rank_gap
          INTO v_user1, v_user2, v_rank_gap
          FROM tmp_gap_calc
         ORDER BY rank_gap DESC
         LIMIT 1;

        IF v_user1 IS NULL OR v_user2 IS NULL THEN
            PERFORM log_message(format(
              '   iteration=%s => no pairs or no rank => stop', 
               v_iteration
            ));
            EXIT;
        END IF;

        /* 2.2) Запоминаем старые weight у обоих */
        SELECT weight INTO v_user1_old_weight 
          FROM users 
         WHERE strapi_document_id = v_user1;

        SELECT weight INTO v_user2_old_weight
          FROM users 
         WHERE strapi_document_id = v_user2;

        /* Прибавляем +100 к обоим */
        UPDATE users
           SET weight = weight + 100
         WHERE strapi_document_id IN (v_user1, v_user2);

        PERFORM log_message(format(
          '   iteration=%s => user1=%s, user2=%s => +100 each, rank_gap=%s', 
           v_iteration, v_user1, v_user2, v_rank_gap
        ));

        PERFORM reset_is_chosen_for_sprint(p_sprint_id);        -- DELETE FROM duel_distributions;

        /* 2.3) Делаем перераспределение */
        PERFORM distribute_all_sprint_matches(p_sprint_id);

        /* 2.4) Считаем новый score */
        v_current_score := calculate_distribution_score(p_sprint_id);

        IF v_current_score < v_best_score THEN
            -- улучшение
            v_best_score := v_current_score;
            v_improved := TRUE;

            EXECUTE 'DROP TABLE IF EXISTS temp_best_distribution';
            EXECUTE 'CREATE TABLE temp_best_distribution AS SELECT * FROM duel_distributions';

            PERFORM log_message(format(
              '   iteration=%s => improved => old_score=?, new_score=%s => SAVED as best', 
               v_iteration, v_current_score
            ));
        ELSE
            -- откат
            UPDATE users
               SET weight = v_user1_old_weight
             WHERE strapi_document_id = v_user1;

            UPDATE users
               SET weight = v_user2_old_weight
             WHERE strapi_document_id = v_user2;

            PERFORM log_message(format(
              '   iteration=%s => no improvement => revert old distribution', 
               v_iteration
            ));

            /* Удаляем только те записи, которые относятся к дуэлям этого спринта */
            DELETE FROM duel_distributions
             WHERE duel_strapi_document_id IN (
                SELECT d.strapi_document_id 
                  FROM duels d
                 WHERE d.sprint_strapi_document_id = p_sprint_id
             );

            /* Восстанавливаем для этих дуэлей из temp_best_distribution */
            INSERT INTO duel_distributions
            SELECT *
              FROM temp_best_distribution tbd
             WHERE tbd.duel_strapi_document_id IN (
                SELECT d.strapi_document_id 
                  FROM duels d
                 WHERE d.sprint_strapi_document_id = p_sprint_id
             );

            /* цикл продолжается — возможно на следующей итерации найдём что-то лучше */
        END IF;
    END LOOP;

    /*
      3) После всех итераций восстанавливаем «лучшее» сохранённое распределение
    */
    PERFORM log_message(format('   all done => restoring best distribution => best_score=%s', v_best_score));

    DELETE FROM duel_distributions
     WHERE duel_strapi_document_id IN (
        SELECT d.strapi_document_id
          FROM duels d
         WHERE d.sprint_strapi_document_id = p_sprint_id
     );

    INSERT INTO duel_distributions
    SELECT *
      FROM temp_best_distribution tbd
     WHERE tbd.duel_strapi_document_id IN (
        SELECT d.strapi_document_id
          FROM duels d
         WHERE d.sprint_strapi_document_id = p_sprint_id
     );

    /*
      4) Финально пересчитываем вес (recalc_user_weight) для всех пользователей
         этого спринта — уже по итоговому лучшему распределению
    */
    PERFORM log_message('handle_biggest_gap => recalc_user_weight for all sprint users start');

    FOR rec_user IN
        SELECT f.user_strapi_document_id AS uid
          FROM filter_users_for_sprint(p_sprint_id) f
    LOOP
        PERFORM recalc_user_weight(p_sprint_id, rec_user.uid);
    END LOOP;

    PERFORM log_message('handle_biggest_gap: FINISHED => final best distribution applied.');
END;
