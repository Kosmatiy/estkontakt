DECLARE
   v_mode TEXT;
   v_total_players INT;
   v_rows_inserted INT := 0;
   v_player_array TEXT[];
   v_pair_count INT;
BEGIN
   -- Валидация режима
   v_mode := COALESCE(upper(p_mode), 'CLEANSLATE');
   IF v_mode NOT IN ('CLEANSLATE', 'GOON') THEN
       RETURN json_build_object('result', 'error', 'message', 'mode = CLEANSLATE | GOON');
   END IF;

   -- CLEANSLATE: очищаем старое распределение
   IF v_mode = 'CLEANSLATE' THEN
       DELETE FROM user_duel_to_review udr
       USING duels d
       WHERE udr.duel_strapi_document_id = d.strapi_document_id
         AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
   END IF;

   -- =====================================================
   -- ШАГ 1: Собираем данные
   -- =====================================================
   
   -- Собираем всех активных игроков
   DROP TABLE IF EXISTS tmp_players;
   CREATE TEMP TABLE tmp_players ON COMMIT DROP AS
   SELECT DISTINCT
       u.strapi_document_id AS player_id,
       COUNT(DISTINCT uda.duel_strapi_document_id || '_' || uda.hash) AS games_count,
       ROW_NUMBER() OVER (ORDER BY u.strapi_document_id) AS position
   FROM users u
   JOIN user_duel_answers uda ON uda.user_strapi_document_id = u.strapi_document_id
   JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
   WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
     AND u.dismissed_at IS NULL
   GROUP BY u.strapi_document_id;

   -- Создаем массив игроков для круговой логики
   SELECT COUNT(*), array_agg(player_id ORDER BY position)
   INTO v_total_players, v_player_array
   FROM tmp_players;

   IF v_total_players = 0 THEN
       RETURN json_build_object('result', 'error', 'message', 'No active players found');
   END IF;

   -- Собираем все пары
   DROP TABLE IF EXISTS tmp_pairs;
   CREATE TEMP TABLE tmp_pairs ON COMMIT DROP AS
   SELECT 
       duel_strapi_document_id,
       hash,
       string_agg(user_strapi_document_id, ',' ORDER BY user_strapi_document_id) AS participants_str,
       array_agg(user_strapi_document_id ORDER BY user_strapi_document_id) AS participants
   FROM user_duel_answers uda
   JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
   WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
   GROUP BY duel_strapi_document_id, hash
   HAVING COUNT(*) = 2;

   SELECT COUNT(*) INTO v_pair_count FROM tmp_pairs;

   -- =====================================================
   -- ШАГ 2: Применяем круговой паттерн
   -- =====================================================
   
   DECLARE
       rec_player RECORD;
       v_reviewer_position INT;
       v_target_position INT;
       v_target_player_id TEXT;
       v_offset INT;
       v_round INT;
       v_assignments_made INT := 0;
       v_base_offsets INT[] := ARRAY[1, 2, 3, 18, 19, 20];
       v_round2_shift INT;
   BEGIN
       -- Для каждого игрока
       FOR rec_player IN SELECT * FROM tmp_players ORDER BY position LOOP
           v_reviewer_position := rec_player.position;
           
           -- Определяем количество раундов (1 или 2)
           FOR v_round IN 1..LEAST(rec_player.games_count, 2) LOOP
               -- Для второго раунда добавляем сдвиг
               IF v_round = 2 THEN
                   v_round2_shift := CASE WHEN v_reviewer_position % 2 = 0 THEN 4 ELSE 3 END;
               ELSE
                   v_round2_shift := 0;
               END IF;
               
               -- Применяем паттерн смещений
               FOREACH v_offset IN ARRAY v_base_offsets LOOP
                   -- Вычисляем целевую позицию по кругу
                   v_target_position := ((v_reviewer_position - 1 + v_round2_shift + v_offset - 1) % v_total_players) + 1;
                   
                   -- Получаем ID игрока на этой позиции
                   v_target_player_id := v_player_array[v_target_position];
                   
                   -- Находим пары, где участвует целевой игрок
                   INSERT INTO user_duel_to_review (
                       reviewer_user_strapi_document_id,
                       duel_strapi_document_id,
                       user_strapi_document_id,
                       hash
                   )
                   SELECT DISTINCT
                       rec_player.player_id,
                       p.duel_strapi_document_id,
                       unnest(p.participants),
                       p.hash
                   FROM tmp_pairs p
                   WHERE v_target_player_id = ANY(p.participants)
                     AND rec_player.player_id != ALL(p.participants)
                     AND NOT EXISTS (
                         SELECT 1 FROM user_duel_to_review existing
                         WHERE existing.reviewer_user_strapi_document_id = rec_player.player_id
                           AND existing.duel_strapi_document_id = p.duel_strapi_document_id
                           AND existing.hash = p.hash
                     )
                   LIMIT 1  -- Берем только одну пару с этим игроком
                   ON CONFLICT DO NOTHING;
                   
                   GET DIAGNOSTICS v_assignments_made = ROW_COUNT;
                   v_rows_inserted := v_rows_inserted + v_assignments_made;
               END LOOP;
           END LOOP;
       END LOOP;
   END;

   -- =====================================================
   -- ШАГ 3: Сбор статистики
   -- =====================================================
   
   DECLARE
       v_stats JSONB;
       v_complete_pairs INT;
       v_incomplete_pairs INT;
   BEGIN
       -- Считаем статистику
       WITH pair_stats AS (
           SELECT 
               duel_strapi_document_id,
               hash,
               COUNT(DISTINCT reviewer_user_strapi_document_id) AS reviewer_count
           FROM user_duel_to_review udr
           JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
           WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
           GROUP BY duel_strapi_document_id, hash
       )
       SELECT 
           COUNT(*) FILTER (WHERE reviewer_count = 6),
           COUNT(*) FILTER (WHERE reviewer_count < 6)
       INTO v_complete_pairs, v_incomplete_pairs
       FROM pair_stats;

       -- Статистика по игрокам
       WITH player_stats AS (
           SELECT 
               p.player_id,
               p.games_count,
               p.games_count * 3 AS expected_reviews,
               COUNT(DISTINCT udr.duel_strapi_document_id || '_' || udr.hash) AS actual_reviews
           FROM tmp_players p
           LEFT JOIN user_duel_to_review udr ON udr.reviewer_user_strapi_document_id = p.player_id
           LEFT JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
                            AND d.sprint_strapi_document_id = p_sprint_strapi_document_id
           GROUP BY p.player_id, p.games_count
       )
       SELECT jsonb_build_object(
           'total_players', v_total_players,
           'total_pairs', v_pair_count,
           'complete_pairs', v_complete_pairs,
           'incomplete_pairs', v_incomplete_pairs,
           'players_with_correct_quota', COUNT(*) FILTER (WHERE expected_reviews = actual_reviews),
           'total_assignments', v_rows_inserted / 2  -- Делим на 2, т.к. каждое назначение создает 2 строки
       ) INTO v_stats
       FROM player_stats;

       RETURN jsonb_build_object(
           'result', 'success',
           'mode', v_mode,
           'algorithm', 'simplified_circular',
           'statistics', v_stats,
           'pattern', jsonb_build_object(
               'base_offsets', v_base_offsets,
               'round2_shift', '3 for odd positions, 4 for even'
           )
       );
   END;

EXCEPTION
   WHEN OTHERS THEN
       RETURN json_build_object(
           'result', 'error',
           'message', SQLERRM,
           'detail', SQLSTATE
       );
END;
