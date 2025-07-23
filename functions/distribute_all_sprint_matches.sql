DECLARE
   /* Служебные переменные */
   v_error           TEXT := NULL;
   v_now             TIMESTAMPTZ := now();
   
   /* Записи из базы */
   v_sprint_rec      sprints%ROWTYPE;
   
   /* Счетчики и статистика */
   v_duels_count     INT := 0;
   v_processed_count INT := 0;
   v_success_count   INT := 0;
   v_error_count     INT := 0;
   
   /* Для цикла */
   rec_duel RECORD;
   
   /* Для обработки ошибок в цикле */
   v_duel_error      TEXT;
BEGIN
   PERFORM log_message(format(
     'distribute_all_sprint_matches: start. sprint=%s, mode=%s', 
     p_sprint_id, p_mode
   ));

/* ---------- 1. Валидация входных параметров ------------------------*/
   -- Проверка на NULL и пустые значения
   IF p_sprint_id IS NULL OR trim(p_sprint_id) = '' THEN
       v_error := 'ID спринта не может быть пустым';
   ELSIF p_mode NOT IN ('REGULAR', 'TEST') THEN
       v_error := 'Режим должен быть REGULAR или TEST';
   END IF;

/* ---------- 2. Проверяем спринт -----------------------------------*/
   IF v_error IS NULL OR p_mode = 'TEST' THEN
       SELECT * INTO v_sprint_rec
       FROM sprints
       WHERE strapi_document_id = p_sprint_id;

       IF NOT FOUND THEN
           v_error := coalesce(v_error||'; ', '') ||
                      format('спринт %s не найден', p_sprint_id);
       END IF;
   END IF;

/* ---------- 3. Проверяем наличие дуэлей ---------------------------*/
   IF v_error IS NULL OR p_mode = 'TEST' THEN
       SELECT COUNT(*)
       INTO v_duels_count
       FROM duels d
       WHERE d.sprint_strapi_document_id = p_sprint_id;

       IF v_duels_count = 0 THEN
           v_error := coalesce(v_error||'; ', '') ||
                      'в спринте нет дуэлей для распределения';
       END IF;
   END IF;

/* ---------- 4. Основная логика распределения ----------------------*/
   IF (p_mode = 'TEST') OR (p_mode = 'REGULAR' AND v_error IS NULL) THEN
       
       PERFORM log_message(format(
         '=== distribute_all_sprint_matches for sprint=%s START ===', 
          p_sprint_id
       ));

       -- Очистка логов (опционально)
       -- PERFORM clear_distribution_logs();

       FOR rec_duel IN
           SELECT d.strapi_document_id AS duel_id,
                  d.type,
                  d.duel_number
             FROM duels d
            WHERE d.sprint_strapi_document_id = p_sprint_id
            ORDER BY CASE 
                      WHEN d.type='FULL-CONTACT' THEN 1
                      WHEN d.type='TRAINING' THEN 2
                      ELSE 999 
                     END,
                     d.created_at
       LOOP
           v_processed_count := v_processed_count + 1;
           v_duel_error := NULL;

           BEGIN
               PERFORM log_message(format(
                 '--- distributing for duel=%s type=%s (#%s) ---', 
                  rec_duel.duel_id, 
                  rec_duel.type,
                  COALESCE(rec_duel.duel_number::TEXT, 'N/A')
               ));

               -- Проверяем, существует ли дуэль и корректна ли она
               IF NOT EXISTS (
                   SELECT 1 FROM duels 
                   WHERE strapi_document_id = rec_duel.duel_id
                     AND sprint_strapi_document_id = p_sprint_id
               ) THEN
                   v_duel_error := format('дуэль %s некорректна или не принадлежит спринту', rec_duel.duel_id);
               END IF;

               IF v_duel_error IS NULL OR p_mode = 'TEST' THEN
                   PERFORM distribute_matches_for_one_duel(
                     rec_duel.duel_id, 
                     p_sprint_id
                   );
                   
                   v_success_count := v_success_count + 1;
                   
                   PERFORM log_message(format(
                     '    ✓ duel %s distributed successfully', 
                     rec_duel.duel_id
                   ));
               END IF;

               IF v_duel_error IS NOT NULL THEN
                   v_error_count := v_error_count + 1;
                   PERFORM log_message(format(
                     '    ✗ duel %s error: %s', 
                     rec_duel.duel_id, v_duel_error
                   ));
                   
                   -- В TEST режиме продолжаем, в REGULAR - можем остановиться
                   IF p_mode = 'REGULAR' THEN
                       v_error := coalesce(v_error||'; ', '') || 
                                 format('ошибка в дуэли %s: %s', rec_duel.duel_id, v_duel_error);
                       -- Можно EXIT для остановки или продолжить
                   END IF;
               END IF;

           EXCEPTION
               WHEN OTHERS THEN
                   v_error_count := v_error_count + 1;
                   v_duel_error := format('техническая ошибка: %s', SQLERRM);
                   
                   PERFORM log_message(format(
                     '    ✗ duel %s exception: %s (SQLSTATE: %s)', 
                     rec_duel.duel_id, SQLERRM, SQLSTATE
                   ));

                   IF p_mode = 'REGULAR' THEN
                       v_error := coalesce(v_error||'; ', '') || 
                                 format('техническая ошибка в дуэли %s: %s', rec_duel.duel_id, SQLERRM);
                       -- В продакшене можем остановиться при критической ошибке
                       -- EXIT; -- раскомментировать для остановки на первой ошибке
                   END IF;
           END;
       END LOOP;

       PERFORM log_message(format(
         '=== distribute_all_sprint_matches DONE === Processed: %s, Success: %s, Errors: %s',
         v_processed_count, v_success_count, v_error_count
       ));
   END IF;

/* ---------- 5. Формируем ответ ------------------------------------*/
   IF v_error IS NULL THEN
       RETURN json_build_object(
           'result', 'success',
           'message', format(
               'Распределение завершено успешно. Обработано дуэлей: %s, успешно: %s, ошибок: %s',
               v_processed_count, v_success_count, v_error_count
           ),
           'statistics', json_build_object(
               'total_duels', v_duels_count,
               'processed', v_processed_count,
               'successful', v_success_count,
               'errors', v_error_count
           )
       );
   ELSIF p_mode = 'TEST' THEN
       RETURN json_build_object(
           'result', 'success',
           'message', format(
               'Распределение выполнено в тестовом режиме с предупреждениями: %s. Статистика - обработано: %s, успешно: %s, ошибок: %s',
               v_error, v_processed_count, v_success_count, v_error_count
           ),
           'statistics', json_build_object(
               'total_duels', v_duels_count,
               'processed', v_processed_count,
               'successful', v_success_count,
               'errors', v_error_count
           ),
           'warnings', v_error
       );
   ELSE
       RETURN json_build_object(
           'result', 'error',
           'message', v_error,
           'statistics', json_build_object(
               'total_duels', v_duels_count,
               'processed', v_processed_count,
               'successful', v_success_count,
               'errors', v_error_count
           )
       );
   END IF;

EXCEPTION
    WHEN OTHERS THEN
        PERFORM log_message(format(
            'distribute_all_sprint_matches: critical exception. SQLSTATE=%s, SQLERRM=%s', 
            SQLSTATE, SQLERRM
        ));
        
        IF p_mode = 'TEST' THEN
            RETURN json_build_object(
                'result', 'success',
                'message', format(
                    'Распределение выполнено с критическими ошибками: %s. Статистика - обработано: %s, успешно: %s',
                    SQLERRM, v_processed_count, v_success_count
                ),
                'statistics', json_build_object(
                    'processed', v_processed_count,
                    'successful', v_success_count,
                    'errors', v_error_count + 1
                ),
                'critical_error', SQLERRM
            );
        ELSE
            RETURN json_build_object(
                'result', 'error',
                'message', 'Критическая ошибка при распределении: ' || SQLERRM,
                'statistics', json_build_object(
                    'processed', v_processed_count,
                    'successful', v_success_count,
                    'errors', v_error_count + 1
                ),
                'sqlstate', SQLSTATE
            );
        END IF;
END;
