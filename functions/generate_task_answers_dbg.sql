DECLARE
    v_sprint            sprints%ROWTYPE;
    v_stream_id         TEXT;

    rec_task            RECORD;
    rec_user            RECORD;

    v_rows_inserted     INT := 0;
    v_no_answer_pairs   INT := 0;
    v_fail_list         TEXT := '';

    v_user_ids          TEXT[];
    v_total_users       INT;
    v_fail_cnt_task     INT;

    v_rand_idx          INT;
    v_answer_text       TEXT;

    answers TEXT[] := ARRAY[
        'Без труда не выловишь и рыбку из пруда.',
        'Один в поле не воин.',
        'Лучше меньше, да лучше.',
        'Не говори «да», пока не увидишь.',
        'Сколько людей, столько мнений.',
        'Где тон, там и рвется.',
        'Молчание — знак согласия.',
        'Умей слушать – найдешь ключ к решению.',
        'Доверяй, но проверяй.',
        'Семь раз отмерь, один раз отрежь.',
        'Работа не волк – в лес не убежит.',
        'Конец – делу венец.',
        'На чужой каравай рот не разевай.',
        'Кто не рискует, тот не пьет шампанского.',
        'Лучше синица в руках, чем журавль в небе.',
        'Вода камень точит.',
        'Сила в единстве.',
        'Сделал дело – гуляй смело.',
        'Не откладывай на завтра то, что можно сделать сегодня.',
        'Дорогу осилит идущий.',
        'Искусство переговоров – это искусство слушания.',
        'У каждой проблемы есть свое решение.',
        'Хорошая сделка – половина победы.',
        'Кто владеет словом, тот владеет миром.',
        'Дело доказывается поступками.',
        'Слово не воробей – вылетит, не поймаешь.',
        'Знать меру – значит быть мудрым.',
        'Умный не спорит – мудрый переслушивает.',
        'Без компромиссов никуда не деться.',
        'Переговоры – дело тонкое, а слово – важное.',
        'Общий язык – залог успеха.',
        'Пустые разговоры – пустые решения.',
        'Терпение и труд – ключ к победе.',
        'Одной речью не пересечь бурю.',
        'Сначала слушай, потом действуй.',
        'Умение слышать – половина успеха.',
        'Дружба и доверие – основа переговоров.',
        'Мудрость на переговорах – залог взаимопонимания.',
        'Каждый голос должен быть услышан.',
        'Команда, как семья, сильна в единстве.',
        'Хороший лидер знает меру слов и дел.',
        'Планирование – шаг к успеху.',
        'Каждая встреча – шанс на победу.',
        'Велик тот, кто умеет слушать.',
        'Уважаем мнение другого – строим будущее.',
        'Соблазна быстрых решений будь осторожен.',
        'Дисциплина и порядок – ключ к управлению.',
        'В переговорах главное не выиграть спор, а найти общий язык.',
        'Своевременное слово спасает от ссоры.',
        'Каждый диалог – возможность для роста.'
    ];
BEGIN
    /* ─── инициализация ─── */
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RAISE INFO 'JSON:%', json_build_object(
            'stage','init','result','error',
            'reason','SPRINT_NOT_FOUND','id',in_sprint_document_id);
        RETURN json_build_object('result','error','message','Sprint not found');
    END IF;

    v_stream_id := v_sprint.stream_strapi_document_id;

    RAISE INFO 'JSON:%', json_build_object(
        'stage','init','mode',mode,
        'sprint',v_sprint.strapi_document_id,
        'stream',v_stream_id,
        'fail_percent',in_fail_percent);

    /* ─── очистка ─── */
    IF mode = 'CLEANSLATE' THEN
        DELETE
          FROM user_task_answers
         USING tasks t, lectures l
         WHERE task_strapi_document_id = t.strapi_document_id
           AND t.lecture_strapi_document_id = l.strapi_document_id
           AND l.sprint_strapi_document_id  = in_sprint_document_id;
        RAISE INFO 'JSON:%', json_build_object(
            'stage','cleanup','rows_deleted',FOUND);
    END IF;

    /* ─── обход заданий ─── */
    FOR rec_task IN
        SELECT t.*
          FROM tasks t
          JOIN lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
         WHERE l.sprint_strapi_document_id = in_sprint_document_id
    LOOP
        /* список пользователей-кандидатов */
        IF mode = 'GOON' THEN
            SELECT ARRAY_AGG(u.strapi_document_id)
              INTO v_user_ids
              FROM users u
             WHERE u.stream_strapi_document_id = v_stream_id
               AND NOT EXISTS (
                     SELECT 1
                       FROM user_task_answers uta
                      WHERE uta.user_strapi_document_id = u.strapi_document_id
                        AND uta.task_strapi_document_id = rec_task.strapi_document_id);
        ELSE
            SELECT ARRAY_AGG(u.strapi_document_id)
              INTO v_user_ids
              FROM users u
             WHERE u.stream_strapi_document_id = v_stream_id;
        END IF;

        v_total_users := COALESCE(array_length(v_user_ids,1),0);
        IF v_total_users = 0 THEN CONTINUE; END IF;

        v_fail_cnt_task := CEIL(v_total_users * in_fail_percent / 100.0);

        RAISE INFO 'JSON:%', json_build_object(
            'stage','task_start',
            'task',rec_task.strapi_document_id,
            'total_users',v_total_users,
            'fail_cnt_task',v_fail_cnt_task);

        /* ── цикл по пользователям в случайном порядке ── */
        FOR rec_user IN
            SELECT uid            AS user_id,
                   ROW_NUMBER() OVER (ORDER BY random()) AS rn
              FROM unnest(v_user_ids) AS uid
        LOOP
            /* пропускаем «неответивших» */
            IF rec_user.rn <= v_fail_cnt_task THEN
                v_no_answer_pairs := v_no_answer_pairs + 1;

                /* собираем человекочитаемый список */
                IF position(rec_user.user_id in v_fail_list) = 0 THEN
                    SELECT
                      CASE WHEN v_fail_list = ''
                           THEN name||' '||surname||' ('||telegram_username||')'
                           ELSE v_fail_list || ', ' ||
                                name||' '||surname||' ('||telegram_username||')'
                      END
                      INTO v_fail_list
                      FROM users WHERE strapi_document_id = rec_user.user_id;
                END IF;
                CONTINUE;
            END IF;

            /* вставка ответа */
            v_rand_idx    := CEIL(random()*array_length(answers,1));
            v_answer_text := answers[v_rand_idx];

            INSERT INTO user_task_answers(
                created_at,
                user_strapi_document_id,
                answer_text,
                task_strapi_document_id,
                hash)
            VALUES (now(), rec_user.user_id, v_answer_text,
                    rec_task.strapi_document_id, '');

            v_rows_inserted := v_rows_inserted + 1;
        END LOOP;

        RAISE INFO 'JSON:%', json_build_object(
            'stage','task_done',
            'task',rec_task.strapi_document_id,
            'inserted_rows_total',v_rows_inserted,
            'no_answer_pairs_total',v_no_answer_pairs);
    END LOOP;

    /* ─── итог ─── */
    RAISE INFO 'JSON:%', json_build_object(
        'stage','finish',
        'inserted_rows',v_rows_inserted,
        'no_answer_pairs',v_no_answer_pairs,
        'distinct_failed_users',
        CASE WHEN v_fail_list='' THEN 'нет' ELSE v_fail_list END);

    RETURN json_build_object(
        'result'          ,'success',
        'inserted_rows'   ,v_rows_inserted,
        'no_answer_pairs' ,v_no_answer_pairs,
        'failed_users'    ,CASE WHEN v_fail_list='' THEN 'нет' ELSE v_fail_list END);
EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'JSON:%', json_build_object(
        'stage','exception','error',SQLERRM);
    RETURN json_build_object('result','error','message',SQLERRM);
END;
