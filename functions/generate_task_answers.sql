DECLARE
    /* ── основные сущности ───────────────────────────────────── */
    v_sprint        sprints%ROWTYPE;
    v_stream_id     TEXT;

    /* ── списки задач и пользователей ────────────────────────── */
    v_task_cnt      INT;
    v_total_users   INT;
    v_fail_cnt      INT;              -- сколько студентов провалят
    v_rows_inserted INT := 0;
    v_no_pairs      INT := 0;         -- пропущено сочетаний

    /* ── курсы/циклы ─────────────────────────────────────────── */
    rec_task        RECORD;
    rec_user        RECORD;

    /* ── списки для отчёта ───────────────────────────────────── */
    v_fail_list     TEXT := '';

    /* ── генератор текста ответа ─────────────────────────────── */
    answers CONSTANT TEXT[] := ARRAY[
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

    /* ── временные таблицы ───────────────────────────────────── */
BEGIN
/* 0. спринт / stream */
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object(
            'result','error',
            'message', format('Спринт %s не найден', in_sprint_document_id)
        );
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* список задач спринта */
    CREATE TEMP TABLE _tasks(task_id TEXT) ON COMMIT DROP;
    INSERT INTO _tasks(task_id)
    SELECT t.strapi_document_id
    FROM   tasks t
    JOIN   lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
    WHERE  l.sprint_strapi_document_id = in_sprint_document_id;

    SELECT COUNT(*) INTO v_task_cnt FROM _tasks;
    IF v_task_cnt = 0 THEN
        RETURN json_build_object(
            'result','error',
            'message', format('В спринте %s нет задач', in_sprint_document_id)
        );
    END IF;

    /* очистка */
    IF mode = 'CLEANSLATE' THEN
        DELETE FROM user_task_answers uta
        USING _tasks t
        WHERE uta.task_strapi_document_id = t.task_id;
    END IF;

    /* список всех студентов потока */
    CREATE TEMP TABLE _users(user_id TEXT PRIMARY KEY) ON COMMIT DROP;
    INSERT INTO _users(user_id)
    SELECT strapi_document_id
    FROM   users
    WHERE  stream_strapi_document_id = v_stream_id;

    SELECT COUNT(*) INTO v_total_users FROM _users;
    IF v_total_users = 0 THEN
        RETURN json_build_object(
            'result','error',
            'message', format('В stream %s нет пользователей', v_stream_id)
        );
    END IF;

    /* 1. выбираем «несдавших» студентов */
    v_fail_cnt := CEIL(v_total_users * in_fail_percent / 100.0);

    CREATE TEMP TABLE _fail_users(
        user_id    TEXT PRIMARY KEY,
        skip_count INT               -- сколько заданий будет пропущено
    ) ON COMMIT DROP;

    INSERT INTO _fail_users(user_id, skip_count)
    SELECT u.user_id,
           FLOOR(random()*v_task_cnt)::INT + 1   -- от 1 до всех
    FROM (
        SELECT user_id,
               ROW_NUMBER() OVER (ORDER BY random()) AS rn
        FROM   _users
    ) u
    WHERE  rn <= v_fail_cnt;

    /* 2. для каждого fail-user выбираем, какие задачи пропустить */
    CREATE TEMP TABLE _skip_pairs(
        user_id TEXT,
        task_id TEXT,
        PRIMARY KEY(user_id,task_id)
    ) ON COMMIT DROP;

    INSERT INTO _skip_pairs(user_id, task_id)
    SELECT fu.user_id,
           t.task_id
    FROM _fail_users fu
    JOIN LATERAL (
        SELECT task_id
        FROM   _tasks
        ORDER  BY random()
        LIMIT  fu.skip_count
    ) t ON TRUE;

    /* список имён «несдавших» для отчёта */
    SELECT string_agg(format('%s %s (@%s)', name, surname, telegram_username), ', ')
      INTO v_fail_list
      FROM users
     WHERE strapi_document_id IN (SELECT user_id FROM _fail_users);

    /* 3. основная генерация ответов */
    FOR rec_task IN SELECT * FROM _tasks LOOP
        FOR rec_user IN
            SELECT u.user_id
            FROM   _users u
            WHERE  NOT EXISTS (      -- режим GOON: пропускаем уже заполненные
                       SELECT 1
                       FROM   user_task_answers
                       WHERE  user_strapi_document_id = u.user_id
                         AND  task_strapi_document_id = rec_task.task_id )
        LOOP
            IF EXISTS (
                   SELECT 1
                   FROM   _skip_pairs
                   WHERE  user_id = rec_user.user_id
                     AND  task_id = rec_task.task_id)
            THEN
                v_no_pairs := v_no_pairs + 1;      -- пропущено
                CONTINUE;
            END IF;

            /* создаём ответ */
            INSERT INTO user_task_answers(
                created_at,
                user_strapi_document_id,
                answer_text,
                task_strapi_document_id,
                hash
            )
            VALUES (
                now(),
                rec_user.user_id,
                answers[ floor(random()*array_length(answers,1))::INT + 1 ],
                rec_task.task_id,
                ''
            );

            v_rows_inserted := v_rows_inserted + 1;
        END LOOP;
    END LOOP;

    /* 4. финальный JSON */
    RETURN json_build_object(
        'result'          , 'success',
        'mode'            , mode,
        'sprint'          , v_sprint.sprint_name,
        'stream_id'       , v_stream_id,
        'total_users'     , v_total_users,
        'failed_users'    , v_fail_cnt,
        'fail_users_list' , COALESCE(v_fail_list,'нет'),
        'inserted_rows'   , v_rows_inserted,
        'no_answer_pairs' , v_no_pairs,
        'message'         , format(
            'Сгенерированы ответы задач: %s%% студентов (=%s) не завершили %s–%s заданий. Создано %s строк, пропущено %s сочетаний. Несдавшие: %s.',
            to_char(in_fail_percent,'FM999990.00'),
            v_fail_cnt,
            1,
            v_task_cnt,
            v_rows_inserted,
            v_no_pairs,
            COALESCE(v_fail_list,'нет')
        )
    );

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
        'result' ,'error',
        'message', SQLERRM
    );
END;
