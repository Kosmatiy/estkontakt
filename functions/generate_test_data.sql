DECLARE
    /* сведения о спринте / стриме */
    v_sprint          sprints%ROWTYPE;
    v_stream_id       TEXT;

    /* курсоры */
    rec_test          RECORD;
    rec_user          RECORD;

    /* счётчики */
    v_total_users     INT;
    v_fail_count      INT;        -- сколько «не сдавших»
    v_inserted_qa     INT := 0;   -- строк user_question_answers

    /* вспомогательные */
    v_max_score       INT;
    v_score1          INT;
    v_score2          INT;
    v_category        TEXT;       -- 'OK' | 'ZERO' | 'NO_ATTEMPT'
    v_has_failed      BOOLEAN;
    v_fail_list       TEXT;
BEGIN
    /* ── 0. спринт + stream ─────────────────────────────────── */
    SELECT * INTO v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object(
            'result',  'error',
            'message', format('Спринт %s не найден', in_sprint_document_id)
        );
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* ── 1. очистка (CLEANSLATE) ───────────────────────────────*/
    IF mode = 'CLEANSLATE' THEN
        DELETE FROM user_question_answers q
        USING tests t, lectures l
        WHERE q.test_strapi_document_id    = t.strapi_document_id
          AND t.lecture_strapi_document_id = l.strapi_document_id
          AND l.sprint_strapi_document_id  = in_sprint_document_id;

        DELETE FROM user_test_answers a
        USING tests t, lectures l
        WHERE a.test_strapi_document_id    = t.strapi_document_id
          AND t.lecture_strapi_document_id = l.strapi_document_id
          AND l.sprint_strapi_document_id  = in_sprint_document_id;
    END IF;

    /* ── 2. пользователи потока ────────────────────────────────*/
    SELECT COUNT(*) INTO v_total_users
    FROM   users
    WHERE  stream_strapi_document_id = v_stream_id;

    IF v_total_users = 0 THEN
        RETURN json_build_object(
            'result','error',
            'message', format('В stream %s нет пользователей', v_stream_id)
        );
    END IF;

    v_fail_count := CEIL(v_total_users * in_fail_percent / 100.0);

    /* ── 3. набор «не сдавших» ────────────────────────────────*/
    CREATE TEMP TABLE _fail_users(
        user_id    TEXT PRIMARY KEY,
        category   TEXT,        -- 'ZERO' | 'NO_ATTEMPT'
        has_failed BOOLEAN DEFAULT FALSE
    ) ON COMMIT DROP;

    INSERT INTO _fail_users(user_id, category)
    SELECT  uid,
            CASE WHEN rn <= CEIL(v_fail_count / 2.0) THEN 'NO_ATTEMPT'
                 ELSE 'ZERO' END
    FROM (
        SELECT u.strapi_document_id AS uid,
               ROW_NUMBER() OVER (ORDER BY random()) AS rn
        FROM   users u
        WHERE  u.stream_strapi_document_id = v_stream_id
        LIMIT  v_fail_count
    ) sub;

    /* список «несдавших» для отчёта */
    SELECT string_agg(format('%s %s (@%s)', name, surname, telegram_username), ', ')
      INTO v_fail_list
      FROM users
     WHERE strapi_document_id IN (SELECT user_id FROM _fail_users);

    /* ── 4. перебираем тесты спринта ───────────────────────────*/
    FOR rec_test IN
        SELECT t.*
        FROM   tests t
        JOIN   lectures l ON l.strapi_document_id = t.lecture_strapi_document_id
        WHERE  l.sprint_strapi_document_id = in_sprint_document_id
    LOOP
        /* число вопросов в тесте */
        SELECT COUNT(*) INTO v_max_score
        FROM   questions
        WHERE  test_strapi_document_id = rec_test.strapi_document_id;

        IF v_max_score = 0 THEN
            CONTINUE;   -- тест без вопросов
        END IF;

        /* ── 5. перебираем пользователей потока ────────────────*/
        FOR rec_user IN
            SELECT *
            FROM   users
            WHERE  stream_strapi_document_id = v_stream_id
        LOOP
            /* режим GOON – пропускаем, если ответы уже есть */
            IF mode = 'GOON' AND EXISTS (
                   SELECT 1
                   FROM   user_test_answers
                   WHERE  user_strapi_document_id = rec_user.strapi_document_id
                     AND  test_strapi_document_id = rec_test.strapi_document_id
               )
            THEN
                CONTINUE;
            END IF;

            /* категория пользователя */
            SELECT category, has_failed
              INTO v_category, v_has_failed
              FROM _fail_users
             WHERE user_id = rec_user.strapi_document_id;

            IF NOT FOUND THEN
                v_category   := 'OK';
                v_has_failed := TRUE;  -- чтобы проходить «как обычный»
            END IF;

            /* ── ЛОГИКА ГЕНЕРАЦИИ ──────────────────────────────*/
            IF v_category = 'OK' OR v_has_failed THEN
            -----------------------------------------------------------------
            --  обычный студент  (или «неудачник», уже проваливший 1-й тест)
            -----------------------------------------------------------------
                v_score1 := GREATEST(1, CEIL(random() * v_max_score));

                v_inserted_qa := v_inserted_qa
                               + generate_attempt(
                                     rec_user.strapi_document_id,
                                     rec_test.strapi_document_id,
                                     1,
                                     v_score1,
                                     v_max_score,
                                     FALSE
                                 );

                /* вторая попытка — 40 % случаев, если балл < max */
                IF v_score1 < v_max_score AND random() < 0.4 THEN
                    v_score2 := v_score1 + CEIL((v_max_score - v_score1) * random());
                    IF v_score2 > v_max_score THEN
                        v_score2 := v_max_score;
                    END IF;

                    v_inserted_qa := v_inserted_qa
                                   + generate_attempt(
                                         rec_user.strapi_document_id,
                                         rec_test.strapi_document_id,
                                         2,
                                         v_score2,
                                         v_max_score,
                                         FALSE
                                     );
                END IF;

            ELSIF v_category = 'ZERO' AND NOT v_has_failed THEN
            -----------------------------------------------------------------
            --  НЕ сдал: сделал попытку(и), но набрал 0
            -----------------------------------------------------------------
                v_inserted_qa := v_inserted_qa
                               + generate_attempt(
                                     rec_user.strapi_document_id,
                                     rec_test.strapi_document_id,
                                     1,
                                     0,
                                     v_max_score,
                                     FALSE
                                 );
                IF random() < 0.5 THEN   -- иногда делает вторую «0»
                    v_inserted_qa := v_inserted_qa
                                   + generate_attempt(
                                         rec_user.strapi_document_id,
                                         rec_test.strapi_document_id,
                                         2,
                                         0,
                                         v_max_score,
                                         FALSE
                                     );
                END IF;

                UPDATE _fail_users
                   SET has_failed = TRUE
                 WHERE user_id = rec_user.strapi_document_id;

            ELSIF v_category = 'NO_ATTEMPT' AND NOT v_has_failed THEN
            -----------------------------------------------------------------
            --  НЕ сдал: вообще не делал попытку по ЭТОМУ тесту
            -----------------------------------------------------------------
                --   «пропуск» = ни QA, ни TA
                INSERT INTO user_test_answers(
                    created_at, user_strapi_document_id,
                    attempt,    user_score,
                    test_strapi_document_id, max_score
                )
                VALUES (
                    now(), rec_user.strapi_document_id,
                    1,     0,
                    rec_test.strapi_document_id, v_max_score
                );

                UPDATE _fail_users
                   SET has_failed = TRUE
                 WHERE user_id = rec_user.strapi_document_id;
            END IF;
        END LOOP;  -- users
    END LOOP;      -- tests

    /* ── финальный JSON ────────────────────────────────────────*/
    /* ── финальный JSON ────────────────────────────────────────*/
    RETURN json_build_object(
        'result'            , 'success',
        'mode'              , mode,
        'sprint'            , v_sprint.sprint_name,
        'stream_id'         , v_stream_id,
        'total_users'       , v_total_users,
        'fail_users_count'  , v_fail_count,
        'fail_users_list'   , COALESCE(v_fail_list,'нет'),
        /* ↓↓↓  изменили только эту строку ↓↓↓ */
        'message' , format(
            'Сгенерированы ответы: %s%% не сдали (%s пользователей). Несдавшие: %s.',
            to_char(in_fail_percent,'FM999990.00'),
            v_fail_count,
            COALESCE(v_fail_list,'нет')
        )
    );
EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
        'result' ,'error',
        'message', SQLERRM
    );
END;
