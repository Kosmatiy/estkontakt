DECLARE
    v_sprint           sprints%ROWTYPE;
    v_execution_id     TEXT := 'exec_' || extract(epoch from now())::bigint || '_' || random()::text;
    v_cleaned_rows     INT := 0;
    v_inserted_rows    INT := 0;
    v_reviewers        TEXT[];
    v_reviewers_count  INT := 0;
    v_pair_counter     INT := 0;

    rec_duel           RECORD;
    c_participants     INT;
    arr_participants   TEXT[] := '{}';
    v_hash             TEXT;

    -- временная переменная, куда будем сохранять результат GET DIAGNOSTICS
    v_rows             INT := 0;
BEGIN
    -- 1) Проверяем, что спринт существует
    SELECT * INTO v_sprint
    FROM sprints
    WHERE strapi_document_id = p_sprint_strapi_document_id;
    
    IF NOT FOUND THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Спринт %s не найден', p_sprint_strapi_document_id)
        );
    END IF;

    -- 2) Удаляем предыдущие назначения
    DELETE FROM user_duel_to_review AS utdr
    USING duels AS d
    WHERE utdr.duel_strapi_document_id = d.strapi_document_id
      AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
    GET DIAGNOSTICS v_cleaned_rows = ROW_COUNT;

    -- 3) Собираем всех потенциальных рецензентов в таблицу и затем в массив
    DROP TABLE IF EXISTS tmp_reviewers;
    CREATE TEMP TABLE tmp_reviewers(
        idx     SERIAL PRIMARY KEY,
        user_id TEXT
    ) ON COMMIT DROP;

    INSERT INTO tmp_reviewers (user_id)
    SELECT u.strapi_document_id
    FROM users u
    WHERE u.stream_strapi_document_id = v_sprint.stream_strapi_document_id
      AND u.dismissed_at IS NULL;

    SELECT array_agg(tr.user_id ORDER BY tr.idx)
    INTO v_reviewers
    FROM tmp_reviewers tr;

    IF v_reviewers IS NULL OR array_length(v_reviewers,1) < 2 THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', 'Слишком мало активных пользователей для проверки'
        );
    END IF;
    v_reviewers_count := array_length(v_reviewers,1);

    -- 4) Цикл по дуэлям, проверяем, что в дуэли ровно 2 участника, тогда обрабатываем
    v_pair_counter := 0;

    FOR rec_duel IN
        SELECT strapi_document_id
        FROM duels
        WHERE sprint_strapi_document_id = p_sprint_strapi_document_id
    LOOP
        -- Получаем hash для этой дуэли (предполагаем, что hash одинаковый для пары)
        SELECT DISTINCT uda.hash
        INTO v_hash
        FROM user_duel_answers uda
        WHERE uda.duel_strapi_document_id = rec_duel.strapi_document_id
          AND uda.hash IS NOT NULL
          AND uda.hash <> ''
        LIMIT 1;
        
        -- Если hash не найден, пропускаем дуэль
        IF v_hash IS NULL THEN
            CONTINUE;
        END IF;

        -- Считаем количество участников в дуэли
        SELECT COUNT(DISTINCT uda.user_strapi_document_id)
        INTO c_participants
        FROM user_duel_answers uda
        WHERE uda.duel_strapi_document_id = rec_duel.strapi_document_id
          AND uda.hash = v_hash;

        IF c_participants = 2 THEN
            -- Получаем участников дуэли через подзапрос в массив
            SELECT ARRAY(
                SELECT DISTINCT uda.user_strapi_document_id
                FROM user_duel_answers uda
                WHERE uda.duel_strapi_document_id = rec_duel.strapi_document_id
                  AND uda.hash = v_hash
                ORDER BY uda.user_strapi_document_id
                LIMIT 2
            )
            INTO arr_participants;

            IF array_length(arr_participants,1) = 2 THEN
                v_pair_counter := v_pair_counter + 1;

                -- Назначаем 6 проверяющих (0..5)
                FOR off IN 0..5 LOOP
                    DECLARE
                        v_idx INT := ((v_pair_counter - 1) + off) % v_reviewers_count + 1;
                        v_reviewer_user TEXT;
                    BEGIN
                        v_reviewer_user := v_reviewers[v_idx];

                        -- Проверяем, что рецензент не является участником дуэли
                        IF v_reviewer_user <> ALL(arr_participants) THEN
                            BEGIN
                                -- Вставляем записи для обоих участников дуэли
                                INSERT INTO user_duel_to_review (
                                    reviewer_user_strapi_document_id,
                                    duel_strapi_document_id,
                                    user_strapi_document_id,
                                    hash
                                )
                                VALUES
                                    (v_reviewer_user, rec_duel.strapi_document_id, arr_participants[1], v_hash),
                                    (v_reviewer_user, rec_duel.strapi_document_id, arr_participants[2], v_hash)
                                ON CONFLICT DO NOTHING;
                                
                            EXCEPTION WHEN OTHERS THEN
                                RAISE NOTICE 'Insert failed for reviewer %: %', v_reviewer_user, SQLERRM;
                            END;

                            GET DIAGNOSTICS v_rows = ROW_COUNT;
                            v_inserted_rows := v_inserted_rows + v_rows;
                        END IF;
                    END;
                END LOOP;
            END IF;
        END IF;
    END LOOP;

    -- 5) Итог
    RETURN json_build_object(
        'execution_id', v_execution_id,
        'status', 'SUCCESS',
        'message', format(
            'Спринт %s: итеративное round-robin. Удалено %s старых записей, добавлено %s.',
            p_sprint_strapi_document_id, v_cleaned_rows, v_inserted_rows
        ),
        'stats', json_build_object(
            'reviewers_count', v_reviewers_count,
            'cleaned_rows', v_cleaned_rows,
            'inserted_rows', v_inserted_rows,
            'pairs_processed', v_pair_counter
        )
    );

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Ошибка выполнения: %s', SQLERRM),
            'details', json_build_object(
                'error_detail', SQLERRM,
                'error_state', SQLSTATE
            )
        );
END;
