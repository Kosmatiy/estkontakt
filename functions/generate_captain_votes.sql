DECLARE
    /* ── спринт / stream ─────────────────────────────────────── */
    v_sprint        sprints%ROWTYPE;
    v_stream_id     TEXT;

    /* ── цикл по командам ───────────────────────────────────── */
    team_id         TEXT;

    /* ── массивы игроков ────────────────────────────────────── */
    team_players    TEXT[];
    candidate_arr   TEXT[];
    voters_arr      TEXT[];

    /* ── служебные ──────────────────────────────────────────── */
    n_players       INT;
    k_candidates    INT;
    n_voters        INT;
    voter_id        TEXT;
    v_candidate_id  TEXT;
    v_inserted      INT := 0;
    v_rc            INT;
BEGIN
    /* 0. спринт и stream */
    SELECT * INTO v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object(
          'result','error',
          'message', format('Спринт %s не найден', in_sprint_document_id)
        );
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* нормализуем границы кандидатов */
    IF in_max_candidates < in_min_candidates THEN
        in_max_candidates := in_min_candidates;
    END IF;
    IF in_min_candidates < 1 THEN
        in_min_candidates := 1;
    END IF;

    /* 1. очистка при CLEANSLATE */
    IF mode = 'CLEANSLATE' THEN
        DELETE FROM user_captain_vote
        WHERE stream_strapi_document_id = v_stream_id;
    END IF;

    /* 2. цикл по командам потока */
    FOR team_id IN
        SELECT DISTINCT team_strapi_document_id
        FROM   users
        WHERE  stream_strapi_document_id = v_stream_id
          AND  dismissed_at IS NULL
          AND  team_strapi_document_id IS NOT NULL
    LOOP
        /* список активных игроков команды */
        SELECT array_agg(strapi_document_id) INTO team_players
        FROM   users
        WHERE  team_strapi_document_id = team_id
          AND  dismissed_at IS NULL;

        n_players := COALESCE(array_length(team_players,1),0);
        IF n_players = 0 THEN CONTINUE; END IF;

        /* 2.1. выбираем k кандидатов */
        k_candidates :=
          LEAST(n_players,
                in_min_candidates +
                floor(random() * (in_max_candidates - in_min_candidates + 1))::INT);

        SELECT array_agg(strapi_document_id) INTO candidate_arr
        FROM (
            SELECT unnest(team_players) AS strapi_document_id
            ORDER BY random()
            LIMIT  k_candidates
        ) sub;

        /* 2.2. выбираем голосующих */
        n_voters := CEIL(n_players * in_percent / 100.0)::INT;
        IF n_voters = 0 THEN CONTINUE; END IF;

        SELECT array_agg(strapi_document_id) INTO voters_arr
        FROM (
            SELECT unnest(team_players) AS strapi_document_id
            ORDER BY random()
            LIMIT  n_voters
        ) sub2;

        /* 2.3. обход voters */
        FOREACH voter_id IN ARRAY voters_arr
        LOOP
            /* GOON: пропуск, если голос уже есть */
            IF mode = 'GOON' THEN
                PERFORM 1
                FROM   user_captain_vote
                WHERE  user_strapi_document_id = voter_id
                  AND  stream_strapi_document_id = v_stream_id;
                IF FOUND THEN CONTINUE; END IF;
            END IF;

            /* определяем кандидата */
            IF voter_id = ANY(candidate_arr) THEN
                v_candidate_id := voter_id;         -- голос «за себя»
            ELSE
                v_candidate_id := candidate_arr[
                                   1 + floor(random()*array_length(candidate_arr,1))::INT ];
            END IF;

            INSERT INTO user_captain_vote(
                user_strapi_document_id,
                candidate_user_strapi_document_id,
                stream_strapi_document_id,
                team_strapi_document_id)
            VALUES (voter_id, v_candidate_id, v_stream_id, team_id)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS v_rc = ROW_COUNT;
            v_inserted := v_inserted + v_rc;
        END LOOP;
    END LOOP;

    RETURN json_build_object(
        'result'           , 'success',
        'generated_records', v_inserted,
        'message'          , format(
           'Голоса капитанов: поток %s, добавлено %s голосов (%.0f%% избирателей, кандидаты %s-%s). Режим %s.',
           v_stream_id,
           v_inserted,
           in_percent,
           in_min_candidates,
           in_max_candidates,
           mode
        )
    );

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
        'result','error',
        'message', SQLERRM
    );
END;
