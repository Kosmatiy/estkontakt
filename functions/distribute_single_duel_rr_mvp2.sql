DECLARE
    v_run   UUID := gen_random_uuid();
    v_mode  TEXT := COALESCE(upper(p_mode),'CLEANSLATE');
    v_stream TEXT;

    reviewers_order TEXT[] := '{}';
    pairs_cycle     TEXT[] := '{}';
    idx_pair        INT     := 1;

    pairs_rec  RECORD;
    v_reviewer TEXT;
    progress   BOOLEAN;
    v_rows     INT := 0;
BEGIN
    /* 0. режим */
    IF v_mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN json_build_object('run',v_run,'result','error','msg','mode CLEANSLATE|GOON');
    END IF;

    /* 1. stream */
    SELECT stream_strapi_document_id INTO v_stream
      FROM sprints WHERE strapi_document_id = p_sprint;
    IF v_stream IS NULL THEN
        RETURN json_build_object('run',v_run,'result','error','msg','sprint not found');
    END IF;

    /* 2. eligible */
    DROP TABLE IF EXISTS _eligible;
    CREATE TEMP TABLE _eligible ON COMMIT DROP AS
    SELECT strapi_document_id
      FROM users
     WHERE stream_strapi_document_id = v_stream
       AND dismissed_at IS NULL
       AND NOT EXISTS (
            SELECT 1 FROM strikes
             WHERE sprint_strapi_document_id = p_sprint
               AND user_strapi_document_id   = users.strapi_document_id);

    /* 3. latest answers */
    DROP TABLE IF EXISTS _latest;
    CREATE TEMP TABLE _latest ON COMMIT DROP AS
    WITH r AS (
        SELECT a.*,
               row_number() OVER (PARTITION BY a.user_strapi_document_id,a.hash
                                  ORDER BY a.created_at DESC) rn
        FROM   user_duel_answers a
        WHERE  a.duel_strapi_document_id = p_duel
          AND  a.user_strapi_document_id IN (SELECT * FROM _eligible))
    SELECT * FROM r WHERE rn = 1;

    /* 4. pairs */
    DROP TABLE IF EXISTS _pairs;
    CREATE TEMP TABLE _pairs ON COMMIT DROP AS
    WITH p AS (
        SELECT hash,
               array_agg(DISTINCT user_strapi_document_id) AS players
        FROM   _latest
        GROUP  BY hash)
    SELECT hash,
           players[1] AS p1,
           players[2] AS p2,
           6          AS need,
           '{}'::TEXT[] AS reviewers
    FROM   p
    WHERE  array_length(players,1) = 2;

    IF NOT FOUND THEN
        RETURN json_build_object('run',v_run,'result','error','msg','no pairs');
    END IF;

    /* 5. quota */
    DROP TABLE IF EXISTS _quota;
    CREATE TEMP TABLE _quota ON COMMIT DROP AS
    SELECT user_strapi_document_id AS reviewer,
           COUNT(*)*3              AS quota_left
    FROM   _latest
    GROUP  BY user_strapi_document_id;

    IF NOT EXISTS (SELECT 1 FROM _quota WHERE quota_left > 0) THEN
        RETURN json_build_object('run',v_run,'result','error','msg','no reviewers with quota');
    END IF;

    /* 6. CLEANSLATE */
    IF v_mode='CLEANSLATE' THEN
        DELETE FROM user_duel_to_review
         WHERE duel_strapi_document_id = p_duel
           AND hash IN (SELECT hash FROM _pairs);
    END IF;

    /* 7. кольца */
    SELECT COALESCE(
           array_agg(reviewer ORDER BY quota_left DESC, reviewer),'{}')
      INTO reviewers_order
      FROM _quota
     WHERE quota_left > 0;

    SELECT COALESCE(array_agg(hash ORDER BY hash),'{}')
      INTO pairs_cycle
      FROM _pairs;

    /* 8. основной цикл */
    progress := TRUE;
    WHILE progress LOOP
        progress := FALSE;

        SELECT COALESCE(
           array_agg(reviewer ORDER BY quota_left DESC, reviewer),'{}')
          INTO reviewers_order
          FROM _quota WHERE quota_left > 0;

        IF array_length(reviewers_order,1) IS NULL THEN
            EXIT;               -- никто больше не может проверять
        END IF;

        FOREACH v_reviewer IN ARRAY reviewers_order LOOP
            IF (SELECT quota_left FROM _quota WHERE reviewer=v_reviewer) = 0 THEN
                CONTINUE;
            END IF;

            FOR i IN 1..array_length(pairs_cycle,1) LOOP
                idx_pair := (idx_pair % array_length(pairs_cycle,1)) + 1;
                SELECT * INTO pairs_rec
                  FROM _pairs WHERE hash = pairs_cycle[idx_pair];

                IF pairs_rec.need = 0
                   OR v_reviewer = ANY(ARRAY[pairs_rec.p1,pairs_rec.p2])
                   OR v_reviewer = ANY(pairs_rec.reviewers) THEN
                    CONTINUE;
                END IF;

                INSERT INTO user_duel_to_review(
                         reviewer_user_strapi_document_id,
                         duel_strapi_document_id,
                         user_strapi_document_id,
                         hash)
                VALUES (v_reviewer,p_duel,pairs_rec.p1,pairs_rec.hash),
                       (v_reviewer,p_duel,pairs_rec.p2,pairs_rec.hash)
                ON CONFLICT DO NOTHING;

                UPDATE _pairs
                   SET need      = need - 1,
                       reviewers = reviewers || v_reviewer
                 WHERE hash = pairs_rec.hash;

                UPDATE _quota
                   SET quota_left = quota_left - 1
                 WHERE reviewer = v_reviewer;

                v_rows   := v_rows + 2;
                progress := TRUE;
                EXIT;           -- к следующему reviewer
            END LOOP;
        END LOOP;
    END LOOP;

    /* 9. проверка */
    IF EXISTS (SELECT 1 FROM _pairs WHERE need > 0) THEN
        RAISE EXCEPTION 'pairs open: %',
                        (SELECT COUNT(*) FROM _pairs WHERE need>0);
    END IF;

    RETURN json_build_object('run',v_run,'result','success','rows',v_rows);
END;
