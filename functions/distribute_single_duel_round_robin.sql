DECLARE
    v_run UUID := gen_random_uuid();
    v_mode TEXT;
    v_stream TEXT;
    v_rows_ins INT := 0;
    v_rows_conf INT := 0;
    v_last INT;
    reviewers TEXT[];
    n_rev INT;
    idx INT := 1;
    rec_pair RECORD;
    v_rev TEXT;
    assigned INT;
    tries INT;
BEGIN
    IF p_debug THEN PERFORM log_rr(v_run,'INFO','START',p_sprint,p_duel); END IF;

    v_mode := COALESCE(upper(p_mode),'CLEANSLATE');
    IF v_mode NOT IN ('CLEANSLATE','GOON') THEN
        IF p_debug THEN PERFORM log_rr(v_run,'ERROR','BAD_MODE',p_sprint,p_duel); END IF;
        RETURN json_build_object('result','error','run_id',v_run,'msg','bad mode');
    END IF;

    SELECT stream_strapi_document_id INTO v_stream FROM sprints WHERE strapi_document_id=p_sprint;
    IF v_stream IS NULL THEN
        IF p_debug THEN PERFORM log_rr(v_run,'ERROR','SPRINT_NOT_FOUND',p_sprint,p_duel); END IF;
        RETURN json_build_object('result','error','run_id',v_run,'msg','sprint not found');
    END IF;

    DROP TABLE IF EXISTS tmp_eligible;
    CREATE TEMP TABLE tmp_eligible ON COMMIT DROP AS
    SELECT strapi_document_id FROM users u WHERE u.stream_strapi_document_id=v_stream AND u.dismissed_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM strikes s WHERE s.user_strapi_document_id=u.strapi_document_id AND s.sprint_strapi_document_id=p_sprint);
    IF NOT EXISTS (SELECT 1 FROM tmp_eligible) THEN
        IF p_debug THEN PERFORM log_rr(v_run,'ERROR','NO_ELIGIBLE',p_sprint,p_duel); END IF;
        RETURN json_build_object('result','error','run_id',v_run,'msg','no eligible');
    END IF;

    DROP TABLE IF EXISTS tmp_latest;
    CREATE TEMP TABLE tmp_latest ON COMMIT DROP AS
    WITH r AS (
        SELECT a.*,row_number() OVER (PARTITION BY a.user_strapi_document_id,a.hash ORDER BY a.created_at DESC) rn
        FROM user_duel_answers a
        WHERE a.duel_strapi_document_id=p_duel AND a.user_strapi_document_id IN (SELECT strapi_document_id FROM tmp_eligible))
    SELECT * FROM r WHERE rn=1;

    DROP TABLE IF EXISTS tmp_pairs;
    CREATE TEMP TABLE tmp_pairs ON COMMIT DROP AS
    WITH p AS (
        SELECT hash,array_agg(DISTINCT user_strapi_document_id) players FROM tmp_latest GROUP BY hash)
    SELECT hash,players[1] player1,players[2] player2 FROM p WHERE array_length(players,1)=2;
    IF NOT EXISTS (SELECT 1 FROM tmp_pairs) THEN
        IF p_debug THEN PERFORM log_rr(v_run,'ERROR','NO_PAIRS',p_sprint,p_duel); END IF;
        RETURN json_build_object('result','error','run_id',v_run,'msg','no pairs');
    END IF;

    DROP TABLE IF EXISTS tmp_quota;
    CREATE TEMP TABLE tmp_quota ON COMMIT DROP AS
    SELECT user_strapi_document_id user_id,COUNT(*)*3 remaining FROM tmp_latest GROUP BY user_strapi_document_id;

    SELECT array_agg(u) INTO reviewers FROM (
        SELECT user_id u FROM tmp_quota q JOIN generate_series(1,q.remaining) ON true ORDER BY user_id) sub;
    n_rev := COALESCE(array_length(reviewers,1),0);
    IF n_rev=0 THEN
        IF p_debug THEN PERFORM log_rr(v_run,'ERROR','ZERO_QUOTA',p_sprint,p_duel); END IF;
        RETURN json_build_object('result','error','run_id',v_run,'msg','zero quota');
    END IF;

    IF v_mode='CLEANSLATE' THEN
        DELETE FROM user_duel_to_review WHERE duel_strapi_document_id=p_duel AND hash IN (SELECT hash FROM tmp_pairs);
    END IF;

    FOR rec_pair IN SELECT * FROM tmp_pairs LOOP
        assigned:=0;
        WHILE assigned<6 LOOP
            tries:=0;
            LOOP
                IF tries>=n_rev THEN RAISE EXCEPTION 'no reviewers for %',rec_pair.hash; END IF;
                IF idx>n_rev THEN idx:=1; END IF;
                v_rev:=reviewers[idx];
                IF v_rev NOT IN (rec_pair.player1,rec_pair.player2) AND (SELECT remaining FROM tmp_quota WHERE user_id=v_rev)>0 THEN
                    INSERT INTO user_duel_to_review(reviewer_user_strapi_document_id,duel_strapi_document_id,user_strapi_document_id,hash)
                    VALUES (v_rev,p_duel,rec_pair.player1,rec_pair.hash),
                           (v_rev,p_duel,rec_pair.player2,rec_pair.hash)
                    ON CONFLICT DO NOTHING;
                    GET DIAGNOSTICS v_last = ROW_COUNT;
                    v_rows_ins:=v_rows_ins+v_last;
                    v_rows_conf:=v_rows_conf+(2-v_last);
                    UPDATE tmp_quota SET remaining=remaining-1 WHERE user_id=v_rev;
                    assigned:=assigned+1;
                END IF;
                idx:=idx+1; tries:=tries+1; EXIT WHEN assigned=6;
            END LOOP;
        END LOOP;
    END LOOP;

    IF EXISTS (SELECT 1 FROM (SELECT hash,COUNT(*) c FROM user_duel_to_review WHERE duel_strapi_document_id=p_duel GROUP BY hash) s WHERE c<>12) THEN
        RAISE EXCEPTION 'pairs incomplete';
    END IF;

    IF p_debug THEN PERFORM log_rr(v_run,'INFO','FINISH',p_sprint,p_duel,NULL,NULL,jsonb_build_object('ins',v_rows_ins,'conf',v_rows_conf)); END IF;
    RETURN json_build_object('result','success','run_id',v_run,'inserted',v_rows_ins,'conflict',v_rows_conf);
END;
