DECLARE
    v_log           jsonb := '[]'::jsonb;
    v_total_players int;
    v_filled        int;
    v_needed        int;
    v_test_result   jsonb;
    v_slot          record;
    v_selected      text;
    stage           int;
BEGIN
    -- 0) Проверка режима
    IF p_mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN jsonb_build_object('status','FAIL','reason','Invalid mode: '||p_mode)::text;
    END IF;

    -- 1) Cleanup
    IF p_mode = 'CLEANSLATE' THEN
        DELETE FROM user_duel_to_review
         WHERE duel_strapi_document_id IN (
           SELECT strapi_document_id
             FROM duels
            WHERE sprint_strapi_document_id = p_sprint_id
         );
        v_log := v_log || jsonb_build_object('step',1,'action','cleanup');
    END IF;

    -- 2) Active players
    CREATE TEMP TABLE tmp_active_players ON COMMIT DROP AS
    SELECT u.strapi_document_id AS user_id,
           ROW_NUMBER() OVER(ORDER BY u.strapi_document_id) AS player_no
      FROM users u
      JOIN user_stream_links usl
        ON usl.user_strapi_document_id = u.strapi_document_id
      JOIN sprints s
        ON s.strapi_document_id = p_sprint_id
     WHERE u.dismissed_at IS NULL
       AND usl.is_active
       AND usl.stream_strapi_document_id = s.stream_strapi_document_id;

    SELECT COUNT(*) INTO v_total_players FROM tmp_active_players;
    IF v_total_players = 0 THEN
        RETURN jsonb_build_object('status','FAIL','reason','No active users','log',v_log)::text;
    END IF;
    v_log := v_log || jsonb_build_object('step',2,'players',v_total_players);

    -- 3) Units (with participant_no)
    CREATE TEMP TABLE tmp_units ON COMMIT DROP AS
    SELECT uda.duel_strapi_document_id AS duel_id,
           uda.hash,
           uda.user_strapi_document_id    AS participant_id,
           uda.rival_user_strapi_document_id AS rival_id,
           d.type                         AS duel_type,
           ap.player_no                   AS participant_no
      FROM user_duel_answers uda
      JOIN duels d
        ON d.strapi_document_id = uda.duel_strapi_document_id
      JOIN tmp_active_players ap
        ON ap.user_id = uda.user_strapi_document_id
     WHERE d.sprint_strapi_document_id = p_sprint_id;
    v_log := v_log || jsonb_build_object('step',3,'units',(SELECT COUNT(*) FROM tmp_units));

    -- 4) Quotas
    CREATE TEMP TABLE tmp_quotas ON COMMIT DROP AS
    SELECT ap.user_id     AS reviewer_id,
           COUNT(u.*)*3    AS quota
      FROM tmp_active_players ap
      LEFT JOIN tmp_units u
        ON u.participant_id = ap.user_id
     GROUP BY ap.user_id;
    v_log := v_log || jsonb_build_object('step',4,'quotas',(SELECT COUNT(*) FROM tmp_quotas));

    -- 5) Slots (now including participant_no)
    CREATE TEMP TABLE tmp_slots (
      duel_id           text,
      hash              text,
      participant_id    text,
      rival_id          text,
      participant_no    int,
      slot_no           int,
      reviewer_id       text     DEFAULT NULL,
      assigned_at_stage int      DEFAULT NULL
    ) ON COMMIT DROP;

    INSERT INTO tmp_slots (duel_id, hash, participant_id, rival_id, participant_no, slot_no)
    SELECT u.duel_id, u.hash, u.participant_id, u.rival_id, u.participant_no, gs.slot_no
      FROM tmp_units u
 CROSS JOIN LATERAL (VALUES (1),(2),(3)) AS gs(slot_no);
    v_log := v_log || jsonb_build_object('step',5,'slots',(SELECT COUNT(*) FROM tmp_slots));

    -- 6) Loads
    CREATE TEMP TABLE tmp_loads ON COMMIT DROP AS
    SELECT reviewer_id, quota, 0 AS assigned, '{}'::jsonb AS assigned_pairs
      FROM tmp_quotas;

    -- 7) Distribution in 3 stages
    FOR stage IN 1..3 LOOP
      LOOP
        SELECT * INTO v_slot
          FROM tmp_slots
         WHERE reviewer_id IS NULL
         ORDER BY duel_id, hash, participant_id, slot_no
         LIMIT 1;
        EXIT WHEN NOT FOUND;

        -- pick candidate with circular shift
        SELECT rl.reviewer_id
          INTO v_selected
          FROM tmp_loads rl
          JOIN tmp_active_players ap
            ON ap.user_id = rl.reviewer_id
         WHERE rl.assigned < rl.quota
           AND rl.reviewer_id <> v_slot.participant_id
           AND rl.reviewer_id <> v_slot.rival_id
           AND NOT (rl.assigned_pairs ? (v_slot.duel_id||'_'||v_slot.hash))
           AND ap.player_no =
               ((v_slot.participant_no + v_slot.slot_no) % v_total_players) + 1
         ORDER BY rl.assigned, rl.reviewer_id
         LIMIT 1;

        EXIT WHEN v_selected IS NULL;

        -- apply assignment
        UPDATE tmp_slots
           SET reviewer_id = v_selected,
               assigned_at_stage = stage
         WHERE duel_id        = v_slot.duel_id
           AND hash           = v_slot.hash
           AND participant_id = v_slot.participant_id
           AND slot_no        = v_slot.slot_no;

        UPDATE tmp_loads
           SET assigned = assigned+1,
               assigned_pairs = assigned_pairs || jsonb_build_object(v_slot.duel_id||'_'||v_slot.hash,true)
         WHERE reviewer_id = v_selected;

        v_log := v_log || jsonb_build_object(
                    'stage',stage,
                    'slot', jsonb_build_object(
                        'duel',v_slot.duel_id,
                        'hash',v_slot.hash,
                        'part',v_slot.participant_id,
                        'slot',v_slot.slot_no
                    ),
                    'rev',v_selected
                 );
      END LOOP;
    END LOOP;

    -- 8) Check all filled
    SELECT COUNT(*) INTO v_filled FROM tmp_slots WHERE reviewer_id IS NOT NULL;
    SELECT COUNT(*) INTO v_needed FROM tmp_slots;
    IF v_filled <> v_needed THEN
      RETURN jsonb_build_object(
        'status','FAIL',
        'reason',format('Not all slots filled: %s/%s',v_filled,v_needed),
        'log',v_log
      )::text;
    END IF;
    v_log := v_log || jsonb_build_object('step',8,'filled',v_filled);

    -- 9) Write out
    INSERT INTO user_duel_to_review(
      reviewer_user_strapi_document_id,
      duel_strapi_document_id,
      user_strapi_document_id,
      hash,
      created_at
    )
    SELECT reviewer_id, duel_id, participant_id, hash, NOW()
      FROM tmp_slots
     WHERE assigned_at_stage IS NOT NULL
    ON CONFLICT DO NOTHING;
    v_log := v_log || jsonb_build_object('step',9,'inserted',v_filled);

    -- 10) Final test
    SELECT test_user_duel_to_review(p_sprint_id) INTO v_test_result;
    IF v_test_result->>'status' <> 'OK' THEN
      RETURN jsonb_build_object(
        'status','FAIL',
        'reason','Test failed: '||v_test_result,
        'log',v_log
      )::text;
    END IF;

    RETURN jsonb_build_object('status','OK','log',v_log)::text;

EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object(
      'status','FAIL',
      'reason',SQLERRM,
      'log',v_log
    )::text;
END;
