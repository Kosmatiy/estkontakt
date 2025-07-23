DECLARE
    /* ── основные переменные ─────────────────── */
    v_sprint          sprints%ROWTYPE;
    v_stream_id       TEXT;
    v_mode            TEXT;

    /* ── итоговые счётчики ───────────────────── */
    v_pairs_total     INT;
    v_pairs_full      INT;
    v_pairs_short     INT;
    v_unassigned      INT;
    v_rows_inserted   INT := 0;

    /* ── рабочие переменные цикла ─────────────── */
    v_source_pair_id  INT;
    v_target_pair_id  INT;
    v_review_user     TEXT;
    v_rowcnt          INT;
    try_counter       INT := 0;
BEGIN
    /* 0. режим */
    v_mode := COALESCE(upper(p_mode),'CLEANSLATE');
    IF v_mode NOT IN ('CLEANSLATE','GOON') THEN
        RETURN json_build_object('result','error','message','mode must be CLEANSLATE or GOON');
    END IF;

    /* 1. спринт / stream */
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = p_sprint_strapi_document_id;
    IF NOT FOUND THEN
        RETURN json_build_object('result','error','message','sprint not found');
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* 2. eligible-студенты */
    CREATE TEMP TABLE tmp_students ON COMMIT DROP AS
    SELECT u.strapi_document_id AS user_id,
           0                    AS quota_left
      FROM users u
     WHERE u.stream_strapi_document_id = v_stream_id
       AND u.dismissed_at IS NULL
       AND NOT EXISTS (
             SELECT 1 FROM strikes s
              WHERE s.sprint_strapi_document_id = p_sprint_strapi_document_id
                AND s.user_strapi_document_id   = u.strapi_document_id
         );
    IF NOT EXISTS (SELECT 1 FROM tmp_students) THEN
        RETURN json_build_object('result','error','message','no eligible students');
    END IF;

    /* 3. последние ответы */
    CREATE TEMP TABLE tmp_latest_answers ON COMMIT DROP AS
    WITH ranked AS (
        SELECT a.*,
               row_number() OVER (
                   PARTITION BY a.user_strapi_document_id,
                                a.duel_strapi_document_id,
                                a.hash
                   ORDER BY a.created_at DESC) AS rn
          FROM user_duel_answers a
          JOIN duels d ON d.strapi_document_id = a.duel_strapi_document_id
         WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
           AND a.user_strapi_document_id   = ANY (SELECT user_id FROM tmp_students)
    )
    SELECT * FROM ranked WHERE rn = 1;

    /* 4. пары (hash+duel) */
    CREATE TEMP TABLE tmp_pairs ON COMMIT DROP AS
    SELECT row_number() OVER ()                     AS pair_id,
           la.hash                                  AS pair_hash,
           la.duel_strapi_document_id               AS duel_id,
           min(la.user_strapi_document_id)          AS user1_id,
           max(la.user_strapi_document_id)          AS user2_id,
           6                                        AS in_left,
           6                                        AS out_left
      FROM tmp_latest_answers la
     GROUP BY la.hash, la.duel_strapi_document_id
    HAVING count(distinct la.user_strapi_document_id)=2;

    SELECT count(*) INTO v_pairs_total FROM tmp_pairs;
    IF v_pairs_total < 7 THEN
        RETURN json_build_object('result','error','message','<7 pairs – 6×6 impossible');
    END IF;

    /* 5. квоты студентов (3 review-пары за каждый свой ответ) */
    UPDATE tmp_students s
       SET quota_left = q.cnt * 3
      FROM (
         SELECT la.user_strapi_document_id AS uid,
                count(*)                   AS cnt
           FROM tmp_latest_answers la
          GROUP BY la.user_strapi_document_id
      ) q
     WHERE q.uid = s.user_id;
    DELETE FROM tmp_students WHERE quota_left = 0;

    /* 6. CLEANSLATE */
    IF v_mode = 'CLEANSLATE' THEN
        DELETE FROM user_duel_to_review utr
         USING duels d
         WHERE d.strapi_document_id        = utr.duel_strapi_document_id
           AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
    END IF;

    /* 7. уже сделанные назначения (GOON) */
    CREATE TEMP TABLE tmp_done ON COMMIT DROP AS
    SELECT utr.reviewer_user_strapi_document_id AS reviewer_id,
           utr.hash                             AS pair_hash,
           utr.duel_strapi_document_id         AS duel_id,
           count(*) / 2                        AS edges_done      -- 2 строки = 1 edge
      FROM user_duel_to_review utr
      JOIN duels d ON d.strapi_document_id = utr.duel_strapi_document_id
     WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
     GROUP BY 1,2,3;

    UPDATE tmp_pairs p
       SET in_left = greatest(6 - d.edges_done,0)
      FROM tmp_done d
     WHERE d.pair_hash = p.pair_hash
       AND d.duel_id   = p.duel_id;

    UPDATE tmp_students s
       SET quota_left = greatest(s.quota_left - d.edges_done,0)
      FROM (
         SELECT reviewer_id, sum(edges_done) AS edges_done
           FROM tmp_done
          GROUP BY 1
      ) d
     WHERE d.reviewer_id = s.user_id;

    /* 8. совместимость пар (нет общих студентов) */
    CREATE TEMP TABLE tmp_compat ON COMMIT DROP AS
    SELECT p1.pair_id AS source_id,
           p2.pair_id AS target_id
      FROM tmp_pairs p1
      JOIN tmp_pairs p2 ON p2.pair_id <> p1.pair_id
     WHERE p2.user1_id NOT IN (p1.user1_id,p1.user2_id)
       AND p2.user2_id NOT IN (p1.user1_id,p1.user2_id);

    /* 9. жадный цикл */
    LOOP
        /* 9.1 источник */
        SELECT p.pair_id
          INTO v_source_pair_id
          FROM tmp_pairs p
          JOIN LATERAL (
              SELECT count(*) AS opts
                FROM tmp_compat c
                JOIN tmp_pairs t ON t.pair_id = c.target_id
               WHERE c.source_id = p.pair_id
                 AND t.in_left  > 0
          ) o ON true
         WHERE p.out_left > 0
         ORDER BY (p.out_left::numeric / (o.opts+1)) DESC,
                  o.opts
         LIMIT 1;
        EXIT WHEN NOT FOUND;

        /* 9.2 цель */
        SELECT t.pair_id
          INTO v_target_pair_id
          FROM tmp_compat c
          JOIN tmp_pairs t ON t.pair_id = c.target_id
         WHERE c.source_id = v_source_pair_id
           AND t.in_left   > 0
         ORDER BY t.in_left DESC, random()
         LIMIT 1;
        IF NOT FOUND THEN
            try_counter := try_counter + 1;
            IF try_counter > 20 THEN EXIT; END IF;
            CONTINUE;
        END IF;
        try_counter := 0;

        /* 9.3 конкретный reviewer-student */
        SELECT CASE
                 WHEN s1.quota_left > 0 THEN p.user1_id
                 ELSE p.user2_id
               END
          INTO v_review_user
          FROM tmp_pairs p
          JOIN tmp_students s1 ON s1.user_id = p.user1_id
          JOIN tmp_students s2 ON s2.user_id = p.user2_id
         WHERE p.pair_id = v_source_pair_id;

        IF v_review_user IS NULL
           OR (SELECT quota_left FROM tmp_students WHERE user_id=v_review_user)=0 THEN
           UPDATE tmp_pairs SET out_left = 0 WHERE pair_id = v_source_pair_id;
           CONTINUE;
        END IF;

        /* 9.4 вставка двух строк */
        INSERT INTO user_duel_to_review(
            reviewer_user_strapi_document_id,
            duel_strapi_document_id,
            user_strapi_document_id,
            hash)
        SELECT v_review_user,
               p.duel_id,
               unnest(ARRAY[p.user1_id,p.user2_id]),
               p.pair_hash
          FROM tmp_pairs p
         WHERE p.pair_id = v_target_pair_id
        ON CONFLICT DO NOTHING;

        /* реальное число вставленных строк */
        GET DIAGNOSTICS v_rowcnt = ROW_COUNT;
        v_rows_inserted := v_rows_inserted + v_rowcnt;

        IF v_rowcnt = 2 THEN       -- вставка удалась
            UPDATE tmp_students
               SET quota_left = quota_left - 1
             WHERE user_id   = v_review_user;

            UPDATE tmp_pairs
               SET out_left = out_left - 1
             WHERE pair_id  = v_source_pair_id;

            UPDATE tmp_pairs
               SET in_left  = in_left - 1
             WHERE pair_id  = v_target_pair_id;
        END IF;
    END LOOP;

    /* 10. финальные цифры */
    SELECT count(*) FILTER (WHERE in_left=0 AND out_left=0) INTO v_pairs_full   FROM tmp_pairs;
    SELECT count(*) FILTER (WHERE in_left>0 OR  out_left>0) INTO v_pairs_short  FROM tmp_pairs;
    SELECT coalesce(sum(quota_left),0)                     INTO v_unassigned   FROM tmp_students;

    /* 11. JSON-ответ */
    RETURN json_build_object(
        'result'          , CASE WHEN v_pairs_short=0 AND v_unassigned=0
                                 THEN 'success' ELSE 'partial' END,
        'pairs_total'     , v_pairs_total,
        'pairs_full'      , v_pairs_full,
        'pairs_incomplete', v_pairs_short,
        'unassigned_slots', v_unassigned,
        'inserted_rows'   , v_rows_inserted,
        'message'         , format(
             'pairs %s, full %s, incomplete %s, free slots %s, rows inserted %s, mode %s',
             v_pairs_total,v_pairs_full,v_pairs_short,v_unassigned,v_rows_inserted,v_mode)
    );
EXCEPTION
    WHEN others THEN
        RETURN json_build_object('result','error','message',SQLERRM);
END;
