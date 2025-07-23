DECLARE
    v_sprint         sprints%ROWTYPE;
    v_stream_id      TEXT;
    v_mode           TEXT;
    v_rows_inserted  INT := 0;
    v_rows_conflict  INT := 0;
    v_cleaned_rows   INT := 0;
    v_total_pairs    INT := 0;
    v_incomplete_pairs INT := 0;
    v_quota_violations INT := 0;
    v_incomplete_list TEXT[] := '{}';
    v_violation_list  TEXT[] := '{}';
    v_execution_id   TEXT;
    v_eligible_count INT;
    v_total_quota    INT;
    v_required_reviews INT;
    rec_pair         RECORD;
    rec_reviewer     RECORD;
    v_batch_rows     INT;
    v_current_quota  INT;
    v_assigned_count INT;
BEGIN
    v_execution_id := 'exec_' || extract(epoch from now())::bigint || '_' || random()::text;
    v_mode := COALESCE(upper(p_mode), 'CLEANSLATE');
    IF v_mode NOT IN ('CLEANSLATE', 'GOON') THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', 'Режим должен быть CLEANSLATE или GOON'
        );
    END IF;
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
    v_stream_id := v_sprint.stream_strapi_document_id;
    IF v_mode = 'CLEANSLATE' THEN
        DELETE FROM user_duel_to_review utdr
        USING duels d
        WHERE utdr.duel_strapi_document_id = d.strapi_document_id
          AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
        GET DIAGNOSTICS v_cleaned_rows = ROW_COUNT;
    END IF;
    DROP TABLE IF EXISTS tmp_strikes;
    CREATE TEMP TABLE tmp_strikes ON COMMIT DROP AS
    SELECT DISTINCT user_strapi_document_id
    FROM strikes
    WHERE sprint_strapi_document_id = p_sprint_strapi_document_id;
    DROP TABLE IF EXISTS tmp_eligible_users;
    CREATE TEMP TABLE tmp_eligible_users ON COMMIT DROP AS
    SELECT u.strapi_document_id AS user_id,
           u.team_strapi_document_id
    FROM users u
    WHERE u.stream_strapi_document_id = v_stream_id
      AND u.dismissed_at IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM tmp_strikes s 
          WHERE s.user_strapi_document_id = u.strapi_document_id
      );
    SELECT COUNT(*) INTO v_eligible_count FROM tmp_eligible_users;
    IF v_eligible_count < 6 THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Недостаточно активных участников: %s (минимум 6)', v_eligible_count)
        );
    END IF;
    DROP TABLE IF EXISTS tmp_latest_answers;
    CREATE TEMP TABLE tmp_latest_answers ON COMMIT DROP AS
    WITH ranked_answers AS (
        SELECT uda.*,
               ROW_NUMBER() OVER (
                   PARTITION BY uda.user_strapi_document_id, uda.duel_strapi_document_id, uda.hash
                   ORDER BY uda.created_at DESC
               ) AS rn
        FROM user_duel_answers uda
        JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
          AND uda.user_strapi_document_id IN (SELECT user_id FROM tmp_eligible_users)
          AND uda.hash IS NOT NULL 
          AND uda.hash != ''
    )
    SELECT * FROM ranked_answers WHERE rn = 1;
    DROP TABLE IF EXISTS tmp_pairs;
    CREATE TEMP TABLE tmp_pairs ON COMMIT DROP AS
    SELECT DISTINCT 
           la.hash,
           la.duel_strapi_document_id,
           d.duel_number,
           d.type AS duel_type
    FROM tmp_latest_answers la
    JOIN duels d ON d.strapi_document_id = la.duel_strapi_document_id
    WHERE EXISTS (
        SELECT 1 FROM tmp_latest_answers la2 
        WHERE la2.hash = la.hash 
          AND la2.duel_strapi_document_id = la.duel_strapi_document_id
        GROUP BY la2.hash, la2.duel_strapi_document_id
        HAVING COUNT(DISTINCT la2.user_strapi_document_id) = 2
    );
    SELECT COUNT(*) INTO v_total_pairs FROM tmp_pairs;
    IF v_total_pairs = 0 THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', 'Не найдено пар для проверки'
        );
    END IF;
    DROP TABLE IF EXISTS tmp_quotas;
    CREATE TEMP TABLE tmp_quotas ON COMMIT DROP AS
    SELECT u.user_id,
           u.team_strapi_document_id,
           COUNT(la.hash) AS answers_count,
           COUNT(la.hash) * 3 AS review_quota,
           0 AS assigned_count
    FROM tmp_eligible_users u
    LEFT JOIN tmp_latest_answers la ON la.user_strapi_document_id = u.user_id
    GROUP BY u.user_id, u.team_strapi_document_id
    HAVING COUNT(la.hash) > 0;
    SELECT COUNT(*), COALESCE(SUM(review_quota), 0) 
    INTO v_eligible_count, v_total_quota 
    FROM tmp_quotas;
    v_required_reviews := v_total_pairs * 6;
    IF v_total_quota < v_required_reviews THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Недостаточно квот: требуется %s, доступно %s', 
                              v_required_reviews, v_total_quota)
        );
    END IF;
    DROP TABLE IF EXISTS tmp_reviewer_ring;
    CREATE TEMP TABLE tmp_reviewer_ring ON COMMIT DROP AS
    SELECT q.user_id,
           q.team_strapi_document_id,
           q.review_quota,
           ROW_NUMBER() OVER (ORDER BY q.user_id) AS position,
           COUNT(*) OVER () AS ring_size
    FROM tmp_quotas q
    WHERE q.review_quota > 0
    ORDER BY q.user_id;
    DROP TABLE IF EXISTS tmp_pair_ring;
    CREATE TEMP TABLE tmp_pair_ring ON COMMIT DROP AS
    SELECT p.hash,
           p.duel_strapi_document_id,
           p.duel_number,
           p.duel_type,
           ROW_NUMBER() OVER (ORDER BY p.duel_number, p.hash) AS position,
           COUNT(*) OVER () AS ring_size
    FROM tmp_pairs p
    ORDER BY p.duel_number, p.hash;
    DROP TABLE IF EXISTS tmp_assignments;
    CREATE TEMP TABLE tmp_assignments ON COMMIT DROP AS
    SELECT DISTINCT
           rr.user_id AS reviewer_id,
           pr.hash,
           pr.duel_strapi_document_id,
           pr.duel_number,
           pr.duel_type,
           la.user_strapi_document_id AS reviewee_id
    FROM tmp_pair_ring pr
    CROSS JOIN generate_series(0, 5) AS off
    JOIN tmp_reviewer_ring rr ON rr.position = ((pr.position - 1 + off) % rr.ring_size) + 1
    JOIN tmp_latest_answers la ON la.hash = pr.hash 
                               AND la.duel_strapi_document_id = pr.duel_strapi_document_id
    JOIN tmp_eligible_users eu_reviewer ON eu_reviewer.user_id = rr.user_id
    JOIN tmp_eligible_users eu_reviewee ON eu_reviewee.user_id = la.user_strapi_document_id
    WHERE rr.user_id != la.user_strapi_document_id
      AND NOT EXISTS (
          SELECT 1 FROM tmp_latest_answers la2 
          WHERE la2.hash = pr.hash 
            AND la2.duel_strapi_document_id = pr.duel_strapi_document_id
            AND la2.user_strapi_document_id = rr.user_id
      );
    DROP TABLE IF EXISTS tmp_reviewer_load;
    CREATE TEMP TABLE tmp_reviewer_load ON COMMIT DROP AS
    SELECT ta.reviewer_id,
           COUNT(*) AS assigned_count,
           q.review_quota
    FROM tmp_assignments ta
    JOIN tmp_quotas q ON q.user_id = ta.reviewer_id
    GROUP BY ta.reviewer_id, q.review_quota;
    DELETE FROM tmp_assignments
    WHERE (reviewer_id, hash, duel_strapi_document_id, reviewee_id) IN (
        SELECT ta.reviewer_id, ta.hash, ta.duel_strapi_document_id, ta.reviewee_id
        FROM tmp_assignments ta
        JOIN (
            SELECT reviewer_id, 
                   ROW_NUMBER() OVER (PARTITION BY reviewer_id ORDER BY duel_number, hash, reviewee_id) as rn
            FROM tmp_assignments
        ) ranked ON ranked.reviewer_id = ta.reviewer_id
        JOIN tmp_quotas q ON q.user_id = ta.reviewer_id
        WHERE ranked.rn > q.review_quota
    );
    DROP TABLE IF EXISTS tmp_pair_stats;
    CREATE TEMP TABLE tmp_pair_stats ON COMMIT DROP AS
    SELECT hash,
           duel_strapi_document_id,
           COUNT(DISTINCT reviewer_id) AS reviewer_count,
           6 - COUNT(DISTINCT reviewer_id) AS needed_reviewers
    FROM tmp_assignments
    GROUP BY hash, duel_strapi_document_id
    HAVING COUNT(DISTINCT reviewer_id) < 6;
    FOR rec_pair IN 
        SELECT * FROM tmp_pair_stats WHERE needed_reviewers > 0 ORDER BY hash, duel_strapi_document_id
    LOOP
        INSERT INTO tmp_assignments (reviewer_id, hash, duel_strapi_document_id, reviewee_id)
        SELECT DISTINCT
               q.user_id,
               rec_pair.hash,
               rec_pair.duel_strapi_document_id,
               la.user_strapi_document_id
        FROM tmp_quotas q
        JOIN tmp_latest_answers la ON la.hash = rec_pair.hash 
                                   AND la.duel_strapi_document_id = rec_pair.duel_strapi_document_id
        WHERE q.user_id != la.user_strapi_document_id
          AND NOT EXISTS (
              SELECT 1 
              FROM tmp_latest_answers la2 
              WHERE la2.hash = rec_pair.hash 
                AND la2.duel_strapi_document_id = rec_pair.duel_strapi_document_id
                AND la2.user_strapi_document_id = q.user_id
          )
          AND NOT EXISTS (
              SELECT 1 
              FROM tmp_assignments ta 
              WHERE ta.reviewer_id = q.user_id 
                AND ta.hash = rec_pair.hash 
                AND ta.duel_strapi_document_id = rec_pair.duel_strapi_document_id
          )
          AND (
              SELECT COUNT(*) FROM tmp_assignments ta2 
              WHERE ta2.reviewer_id = q.user_id
          ) < q.review_quota
        ORDER BY random()
        LIMIT LEAST(rec_pair.needed_reviewers, 
                    (SELECT COUNT(*) 
                     FROM tmp_latest_answers la3 
                     WHERE la3.hash = rec_pair.hash 
                       AND la3.duel_strapi_document_id = rec_pair.duel_strapi_document_id) 
                    * (6 - rec_pair.reviewer_count));
    END LOOP;
    INSERT INTO user_duel_to_review (
        reviewer_user_strapi_document_id,
        duel_strapi_document_id,
        user_strapi_document_id,
        hash
    )
    SELECT DISTINCT
           ta.reviewer_id,
           ta.duel_strapi_document_id,
           ta.reviewee_id,
           ta.hash
    FROM tmp_assignments ta
    ON CONFLICT (reviewer_user_strapi_document_id, hash, duel_strapi_document_id, user_strapi_document_id) 
    DO NOTHING;
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
    SELECT array_agg(format('%s:%s (рецензентов: %s)', duel_number, hash, reviewer_count))
    INTO v_incomplete_list
    FROM (
        SELECT p.hash, p.duel_number, COUNT(DISTINCT utdr.reviewer_user_strapi_document_id) as reviewer_count
        FROM tmp_pairs p
        LEFT JOIN user_duel_to_review utdr ON utdr.hash = p.hash 
                                           AND utdr.duel_strapi_document_id = p.duel_strapi_document_id
        GROUP BY p.hash, p.duel_number, p.duel_strapi_document_id
        HAVING COUNT(DISTINCT utdr.reviewer_user_strapi_document_id) < 6
    ) incomplete;
    v_incomplete_pairs := COALESCE(array_length(v_incomplete_list, 1), 0);
    SELECT array_agg(format('User %s: назначено %s, квота %s', user_id, assigned, quota))
    INTO v_violation_list
    FROM (
        SELECT q.user_id, COUNT(*) as assigned, q.review_quota as quota
        FROM tmp_quotas q
        JOIN user_duel_to_review utdr ON utdr.reviewer_user_strapi_document_id = q.user_id
        JOIN duels d ON d.strapi_document_id = utdr.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
        GROUP BY q.user_id, q.review_quota
        HAVING COUNT(*) > q.review_quota
    ) violations;
    v_quota_violations := COALESCE(array_length(v_violation_list, 1), 0);
    IF v_incomplete_pairs > 0 OR v_quota_violations > 0 THEN
        IF v_mode = 'CLEANSLATE' THEN
            DELETE FROM user_duel_to_review utdr
            USING duels d
            WHERE utdr.duel_strapi_document_id = d.strapi_document_id
              AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
        END IF;
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', 'Валидация не пройдена',
            'incomplete_pairs', v_incomplete_list,
            'quota_violations', v_violation_list,
            'stats', json_build_object(
                'total_pairs', v_total_pairs,
                'eligible_reviewers', v_eligible_count,
                'total_quota', v_total_quota,
                'required_reviews', v_required_reviews
            )
        );
    END IF;
    RETURN json_build_object(
        'execution_id', v_execution_id,
        'status', 'SUCCESS',
        'message', format('Спринт %s: успешно распределено %s назначений для %s пар',
                          p_sprint_strapi_document_id, v_rows_inserted, v_total_pairs),
        'stats', json_build_object(
            'mode', v_mode,
            'cleaned_rows', v_cleaned_rows,
            'inserted_rows', v_rows_inserted,
            'total_pairs', v_total_pairs,
            'eligible_reviewers', v_eligible_count,
            'total_quota', v_total_quota,
            'required_reviews', v_required_reviews,
            'incomplete_pairs', v_incomplete_pairs,
            'quota_violations', v_quota_violations
        )
    );
EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object(
            'execution_id', v_execution_id,
            'status', 'ERROR',
            'message', format('Ошибка выполнения: %s', SQLERRM)
        );
END;
