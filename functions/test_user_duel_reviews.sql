DECLARE
    v_bad_pairs_cnt      INT := 0;
    v_bad_reviewers_cnt  INT := 0;
    v_self_reviews_cnt   INT := 0;
    v_pairs_total        INT := 0;
    v_rows_total         INT := 0;
    v_expected_rows      INT := 0;
BEGIN
    /* лог-старт */
    PERFORM log_message(
        format('[TEST] sprint=%s – старт проверки распределения ревью', in_sprint_id)
    );

    /*──────────────── 1. пары с ошибками  ───────────────*/
    DROP TABLE IF EXISTS _bad_pairs;
    CREATE TEMP TABLE _bad_pairs ON COMMIT DROP AS
    SELECT hash
      FROM user_duel_to_review ur
      JOIN duels d ON d.strapi_document_id = ur.duel_strapi_document_id
     WHERE d.sprint_strapi_document_id = in_sprint_id
     GROUP BY hash
    HAVING COUNT(*) <> 6
       OR COUNT(DISTINCT reviewer_user_strapi_document_id) <> 6;

    SELECT COUNT(*) INTO v_bad_pairs_cnt FROM _bad_pairs;

    IF v_bad_pairs_cnt > 0 THEN
        PERFORM log_message(format('[TEST] ошибок в парах: %s', v_bad_pairs_cnt));
    END IF;

    /*──────────────── 2. ревьюеры с ≠3 парами ────────────*/
    DROP TABLE IF EXISTS _bad_reviewers;
    CREATE TEMP TABLE _bad_reviewers ON COMMIT DROP AS
    SELECT reviewer_user_strapi_document_id
      FROM user_duel_to_review ur
      JOIN duels d ON d.strapi_document_id = ur.duel_strapi_document_id
     WHERE d.sprint_strapi_document_id = in_sprint_id
     GROUP BY reviewer_user_strapi_document_id
    HAVING COUNT(DISTINCT hash) <> 3;

    SELECT COUNT(*) INTO v_bad_reviewers_cnt FROM _bad_reviewers;

    IF v_bad_reviewers_cnt > 0 THEN
        PERFORM log_message(format('[TEST] ревьюеров с ≠3 парами: %s',
                                   v_bad_reviewers_cnt));
    END IF;

    /*──────────────── 3. «сам себя проверяю» ─────────────*/
    DROP TABLE IF EXISTS _self_reviews;
    CREATE TEMP TABLE _self_reviews ON COMMIT DROP AS
    WITH pairs AS (
        SELECT hash,
               MIN(user_strapi_document_id) AS p1,
               MAX(user_strapi_document_id) AS p2
          FROM duel_distributions dd
          JOIN duels d2 ON d2.strapi_document_id = dd.duel_strapi_document_id
         WHERE d2.sprint_strapi_document_id = in_sprint_id
         GROUP BY hash
    )
    SELECT ur.*
      FROM user_duel_to_review ur
      JOIN duels d ON d.strapi_document_id = ur.duel_strapi_document_id
      JOIN pairs  p ON p.hash = ur.hash
     WHERE d.sprint_strapi_document_id = in_sprint_id
       AND ur.reviewer_user_strapi_document_id IN (p.p1, p.p2);

    SELECT COUNT(*) INTO v_self_reviews_cnt FROM _self_reviews;

    IF v_self_reviews_cnt > 0 THEN
        PERFORM log_message(format('[TEST] найдено %s саморевью',
                                   v_self_reviews_cnt));
    END IF;

    /*──────────────── 4. контроль арифметики ─────────────*/
    SELECT COUNT(DISTINCT hash)
      INTO v_pairs_total
      FROM duel_distributions dd
      JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
     WHERE d.sprint_strapi_document_id = in_sprint_id;

    SELECT COUNT(*)
      INTO v_rows_total
      FROM user_duel_to_review ur
      JOIN duels d ON d.strapi_document_id = ur.duel_strapi_document_id
     WHERE d.sprint_strapi_document_id = in_sprint_id;

    v_expected_rows := v_pairs_total * 6;

    PERFORM log_message(
        format('[TEST] пар=%s, строк=%s, ожидалось=%s',
               v_pairs_total, v_rows_total, v_expected_rows)
    );

    /*──────────────── 5. JSON-ответ ──────────────────────*/
    RETURN jsonb_build_object(
        'result'            , 'success',
        'sprint_id'         , in_sprint_id,
        'pairs_total'       , v_pairs_total,
        'rows_total'        , v_rows_total,
        'expected_rows'     , v_expected_rows,
        'bad_pairs_cnt'     , v_bad_pairs_cnt,
        'bad_reviewers_cnt' , v_bad_reviewers_cnt,
        'self_reviews_cnt'  , v_self_reviews_cnt,
        'status'            , CASE
                                 WHEN v_bad_pairs_cnt     = 0
                                  AND v_bad_reviewers_cnt = 0
                                  AND v_self_reviews_cnt  = 0
                                  AND v_rows_total        = v_expected_rows
                                 THEN 'OK'
                                 ELSE 'PROBLEMS_FOUND'
                               END
    );
END;
