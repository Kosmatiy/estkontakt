DECLARE
    v_score NUMERIC := 0;
BEGIN
    /*
      1) Собираем уникальные пары (LEAST, GREATEST) из duel_distributions + duels
         по sprint_strapi_document_id, is_failed=FALSE
      2) Для каждой пары смотрим rank(u1) и rank(u2) в view_rank_scores
      3) gap = |rank1 - rank2|
      4) Считаем сколько пар с таким gap
      5) score = ∑( gap * count )
    */
    DROP TABLE IF EXISTS tmp_unique_pairs;
    CREATE TEMP TABLE tmp_unique_pairs AS
    SELECT DISTINCT
           LEAST(dd.user_strapi_document_id, dd.rival_strapi_document_id) AS userA,
           GREATEST(dd.user_strapi_document_id, dd.rival_strapi_document_id) AS userB
      FROM duel_distributions dd
      JOIN duels d ON d.strapi_document_id = dd.duel_strapi_document_id
     WHERE d.sprint_strapi_document_id = p_sprint_id
       AND dd.is_failed = FALSE;

    DROP TABLE IF EXISTS tmp_gaps;
    CREATE TEMP TABLE tmp_gaps(gap INT);

    INSERT INTO tmp_gaps(gap)
    SELECT
      ABS(vs1.user_duels_total_rank - vs2.user_duels_total_rank) AS gap
    FROM tmp_unique_pairs p
    JOIN view_rank_scores vs1 ON vs1.user_strapi_document_id = p.userA
    JOIN view_rank_scores vs2 ON vs2.user_strapi_document_id = p.userB;

    SELECT SUM(gap * cnt)
      INTO v_score
      FROM (
        SELECT gap, COUNT(*) AS cnt
          FROM tmp_gaps
         GROUP BY gap
      ) sub;

    IF v_score IS NULL THEN
        v_score := 0;
    END IF;

    RETURN v_score;
END;
