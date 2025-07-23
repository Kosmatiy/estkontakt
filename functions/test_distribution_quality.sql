DECLARE
    v_max_diff INT := 0;
    i INT;
    v_count INT;
    v_sprint_duels_count INT;
BEGIN
    -- Логируем старт
    PERFORM log_message(format(
      'test_distribution_quality(sprint=%s): START', p_sprint_id
    ));

    /*
      1) Собираем "уникальные" пары игроков (u1,u2),
         учитывая is_failed=FALSE и нужный sprint:
    */
    DROP TABLE IF EXISTS tmp_pairs;
    CREATE TEMP TABLE tmp_pairs AS
    WITH cte_pairs AS (
       SELECT DISTINCT
              LEAST(dd.user_strapi_document_id, dd.rival_strapi_document_id)  AS user1,
              GREATEST(dd.user_strapi_document_id, dd.rival_strapi_document_id) AS user2
         FROM duel_distributions dd
         JOIN duels d
           ON d.strapi_document_id = dd.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
          AND dd.is_failed = FALSE
    )
    SELECT p.user1, p.user2
      FROM cte_pairs p;

    /* 
       2) Соединяем с view_rank_scores, чтобы получить rank каждого
          и считаем разницу:
    */
    DROP TABLE IF EXISTS tmp_pairs_diff;
    CREATE TEMP TABLE tmp_pairs_diff AS
    WITH cte_ranks AS (
       SELECT t.user1,
              t.user2,
              vs1.user_duels_total_rank AS rank1,
              vs2.user_duels_total_rank AS rank2
         FROM tmp_pairs t
         JOIN view_rank_scores vs1 
           ON vs1.user_strapi_document_id = t.user1
         JOIN view_rank_scores vs2
           ON vs2.user_strapi_document_id = t.user2
    )
    SELECT user1,
           user2,
           rank1,
           rank2,
           ABS(rank1 - rank2) AS diff
      FROM cte_ranks;

    /*
      3) Определяем максимальную разницу:
    */
    SELECT COALESCE(MAX(diff), 0)
      INTO v_max_diff
      FROM tmp_pairs_diff;

    /*
      4) Логируем общую инфо:
         - cколько "пар" всего,
         - какая максимальная разница
    */
    SELECT COUNT(*)
      INTO v_sprint_duels_count
      FROM tmp_pairs_diff;

    PERFORM log_message(format(
      '   Found %s unique pairs in sprint=%s; Max diff=%s',
       v_sprint_duels_count, p_sprint_id, v_max_diff
    ));

    /*
      5) В цикле от v_max_diff до 0 —
         смотрим, сколько пар имеют diff=i
    */
    IF v_max_diff > 0 THEN
        FOR i IN REVERSE v_max_diff..0 LOOP
            SELECT COUNT(*)
              INTO v_count
              FROM tmp_pairs_diff
             WHERE diff = i;

            IF v_count>0 THEN
                PERFORM log_message(format(
                  '   diff=%s => %s pairs', i, v_count
                ));
            END IF;
        END LOOP;
    ELSE
        -- Если max_diff=0, значит все пары "рангово равны"
        SELECT COUNT(*)
          INTO v_count
          FROM tmp_pairs_diff
         WHERE diff=0;

        IF v_count>0 THEN
            PERFORM log_message(format(
              '   All pairs have diff=0 => %s pairs total', v_count
            ));
        ELSE
            PERFORM log_message('   No pairs or no rank data => skip');
        END IF;
    END IF;

    PERFORM log_message(format(
      'test_distribution_quality(sprint=%s): DONE', p_sprint_id
    ));
END;
