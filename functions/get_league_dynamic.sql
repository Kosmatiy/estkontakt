DECLARE
    v_sprint_number int;
    v_user_stream_id text;  
    v_sprint_stream_id text;  
BEGIN
    /* 0. Проверяем номер спринта И получаем поток спринта */
    SELECT s.sprint_number, s.stream_strapi_document_id
      INTO v_sprint_number, v_sprint_stream_id
      FROM sprints s
     WHERE s.strapi_document_id = p_sprint;

    IF v_sprint_number IS NULL THEN
        RAISE EXCEPTION 'Спринт % не найден', p_sprint;
    END IF;
    
    -- Получаем активный поток пользователя из user_stream_links
    SELECT usl.stream_strapi_document_id
      INTO v_user_stream_id
      FROM user_stream_links usl
     WHERE usl.user_strapi_document_id = p_user
       AND usl.is_active = TRUE
     LIMIT 1;

    IF v_user_stream_id IS NULL THEN
        RAISE EXCEPTION 'У пользователя % нет активной привязки к потоку', p_user;
    END IF;
    
    -- Проверяем, что пользователь в том же потоке, что и спринт
    IF v_user_stream_id != v_sprint_stream_id THEN
        RAISE EXCEPTION 'Пользователь % не участвует в потоке спринта %', p_user, p_sprint;
    END IF;
    
    IF v_sprint_number = 1 THEN        -- лига появляется со 2‑го спринта
        RETURN;
    END IF;

    /* ----------------- основная логика с учетом потоков ----------------- */
    RETURN QUERY
    WITH /* 1. Баллы до текущего спринта (ТОЛЬКО ИЗ ТОГО ЖЕ ПОТОКА) */
    base_pts AS (
        SELECT
            usl.user_strapi_document_id                    AS user_id,
            COALESCE(SUM(v.user_total_score),0)            AS pts
        FROM user_stream_links usl
        LEFT JOIN view_rank_scores v
               ON v.user_strapi_document_id = usl.user_strapi_document_id
              AND v.stream_strapi_document_id = usl.stream_strapi_document_id  -- ← ИСПРАВЛЕНО: фильтрация по потоку пользователя
              AND v.sprint_strapi_document_id IN (
                     SELECT s_prev.strapi_document_id
                     FROM   sprints s_prev
                     WHERE  s_prev.sprint_number < v_sprint_number
                       AND  s_prev.stream_strapi_document_id = v_user_stream_id)  -- ← ИСПРАВЛЕНО: только спринты ЭТОГО потока!
        WHERE usl.stream_strapi_document_id = v_user_stream_id  
          AND usl.is_active = TRUE  
        GROUP BY usl.user_strapi_document_id
    ),

    /* 2. Абсолютный базовый ранг (уникальный, только внутри потока) */
    base_ranked AS (
        SELECT
            user_id,
            pts,
            ROW_NUMBER() OVER (ORDER BY pts DESC, user_id)::int AS abs_base_rank
        FROM base_pts
    ),

    /* 3. Баллы текущего спринта (ТОЛЬКО ЭТОГО ПОТОКА) */
    sprint_pts AS (
        SELECT v.user_strapi_document_id AS user_id,
               v.user_total_score        AS sprint_pts
        FROM   view_rank_scores v
        WHERE  v.sprint_strapi_document_id = p_sprint
          AND  v.stream_strapi_document_id = v_user_stream_id  -- ← ИСПРАВЛЕНО: фильтрация по потоку
    ),

    /* 4. Абсолютный ранг после спринта (внутри потока) */
    now_ranked AS (
        SELECT
            bp.user_id,
            bp.pts                                       AS base_points,
            COALESCE(sp.sprint_pts,0)                    AS sprint_points,
            bp.pts + COALESCE(sp.sprint_pts,0)           AS total_now,
            ROW_NUMBER() OVER (ORDER BY
                               bp.pts + COALESCE(sp.sprint_pts,0) DESC,
                               bp.user_id)::int          AS abs_now_rank
        FROM   base_pts bp
        LEFT  JOIN sprint_pts sp USING (user_id)
    ),

    /* 5. Определяем «окно» 10 игроков вокруг p_user  (5‑4 правило) */
    my_abs_rank AS (
        SELECT br.abs_base_rank
        FROM   base_ranked br
        WHERE  br.user_id = p_user
    ),
    league_window AS (
        SELECT br.*
        FROM   base_ranked br, my_abs_rank m
        WHERE  br.abs_base_rank
               BETWEEN GREATEST(1, m.abs_base_rank-4) AND m.abs_base_rank+5
    ),

    /* 6‑а. Лиговый базовый ранг (1‑10) */
    league_base_ranks AS (
        SELECT
            lw.user_id,
            ROW_NUMBER() OVER (ORDER BY lw.abs_base_rank)::int AS base_league_rank
        FROM league_window lw
    ),

    /* 6‑b. Лиговый ранг «после» (1‑10) */
    league_now_ranks AS (
        SELECT
            lw.user_id,
            ROW_NUMBER() OVER (
                ORDER BY nr.abs_now_rank
            )::int AS now_league_rank
        FROM league_window lw
        JOIN now_ranked nr USING (user_id)
    )

    /* 7. Финальный вывод */
    SELECT
        lw.user_id                                             AS user_strapi_document_id,
        u.team_strapi_document_id                              AS team_strapi_document_id,

        lw.abs_base_rank                                       AS base_rank,
        nr.abs_now_rank                                        AS now_rank,
        (lw.abs_base_rank - nr.abs_now_rank)                   AS rank_delta,

        lb.base_league_rank,
        ln.now_league_rank,
        (lb.base_league_rank - ln.now_league_rank)             AS league_rank_delta,

        nr.base_points,
        nr.sprint_points,
        nr.total_now
    FROM   league_window      lw
    JOIN   now_ranked         nr USING (user_id)
    JOIN   league_base_ranks  lb USING (user_id)
    JOIN   league_now_ranks   ln USING (user_id)
    LEFT  JOIN users          u  ON u.strapi_document_id = lw.user_id
    ORDER  BY ln.now_league_rank;   -- показываем лигу от 1 до 10
END;
