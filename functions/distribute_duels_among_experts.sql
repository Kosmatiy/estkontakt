DECLARE
    v_stream_id       TEXT;               -- stream текущего спринта

    total_duels       INT;                -- уникальные дуэль-пары
    assigned_count    INT := 0;           -- роздано дуэль-пар
    v_pairs_skipped   INT := 0;           -- пар без эксперта

    n_experts         INT;                -- число экспертов
    expert_ids        TEXT[];
    expert_loads      DOUBLE PRECISION[];
    expert_floor      INT[]   := '{}';
    expert_fraction   DOUBLE PRECISION[] := '{}';
    sum_w             DOUBLE PRECISION := 0;
    leftover          INT;
    sum_floor         INT := 0;

    duel_cursor       REFCURSOR;
    rec_duel          RECORD;

    idx               INT;
    needed            INT;
    current_expert_id TEXT;

    v_rows_inserted   INT := 0;
    v_rows_conflict   INT := 0;
    v_rc              INT;
BEGIN
    ------------------------------------------------------------------
    -- 0. Получаем stream спринта
    ------------------------------------------------------------------
    SELECT stream_strapi_document_id
      INTO v_stream_id
      FROM sprints
     WHERE strapi_document_id = p_sprint_strapi_document_id;

    IF v_stream_id IS NULL THEN
        RETURN json_build_object(
            'result' , 'error',
            'message', format('Спринт %s не найден.', p_sprint_strapi_document_id)
        );
    END IF;

    ------------------------------------------------------------------
    -- 1. Очищаем старые назначения по этому спринту
    ------------------------------------------------------------------
    DELETE FROM expert_duel_to_review edr
    USING duels d
    WHERE edr.duel_strapi_document_id = d.strapi_document_id
      AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;

    ------------------------------------------------------------------
    -- 2. Собираем все дуэли спринта
    ------------------------------------------------------------------
    DROP TABLE IF EXISTS tmp_duels;
    CREATE TEMP TABLE tmp_duels ON COMMIT DROP AS
    SELECT DISTINCT
           uda.duel_strapi_document_id,
           uda.user_strapi_document_id         AS owner_a,
           uda.rival_user_strapi_document_id   AS owner_b
    FROM user_duel_answers uda
    JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id;

    SELECT COUNT(*) INTO total_duels FROM tmp_duels;
    IF total_duels = 0 THEN
        RETURN json_build_object(
            'result' , 'success',
            'message', format('Спринт %s: дуэлей нет, распределение не требуется.', p_sprint_strapi_document_id)
        );
    END IF;

    ------------------------------------------------------------------
    -- 3. Собираем экспертов данного stream’а с ненулевой нагрузкой
    ------------------------------------------------------------------
    DROP TABLE IF EXISTS tmp_experts;
    CREATE TEMP TABLE tmp_experts ON COMMIT DROP AS
    SELECT e.strapi_document_id                  AS expert_id,
           COALESCE(ew.duel_workload,0)::DOUBLE PRECISION AS workload
    FROM expert_workload ew
    JOIN experts e ON e.strapi_document_id = ew.expert_strapi_document_id
    WHERE ew.stream_strapi_document_id = v_stream_id
      AND COALESCE(ew.duel_workload,0) > 0
      AND e.dismissed_at IS NULL;

    SELECT COUNT(*) INTO n_experts FROM tmp_experts;
    IF n_experts = 0 THEN
        RETURN json_build_object(
            'result' , 'success',
            'message', format('Спринт %s: нет экспертов с ненулевой duel_workload.', p_sprint_strapi_document_id)
        );
    END IF;

    SELECT array_agg(expert_id),
           array_agg(workload)
      INTO expert_ids, expert_loads
      FROM tmp_experts;

    ------------------------------------------------------------------
    -- 4. Вычисляем квоты (метод floor + остаток)
    ------------------------------------------------------------------
    FOR idx IN 1..n_experts LOOP
        sum_w := sum_w + expert_loads[idx];
    END LOOP;

    IF sum_w = 0 THEN
        RETURN json_build_object(
            'result' , 'success',
            'message', format('Спринт %s: суммарная duel_workload = 0.', p_sprint_strapi_document_id)
        );
    END IF;

    FOR idx IN 1..n_experts LOOP
        DECLARE
            ideal DOUBLE PRECISION := total_duels * (expert_loads[idx] / sum_w);
            fl    INT := floor(ideal);
        BEGIN
            expert_floor    := expert_floor    || fl;
            expert_fraction := expert_fraction || (ideal - fl);
            sum_floor       := sum_floor + fl;
        END;
    END LOOP;

    leftover := total_duels - sum_floor;

    IF leftover > 0 THEN
        FOR idx IN
            SELECT ordinality
              FROM unnest(expert_fraction) WITH ORDINALITY AS t(frac, ordinality)
             ORDER BY frac DESC, ordinality
             LIMIT leftover
        LOOP
            expert_floor[idx] := expert_floor[idx] + 1;
        END LOOP;
    END IF;

    ------------------------------------------------------------------
    -- 5. Раздаём дуэли
    ------------------------------------------------------------------
    OPEN duel_cursor FOR
        SELECT * FROM tmp_duels ORDER BY duel_strapi_document_id;

    FOR idx IN 1..n_experts LOOP
        needed := expert_floor[idx];
        current_expert_id := expert_ids[idx];

        WHILE needed > 0 LOOP
            FETCH duel_cursor INTO rec_duel;
            EXIT WHEN NOT FOUND;

            -- owner_a
            INSERT INTO expert_duel_to_review(
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                hash,
                user_strapi_document_id,
                created_at
            )
            VALUES (
                current_expert_id,
                rec_duel.duel_strapi_document_id,
                LEAST(rec_duel.owner_a, rec_duel.owner_b) || '_' ||
                GREATEST(rec_duel.owner_a, rec_duel.owner_b),
                rec_duel.owner_a,
                now()
            )
            ON CONFLICT DO NOTHING;
            GET DIAGNOSTICS v_rc = ROW_COUNT;
            IF v_rc = 1 THEN
                v_rows_inserted := v_rows_inserted + 1;
            ELSE
                v_rows_conflict := v_rows_conflict + 1;
            END IF;

            -- owner_b
            INSERT INTO expert_duel_to_review(
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                hash,
                user_strapi_document_id,
                created_at
            )
            VALUES (
                current_expert_id,
                rec_duel.duel_strapi_document_id,
                LEAST(rec_duel.owner_a, rec_duel.owner_b) || '_' ||
                GREATEST(rec_duel.owner_a, rec_duel.owner_b),
                rec_duel.owner_b,
                now()
            )
            ON CONFLICT DO NOTHING;
            GET DIAGNOSTICS v_rc = ROW_COUNT;
            IF v_rc = 1 THEN
                v_rows_inserted := v_rows_inserted + 1;
            ELSE
                v_rows_conflict := v_rows_conflict + 1;
            END IF;

            assigned_count := assigned_count + 1;
            needed := needed - 1;
        END LOOP;
    END LOOP;

    CLOSE duel_cursor;
    v_pairs_skipped := total_duels - assigned_count;

    ------------------------------------------------------------------
    -- 6. Возвращаем отчёт
    ------------------------------------------------------------------
    RETURN json_build_object(
        'result'  , 'success',
        'message' , format(
            'Спринт %s: вставлено %s строк, %s конфликтов, пропущено %s пар из %s, обработано %s дуэлей, режим %s.',
            p_sprint_strapi_document_id,
            v_rows_inserted,
            v_rows_conflict,
            v_pairs_skipped,
            total_duels,
            assigned_count,
            p_mode
        )
    );
END;
