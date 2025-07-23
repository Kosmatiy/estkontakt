DECLARE
    v_count INT;
BEGIN
    -- 1) Генерируем слоты
    CREATE TEMP TABLE review_slots (
        duel_id        TEXT,
        hash           TEXT,
        participant_id TEXT,
        slot_no        INT
    ) ON COMMIT DROP;

    WITH active_users AS (
        SELECT u.strapi_document_id AS user_id
          FROM users u
          JOIN user_stream_links usl
            ON usl.user_strapi_document_id = u.strapi_document_id
          JOIN sprints s
            ON s.strapi_document_id = p_sprint_id
         WHERE u.dismissed_at IS NULL
           AND usl.is_active
           AND usl.stream_strapi_document_id = s.stream_strapi_document_id
    ),
    units AS (
        SELECT uda.duel_strapi_document_id AS duel_id,
               uda.hash,
               uda.user_strapi_document_id AS participant_id
          FROM user_duel_answers uda
          JOIN duels d
            ON d.strapi_document_id = uda.duel_strapi_document_id
          JOIN active_users au
            ON au.user_id = uda.user_strapi_document_id
         WHERE d.sprint_strapi_document_id = p_sprint_id
    )
    INSERT INTO review_slots (duel_id, hash, participant_id, slot_no)
    SELECT u.duel_id, u.hash, u.participant_id, gs
      FROM units u
CROSS JOIN generate_series(1,6) AS gs;

    -- 2) Считаем количество
    SELECT COUNT(*) INTO v_count FROM review_slots;

    RETURN 'total slots = ' || v_count;
END;
