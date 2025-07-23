DECLARE
    rec RECORD;

    -- Для генерации случайных оценок:
    fc_categories TEXT[] := ARRAY['result_mark','image_mark','attention_mark'];
    t_categories  TEXT[] := ARRAY['skill1_mark','skill2_mark'];

BEGIN
    ----------------------------------------------------------------------------
    -- 1) Собираем из user_duel_answers все записи для спринта p_sprint_id.
    --    НЕ фильтруем part=1, берём все (part=1, part=2 и т.д.).
    --    Если у одной (user, duel, hash, answer_part) несколько ответов,
    --    берём САМЫЙ ПОЗДНИЙ по created_at.
    ----------------------------------------------------------------------------

    CREATE TEMP TABLE IF NOT EXISTS _student_ready_answers ON COMMIT DROP AS
    WITH last_ones AS (
      SELECT
        uda.*,
        d.type AS duel_type,
        row_number() OVER (
          PARTITION BY uda.user_strapi_document_id,
                       uda.duel_strapi_document_id,
                       uda.hash,
                       uda.answer_part
          ORDER BY uda.created_at DESC
        ) AS rn
      FROM user_duel_answers uda
      JOIN duels d
        ON d.strapi_document_id = uda.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
        AND COALESCE(uda.status, 'ok') = 'ok'
    )
    SELECT
      duel_answer_id,
      user_strapi_document_id,
      duel_strapi_document_id,
      hash,
      duel_type
    FROM last_ones
    WHERE rn = 1;   -- только самый поздний ответ в каждой группе

    RAISE NOTICE 'Шаг1: Собрано % актуальных ответов (status=ok) из user_duel_answers, sprint=%',
                 (SELECT count(*) FROM _student_ready_answers),
                 p_sprint_id;

    ----------------------------------------------------------------------------
    -- 2) Создать expert_duel_to_review: по одной записи на каждый выбранный ответ
    --    reviewer_user_strapi_document_id = p_expert_id
    --    ON CONFLICT (hash, duel_strapi_document_id) DO NOTHING (пропускаем дубликаты)
    ----------------------------------------------------------------------------

    CREATE TEMP TABLE IF NOT EXISTS _expert_tasks ON COMMIT DROP AS
    SELECT
      nextval('expert_duel_to_review_id_seq')::bigint AS new_id,
      p_expert_id::text AS reviewer_user_strapi_document_id,
      s.user_strapi_document_id AS user_strapi_document_id,
      s.duel_strapi_document_id AS duel_strapi_document_id,
      s.hash
    FROM _student_ready_answers s;

    INSERT INTO expert_duel_to_review(
      id, 
      created_at,
      reviewer_user_strapi_document_id,
      duel_strapi_document_id,
      hash,
      user_strapi_document_id
    )
    SELECT
      new_id,
      now(),
      reviewer_user_strapi_document_id,
      duel_strapi_document_id,
      hash,
      user_strapi_document_id
    FROM _expert_tasks
    ON CONFLICT (hash, duel_strapi_document_id) DO NOTHING;

    RAISE NOTICE 'Шаг2: Добавлено (с пропуском дубликатов) % записей в expert_duel_to_review',
                 (SELECT count(*) FROM _expert_tasks);

    ----------------------------------------------------------------------------
    -- 3) Генерируем random marks и вставляем в expert_duel_reviewed
    --    с учётом типа схватки (FULL-CONTACT / TRAINING).
    --    ON CONFLICT (hash, duel_strapi_document_id) DO NOTHING
    ----------------------------------------------------------------------------

    CREATE TEMP TABLE IF NOT EXISTS _expert_to_review_mark ON COMMIT DROP AS
    SELECT
      t.new_id,
      t.reviewer_user_strapi_document_id AS reviewer,
      t.user_strapi_document_id AS answer_owner,
      t.duel_strapi_document_id AS duel_id,
      t.hash,
      d.type AS duel_type
    FROM _expert_tasks t
    JOIN duels d
      ON d.strapi_document_id = t.duel_strapi_document_id;

    FOR rec IN SELECT * FROM _expert_to_review_mark LOOP
      IF rec.duel_type = 'FULL-CONTACT' THEN
        -- Случайно 0..1 по каждой из трёх категорий
        EXECUTE format($fc$
          INSERT INTO expert_duel_reviewed(
            created_at,
            reviewer_user_strapi_document_id,
            duel_strapi_document_id,
            user_strapi_document_id,
            comment,
            is_valid,
            hash,
            result_mark,
            image_mark,
            attention_mark,
            skill1_mark,
            skill2_mark
          )
          VALUES(
            now(),
            %L,   -- reviewer
            %L,   -- duel_id
            %L,   -- answer_owner
            %L,   -- comment
            true,
            %L,   -- hash
            %s,   -- result
            %s,   -- image
            %s,   -- attention
            null,
            null
          )
          ON CONFLICT (hash, duel_strapi_document_id) DO NOTHING
        $fc$,
          rec.reviewer,
          rec.duel_id,
          rec.answer_owner,
          'Expert auto comment for FULL-CONTACT',
          rec.hash,
          (floor(random()*2)::int)::text, -- result_mark (0..1)
          (floor(random()*2)::int)::text, -- image_mark
          (floor(random()*2)::int)::text  -- attention_mark
        );

      ELSIF rec.duel_type = 'TRAINING' THEN
        -- Случайно 0..2 по skill1/skill2
        EXECUTE format($tr$
          INSERT INTO expert_duel_reviewed(
            created_at,
            reviewer_user_strapi_document_id,
            duel_strapi_document_id,
            user_strapi_document_id,
            comment,
            is_valid,
            hash,
            result_mark,
            image_mark,
            attention_mark,
            skill1_mark,
            skill2_mark
          )
          VALUES(
            now(),
            %L,    -- reviewer
            %L,    -- duel_id
            %L,    -- answer_owner
            %L,    -- comment
            true,
            %L,    -- hash
            null,
            null,
            null,
            %s,    -- skill1
            %s     -- skill2
          )
          ON CONFLICT (hash, duel_strapi_document_id) DO NOTHING
        $tr$,
          rec.reviewer,
          rec.duel_id,
          rec.answer_owner,
          'Expert auto comment for TRAINING',
          rec.hash,
          (floor(random()*3)::int)::text,  -- skill1_mark (0..2)
          (floor(random()*3)::int)::text   -- skill2_mark
        );
      END IF;
    END LOOP;

    RAISE NOTICE 'Шаг3: random marks для % записей (дубликаты пропущены)',
                 (SELECT count(*) FROM _expert_to_review_mark);

    ----------------------------------------------------------------------------
    -- 4) Для TRAINING прописываем skill1_strapi_document_id, skill2_strapi_document_id
    ----------------------------------------------------------------------------
    UPDATE expert_duel_reviewed edr
    SET
      skill1_strapi_document_id = 'x32dfbgykyu92o6wb92nm50i',
      skill2_strapi_document_id = 'zk0vk04tbl4n7pe9s0w1py78'
    FROM duels d
    WHERE edr.duel_strapi_document_id = d.strapi_document_id
      AND d.type = 'TRAINING';

    RAISE NOTICE 'Шаг4: Обновили skill1/skill2_strapi_document_id (TRAINING)';

END
