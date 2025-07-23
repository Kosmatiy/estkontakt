DECLARE
    -- Массив всех студентов, которым назначены проверки в этом спринте
    all_reviewers TEXT[];

    total_count int;  -- всего таких студентов
    fail_count int;   -- сколько хотим «завалить»
    pass_count int;   -- остальные

    fail_set TEXT[];  -- массив id студентов, которые пропустят 1 проверку
    good_set TEXT[];  -- остальные

    user_id text;     -- вспомогательная для цикла FOREACH
BEGIN
    ----------------------------------------------------------------------------
    -- Шаг 1. Собираем всех reviewer_user_strapi_document_id, кому назначены
    --        проверки (user_duel_to_review) для схваток из спринта p_sprint_id.
    ----------------------------------------------------------------------------
    WITH rev AS (
      SELECT DISTINCT
             t.reviewer_user_strapi_document_id AS reviewer
      FROM user_duel_to_review t
      JOIN duels d
         ON d.strapi_document_id = t.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
    )
    SELECT array_agg(reviewer)
      INTO all_reviewers
    FROM rev;

    IF all_reviewers IS NULL OR array_length(all_reviewers, 1) IS NULL THEN
      RAISE NOTICE 'Нет ни одного студента-ревьюера для спринта=%', p_sprint_id;
      RETURN;
    END IF;

    total_count := array_length(all_reviewers, 1);
    fail_count := CEIL(total_count * p_fail_fraction)::int;
    pass_count := total_count - fail_count;

    RAISE NOTICE 'Всего ревьюеров=%, p_fail_fraction=%, => отсекаем %', 
                 total_count, p_fail_fraction, fail_count;

    ----------------------------------------------------------------------------
    -- Шаг 2. «Перемешиваем» список студентов (shuffle), чтобы случайно
    --         выбрать fail_count «неудачников».
    ----------------------------------------------------------------------------
    CREATE TEMP TABLE _temp_studs(sid text);
    FOREACH user_id IN ARRAY all_reviewers
    LOOP
      INSERT INTO _temp_studs(sid) VALUES(user_id);
    END LOOP;

    CREATE TEMP TABLE _temp_studs_shuffled AS
    SELECT sid
    FROM _temp_studs
    ORDER BY random();

    -- fail_set = первые fail_count
    SELECT array_agg(sid ORDER BY sid)
      INTO fail_set
    FROM (
      SELECT sid
      FROM _temp_studs_shuffled
      LIMIT fail_count
    ) AS foo;

    -- good_set = остальные
    SELECT array_agg(sid ORDER BY sid)
      INTO good_set
    FROM (
      SELECT sid
      FROM _temp_studs_shuffled
      OFFSET fail_count
    ) AS bar;

    IF fail_set IS NULL THEN
      fail_set := ARRAY[]::text[];
    END IF;
    IF good_set IS NULL THEN
      good_set := ARRAY[]::text[];
    END IF;

    RAISE NOTICE 'fail_set=%', fail_set;
    RAISE NOTICE 'good_set=%', good_set;

    ----------------------------------------------------------------------------
    -- Шаг 3. Собираем все проверки (user_duel_to_review) => _assignments,
    --         но относящиеся к этому спринту.
    ----------------------------------------------------------------------------
    CREATE TEMP TABLE _assignments AS
    SELECT
      t.id AS toreview_id,
      t.reviewer_user_strapi_document_id AS reviewer,
      t.user_strapi_document_id AS answer_owner,
      t.duel_strapi_document_id AS duel_id,
      t.hash,
      d.type AS duel_type
    FROM user_duel_to_review t
    JOIN duels d ON d.strapi_document_id = t.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_id;

    -- Из fail_set убираем ровно по 1 проверке у каждого «неудачника»
    CREATE TEMP TABLE _fail_skips AS
    SELECT reviewer, toreview_id
    FROM (
      SELECT a.*,
             row_number() OVER (PARTITION BY a.reviewer ORDER BY random()) AS rn
      FROM _assignments a
      WHERE a.reviewer = ANY(fail_set)
    ) sub
    WHERE rn = 1;

    -- Остальные проверки => _final_todo
    CREATE TEMP TABLE _final_todo AS
    SELECT a.*
    FROM _assignments a
    LEFT JOIN _fail_skips fs ON fs.toreview_id = a.toreview_id
    WHERE fs.toreview_id IS NULL;

    ----------------------------------------------------------------------------
    -- Шаг 4. Для каждой записи _final_todo вставляем user_duel_reviewed
    --         со случайными оценками: FULL-CONTACT => 0..1, TRAINING => 0..2
    ----------------------------------------------------------------------------
    DECLARE
      rec RECORD;
      r_full int;
      i_full int;
      a_full int;
      s1_train int;
      s2_train int;
    BEGIN
      FOR rec IN SELECT * FROM _final_todo LOOP
        IF rec.duel_type = 'FULL-CONTACT' THEN
          r_full := floor(random()*2)::int; -- 0..1
          i_full := floor(random()*2)::int;
          a_full := floor(random()*2)::int;

          EXECUTE format($qfc$
  INSERT INTO user_duel_reviewed(
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
  VALUES(now(), %L, %L, %L, %L, true, %L, %s, %s, %s, null, null)
$qfc$,
            rec.reviewer,        -- %L
            rec.duel_id,         -- %L
            rec.answer_owner,    -- %L
            'Auto comment: user='||rec.reviewer||' -> '||rec.answer_owner, -- %L
            rec.hash,            -- %L
            r_full::text,        -- %s
            i_full::text,        -- %s
            a_full::text         -- %s
          );

        ELSIF rec.duel_type = 'TRAINING' THEN
          s1_train := floor(random()*3)::int; -- 0..2
          s2_train := floor(random()*3)::int;

          EXECUTE format($qtr$
  INSERT INTO user_duel_reviewed(
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
  VALUES(now(), %L, %L, %L, %L, true, %L, null, null, null, %s, %s)
$qtr$,
            rec.reviewer,
            rec.duel_id,
            rec.answer_owner,
            'Auto comment: user='||rec.reviewer||' -> '||rec.answer_owner,
            rec.hash,
            s1_train::text,
            s2_train::text
          );
        END IF;
      END LOOP;
    END;

    RAISE NOTICE 'Сгенерировано % вставок (из % всего). Отсеяно ~% студентов.',
                 (SELECT count(*) FROM _final_todo),
                 (SELECT count(*) FROM _assignments),
                 fail_count;
END
