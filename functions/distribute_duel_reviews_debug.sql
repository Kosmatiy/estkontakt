DECLARE
  -- Массив для накопления логов
  logs              TEXT[] := ARRAY[]::TEXT[];
  -- Счётчики слотов
  needed_slots      INT;
  filled_slots      INT;
  -- Номер этапа (1–3)
  rec_stage         INT;
  -- Параметры текущего слота
  rec_slot_id       INT;
  rec_duel_id       TEXT;
  rec_hash          TEXT;
  rec_participant   TEXT;
  rec_reviewer      TEXT;
  -- Результат теста и метрики
  result            JSON;
  bad_pairs         INT;
  duplicates        INT;
  quota_violations  INT;
  -- Детали нарушений
  bad_pair_details         JSONB := '[]'::JSONB;
  quota_violation_details  JSONB := '[]'::JSONB;
BEGIN
  -- 0) Старт
  logs := logs || ARRAY[ format('START distribution: sprint=%s mode=%s', p_sprint_id, p_mode) ];

  -- 1) CLEANSLATE or GOON
  IF upper(p_mode) = 'CLEANSLATE' THEN
    logs := logs || ARRAY['MODE=CLEANSLATE: deleting old assignments...'];
    DELETE FROM user_duel_to_review udr
    USING duels d
    WHERE udr.duel_strapi_document_id = d.strapi_document_id
      AND d.sprint_strapi_document_id = p_sprint_id;
    logs := logs || ARRAY['Old assignments removed.'];
  ELSE
    logs := logs || ARRAY['MODE=GOON: keeping existing assignments.'];
  END IF;

  -- 2) Построить review_slots
  logs := logs || ARRAY['STEP: build review_slots'];
  CREATE TEMP TABLE review_slots ON COMMIT DROP AS
  SELECT
    ua.duel_strapi_document_id   AS duel_id,
    ua.hash,
    ua.user_strapi_document_id   AS participant,
    gs                           AS slot_no,
    NULL::TEXT                   AS reviewer,
    0                            AS stage_filled
  FROM user_duel_answers ua
  JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
  CROSS JOIN generate_series(1,3) AS gs
  WHERE d.sprint_strapi_document_id = p_sprint_id
  ORDER BY ua.duel_strapi_document_id, ua.hash, ua.user_strapi_document_id, gs;
  GET DIAGNOSTICS needed_slots = ROW_COUNT;
  logs := logs || ARRAY[ format('-> created %s review_slots', needed_slots) ];

  -- 3) Построить review_quota
  logs := logs || ARRAY['STEP: build review_quota'];
  CREATE TEMP TABLE review_quota ON COMMIT DROP AS
  SELECT
    ua.user_strapi_document_id  AS reviewer,
    COUNT(*) * 3                 AS quota,
    0                            AS assigned
  FROM user_duel_answers ua
  JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
  WHERE d.sprint_strapi_document_id = p_sprint_id
  GROUP BY ua.user_strapi_document_id;
  logs := logs || ARRAY[ format('-> prepared quotas for %s reviewers', (SELECT COUNT(*) FROM review_quota)) ];

  -- 4) Три этапа распределения
  FOR rec_stage IN 1..3 LOOP
    logs := logs || ARRAY[
      format(
        '=== STAGE %s: %s ===',
        rec_stage,
        CASE rec_stage
          WHEN 1 THEN 'strict duel + team'
          WHEN 2 THEN 'strict duel any team'
          ELSE           'any player'
        END
      )
    ];

    LOOP
      -- 4.a) взять первый свободный слот
      SELECT
        rs.duel_id, rs.hash, rs.participant
      INTO
        rec_duel_id, rec_hash, rec_participant
      FROM review_slots rs
      WHERE rs.reviewer IS NULL
      ORDER BY rs.duel_id, rs.hash, rs.participant, rs.slot_no
      LIMIT 1;

      EXIT WHEN NOT FOUND;
      logs := logs || ARRAY[ format(' * slot=(%s,%s,%s)', rec_duel_id, rec_hash, rec_participant) ];

      -- 4.b) найти ревьюера
      SELECT rq.reviewer
      INTO rec_reviewer
      FROM review_quota rq
      WHERE rq.reviewer   <> rec_participant
        AND rq.assigned   < rq.quota
        AND (
          -- этап 1: строгий duel + team
          (rec_stage=1
            AND EXISTS (
              SELECT 1 FROM user_duel_answers x
               WHERE x.user_strapi_document_id = rq.reviewer
                 AND x.duel_strapi_document_id = rec_duel_id
            )
            AND NOT EXISTS (
              SELECT 1
              FROM duels dd
              JOIN users u1 ON u1.strapi_document_id = rec_participant
              JOIN users u2 ON u2.strapi_document_id = rq.reviewer
              WHERE dd.strapi_document_id = rec_duel_id
                AND dd.type = 'FULL-CONTACT'
                AND u1.team_strapi_document_id = u2.team_strapi_document_id
            )
          )
          -- этап 2: строгий duel, любую команду
          OR (rec_stage=2
            AND EXISTS (
              SELECT 1 FROM user_duel_answers x
               WHERE x.user_strapi_document_id = rq.reviewer
                 AND x.duel_strapi_document_id = rec_duel_id
            )
          )
          -- этап 3: любой игрок
          OR rec_stage=3
        )
      ORDER BY rq.assigned, rq.reviewer
      LIMIT 1;

      IF NOT FOUND THEN
        logs := logs || ARRAY[ format('   -> NO reviewer for (%s,%s) at stage %s', rec_duel_id, rec_hash, rec_stage) ];
        EXIT;
      END IF;
      logs := logs || ARRAY[
        format(
          '   -> selected reviewer %s (load %s/%s)',
          rec_reviewer,
          (SELECT assigned FROM review_quota WHERE reviewer=rec_reviewer),
          (SELECT quota    FROM review_quota WHERE reviewer=rec_reviewer)
        )
      ];

      -- 4.c) назначить
      UPDATE review_slots
         SET reviewer = rec_reviewer, stage_filled = rec_stage
       WHERE ctid IN (
         SELECT ctid FROM review_slots
         WHERE duel_id=rec_duel_id AND hash=rec_hash AND participant=rec_participant AND reviewer IS NULL
         ORDER BY slot_no LIMIT 1
       );
      UPDATE review_quota
         SET assigned = assigned + 1
       WHERE reviewer = rec_reviewer;

      logs := logs || ARRAY[
        format(
          '   -> slot for (%s,%s) filled by %s; new load %s/%s',
          rec_duel_id, rec_hash, rec_reviewer,
          (SELECT assigned FROM review_quota WHERE reviewer=rec_reviewer),
          (SELECT quota    FROM review_quota WHERE reviewer=rec_reviewer)
        )
      ];
    END LOOP;
  END LOOP;

  -- 5) Проверка заполненности
  SELECT COUNT(*) INTO filled_slots FROM review_slots WHERE reviewer IS NOT NULL;
  IF filled_slots <> needed_slots THEN
    logs := logs || ARRAY[ format('!!! Only %s of %s slots filled !!!', filled_slots, needed_slots) ];
    RETURN json_build_object(
      'status',       'FAILED',
      'filled_slots', filled_slots,
      'needed_slots', needed_slots,
      'logs',         logs
    );
  END IF;
  logs := logs || ARRAY[ format('+++ All %s slots filled +++', needed_slots) ];

  -- 6) Запись в user_duel_to_review
  INSERT INTO user_duel_to_review (
    reviewer_user_strapi_document_id,
    duel_strapi_document_id,
    user_strapi_document_id,
    hash
  )
  SELECT
    rs.reviewer, rs.duel_id, rs.participant, rs.hash
  FROM review_slots rs
  ON CONFLICT DO NOTHING;
  logs := logs || ARRAY['Inserted assignments into user_duel_to_review'];

  -- 7) Финальный тест
  logs := logs || ARRAY['Calling test_user_duel_to_review...'];
  result := test_user_duel_to_review(p_sprint_id);

  bad_pairs        := (result->>'bad_pairs')::INT;
  duplicates       := (result->>'duplicates')::INT;
  quota_violations := (result->>'quota_violations')::INT;

  IF bad_pairs > 0 THEN
    logs := logs || ARRAY['Found bad_pairs > 0, collecting details...'];
    SELECT jsonb_agg(jsonb_build_object(
      'duel_strapi_document_id', duel_strapi_document_id,
      'hash',                   hash,
      'reviewers_count',        reviewers_count
    )) INTO bad_pair_details
    FROM (
      SELECT duel_strapi_document_id, hash, COUNT(DISTINCT reviewer_user_strapi_document_id) AS reviewers_count
      FROM user_duel_to_review udr
      JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
      GROUP BY 1,2
      HAVING COUNT(DISTINCT reviewer_user_strapi_document_id) <> 6
    ) t;
  END IF;

  IF quota_violations > 0 THEN
    logs := logs || ARRAY['Found quota_violations > 0, collecting details...'];
    SELECT jsonb_agg(jsonb_build_object(
      'reviewer',         reviewer,
      'expected_quota',   expected_quota,
      'assigned_reviews', assigned_reviews
    )) INTO quota_violation_details
    FROM (
      WITH quotas AS (
        SELECT ua.user_strapi_document_id AS reviewer, COUNT(*)*3 AS expected_quota
        FROM user_duel_answers ua
        JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
        GROUP BY ua.user_strapi_document_id
      ), actual AS (
        SELECT udr.reviewer_user_strapi_document_id AS reviewer, COUNT(*) AS assigned_reviews
        FROM user_duel_to_review udr
        JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
        GROUP BY udr.reviewer_user_strapi_document_id
      )
      SELECT q.reviewer, q.expected_quota, COALESCE(a.assigned_reviews,0) AS assigned_reviews
      FROM quotas q
      LEFT JOIN actual a USING(reviewer)
      WHERE COALESCE(a.assigned_reviews,0) <> q.expected_quota
    ) t;
  END IF;

  logs := logs || ARRAY['Distribution complete. Returning result.'];
  RETURN json_build_object(
    'status',                  result->>'status',
    'bad_pairs',               bad_pairs,
    'duplicates',              duplicates,
    'quota_violations',        quota_violations,
    'logs',                    logs,
    'bad_pair_details',        bad_pair_details,
    'quota_violation_details', quota_violation_details
  );
END;
