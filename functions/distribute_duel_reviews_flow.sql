DECLARE
  v_total_units  INTEGER;
  v_flow_needed  INTEGER;
  v_flow_obtained INTEGER;
BEGIN
  -- 1. Собрать список всех unit (ответов) и ревьюеров
  DROP TABLE IF EXISTS tmp_units  CASCADE;
  CREATE TEMP TABLE tmp_units AS
  SELECT
    ua.duel_strapi_document_id   AS duel_id,
    ua.hash                      AS hash,
    ua.user_strapi_document_id   AS participant_id,
    ROW_NUMBER() OVER ()         AS unit_id
  FROM user_duel_answers ua
  JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
  WHERE d.sprint_strapi_document_id = p_sprint_id
    AND ua.user_strapi_document_id NOT IN (
      SELECT strapi_document_id FROM users WHERE dismissed_at IS NOT NULL
    );

  DROP TABLE IF EXISTS tmp_reviewers CASCADE;
  CREATE TEMP TABLE tmp_reviewers AS
  SELECT
    u.strapi_document_id       AS reviewer_id,
    COUNT(ua.*) * 3 AS quota
  FROM users u
  JOIN user_duel_answers ua
    ON ua.user_strapi_document_id = u.strapi_document_id
  JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
  WHERE d.sprint_strapi_document_id = p_sprint_id
    AND u.dismissed_at IS NULL
  GROUP BY u.strapi_document_id;

  -- 2. Подсчитать потребность
  SELECT COUNT(*) INTO v_total_units FROM tmp_units;
  v_flow_needed := v_total_units * 3;

  -- 3. Подготовить таблицу рёбер для трёх этапов
  DROP TABLE IF EXISTS tmp_edges CASCADE;
  CREATE TEMP TABLE tmp_edges (
    src   TEXT,
    tgt   TEXT,
    cap   INTEGER,
    stage SMALLINT
  );

  -- 3.1. Источник→unit (cap=3)
  INSERT INTO tmp_edges(src,tgt,cap,stage)
  SELECT 'S', unit_id::TEXT, 3, 0
  FROM tmp_units;

  -- 3.2. reviewer→сток (cap=quota)
  INSERT INTO tmp_edges(src,tgt,cap,stage)
  SELECT reviewer_id, 'T', quota, 0
  FROM tmp_reviewers;

  -- 3.3. unit→reviewer (bipartite), stage = 1,2,3
  INSERT INTO tmp_edges(src,tgt,cap,stage)
  SELECT
    u.unit_id::TEXT               AS src,
    r.reviewer_id                 AS tgt,
    v_flow_needed                 AS cap,
    -- вычисляем этап доступности
    CASE
      WHEN u.duel_id IN (
         SELECT duel_strapi_document_id
           FROM user_duel_answers
          WHERE user_strapi_document_id = r.reviewer_id
      ) AND
      -- для FULL-CONTACT учёт команды:
      ( d.type <> 'FULL-CONTACT'
        OR (
           u.participant_id NOT IN (SELECT team_member_id FROM teams WHERE team_id = r.team_strapi_document_id)
           AND u.opponent_id   NOT IN (SELECT team_member_id FROM teams WHERE team_id = r.team_strapi_document_id)
        )
      )
      THEN 1
      WHEN u.duel_id IN (
         SELECT duel_strapi_document_id
           FROM user_duel_answers
          WHERE user_strapi_document_id = r.reviewer_id
      ) THEN 2
      ELSE 3
    END AS stage
  FROM tmp_units u
  CROSS JOIN tmp_reviewers r
  JOIN duels d ON d.strapi_document_id = u.duel_id
  WHERE r.reviewer_id <> u.participant_id;

  -- 4. Если CLEANSLATE — убрать старые назначения
  IF upper(p_mode) = 'CLEANSLATE' THEN
    DELETE FROM user_duel_to_review
     USING duels
    WHERE user_duel_to_review.duel_strapi_document_id = duels.strapi_document_id
      AND duels.sprint_strapi_document_id = p_sprint_id;
  END IF;

  -- 5. Последовательный max-flow по стадиям
  FOR stage IN 1..3 LOOP
    -- вызвать внешний flow-движок, передав tmp_edges WHERE stage<=current
    PERFORM maxflow_compute(
      source      := 'S',
      sink        := 'T',
      edges_table := 'tmp_edges',
      capacity    := 'cap',
      from_col    := 'src',
      to_col      := 'tgt',
      filter      := format('stage <= %s', stage)
    );
    -- прочитать, сколько потока получилось
    SELECT flow_value INTO v_flow_obtained FROM maxflow_status() LIMIT 1;
    EXIT WHEN v_flow_obtained >= v_flow_needed;
  END LOOP;

  IF v_flow_obtained < v_flow_needed THEN
    RETURN json_build_object(
      'status','FAILED',
      'message','Недостаточно кандидатов даже после 3 этапов'
    );
  END IF;

  -- 6. Извлечь каждую единицу потока u→r и вставить в user_duel_to_review
  INSERT INTO user_duel_to_review (
    reviewer_user_strapi_document_id,
    duel_strapi_document_id,
    user_strapi_document_id,
    hash
  )
  SELECT
    fe.tgt               AS reviewer_user_strapi_document_id,
    u.duel_id,
    u.participant_id,
    u.hash
  FROM flow_edges fe
  JOIN tmp_units u
    ON fe.src = u.unit_id::TEXT
  WHERE fe.flow > 0
    AND fe.tgt <> 'T';

  -- 7. Финальная проверка
  RETURN test_user_duel_to_review(p_sprint_id);
END;
