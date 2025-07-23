BEGIN
  -- 2.1 Все ответы (units)
  DROP TABLE IF EXISTS tmp_units CASCADE;
  CREATE TEMP TABLE tmp_units AS
  SELECT
    row_number() OVER ()                 AS unit_id,
    d.strapi_document_id                 AS duel_id,
    ua.hash,
    ua.user_strapi_document_id           AS participant_id
  FROM user_duel_answers ua
  JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
  WHERE d.sprint_strapi_document_id = p_sprint_id;

  -- 2.2 Каждый unit => 3 unit-slots
  DROP TABLE IF EXISTS tmp_unit_slots CASCADE;
  CREATE TEMP TABLE tmp_unit_slots AS
  SELECT
    (u.unit_id::TEXT || '_' || gs)      AS slot_id_text,
    u.unit_id,
    u.duel_id,
    u.hash,
    u.participant_id,
    -- разбираем hash
    split_part(u.hash,'_',1)            AS p1,
    split_part(u.hash,'_',2)            AS p2,
    -- команды участников
    us1.team_strapi_document_id         AS participant_team,
    us2.team_strapi_document_id         AS opponent_team,
    -- вычисляем opponent_id
    CASE
      WHEN split_part(u.hash,'_',1)=u.participant_id
      THEN split_part(u.hash,'_',2)
      ELSE split_part(u.hash,'_',1)
    END                                  AS opponent_id
  FROM tmp_units u
  CROSS JOIN generate_series(1,3) AS gs
  JOIN users us1 ON us1.strapi_document_id = u.participant_id
  JOIN users us2 ON us2.strapi_document_id =
    CASE
      WHEN split_part(u.hash,'_',1)=u.participant_id
      THEN split_part(u.hash,'_',2)
      ELSE split_part(u.hash,'_',1)
    END;

  -- 2.3 Слоты ревьюеров по квоте (3 × answers_count)
  DROP TABLE IF EXISTS tmp_slots CASCADE;
  CREATE TEMP TABLE tmp_slots AS
  SELECT
    row_number() OVER ()                     AS slot_id,
    q.user_id                               AS reviewer_id,
    usr.team_strapi_document_id             AS reviewer_team
  FROM (
    SELECT
      ua.user_strapi_document_id           AS user_id,
      COUNT(*) * 3                         AS quota
    FROM user_duel_answers ua
    JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
    WHERE d.sprint_strapi_document_id = p_sprint_id
    GROUP BY ua.user_strapi_document_id
  ) q
  JOIN users usr ON usr.strapi_document_id = q.user_id
  CROSS JOIN generate_series(1, q.quota);

  -- 2.4 Промежуточная таблица назначений
  DROP TABLE IF EXISTS tmp_assign CASCADE;
  CREATE TEMP TABLE tmp_assign(
    reviewer_slot_id INT,
    unit_slot_id     TEXT
  ) ON COMMIT DROP;
END;
