DECLARE
  -- Для очистки и отладки
  needed_slots  INT;
  filled_slots  INT;
  rec_stage     INT;
  -- Переменные для обработки слота
  rec_slot_id      INT;
  rec_duel_id      TEXT;
  rec_hash         TEXT;
  rec_participant  TEXT;
  rec_reviewer     TEXT;
BEGIN
  RAISE NOTICE '=== START distribute_duel_reviews_verbose(sprint=%, mode=%) ===', p_sprint_id, p_mode;

  -- 1) Очистка старых записей
  IF upper(p_mode) = 'CLEANSLATE' THEN
    RAISE NOTICE '[CLEANSLATE] Deleting old assignments...';
    DELETE FROM user_duel_to_review udr
    USING duels d
    WHERE udr.duel_strapi_document_id = d.strapi_document_id
      AND d.sprint_strapi_document_id = p_sprint_id;
    RAISE NOTICE '  -> Old assignments removed.';
  ELSE
    RAISE NOTICE '[GOON] Keeping existing assignments.';
  END IF;

  -- 2) Создаём слоты (каждый ответ × 3)
  RAISE NOTICE 'Building review_slots table...';
  CREATE TEMP TABLE review_slots (
    slot_id      SERIAL PRIMARY KEY,
    duel_id      TEXT    NOT NULL,
    hash         TEXT    NOT NULL,
    participant  TEXT    NOT NULL,
    slot_no      INT     NOT NULL,
    reviewer     TEXT,
    stage_filled INT     DEFAULT 0
  ) ON COMMIT DROP;

  INSERT INTO review_slots(duel_id, hash, participant, slot_no)
  SELECT ua.duel_strapi_document_id, ua.hash, ua.user_strapi_document_id, gs
  FROM user_duel_answers ua
  JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
  CROSS JOIN generate_series(1,3) AS gs
  WHERE d.sprint_strapi_document_id = p_sprint_id;
  GET DIAGNOSTICS needed_slots = ROW_COUNT;
  RAISE NOTICE '  -> Created % slots (each answer ×3).', needed_slots;

  -- 3) Создаём квоты ревьюеров
  RAISE NOTICE 'Building review_quota table...';
  CREATE TEMP TABLE review_quota (
    reviewer TEXT PRIMARY KEY,
    quota    INT NOT NULL,
    assigned INT NOT NULL DEFAULT 0
  ) ON COMMIT DROP;

  INSERT INTO review_quota(reviewer, quota)
  SELECT ua.user_strapi_document_id, COUNT(*) * 3
  FROM user_duel_answers ua
  JOIN duels d ON d.strapi_document_id = ua.duel_strapi_document_id
  WHERE d.sprint_strapi_document_id = p_sprint_id
  GROUP BY ua.user_strapi_document_id;
  RAISE NOTICE '  -> Prepared quotas for % reviewers.', (SELECT COUNT(*) FROM review_quota);

  -- 4) Три этапа распределения
  FOR rec_stage IN 1..3 LOOP
    RAISE NOTICE '--- Stage %: % ---', rec_stage,
      CASE rec_stage
        WHEN 1 THEN 'strict duel + team'
        WHEN 2 THEN 'strict duel any team'
        ELSE            'any player'
      END;

    LOOP
      -- 4.a) Выбрать ещё не заполненный слот
      SELECT rs.slot_id, rs.duel_id, rs.hash, rs.participant
        INTO rec_slot_id, rec_duel_id, rec_hash, rec_participant
      FROM review_slots rs
      WHERE rs.reviewer IS NULL
      ORDER BY rs.slot_id
      LIMIT 1;

      IF NOT FOUND THEN
        RAISE NOTICE '  -> No more unfilled slots on this stage.';
        EXIT;
      END IF;
      RAISE NOTICE '    * Slot %: duel=%, hash=%, participant=%',
        rec_slot_id, rec_duel_id, rec_hash, rec_participant;

      -- 4.b) Найти подходящего рецензента
      SELECT rq.reviewer
        INTO rec_reviewer
      FROM review_quota rq
      WHERE rq.reviewer <> rec_participant
        AND rq.assigned < rq.quota
        AND (
          -- этап 1: рецензент играл ту же дуэль и (для FULL-CONTACT) не из той же команды
          (rec_stage=1
            AND EXISTS (
              SELECT 1 FROM user_duel_answers x
               WHERE x.user_strapi_document_id = rq.reviewer
                 AND x.duel_strapi_document_id = rec_duel_id
            )
            AND NOT EXISTS (
              SELECT 1 FROM duels dd
               WHERE dd.strapi_document_id = rec_duel_id
                 AND dd.type='FULL-CONTACT'
                 AND EXISTS (
                   SELECT 1 FROM users u1 JOIN users u2
                     ON u1.team_strapi_document_id = u2.team_strapi_document_id
                   WHERE u1.strapi_document_id = rec_participant
                     AND u2.strapi_document_id = rq.reviewer
                 )
            )
          )
          -- этап 2: рецензент играл ту же дуэль (команда неважна)
          OR (rec_stage=2
            AND EXISTS (
              SELECT 1 FROM user_duel_answers x
               WHERE x.user_strapi_document_id = rq.reviewer
                 AND x.duel_strapi_document_id = rec_duel_id
            )
          )
          -- этап 3: любой участник, кроме самого
          OR rec_stage=3
        )
      ORDER BY rq.assigned, rq.reviewer
      LIMIT 1;

      IF NOT FOUND THEN
        RAISE NOTICE '      -> No reviewer found for slot % at stage %.', rec_slot_id, rec_stage;
        EXIT;
      END IF;
      RAISE NOTICE '      -> Selected reviewer % (assigned %/%).',
        rec_reviewer,
        (SELECT assigned FROM review_quota WHERE reviewer=rec_reviewer),
        (SELECT quota    FROM review_quota WHERE reviewer=rec_reviewer);

      -- 4.c) Записать назначение
      UPDATE review_slots
         SET reviewer = rec_reviewer, stage_filled = rec_stage
       WHERE slot_id = rec_slot_id;

      UPDATE review_quota
         SET assigned = assigned + 1
       WHERE reviewer = rec_reviewer;

      RAISE NOTICE '      -> Slot % now filled by %. New load %/%.',
        rec_slot_id, rec_reviewer,
        (SELECT assigned FROM review_quota WHERE reviewer=rec_reviewer),
        (SELECT quota    FROM review_quota WHERE reviewer=rec_reviewer);
    END LOOP;
  END LOOP;

  -- 5) Проверяем полное заполнение
  SELECT COUNT(*) INTO filled_slots FROM review_slots WHERE reviewer IS NOT NULL;
  IF filled_slots <> needed_slots THEN
    RAISE NOTICE '!!! Only % of % slots filled.', filled_slots, needed_slots;
    RETURN json_build_object(
      'status','FAILED',
      'message', FORMAT('Filled % of % slots', filled_slots, needed_slots)
    );
  END IF;
  RAISE NOTICE '+++ All % slots successfully filled.', needed_slots;

  -- 6) Вставляем в основную таблицу
  INSERT INTO user_duel_to_review(
    reviewer_user_strapi_document_id,
    duel_strapi_document_id,
    user_strapi_document_id,
    hash
  )
  SELECT
    rs.reviewer,
    rs.duel_id,
    rs.participant,
    rs.hash
  FROM review_slots rs
  ON CONFLICT DO NOTHING;
  RAISE NOTICE 'Inserted assignments into user_duel_to_review.';

  -- 7) Финальный тест
  RAISE NOTICE 'Calling test_user_duel_to_review...';
  RETURN test_user_duel_to_review(p_sprint_id);

END;
