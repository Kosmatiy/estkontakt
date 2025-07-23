DECLARE
  rec_unit RECORD;
  cnt      INT;
  v_slot   INT;
BEGIN
  FOR rec_unit IN
    SELECT * FROM tmp_unit_slots ORDER BY duel_id, hash, slot_id_text
  LOOP
    -- каждому unit-slot по 3 ревью
    FOR cnt IN 1..3 LOOP
      v_slot := NULL;
      SELECT ts.slot_id
        INTO v_slot
      FROM tmp_slots ts
      LEFT JOIN tmp_assign ta
        ON ta.reviewer_slot_id = ts.slot_id
      WHERE ta.reviewer_slot_id IS NULL
        AND ts.reviewer_id      <> rec_unit.participant_id
        AND (
          (p_stage = 'strict_duel_and_team'
           AND rec_unit.duel_id IN (
             SELECT duel_strapi_document_id
               FROM user_duel_answers
              WHERE user_strapi_document_id = ts.reviewer_id
           )
           AND (
             rec_unit.duel_id NOT IN (
               SELECT strapi_document_id FROM duels WHERE type <> 'FULL-CONTACT'
             )
             OR (rec_unit.participant_team <> ts.reviewer_team
                 AND rec_unit.opponent_team    <> ts.reviewer_team)
           )
          )
          OR (p_stage = 'strict_duel_any_team'
              AND rec_unit.duel_id IN (
                SELECT duel_strapi_document_id
                  FROM user_duel_answers
                 WHERE user_strapi_document_id = ts.reviewer_id
              )
             )
          OR (p_stage = 'any_player')
        )
      ORDER BY ts.reviewer_id
      LIMIT 1;

      IF v_slot IS NOT NULL THEN
        INSERT INTO tmp_assign(reviewer_slot_id, unit_slot_id)
        VALUES (v_slot, rec_unit.slot_id_text);
      END IF;
    END LOOP;
  END LOOP;
END;
