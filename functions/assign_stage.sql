DECLARE
    rec_slot RECORD;
BEGIN
    FOR rec_slot IN
      SELECT user_id, slot_index FROM generate_reviewer_slots(p_sprint_id)
    LOOP
      INSERT INTO tmp_assign(user_id, slot_index, unit_slot)
      SELECT
        rec_slot.user_id,
        rec_slot.slot_index,
        us.unit_slot
      FROM generate_unit_slots(p_sprint_id) AS us
      LEFT JOIN tmp_assign ta ON ta.unit_slot = us.unit_slot
      JOIN load_user_units(p_sprint_id)   AS u
        ON u.unit_index = us.unit_index
      WHERE ta.unit_slot IS NULL
        AND u.participant <> rec_slot.user_id
      LIMIT 1;
      -- сразу же продолжаем к следующему слоту
    END LOOP;
END;
