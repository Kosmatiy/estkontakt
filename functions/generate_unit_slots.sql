SELECT
      u.unit_index::TEXT || '_' || gs AS unit_slot,
      u.unit_index
    FROM load_user_units(p_sprint_id) AS u
    CROSS JOIN generate_series(1,3) AS gs;
