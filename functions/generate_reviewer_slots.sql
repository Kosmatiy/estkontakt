SELECT
      q.user_id,
      gs AS slot_index
    FROM calc_reviewer_quotas(p_sprint_id) AS q
    CROSS JOIN generate_series(1, q.quota) AS gs
    ORDER BY q.user_id, gs;
