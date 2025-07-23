BEGIN
    INSERT INTO duel_rr_log(run_id,level,sprint_id,duel_id,pair_hash,reviewer_id,event,details)
    VALUES(p_run,p_lvl,p_sprint,p_duel,p_hash,p_rev,p_evt,p_det);
END;
