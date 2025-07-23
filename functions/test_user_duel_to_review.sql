DECLARE
    bad_pairs       INT;
    dup_cnt         INT;
    quota_violations INT;
    status_text     TEXT := 'OK';
BEGIN
    -- каждая пара (hash+duel) имеет ровно 6 ревьюеров
    SELECT COUNT(*) INTO bad_pairs
    FROM (
      SELECT duel_strapi_document_id, hash
      FROM user_duel_to_review udr
      JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
      GROUP BY 1,2
      HAVING COUNT(DISTINCT reviewer_user_strapi_document_id) <> 6
    ) t;

    -- дубликаты назначений
    SELECT COUNT(*) INTO dup_cnt
    FROM (
      SELECT reviewer_user_strapi_document_id, duel_strapi_document_id, user_strapi_document_id, hash
      FROM user_duel_to_review udr
      JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
      GROUP BY 1,2,3,4
      HAVING COUNT(*) > 1
    ) t;

    -- проверки по квотам
    SELECT COUNT(*) INTO quota_violations
    FROM (
      SELECT r.user_id, COUNT(*) AS assigned
      FROM user_duel_to_review udr
      JOIN (
        SELECT user_id, quota FROM calc_reviewer_quotas(p_sprint_id)
      ) r ON r.user_id = udr.reviewer_user_strapi_document_id
      JOIN duels d ON d.strapi_document_id = udr.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
      GROUP BY r.user_id, r.quota
      HAVING COUNT(*) <> r.quota
    ) t;

    IF bad_pairs>0 OR dup_cnt>0 OR quota_violations>0 THEN
      status_text := 'FAILED';
    END IF;

    RETURN json_build_object(
      'status',           status_text,
      'bad_pairs',        bad_pairs,
      'duplicates',       dup_cnt,
      'quota_violations', quota_violations
    );
END;
