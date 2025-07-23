DECLARE
    rec        RECORD;
    reports    JSON[];
    one_report JSON;
BEGIN
    reports := ARRAY[]::JSON[];

    FOR rec IN
        SELECT 
          uda.duel_strapi_document_id AS duel_id,
          uda.hash
        FROM user_duel_answers uda
        JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
        WHERE d.sprint_strapi_document_id = p_sprint_id
        GROUP BY uda.duel_strapi_document_id, uda.hash
    LOOP
        -- вызываем распределитель по одной паре
        one_report := distribute_single_duel(rec.duel_id, rec.hash);
        reports := array_append(reports, json_build_object(
            'duel_id', rec.duel_id,
            'hash',    rec.hash,
            'report',  one_report
        ));
    END LOOP;

    RETURN json_build_object(
      'sprint_id', p_sprint_id,
      'results',   reports
    );
END;
