BEGIN
  RETURN (
    SELECT json_agg(
      json_build_object(
         'question_strapi_document_id', unioned.strapi_document_id,
         'test_strapi_document_id',     unioned.test_strapi_document_id,
         'question_id',                 unioned.id,
         'answer',                      unioned.answer,
         'is_correct',                  unioned.is_correct,
         'answer_letter',               unioned.answer_letter,
         'comment',                     unioned.comment,
         'variant_right',               unioned.variant_right
      )
    )
    FROM (
      SELECT *
      FROM (
        -- Вариант A
        SELECT 
          v.strapi_document_id,
          v.test_strapi_document_id,
          v.id,
          v.variant_a AS answer, 
          (v.variant_right = 'a') AS is_correct,
          'a' AS answer_letter,
          v.comment_a AS comment,
          v.variant_right AS variant_right
        FROM view_questions v
        WHERE v.test_strapi_document_id = _test_strapi_document_id

        UNION ALL

        -- Вариант B
        SELECT 
          v.strapi_document_id,
          v.test_strapi_document_id,
          v.id,
          v.variant_b AS answer, 
          (v.variant_right = 'b') AS is_correct,
          'b' AS answer_letter,
          v.comment_b AS comment,
          v.variant_right AS variant_right
        FROM view_questions v
        WHERE v.test_strapi_document_id = _test_strapi_document_id

        UNION ALL

        -- Вариант C
        SELECT 
          v.strapi_document_id,
          v.test_strapi_document_id,
          v.id,
          v.variant_c AS answer, 
          (v.variant_right = 'c') AS is_correct,
          'c' AS answer_letter,
          v.comment_c AS comment,
          v.variant_right AS variant_right
        FROM view_questions v
        WHERE v.test_strapi_document_id = _test_strapi_document_id

        UNION ALL

        -- Вариант D
        SELECT 
          v.strapi_document_id,
          v.test_strapi_document_id,
          v.id,
          v.variant_d AS answer, 
          (v.variant_right = 'd') AS is_correct,
          'd' AS answer_letter,
          v.comment_d AS comment,
          v.variant_right AS variant_right
        FROM view_questions v
        WHERE v.test_strapi_document_id = _test_strapi_document_id
      ) AS combined
      ORDER BY random()  -- перемешивание всех вариантов ответа
    ) AS unioned
  );
END;
