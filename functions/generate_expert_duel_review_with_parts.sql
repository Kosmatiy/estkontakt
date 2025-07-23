DECLARE
    rec RECORD;
    cnt INTEGER := 0;
    temp_count INTEGER;
BEGIN
    -- Удаляем временную таблицу, если она уже существует
    DROP TABLE IF EXISTS temp_answers;

    -- Создаём временную таблицу с отфильтрованными ответами без дубликатов
    CREATE TEMP TABLE temp_answers AS
    SELECT *
    FROM (
      SELECT uda.*,
             d.type AS duel_type,
             row_number() OVER (
                  PARTITION BY uda.user_strapi_document_id, uda.hash, uda.answer_part
                  ORDER BY uda.created_at DESC
             ) AS rn
      FROM user_duel_answers uda
      JOIN duels d ON d.strapi_document_id = uda.duel_strapi_document_id
      WHERE COALESCE(uda.status, 'ok') = 'ok'
        AND d.sprint_strapi_document_id = p_sprint_id
    ) sub
    WHERE rn = 1;

    SELECT count(*) INTO temp_count FROM temp_answers;
    RAISE NOTICE 'Найдено % записей во временной таблице temp_answers', temp_count;

    -- Группируем по дуэли: по duel_strapi_document_id, hash и duel_type.
    -- В массивы собираются user_strapi_document_id и answer_part, сортировка по answer_part гарантирует, что AnswerPart=1 окажется первым.
    FOR rec IN
      SELECT duel_strapi_document_id, hash, duel_type,
             array_agg(user_strapi_document_id ORDER BY answer_part) AS users,
             array_agg(answer_part ORDER BY answer_part) AS parts
      FROM temp_answers
      GROUP BY duel_strapi_document_id, hash, duel_type
    LOOP
      IF array_length(rec.users, 1) <> 2 THEN
         RAISE NOTICE 'Дуэль % с hash % имеет % ответов – пропускаем', 
                      rec.duel_strapi_document_id, rec.hash, array_length(rec.users,1);
         CONTINUE;
      END IF;

      IF rec.duel_type = 'FULL-CONTACT' THEN
         DECLARE
            userA TEXT := rec.users[1];
            userB TEXT := rec.users[2];
            res_mark_a INT;
            res_mark_b INT;
            img_mark_a INT;
            img_mark_b INT;
            att_mark_a INT;
            att_mark_b INT;
         BEGIN
            -- Подбрасываем монетку для каждой категории
            IF floor(random() * 2)::int = 0 THEN
               res_mark_a := 1; res_mark_b := 0;
            ELSE
               res_mark_a := 0; res_mark_b := 1;
            END IF;
            IF floor(random() * 2)::int = 0 THEN
               img_mark_a := 1; img_mark_b := 0;
            ELSE
               img_mark_a := 0; img_mark_b := 1;
            END IF;
            IF floor(random() * 2)::int = 0 THEN
               att_mark_a := 1; att_mark_b := 0;
            ELSE
               att_mark_a := 0; att_mark_b := 1;
            END IF;

            INSERT INTO expert_duel_reviewed(
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                comment,
                is_valid,
                hash,
                result_mark,
                image_mark,
                attention_mark
            )
            VALUES
            (p_expert_id, rec.duel_strapi_document_id, userA,
             'Expert auto comment (FULL-CONTACT)', TRUE,
             rec.hash,
             res_mark_a, img_mark_a, att_mark_a),
            (p_expert_id, rec.duel_strapi_document_id, userB,
             'Expert auto comment (FULL-CONTACT)', TRUE,
             rec.hash,
             res_mark_b, img_mark_b, att_mark_b)
            ON CONFLICT (duel_strapi_document_id, hash, user_strapi_document_id) DO NOTHING;

            INSERT INTO expert_duel_to_review(
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                hash,
                user_strapi_document_id
            )
            VALUES
            (p_expert_id, rec.duel_strapi_document_id, rec.hash, userA),
            (p_expert_id, rec.duel_strapi_document_id, rec.hash, userB)
            ON CONFLICT (duel_strapi_document_id, hash, user_strapi_document_id) DO NOTHING;

            cnt := cnt + 2;
         END;
      ELSIF rec.duel_type = 'TRAINING' THEN
         DECLARE
            userA TEXT := rec.users[1];
            userB TEXT := rec.users[2];
            skill1_a INT;
            skill2_a INT;
            skill1_b INT;
            skill2_b INT;
         BEGIN
            skill1_a := floor(random() * 3)::int;
            skill2_a := floor(random() * 3)::int;
            skill1_b := floor(random() * 3)::int;
            skill2_b := floor(random() * 3)::int;

            INSERT INTO expert_duel_reviewed(
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                comment,
                is_valid,
                hash,
                skill1_mark,
                skill2_mark,
                skill1_strapi_document_id,
                skill2_strapi_document_id
            )
            VALUES
            (p_expert_id, rec.duel_strapi_document_id, userA,
             'Expert auto comment (TRAINING)', TRUE,
             rec.hash,
             skill1_a, skill2_a,
             'x32dfbgykyu92o6wb92nm50i', 'zk0vk04tbl4n7pe9s0w1py78'),
            (p_expert_id, rec.duel_strapi_document_id, userB,
             'Expert auto comment (TRAINING)', TRUE,
             rec.hash,
             skill1_b, skill2_b,
             'x32dfbgykyu92o6wb92nm50i', 'zk0vk04tbl4n7pe9s0w1py78')
            ON CONFLICT (duel_strapi_document_id, hash, user_strapi_document_id) DO NOTHING;

            INSERT INTO expert_duel_to_review(
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                hash,
                user_strapi_document_id
            )
            VALUES
            (p_expert_id, rec.duel_strapi_document_id, rec.hash, userA),
            (p_expert_id, rec.duel_strapi_document_id, rec.hash, userB)
            ON CONFLICT (duel_strapi_document_id, hash, user_strapi_document_id) DO NOTHING;

            cnt := cnt + 2;
         END;
      ELSE
         RAISE NOTICE 'Неизвестный тип схватки: %', rec.duel_type;
      END IF;
    END LOOP;

    RAISE NOTICE 'Обработано % строк экспертных оценок', cnt;
END;
