DECLARE
    rec           RECORD;
    duel_rec      RECORD;
    existing_rec  RECORD;
    -- Параметры для FULL‑CONTACT оценок:
    new_result    int;
    new_image     int;
    new_attention int;
    -- Параметры для TRAINING оценок:
    new_skill1    int;
    new_skill2    int;
    -- Переменная для формирования комментария:
    v_comment     text;
BEGIN
    --------------------------------------------------------------------------
    -- 1. Удаляем все записи из user_duel_reviewed для дуэлей, относящихся к спринту p_sprint_strapi_document_id
    --------------------------------------------------------------------------
    DELETE FROM user_duel_reviewed ud
    USING duels d
    WHERE ud.duel_strapi_document_id = d.strapi_document_id
      AND d.sprint_strapi_document_id = p_sprint_strapi_document_id;
      
    RAISE NOTICE 'Deleted reviews from user_duel_reviewed for sprint %', p_sprint_strapi_document_id;
    
    --------------------------------------------------------------------------
    -- 2. Обрабатываем записи из user_duel_to_review, относящиеся только к данному спринту
    --------------------------------------------------------------------------
    FOR rec IN
         SELECT t.*
         FROM user_duel_to_review t
         JOIN duels d ON d.strapi_document_id = t.duel_strapi_document_id
         WHERE d.sprint_strapi_document_id = p_sprint_strapi_document_id
    LOOP
        -- Находим соответствующую дуэль
        SELECT *
          INTO duel_rec
          FROM duels
         WHERE strapi_document_id = rec.duel_strapi_document_id
         LIMIT 1;
         
        IF NOT FOUND THEN
            RAISE NOTICE 'Duel with id = % not found. Skipping record id = %', rec.duel_strapi_document_id, rec.id;
            CONTINUE;
        END IF;
        
        IF duel_rec.type = 'TRAINING' THEN
            -- Генерация случайных значений для TRAINING оценок (от 0 до 2)
            new_skill1 := floor(random() * 3)::int;
            new_skill2 := floor(random() * 3)::int;
            
            v_comment := format(
              'Auto comment (TRAINING): Reviewer %s reviewed user %s for duel %s. Assigned skill1_mark = %s, skill2_mark = %s. Duel skill IDs: %s, %s.',
              rec.reviewer_user_strapi_document_id,
              rec.user_strapi_document_id,
              rec.duel_strapi_document_id,
              new_skill1, new_skill2,
              COALESCE(duel_rec.skill1_strapi_document_id, 'NULL'),
              COALESCE(duel_rec.skill2_strapi_document_id, 'NULL')
            );
            
            INSERT INTO user_duel_reviewed(
                created_at,
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                comment,
                is_valid,
                hash,
                result_mark,
                image_mark,
                attention_mark,
                skill1_mark,
                skill2_mark,
                skill1_strapi_document_id,
                skill2_strapi_document_id
            )
            VALUES (
                now(),
                rec.reviewer_user_strapi_document_id,
                rec.duel_strapi_document_id,
                rec.user_strapi_document_id,
                v_comment,
                TRUE,
                rec.hash,
                NULL, NULL, NULL,  -- Для TRAINING поля FULL‑CONTACT оставляем NULL
                new_skill1,
                new_skill2,
                duel_rec.skill1_strapi_document_id,
                duel_rec.skill2_strapi_document_id
            );
            
        ELSIF duel_rec.type = 'FULL-CONTACT' THEN
            -- Если для данной дуэли (по duel_strapi_document_id и hash) уже есть запись, берем её оценки для комплементарности
            SELECT result_mark, image_mark, attention_mark
              INTO existing_rec
              FROM user_duel_reviewed
             WHERE duel_strapi_document_id = rec.duel_strapi_document_id
               AND hash = rec.hash
             LIMIT 1;
             
            IF FOUND THEN
                new_result := 1 - COALESCE(existing_rec.result_mark, 0);
                new_image  := 1 - COALESCE(existing_rec.image_mark, 0);
                new_attention := 1 - COALESCE(existing_rec.attention_mark, 0);
            ELSE
                new_result := floor(random()*2)::int;
                new_image  := floor(random()*2)::int;
                new_attention := floor(random()*2)::int;
            END IF;
            
            v_comment := format(
              'Auto comment (FULL‑CONTACT): Reviewer %s reviewed user %s for duel %s. Assigned result_mark = %s, image_mark = %s, attention_mark = %s. (Hash: %s)',
              rec.reviewer_user_strapi_document_id,
              rec.user_strapi_document_id,
              rec.duel_strapi_document_id,
              new_result, new_image, new_attention,
              rec.hash
            );
            
            INSERT INTO user_duel_reviewed(
                created_at,
                reviewer_user_strapi_document_id,
                duel_strapi_document_id,
                user_strapi_document_id,
                comment,
                is_valid,
                hash,
                result_mark,
                image_mark,
                attention_mark,
                skill1_mark,
                skill2_mark,
                skill1_strapi_document_id,
                skill2_strapi_document_id
            )
            VALUES (
                now(),
                rec.reviewer_user_strapi_document_id,
                rec.duel_strapi_document_id,
                rec.user_strapi_document_id,
                v_comment,
                TRUE,
                rec.hash,
                new_result,
                new_image,
                new_attention,
                NULL, NULL,  -- Для FULL‑CONTACT поля TRAINING оставляем NULL
                NULL, NULL
            );
            
        ELSE
            RAISE NOTICE 'Unsupported duel type % for record id %. Skipping.', duel_rec.type, rec.id;
            CONTINUE;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Generation of duel reviews completed for sprint %', p_sprint_strapi_document_id;
END;
