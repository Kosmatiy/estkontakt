BEGIN
    IF p_duel_type = 'FULL-CONTACT' THEN
       -- Просто ставим result/image/attention => random(0..1)
       INSERT INTO expert_duel_reviewed(
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
         skill2_mark
       )
       VALUES(
         now(),
         p_expert_id,
         p_duel_id,
         p_user_id,
         'Expert auto comment (1 user FULL-C)',
         true,
         p_hash_exp,
         floor(random()*2)::int,
         floor(random()*2)::int,
         floor(random()*2)::int,
         null,
         null
       )
       ON CONFLICT (hash, duel_strapi_document_id)
       DO NOTHING;

    ELSIF p_duel_type = 'TRAINING' THEN
       INSERT INTO expert_duel_reviewed(
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
         skill2_mark
       )
       VALUES(
         now(),
         p_expert_id,
         p_duel_id,
         p_user_id,
         'Expert auto comment (1 user TRAIN)',
         true,
         p_hash_exp,
         null,
         null,
         null,
         floor(random()*3)::int,
         floor(random()*3)::int
       )
       ON CONFLICT (hash, duel_strapi_document_id)
       DO NOTHING;
    END IF;
END
