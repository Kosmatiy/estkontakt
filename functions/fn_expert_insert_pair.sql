DECLARE
    coin int;
    valA int;
    valB int;
BEGIN
    IF p_duel_type = 'FULL-CONTACT' THEN
       -- Сразу делаем 2 INSERT (hash = p_hash_exp), потом update result/image/attention

       -- INSERT userA
       INSERT INTO expert_duel_reviewed(
         created_at,
         reviewer_user_strapi_document_id,
         duel_strapi_document_id,
         user_strapi_document_id,
         comment,
         is_valid,
         hash
       )
       VALUES(
         now(),
         p_expert_id,
         p_duel_id,
         p_userA,
         'Expert auto comment (pair FULL-C) userA',
         true,
         p_hash_exp
       )
       ON CONFLICT (hash, duel_strapi_document_id)
       DO NOTHING;

       -- INSERT userB
       INSERT INTO expert_duel_reviewed(
         created_at,
         reviewer_user_strapi_document_id,
         duel_strapi_document_id,
         user_strapi_document_id,
         comment,
         is_valid,
         hash
       )
       VALUES(
         now(),
         p_expert_id,
         p_duel_id,
         p_userB,
         'Expert auto comment (pair FULL-C) userB',
         true,
         p_hash_exp
       )
       ON CONFLICT (hash, duel_strapi_document_id)
       DO NOTHING;

       -- RESULT
       coin := floor(random()*2)::int;
       IF coin=0 THEN valA=1; valB=0; ELSE valA=0; valB=1; END IF;
       UPDATE expert_duel_reviewed
         SET result_mark=valA
         WHERE hash=p_hash_exp
           AND duel_strapi_document_id=p_duel_id
           AND user_strapi_document_id=p_userA;

       UPDATE expert_duel_reviewed
         SET result_mark=valB
         WHERE hash=p_hash_exp
           AND duel_strapi_document_id=p_duel_id
           AND user_strapi_document_id=p_userB;

       -- IMAGE
       coin := floor(random()*2)::int;
       IF coin=0 THEN valA=1; valB=0; ELSE valA=0; valB=1; END IF;
       UPDATE expert_duel_reviewed
         SET image_mark=valA
         WHERE hash=p_hash_exp
           AND duel_strapi_document_id=p_duel_id
           AND user_strapi_document_id=p_userA;

       UPDATE expert_duel_reviewed
         SET image_mark=valB
         WHERE hash=p_hash_exp
           AND duel_strapi_document_id=p_duel_id
           AND user_strapi_document_id=p_userB;

       -- ATTENTION
       coin := floor(random()*2)::int;
       IF coin=0 THEN valA=1; valB=0; ELSE valA=0; valB=1; END IF;
       UPDATE expert_duel_reviewed
         SET attention_mark=valA
         WHERE hash=p_hash_exp
           AND duel_strapi_document_id=p_duel_id
           AND user_strapi_document_id=p_userA;

       UPDATE expert_duel_reviewed
         SET attention_mark=valB
         WHERE hash=p_hash_exp
           AND duel_strapi_document_id=p_duel_id
           AND user_strapi_document_id=p_userB;

    ELSIF p_duel_type = 'TRAINING' THEN
       -- Каждый skill1=0..2, skill2=0..2
       INSERT INTO expert_duel_reviewed(
         created_at,
         reviewer_user_strapi_document_id,
         duel_strapi_document_id,
         user_strapi_document_id,
         comment,
         is_valid,
         hash,
         skill1_mark,
         skill2_mark
       )
       VALUES(
         now(), p_expert_id, p_duel_id, p_userA,
         'Expert auto comment (pair TRAIN) userA',
         true,
         p_hash_exp,
         floor(random()*3)::int,
         floor(random()*3)::int
       )
       ON CONFLICT (hash, duel_strapi_document_id)
       DO NOTHING;

       INSERT INTO expert_duel_reviewed(
         created_at,
         reviewer_user_strapi_document_id,
         duel_strapi_document_id,
         user_strapi_document_id,
         comment,
         is_valid,
         hash,
         skill1_mark,
         skill2_mark
       )
       VALUES(
         now(), p_expert_id, p_duel_id, p_userB,
         'Expert auto comment (pair TRAIN) userB',
         true,
         p_hash_exp,
         floor(random()*3)::int,
         floor(random()*3)::int
       )
       ON CONFLICT (hash, duel_strapi_document_id)
       DO NOTHING;
    END IF;
END
