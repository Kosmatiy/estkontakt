DECLARE
    extended_hash text := p_hash || '__p' || p_part::text;  -- <-- отличаем part=1/2
    coin int;
    valA int;
    valB int;
    userA text;
    userB text;
BEGIN
    IF cardinality(p_users)=1 THEN
        -- Единственный участник
        userA := p_users[1];
        IF p_duel_type='FULL-CONTACT' THEN
           -- Случайные 0..1
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
             attention_mark
           )
           VALUES(
             now(),
             p_expert_id,
             p_duel_id,
             userA,
             'Expert auto comment (single FULL-C)',
             true,
             extended_hash,
             floor(random()*2)::int,
             floor(random()*2)::int,
             floor(random()*2)::int
           )
           ON CONFLICT (hash, duel_strapi_document_id)
           DO NOTHING;
        ELSE
           -- TRAINING => skill1, skill2 = 0..2
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
             now(),
             p_expert_id,
             p_duel_id,
             userA,
             'Expert auto comment (single TRAIN)',
             true,
             extended_hash,
             floor(random()*3)::int,
             floor(random()*3)::int
           )
           ON CONFLICT (hash, duel_strapi_document_id)
           DO NOTHING;
        END IF;

    ELSIF cardinality(p_users)=2 THEN
        userA := p_users[1];
        userB := p_users[2];

        IF p_duel_type='FULL-CONTACT' THEN
           -- 2 INSERT'а (или UPSERT), потом update result/image/attention
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
             now(), p_expert_id, p_duel_id, userA,
             'Expert auto comment FULL-C userA', true, extended_hash
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
             hash
           )
           VALUES(
             now(), p_expert_id, p_duel_id, userB,
             'Expert auto comment FULL-C userB', true, extended_hash
           )
           ON CONFLICT (hash, duel_strapi_document_id)
           DO NOTHING;

           -- Распределим 3 категории:
           -- result
           coin := floor(random()*2)::int;
           IF coin=0 THEN valA=1; valB=0; ELSE valA=0; valB=1; END IF;
           UPDATE expert_duel_reviewed
             SET result_mark=valA
             WHERE hash=extended_hash
               AND duel_strapi_document_id=p_duel_id
               AND user_strapi_document_id=userA;
           UPDATE expert_duel_reviewed
             SET result_mark=valB
             WHERE hash=extended_hash
               AND duel_strapi_document_id=p_duel_id
               AND user_strapi_document_id=userB;

           -- image
           coin := floor(random()*2)::int;
           IF coin=0 THEN valA=1; valB=0; ELSE valA=0; valB=1; END IF;
           UPDATE expert_duel_reviewed
             SET image_mark=valA
             WHERE hash=extended_hash
               AND duel_strapi_document_id=p_duel_id
               AND user_strapi_document_id=userA;
           UPDATE expert_duel_reviewed
             SET image_mark=valB
             WHERE hash=extended_hash
               AND duel_strapi_document_id=p_duel_id
               AND user_strapi_document_id=userB;

           -- attention
           coin := floor(random()*2)::int;
           IF coin=0 THEN valA=1; valB=0; ELSE valA=0; valB=1; END IF;
           UPDATE expert_duel_reviewed
             SET attention_mark=valA
             WHERE hash=extended_hash
               AND duel_strapi_document_id=p_duel_id
               AND user_strapi_document_id=userA;
           UPDATE expert_duel_reviewed
             SET attention_mark=valB
             WHERE hash=extended_hash
               AND duel_strapi_document_id=p_duel_id
               AND user_strapi_document_id=userB;

        ELSE
           -- TRAINING => 2 INSERT
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
             now(), p_expert_id, p_duel_id, userA,
             'Expert auto comment TRAIN userA', true, extended_hash,
             floor(random()*3)::int, floor(random()*3)::int
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
             now(), p_expert_id, p_duel_id, userB,
             'Expert auto comment TRAIN userB', true, extended_hash,
             floor(random()*3)::int, floor(random()*3)::int
           )
           ON CONFLICT (hash, duel_strapi_document_id)
           DO NOTHING;
        END IF;
    ELSE
       -- >2 или 0 участников => пропустим
       RAISE NOTICE '[ExpertReview group] hash=% part=% => % users => skip',
         p_hash, p_part, cardinality(p_users);
    END IF;
END
