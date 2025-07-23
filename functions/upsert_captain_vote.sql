DECLARE
    voter_rec      users%ROWTYPE;
    cand_rec       users%ROWTYPE;
    v_error        TEXT := NULL;
BEGIN
/* ---------- 1. Проверяем голосующего -------------------------------- */
    SELECT * INTO voter_rec
    FROM   users
    WHERE  strapi_document_id = v_user_strapi_document_id;

    IF NOT FOUND THEN
        v_error := format('Пользователь %s не найден', v_user_strapi_document_id);
    ELSIF voter_rec.dismissed_at IS NOT NULL THEN
        v_error := format('Пользователь %s отчислен и не может голосовать',
                          v_user_strapi_document_id);
    END IF;

/* ---------- 2. Проверяем кандидата ---------------------------------- */
    IF v_error IS NULL THEN
        SELECT * INTO cand_rec
        FROM   users
        WHERE  strapi_document_id = v_candidate_user_strapi_document_id;

        IF NOT FOUND THEN
            v_error := format('Кандидат %s не найден',
                              v_candidate_user_strapi_document_id);
        ELSIF cand_rec.dismissed_at IS NOT NULL THEN
            v_error := format('Кандидат %s отчислен и не может быть выбран',
                              v_candidate_user_strapi_document_id);
        END IF;
    END IF;

/* ---------- 3. (опц.) проверяем, одна ли команда -------------------- */
    IF v_error IS NULL
       AND voter_rec.team_strapi_document_id IS NOT NULL
       AND cand_rec.team_strapi_document_id IS NOT NULL
       AND voter_rec.team_strapi_document_id <> cand_rec.team_strapi_document_id THEN
        v_error := 'Кандидат должен быть из той же команды, что и голосующий';
    END IF;

/* ---------- 4. Пишем (UPSERT) --------------------------------------- */
    IF v_error IS NULL THEN
        INSERT INTO user_captain_vote (
                   user_strapi_document_id,
                   candidate_user_strapi_document_id,
                   stream_strapi_document_id,
                   team_strapi_document_id )
        VALUES (voter_rec.strapi_document_id,
                cand_rec.strapi_document_id,
                voter_rec.stream_strapi_document_id,
                voter_rec.team_strapi_document_id)
        ON CONFLICT (user_strapi_document_id)                -- уникальный ключ
        DO UPDATE
           SET candidate_user_strapi_document_id = EXCLUDED.candidate_user_strapi_document_id,
               stream_strapi_document_id         = EXCLUDED.stream_strapi_document_id,
               team_strapi_document_id           = EXCLUDED.team_strapi_document_id,
               created_at                        = now();     -- “перезаписываем” время

        RETURN json_build_object(
                 'result' , 'success',
                 'message', format(
                     'Голос пользователя %s записан за кандидата %s',
                     v_user_strapi_document_id,
                     v_candidate_user_strapi_document_id) );
    ELSE
        RETURN json_build_object(
                 'result' , 'error',
                 'message', v_error );
    END IF;
END;
