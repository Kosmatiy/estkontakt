DECLARE
    rec_q       RECORD;
    good_left   INT := p_good_cnt;
    qa_rows     INT := 0;
    wrong_ans   TEXT;
BEGIN
    /* ответы на вопросы */
    IF NOT p_skip_qa THEN
        FOR rec_q IN
            SELECT *
            FROM   questions
            WHERE  test_strapi_document_id = p_test_id
            ORDER  BY random()
        LOOP
            IF good_left > 0 THEN
                INSERT INTO user_question_answers(
                    created_at, user_strapi_document_id, user_answer,
                    right_answer, attempt, score,
                    question_strapi_document_id, test_strapi_document_id
                )
                VALUES (
                    now(), p_user_id,
                    rec_q.variant_right, rec_q.variant_right,
                    p_attempt, 1,
                    rec_q.strapi_document_id, p_test_id
                );
                good_left := good_left - 1;
            ELSE
                LOOP
                    SELECT (ARRAY['a','b','c','d'])[ceil(random()*4)] INTO wrong_ans;
                    EXIT WHEN wrong_ans <> rec_q.variant_right;
                END LOOP;

                INSERT INTO user_question_answers(
                    created_at, user_strapi_document_id, user_answer,
                    right_answer, attempt, score,
                    question_strapi_document_id, test_strapi_document_id
                )
                VALUES (
                    now(), p_user_id,
                    wrong_ans, rec_q.variant_right,
                    p_attempt, 0,
                    rec_q.strapi_document_id, p_test_id
                );
            END IF;
            qa_rows := qa_rows + 1;
        END LOOP;
    END IF;

    /* строка user_test_answers */
    INSERT INTO user_test_answers(
        created_at, user_strapi_document_id,
        attempt, user_score,
        test_strapi_document_id, max_score
    )
    VALUES (
        now(), p_user_id,
        p_attempt, p_good_cnt,
        p_test_id, p_max_score
    );

    RETURN qa_rows;
END;
