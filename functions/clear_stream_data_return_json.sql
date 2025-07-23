DECLARE
    v_sprint       sprints%ROWTYPE;
    v_stream_id    TEXT;
    deleted_count  INT := 0;
BEGIN
    /* ── спринт и stream ─────────────────────────────────────── */
    SELECT * INTO v_sprint
    FROM   sprints
    WHERE  strapi_document_id = in_sprint_strapi_document_id;

    IF NOT FOUND THEN
        RAISE INFO 'JSON:%', json_build_object(
            'stage' , 'init',
            'result', 'error',
            'reason', 'SPRINT_NOT_FOUND',
            'sprint', in_sprint_strapi_document_id,
            'mode'  , in_mode);
        RETURN json_build_object(
            'result' ,'error',
            'message', format('Спринт %s не найден', in_sprint_strapi_document_id));
    END IF;

    v_stream_id := v_sprint.stream_strapi_document_id;

    /* ── режимы ------------------------------------------------ */
    -----------------------------------------------------------------
    IF in_mode = 'TESTS' THEN
        WITH del1 AS (
            DELETE FROM user_question_answers q
            USING tests t, lectures l, users u
            WHERE q.test_strapi_document_id      = t.strapi_document_id
              AND t.lecture_strapi_document_id   = l.strapi_document_id
              AND l.sprint_strapi_document_id    = in_sprint_strapi_document_id
              AND q.user_strapi_document_id      = u.strapi_document_id
              AND u.stream_strapi_document_id    = v_stream_id
            RETURNING 1 ),
        del2 AS (
            DELETE FROM user_test_answers a
            USING tests t, lectures l, users u
            WHERE a.test_strapi_document_id      = t.strapi_document_id
              AND t.lecture_strapi_document_id   = l.strapi_document_id
              AND l.sprint_strapi_document_id    = in_sprint_strapi_document_id
              AND a.user_strapi_document_id      = u.strapi_document_id
              AND u.stream_strapi_document_id    = v_stream_id
            RETURNING 1 )
        SELECT COUNT(*) FROM del1 UNION ALL SELECT COUNT(*) FROM del2
        INTO deleted_count;

    ELSIF in_mode = 'TASKS' THEN
        WITH del AS (
            DELETE FROM user_task_answers uta
            USING tasks t, lectures l, users u
            WHERE uta.task_strapi_document_id    = t.strapi_document_id
              AND t.lecture_strapi_document_id   = l.strapi_document_id
              AND l.sprint_strapi_document_id    = in_sprint_strapi_document_id
              AND uta.user_strapi_document_id    = u.strapi_document_id
              AND u.stream_strapi_document_id    = v_stream_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'TASK_REVIEWES_DISTRIBUTIONS_STUDENTS' THEN
        /*  удаляем назначения студентов на проверку задач
            (таблица user_task_to_review)                         */
        WITH del AS (
            DELETE FROM user_task_to_review utr
            USING tasks t, lectures l, users u
            WHERE utr.task_strapi_document_id   = t.strapi_document_id
              AND t.lecture_strapi_document_id  = l.strapi_document_id
              AND l.sprint_strapi_document_id   = in_sprint_strapi_document_id
              AND utr.reviewer_user_strapi_document_id = u.strapi_document_id
              AND u.stream_strapi_document_id   = v_stream_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'TASK_REVIEWES_DISTRIBUTIONS_EXPERTS' THEN
        /*  удаляем назначения экспертов на проверку задач
            (таблица expert_task_to_review)                       */
        WITH del AS (
            DELETE FROM expert_task_to_review etr
            USING tasks t, lectures l
            WHERE etr.task_strapi_document_id   = t.strapi_document_id
              AND t.lecture_strapi_document_id  = l.strapi_document_id
              AND l.sprint_strapi_document_id   = in_sprint_strapi_document_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'TASK_REVIEWES_STUDENTS' THEN
        WITH del AS (
            DELETE FROM user_task_reviewed ur
            WHERE EXISTS (
                SELECT 1
                FROM   tasks    t
                JOIN   lectures l ON l.strapi_document_id     = t.lecture_strapi_document_id
                JOIN   users    u ON u.strapi_document_id     = ur.reviewer_user_strapi_document_id
                WHERE  t.strapi_document_id        = ur.task_strapi_document_id
                  AND  l.sprint_strapi_document_id = in_sprint_strapi_document_id
                  AND  u.stream_strapi_document_id = v_stream_id)
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'TASK_REVIEWES_EXPERTS' THEN
        WITH del AS (
            DELETE FROM expert_task_reviewed er
            USING tasks t, lectures l
            WHERE er.task_strapi_document_id     = t.strapi_document_id
              AND t.lecture_strapi_document_id   = l.strapi_document_id
              AND l.sprint_strapi_document_id    = in_sprint_strapi_document_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'DUELS' THEN
        WITH del AS (
            DELETE FROM user_duel_answers uda
            USING duels d, users u
            WHERE uda.duel_strapi_document_id    = d.strapi_document_id
              AND d.sprint_strapi_document_id     = in_sprint_strapi_document_id
              AND uda.user_strapi_document_id     = u.strapi_document_id
              AND u.stream_strapi_document_id     = v_stream_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'DUEL_REVIEWES_DISTRIBUTIONS_STUDENTS' THEN
        /*  удаляем назначения студентов на проверку дуэлей
            (таблица user_duel_to_review)                         */
        WITH del AS (
            DELETE FROM user_duel_to_review utr
            USING duels d, users u
            WHERE utr.duel_strapi_document_id         = d.strapi_document_id
              AND d.sprint_strapi_document_id         = in_sprint_strapi_document_id
              AND utr.reviewer_user_strapi_document_id = u.strapi_document_id
              AND u.stream_strapi_document_id         = v_stream_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'DUEL_REVIEWES_DISTRIBUTIONS_EXPERTS' THEN
        /*  удаляем назначения экспертов на проверку дуэлей
            (таблица expert_duel_to_review)                       */
        WITH del AS (
            DELETE FROM expert_duel_to_review etr
            USING duels d
            WHERE etr.duel_strapi_document_id = d.strapi_document_id
              AND d.sprint_strapi_document_id = in_sprint_strapi_document_id
            RETURNING 1
        )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'DUEL_REVIEWES_STUDENTS' THEN
        WITH del AS (
            DELETE FROM user_duel_reviewed ur
            USING duels d, users u
            WHERE ur.duel_strapi_document_id      = d.strapi_document_id
              AND d.sprint_strapi_document_id     = in_sprint_strapi_document_id
              AND ur.reviewer_user_strapi_document_id = u.strapi_document_id
              AND u.stream_strapi_document_id     = v_stream_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'DUEL_REVIEWES_EXPERTS' THEN
        WITH del AS (
            DELETE FROM expert_duel_reviewed er
            USING duels d
            WHERE er.duel_strapi_document_id      = d.strapi_document_id
              AND d.sprint_strapi_document_id     = in_sprint_strapi_document_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'DUEL_DISTRIBUTIONS' THEN
        WITH del AS (
            DELETE FROM duel_distributions dd
            USING duels d
            WHERE dd.duel_strapi_document_id      = d.strapi_document_id
              AND d.sprint_strapi_document_id     = in_sprint_strapi_document_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'USER_SPRINT_STATE' THEN
        WITH del AS (
            DELETE FROM user_sprint_state uss
            USING duels d
            WHERE uss.duel_strapi_document_id     = d.strapi_document_id
              AND d.sprint_strapi_document_id     = in_sprint_strapi_document_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'USER_TEAM_EVENTS' THEN
        WITH del AS (
            DELETE FROM user_team_events ute
            USING team_events te
            WHERE ute.team_event_strapi_document_id = te.strapi_document_id
              AND te.sprint_strapi_document_id      = in_sprint_strapi_document_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'EXPERT_TEAM_EVENTS_MARKS' THEN
        WITH del AS (
            DELETE FROM expert_team_events_marks em
            USING team_events te
            WHERE em.team_event_strapi_document_id  = te.strapi_document_id
              AND te.sprint_strapi_document_id       = in_sprint_strapi_document_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSIF in_mode = 'USER_CAPTAIN_VOTES' THEN
        WITH del AS (
            DELETE FROM user_captain_vote
            WHERE stream_strapi_document_id = v_stream_id
            RETURNING 1 )
        SELECT COUNT(*) INTO deleted_count FROM del;

    ELSE
        RAISE INFO 'JSON:%', json_build_object(
            'stage' , 'error',
            'result', 'unknown_mode',
            'mode'  , in_mode);
        RETURN json_build_object(
            'result','error',
            'message', format('Unknown mode: %s', in_mode));
    END IF;

    /* ── финальный лог ----------------------------------------- */
    RAISE INFO 'JSON:%', json_build_object(
        'stage'          , 'finish',
        'result'         , 'success',
        'mode'           , in_mode,
        'deleted_records', deleted_count,
        'stream'         , v_stream_id);

    RETURN json_build_object(
        'result'         , 'success',
        'mode'           , in_mode,
        'deleted_records', deleted_count,
        'message'        , format('Удалено %s записей (stream %s)',
                                  deleted_count, v_stream_id));

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'JSON:%', json_build_object(
        'stage' , 'exception',
        'error' , SQLERRM,
        'mode'  , in_mode);
    RETURN json_build_object('result','error','message',SQLERRM);
END;
