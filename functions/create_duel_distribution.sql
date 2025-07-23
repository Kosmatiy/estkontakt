DECLARE
    v_error           TEXT := NULL;
    v_now             TIMESTAMPTZ := now();
    
    v_duel_rec        duels%ROWTYPE;
    v_sprint_rec      sprints%ROWTYPE;
    v_user_rec        users%ROWTYPE;
    v_opponent_rec    users%ROWTYPE;
    
    v_is_extra_user   BOOLEAN := FALSE;
    v_is_extra_opp    BOOLEAN := FALSE;
    v_is_forced_user  BOOLEAN := FALSE;
    v_is_forced_opp   BOOLEAN := FALSE;
    v_weight          NUMERIC;
    v_sprint_number   INT := 1;  -- Дефолт, как в оригинале
    
    v_count_A         INT;
    v_count_B         INT;
    v_existing_count  INT;
    
    v_repeats_ok_user BOOLEAN;
    v_repeats_ok_opp  BOOLEAN;
BEGIN
    PERFORM log_message(format('create_duel_distribution: start. duel=%s, user=%s, opponent=%s, sprint=%s, mode=%s',
                               p_duel_strapi_id, p_user_id, p_opponent_id, p_sprint_id, p_mode));

    /* 1. Валидация входных параметров */
    IF p_duel_strapi_id IS NULL OR trim(p_duel_strapi_id) = '' THEN v_error := 'ID дуэли не может быть пустым'; END IF;
    IF p_user_id IS NULL OR trim(p_user_id) = '' THEN v_error := coalesce(v_error || '; ', '') || 'ID пользователя не может быть пустым'; END IF;
    IF p_opponent_id IS NULL OR trim(p_opponent_id) = '' THEN v_error := coalesce(v_error || '; ', '') || 'ID противника не может быть пустым'; END IF;
    IF p_sprint_id IS NULL OR trim(p_sprint_id) = '' THEN v_error := coalesce(v_error || '; ', '') || 'ID спринта не может быть пустым'; END IF;
    IF p_user_id = p_opponent_id THEN v_error := coalesce(v_error || '; ', '') || 'Пользователь не может играть сам с собой'; END IF;
    IF p_weight_coef IS NULL OR p_weight_coef < 0 OR p_weight_coef > 1 THEN v_error := coalesce(v_error || '; ', '') || 'Коэффициент веса должен быть между 0 и 1'; END IF;

    /* 2. Проверяем дуэль */
    IF v_error IS NULL OR p_mode = 'TEST' THEN
        SELECT * INTO v_duel_rec FROM duels WHERE strapi_document_id = p_duel_strapi_id;
        IF NOT FOUND THEN v_error := coalesce(v_error || '; ', '') || format('дуэль %s не найдена', p_duel_strapi_id); END IF;
        IF v_duel_rec.sprint_strapi_document_id != p_sprint_id THEN v_error := coalesce(v_error || '; ', '') || 'дуэль не относится к указанному спринту'; END IF;
    END IF;

    /* 3. Проверяем спринт */
    IF v_error IS NULL OR p_mode = 'TEST' THEN
        SELECT * INTO v_sprint_rec FROM sprints WHERE strapi_document_id = p_sprint_id;
        IF NOT FOUND THEN v_error := coalesce(v_error || '; ', '') || format('спринт %s не найден', p_sprint_id); 
        ELSE v_sprint_number := COALESCE(v_sprint_rec.sprint_number, 1); END IF;
    END IF;

    /* 4. Проверяем пользователя */
    IF v_error IS NULL OR p_mode = 'TEST' THEN
        SELECT * INTO v_user_rec FROM users WHERE strapi_document_id = p_user_id;
        IF NOT FOUND THEN v_error := coalesce(v_error || '; ', '') || format('пользователь %s не найден', p_user_id); END IF;
        IF v_user_rec.dismissed_at IS NOT NULL THEN v_error := coalesce(v_error || '; ', '') || format('пользователь %s отчислен', p_user_id); END IF;
        IF v_user_rec.stream_strapi_document_id != v_sprint_rec.stream_strapi_document_id THEN v_error := coalesce(v_error || '; ', '') || 'пользователь не относится к потоку спринта'; END IF;
    END IF;

    /* 5. Проверяем противника */
    IF v_error IS NULL OR p_mode = 'TEST' THEN
        SELECT * INTO v_opponent_rec FROM users WHERE strapi_document_id = p_opponent_id;
        IF NOT FOUND THEN v_error := coalesce(v_error || '; ', '') || format('противник %s не найден', p_opponent_id); END IF;
        IF v_opponent_rec.dismissed_at IS NOT NULL THEN v_error := coalesce(v_error || '; ', '') || format('противник %s отчислен', p_opponent_id); END IF;
        IF v_opponent_rec.stream_strapi_document_id != v_sprint_rec.stream_strapi_document_id THEN v_error := coalesce(v_error || '; ', '') || 'противник не относится к потоку спринта'; END IF;
        IF v_user_rec.team_strapi_document_id = v_opponent_rec.team_strapi_document_id THEN v_error := coalesce(v_error || '; ', '') || 'пользователи из одной команды не могут играть друг против друга'; END IF;
    END IF;

    /* 6. Проверяем дублирование и штрафы */
    IF v_error IS NULL OR p_mode = 'TEST' THEN
        SELECT COUNT(*) INTO v_existing_count FROM duel_distributions dd 
        WHERE dd.duel_strapi_document_id = p_duel_strapi_id AND dd.is_failed = FALSE 
        AND ((dd.user_strapi_document_id = p_user_id AND dd.rival_strapi_document_id = p_opponent_id) 
             OR (dd.user_strapi_document_id = p_opponent_id AND dd.rival_strapi_document_id = p_user_id));
        IF v_existing_count > 0 THEN v_error := coalesce(v_error || '; ', '') || 'схватка между этими пользователями уже существует'; END IF;

        IF EXISTS (SELECT 1 FROM strikes s WHERE s.user_strapi_document_id = p_user_id AND s.sprint_strapi_document_id = p_sprint_id) THEN
            v_error := coalesce(v_error || '; ', '') || format('пользователь %s имеет штраф в этом спринте', p_user_id); END IF;
        IF EXISTS (SELECT 1 FROM strikes s WHERE s.user_strapi_document_id = p_opponent_id AND s.sprint_strapi_document_id = p_sprint_id) THEN
            v_error := coalesce(v_error || '; ', '') || format('противник %s имеет штраф в этом спринте', p_opponent_id); END IF;
    END IF;

    /* 7. Создаем распределение (если нет ошибок или в 'TEST') */
    IF (p_mode = 'TEST') OR (p_mode = 'REGULAR' AND v_error IS NULL) THEN
        -- Логика из оригинальной функции (без изменений)
        SELECT COUNT(*) INTO v_count_A FROM duel_distributions dd WHERE dd.duel_strapi_document_id = p_duel_strapi_id AND dd.is_failed = FALSE AND dd.user_strapi_document_id = p_user_id;
        v_is_extra_user := (v_count_A > 0);

        SELECT COUNT(*) INTO v_count_B FROM duel_distributions dd WHERE dd.duel_strapi_document_id = p_duel_strapi_id AND dd.is_failed = FALSE AND dd.user_strapi_document_id = p_opponent_id;
        v_is_extra_opp := (v_count_B > 0);

        SELECT COALESCE(uss.is_repeats_ok, FALSE) INTO v_repeats_ok_user FROM user_sprint_state uss WHERE uss.duel_strapi_document_id = p_duel_strapi_id AND uss.user_strapi_document_id = p_user_id LIMIT 1;
        SELECT COALESCE(uss.is_repeats_ok, FALSE) INTO v_repeats_ok_opp FROM user_sprint_state uss WHERE uss.duel_strapi_document_id = p_duel_strapi_id AND uss.user_strapi_document_id = p_opponent_id LIMIT 1;

        v_is_forced_user := CASE WHEN v_is_extra_user AND NOT v_repeats_ok_user THEN TRUE ELSE FALSE END;
        v_is_forced_opp := CASE WHEN v_is_extra_opp AND NOT v_repeats_ok_opp THEN TRUE ELSE FALSE END;

        v_weight := 1 - p_weight_coef * v_sprint_number;

        INSERT INTO duel_distributions(duel_strapi_document_id, user_strapi_document_id, rival_strapi_document_id, weight, is_extra, is_repeat, is_late, is_failed, is_forced, created_at)
        VALUES
            (p_duel_strapi_id, p_user_id, p_opponent_id, v_weight, v_is_extra_user, p_is_repeat, p_is_late, FALSE, v_is_forced_user, v_now),
            (p_duel_strapi_id, p_opponent_id, p_user_id, v_weight, v_is_extra_opp, p_is_repeat, p_is_late, FALSE, v_is_forced_opp, v_now);

        PERFORM set_is_chosen(p_duel_strapi_id, p_user_id, TRUE);
        PERFORM set_is_chosen(p_duel_strapi_id, p_opponent_id, TRUE);
        PERFORM recalc_user_weight(p_sprint_id, p_user_id);
        PERFORM recalc_user_weight(p_sprint_id, p_opponent_id);

        PERFORM log_message(format('... done inserting distribution, user=%s vs opp=%s ...', p_user_id, p_opponent_id));
    END IF;

    /* 8. Формируем ответ */
    IF v_error IS NULL THEN
        RETURN json_build_object('result', 'success', 'message', format('Схватка успешно создана между %s и %s (вес: %s)', p_user_id, p_opponent_id, v_weight));
    ELSIF p_mode = 'TEST' THEN
        RETURN json_build_object('result', 'success', 'message', 'схватка создана с предупреждениями: ' || v_error);
    ELSE
        RETURN json_build_object('result', 'error', 'message', v_error);
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        PERFORM log_message(format('create_duel_distribution: exception. SQLSTATE=%s, SQLERRM=%s', SQLSTATE, SQLERRM));
        IF p_mode = 'TEST' THEN
            RETURN json_build_object('result', 'success', 'message', 'схватка создана с техническими ошибками: ' || SQLERRM);
        ELSE
            RETURN json_build_object('result', 'error', 'message', 'Техническая ошибка: ' || SQLERRM);
        END IF;
END;
