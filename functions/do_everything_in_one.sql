DECLARE
    -- Шаг 1. Сбор ревьюеров для спринта
    all_reviewers      TEXT[];
    total_count        int;
    fail_count         int;
    good_set           TEXT[];
    fail_set           TEXT[];
    user_id            text;
    
    -- Шаг 3. Переменные для обработки назначений
    rec RECORD;
    
    -- Для случайных оценок для FULL‑CONTACT (одна запись получает набор оценок, другая – противоположный)
    coin int;
    
    -- Для случайных оценок для TRAINING
    r_FULL int;   -- для FULL‑CONTACT: result_mark
    i_FULL int;   -- для FULL‑CONTACT: image_mark
    a_FULL int;   -- для FULL‑CONTACT: attention_mark
    s1_TR int;    -- для TRAINING: skill1_mark
    s2_TR int;    -- для TRAINING: skill2_mark
    
    -- Переменные для группировки назначений
    cur_reviewer   text;
    cur_duel_id    text;
    cur_hash       text;
    cur_duel_type  text;
    owners         text[];  -- массив из двух answer_owner
    cnt_group      int;
    
    temp_count INTEGER;
    
    -- Шаг 4. Переменные для обработки пар из duel_distributions
    pair_rec RECORD;
    coin2       int;
    skill_val   int;
    cat         text;
    userA       text;
    userB       text;
    v_duel_id_inner   text;  -- идентификатор дуэли для обновлений
    duel_type_inner   text;
    
    -- Массивы для категорий обновлений (шаг 4)
    fc_categories TEXT[] := ARRAY['result_mark','image_mark','attention_mark'];
    t_categories  TEXT[] := ARRAY['skill1_mark','skill2_mark'];
    
    -- Переменная для логирования ошибок
    v_msg         TEXT;
BEGIN
    --------------------------------------------------------------------------
    -- Шаг 1. Собираем всех ревьюеров из user_duel_to_review для спринта
    --------------------------------------------------------------------------
    WITH rev AS (
      SELECT DISTINCT t.reviewer_user_strapi_document_id AS reviewer
      FROM user_duel_to_review t
      JOIN duels d ON d.strapi_document_id = t.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
    )
    SELECT array_agg(reviewer) INTO all_reviewers FROM rev;
    
    IF all_reviewers IS NULL OR array_length(all_reviewers,1) IS NULL THEN
      RAISE NOTICE 'Нет ревьюеров для спринта %', p_sprint_id;
      RETURN;
    END IF;
    
    total_count := array_length(all_reviewers,1);
    fail_count  := CEIL(total_count * p_fail_fraction)::int;
    
    RAISE NOTICE 'Всего ревьюеров=%, fail_fraction=%, fail_count=%', total_count, p_fail_fraction, fail_count;
    
    --------------------------------------------------------------------------
    -- Шаг 2. Перемешиваем всех ревьюеров, сохраняем результат в временной таблице _shuffled
    --------------------------------------------------------------------------
    DROP TABLE IF EXISTS _shuffled;
    CREATE TEMP TABLE _shuffled (sid text) ON COMMIT DROP;
    
    INSERT INTO _shuffled (sid)
    SELECT unnest(all_reviewers) ORDER BY random();
    
    -- Определяем fail_set и good_set
    SELECT array_agg(sid) INTO fail_set FROM _shuffled LIMIT fail_count;
    SELECT array_agg(sid) INTO good_set FROM _shuffled OFFSET fail_count;
    
    IF fail_set IS NULL THEN fail_set := ARRAY[]::text[]; END IF;
    IF good_set IS NULL THEN good_set := ARRAY[]::text[]; END IF;
    
    RAISE NOTICE 'fail_set=%', fail_set;
    RAISE NOTICE 'good_set=%', good_set;
    
    --------------------------------------------------------------------------
    -- Шаг 3. Формируем назначения для ревью для спринта.
    -- Из таблицы user_duel_to_review выбираем только последние записи для каждой комбинации 
    -- (reviewer_user_strapi_document_id, duel_strapi_document_id, hash, user_strapi_document_id)
    --------------------------------------------------------------------------
    CREATE TEMP TABLE _udtr_group AS
    WITH latest AS (
      SELECT t.*,
             row_number() OVER (
               PARTITION BY t.reviewer_user_strapi_document_id, 
                            t.duel_strapi_document_id, 
                            t.hash, 
                            t.user_strapi_document_id
               ORDER BY t.created_at DESC
             ) as rn
      FROM user_duel_to_review t
      JOIN duels d ON d.strapi_document_id = t.duel_strapi_document_id
      WHERE d.sprint_strapi_document_id = p_sprint_id
    )
    SELECT 
      id AS toreview_id,
      reviewer_user_strapi_document_id AS reviewer,
      user_strapi_document_id AS answer_owner,
      duel_strapi_document_id AS duel_id,
      hash,
      d.type AS duel_type
    FROM latest l
    JOIN duels d ON d.strapi_document_id = l.duel_strapi_document_id
    WHERE l.rn = 1;
    
    SELECT count(*) INTO temp_count FROM _udtr_group;
    RAISE NOTICE 'Всего назначений (последних записей)=%', temp_count;
    
    -- Исключаем назначения, где reviewer входит в fail_set (не ставят оценки)
    CREATE TEMP TABLE _final_todo AS
      SELECT * FROM _udtr_group
      WHERE reviewer <> ANY(fail_set);
    
    SELECT count(*) INTO temp_count FROM _final_todo;
    RAISE NOTICE 'После исключения fail_set осталось назначений=%', temp_count;
    
    -- Группируем записи по (reviewer, duel_id, hash, duel_type) – ожидается 2 записи (для двух участников дуэли)
    FOR rec IN
      SELECT ft.reviewer,
             ft.duel_id,
             ft.hash,
             ft.duel_type,
             array_agg(ft.answer_owner ORDER BY ft.answer_owner) AS owners,
             count(*) AS cnt
      FROM _final_todo ft
      GROUP BY ft.reviewer, ft.duel_id, ft.hash, ft.duel_type
    LOOP
      IF rec.cnt <> 2 THEN
        RAISE NOTICE 'Группа (reviewer=%, duel_id=%, hash=%) имеет % записей (ожидается 2) – пропускаем.', rec.reviewer, rec.duel_id, rec.hash, rec.cnt;
        CONTINUE;
      END IF;
      
      cur_reviewer := rec.reviewer;
      cur_duel_id  := rec.duel_id;
      cur_hash     := rec.hash;
      cur_duel_type:= rec.duel_type;
      owners       := rec.owners;
      
      IF cur_duel_type = 'FULL‑CONTACT' THEN
        -- Для FULL‑CONTACT генерируем монетку; если coin = 0, первая запись получает (1,1,1), вторая – (0,0,0); иначе наоборот.
        coin := floor(random()*2)::int;
        r_FULL := coin;
        i_FULL := coin;
        a_FULL := coin;
    
        EXECUTE format($qfc$
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
            skill2_mark
          )
          VALUES
          (now(), %L, %L, %L, %L, true, %L, %s, %s, %s, null, null),
          (now(), %L, %L, %L, %L, true, %L, %s, %s, %s, null, null)
        $qfc$,
          cur_reviewer, cur_duel_id, owners[1],
            'Auto comment: ' || cur_reviewer || ' -> ' || owners[1],
          cur_hash, r_FULL::text, i_FULL::text, a_FULL::text,
          cur_reviewer, cur_duel_id, owners[2],
            'Auto comment: ' || cur_reviewer || ' -> ' || owners[2],
          cur_hash, (1 - r_FULL)::text, (1 - i_FULL)::text, (1 - a_FULL)::text
        );
      
      ELSIF cur_duel_type = 'TRAINING' THEN
        s1_TR := floor(random()*3)::int;
        s2_TR := floor(random()*3)::int;
        
        EXECUTE format($qtr$
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
            skill2_mark
          )
          VALUES
          (now(), %L, %L, %L, %L, true, %L, null, null, null, %s, %s),
          (now(), %L, %L, %L, %L, true, %L, null, null, null, %s, %s)
        $qtr$,
          cur_reviewer, cur_duel_id, owners[1],
            'Auto comment: ' || cur_reviewer || ' -> ' || owners[1],
          cur_hash, s1_TR::text, s2_TR::text,
          cur_reviewer, cur_duel_id, owners[2],
            'Auto comment: ' || cur_reviewer || ' -> ' || owners[2],
          cur_hash, (floor(random()*3)::int)::text, (floor(random()*3)::int)::text
        );
      
      ELSE
        RAISE NOTICE 'Неизвестный тип дуэли: %', cur_duel_type;
      END IF;
    END LOOP;
    
    RAISE NOTICE 'Шаг3: Обработано групп назначений (вставлено оценок) из _final_todo.';
    
    --------------------------------------------------------------------------
    -- Шаг 4. Обрабатываем пары из duel_distributions (где ровно 2 записи по hash)
    -- Обновляем оценки в user_duel_reviewed по категориям
    --------------------------------------------------------------------------
    FOR pair_rec IN (
      WITH pairs AS (
        SELECT
          dd.hash,
          MIN(dd.duel_strapi_document_id) AS duel_id,
          ARRAY_AGG(dd.user_strapi_document_id) AS users,
          COUNT(*) AS cnt
        FROM duel_distributions dd
        GROUP BY dd.hash
        HAVING COUNT(*) = 2
      )
      SELECT
        p.hash,
        p.users[1] AS userA,
        p.users[2] AS userB,
        p.duel_id,
        d.type AS duel_type
      FROM pairs p
      JOIN duels d ON d.strapi_document_id = p.duel_id
    ) LOOP
      userA := pair_rec.userA;
      userB := pair_rec.userB;
      v_duel_id_inner := pair_rec.duel_id;
      duel_type_inner := pair_rec.duel_type;
      
      IF duel_type_inner = 'FULL‑CONTACT' THEN
        FOREACH cat IN ARRAY fc_categories LOOP
          coin2 := floor(random()*2)::int;
          IF coin2 = 0 THEN
            EXECUTE format(
              'UPDATE user_duel_reviewed SET %I = 1 WHERE user_strapi_document_id = %L AND duel_strapi_document_id = %L',
              cat, userA, v_duel_id_inner
            );
            EXECUTE format(
              'UPDATE user_duel_reviewed SET %I = 0 WHERE user_strapi_document_id = %L AND duel_strapi_document_id = %L',
              cat, userB, v_duel_id_inner
            );
          ELSE
            EXECUTE format(
              'UPDATE user_duel_reviewed SET %I = 0 WHERE user_strapi_document_id = %L AND duel_strapi_document_id = %L',
              cat, userA, v_duel_id_inner
            );
            EXECUTE format(
              'UPDATE user_duel_reviewed SET %I = 1 WHERE user_strapi_document_id = %L AND duel_strapi_document_id = %L',
              cat, userB, v_duel_id_inner
            );
          END IF;
        END LOOP;
    
        EXECUTE format(
          'UPDATE user_duel_reviewed SET skill1_mark = NULL, skill2_mark = NULL WHERE duel_strapi_document_id = %L AND user_strapi_document_id IN (%L, %L)',
          v_duel_id_inner, userA, userB
        );
      
      ELSIF duel_type_inner = 'TRAINING' THEN
        FOREACH cat IN ARRAY t_categories LOOP
          skill_val := floor(random()*3)::int;
          EXECUTE format(
            'UPDATE user_duel_reviewed SET %I = %s WHERE user_strapi_document_id = %L AND duel_strapi_document_id = %L',
            cat, skill_val::text, userA, v_duel_id_inner
          );
          skill_val := floor(random()*3)::int;
          EXECUTE format(
            'UPDATE user_duel_reviewed SET %I = %s WHERE user_strapi_document_id = %L AND duel_strapi_document_id = %L',
            cat, skill_val::text, userB, v_duel_id_inner
          );
        END LOOP;
    
        EXECUTE format(
          'UPDATE user_duel_reviewed SET result_mark = NULL, image_mark = NULL, attention_mark = NULL WHERE duel_strapi_document_id = %L AND user_strapi_document_id IN (%L, %L)',
          v_duel_id_inner, userA, userB
        );
      END IF;
    END LOOP;
    
    RAISE NOTICE 'Шаг4: Распределение оценок (FULL‑CONTACT / TRAINING) завершено.';
    
    --------------------------------------------------------------------------
    -- Шаг 5. Обновляем поля skill1_strapi_document_id и skill2_strapi_document_id для TRAINING
    --------------------------------------------------------------------------
    UPDATE user_duel_reviewed AS udr
    SET
      skill1_strapi_document_id = 'x32dfbgykyu92o6wb92nm50i',
      skill2_strapi_document_id = 'zk0vk04tbl4n7pe9s0w1py78'
    FROM duels d
    WHERE udr.duel_strapi_document_id = d.strapi_document_id
      AND d.type = 'TRAINING';
    
    RAISE NOTICE 'Шаг5: Обновление skill1_strapi_document_id и skill2_strapi_document_id для TRAINING завершено.';
    
    RAISE NOTICE 'Функция do_everything_in_one выполнена для спринта %', p_sprint_id;
    
EXCEPTION WHEN OTHERS THEN
    v_msg := format('Error in do_everything_in_one for sprint=%s: %s', p_sprint_id, SQLERRM);
    INSERT INTO distribution_logs(log_message) VALUES(v_msg);
    RAISE;
END;
