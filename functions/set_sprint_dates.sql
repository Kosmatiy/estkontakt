DECLARE
    sprint_rec     RECORD;
    new_start      TIMESTAMPTZ;
    new_end        TIMESTAMPTZ;
    ok_period      BOOLEAN;
    duration_days  INTEGER;
    msg            TEXT;
BEGIN
    -- 1. Находим спринт
    SELECT * INTO sprint_rec
      FROM sprints
     WHERE strapi_document_id = in_sprint_document_id;
    IF NOT FOUND THEN
        RETURN json_build_object(
           'result', 'error',
           'message', format('Спринт с id «%s» не найден.', in_sprint_document_id)
        );
    END IF;

    -- 2. Определяем новые границы спринта
    IF in_start_or_end = 'START' THEN
        new_start := in_datetime;
        new_end   := sprint_rec.date_end;
    ELSIF in_start_or_end = 'END' THEN
        new_start := sprint_rec.date_start;
        new_end   := in_datetime;
    ELSIF in_start_or_end = 'BOTH' THEN
        new_start := in_datetime;
        new_end   := in_datetime + make_interval(days => in_duration_days);
    ELSE
        RETURN json_build_object(
           'result', 'error',
           'message', 'Параметр start_or_end должен быть одним из: START, END или BOTH.'
        );
    END IF;

    -- 3. Проверяем, что начало раньше конца
    IF new_start >= new_end THEN
        RETURN json_build_object(
           'result', 'error',
           'message', format(
             'Невозможно установить: новое начало (%s) не раньше конца (%s).',
             new_start, new_end
           )
        );
    END IF;

    -- 4. Обновляем спринт
    UPDATE sprints
       SET date_start = new_start,
           date_end   = new_end
     WHERE strapi_document_id = in_sprint_document_id;

    -- 5. Вычисляем фактическую продолжительность в днях
    duration_days := FLOOR(EXTRACT(EPOCH FROM new_end - new_start) / 86400)::INT;

    -- 6. Формируем подробное сообщение на русском
    msg := format(
      'Спринт «%s» успешно обновлён.' ||
      ' Новое начало: %s.' ||
      ' Новое окончание: %s.' ||
      ' Продолжительность: %s %s.',
      in_sprint_document_id,
      new_start,
      new_end,
      duration_days,
      CASE WHEN duration_days = 1 THEN 'день' ELSE 'дней' END
    );

    RETURN json_build_object(
      'result', 'success',
      'message', msg
    );

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
      'result', 'error',
      'message', SQLERRM
    );
END;
