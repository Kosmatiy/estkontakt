DECLARE
    sprint_rec    RECORD;
    event_rec     RECORD;
    new_start     TIMESTAMPTZ;
    new_end       TIMESTAMPTZ;
    start_ok      BOOLEAN;
    end_ok        BOOLEAN;
    msg           TEXT;
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

    -- 2. Находим событие
    SELECT * INTO event_rec
      FROM events
     WHERE sprint_strapi_document_id = in_sprint_document_id
       AND sprint_phase = in_sprint_phase;
    IF NOT FOUND THEN
        RETURN json_build_object(
           'result', 'error',
           'message', format('Событие для спринта «%s», фазы %s не найдено.', in_sprint_document_id, in_sprint_phase)
        );
    END IF;

    -- 3. Вычисляем новые даты события
    IF in_start_or_end = 'START' THEN
        new_start := in_datetime;
        new_end   := event_rec.datetime_end;
    ELSIF in_start_or_end = 'END' THEN
        new_start := event_rec.datetime_start;
        new_end   := in_datetime;
    ELSIF in_start_or_end = 'BOTH' THEN
        new_start := in_datetime;
        new_end   := in_datetime + (in_duration || ' minutes')::INTERVAL;
    ELSE
        RETURN json_build_object(
           'result', 'error',
           'message', 'Параметр start_or_end должен быть одним из: START, END, BOTH.'
        );
    END IF;

    -- 4. Обновляем запись в events
    UPDATE events
       SET datetime_start = new_start,
           datetime_end   = new_end
     WHERE strapi_document_id = event_rec.strapi_document_id;

    -- 5. Проверяем, входят ли новые даты в границы спринта
    start_ok := (new_start >= sprint_rec.date_start AND new_start <= sprint_rec.date_end);
    end_ok   := (new_end   >= sprint_rec.date_start AND new_end   <= sprint_rec.date_end);

    -- 6. Формируем подробное сообщение на русском
    msg := format(
      'Событие «%s» (фаза %s) обновлено.%s Новое начало: %s.%s Новое окончание: %s.%s ' ||
      'Дата начала спринта: %s, дата окончания спринта: %s.%s',
      event_rec.strapi_document_id,
      in_sprint_phase,
      CASE WHEN in_start_or_end IN ('START','BOTH') THEN '' ELSE ' Поле начала не изменялось.' END,
      new_start,
      CASE WHEN start_ok THEN ' В пределах спринта.' ELSE ' ВНЕ границ спринта!' END,
      new_end,
      CASE WHEN end_ok THEN ' В пределах спринта.' ELSE ' ВНЕ границ спринта!' END,
      sprint_rec.date_start,
      sprint_rec.date_end,
      CASE WHEN in_start_or_end = 'BOTH'
           THEN format(' Продолжительность между началом и концом: %s минут.', in_duration)
           ELSE ''
      END
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
