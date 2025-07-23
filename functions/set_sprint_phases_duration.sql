DECLARE
    sprint_rec    RECORD;
    ev1           RECORD;
    ev2           RECORD;
    ev3           RECORD;
    ev4           RECORD;
    phase1_start  TIMESTAMPTZ;
    phase1_end    TIMESTAMPTZ;
    phase2_start  TIMESTAMPTZ;
    phase2_end    TIMESTAMPTZ;
    phase3_start  TIMESTAMPTZ;
    phase3_end    TIMESTAMPTZ;
    phase4_start  TIMESTAMPTZ;
    phase4_end    TIMESTAMPTZ;
    msg           TEXT := '';
BEGIN
    -- Найти спринт
    SELECT * INTO sprint_rec
      FROM sprints
     WHERE strapi_document_id = in_sprint_strapi_document_id;
    IF NOT FOUND THEN
        RETURN json_build_object(
          'result','error',
          'message', format('Спринт «%s» не найден.', in_sprint_strapi_document_id)
        );
    END IF;

    IF in_mode = 'NORMAL' THEN
        -- рассчитать фазы
        phase1_start := sprint_rec.date_start;
        phase1_end   := phase1_start + INTERVAL '48 hours';
        phase2_start := phase1_end;
        phase2_end   := phase2_start + INTERVAL '24 hours';
        phase3_start := phase2_end;
        phase3_end   := phase3_start + INTERVAL '48 hours';
        phase4_start := phase3_end;
        phase4_end   := phase4_start + INTERVAL '48 hours';

        -- обновить события
        UPDATE events
           SET datetime_start = phase1_start, datetime_end = phase1_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 1
         RETURNING * INTO ev1;
        UPDATE events
           SET datetime_start = phase2_start, datetime_end = phase2_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 2
         RETURNING * INTO ev2;
        UPDATE events
           SET datetime_start = phase3_start, datetime_end = phase3_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 3
         RETURNING * INTO ev3;
        UPDATE events
           SET datetime_start = phase4_start, datetime_end = phase4_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 4
         RETURNING * INTO ev4;

        msg := format(
          'Фаза 1: %s — %s; ' ||
          'Фаза 2: %s — %s; ' ||
          'Фаза 3: %s — %s; ' ||
          'Фаза 4: %s — %s.',
          phase1_start, phase1_end,
          phase2_start, phase2_end,
          phase3_start, phase3_end,
          phase4_start, phase4_end
        );

        RETURN json_build_object(
          'result','success',
          'message', 'NORMAL: ' || msg
        );

    ELSIF in_mode = 'CUSTOM' THEN
        -- фазы подряд, длина in_duration_minutes каждая
        phase1_start := sprint_rec.date_start;
        phase1_end   := phase1_start + make_interval(mins => in_duration_minutes);
        phase2_start := phase1_end;
        phase2_end   := phase2_start + make_interval(mins => in_duration_minutes);
        phase3_start := phase2_end;
        phase3_end   := phase3_start + make_interval(mins => in_duration_minutes);
        phase4_start := phase3_end;
        phase4_end   := phase4_start + make_interval(mins => in_duration_minutes);

        -- обновить события
        UPDATE events
           SET datetime_start = phase1_start, datetime_end = phase1_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 1
         RETURNING * INTO ev1;
        UPDATE events
           SET datetime_start = phase2_start, datetime_end = phase2_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 2
         RETURNING * INTO ev2;
        UPDATE events
           SET datetime_start = phase3_start, datetime_end = phase3_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 3
         RETURNING * INTO ev3;
        UPDATE events
           SET datetime_start = phase4_start, datetime_end = phase4_end
         WHERE sprint_strapi_document_id = in_sprint_strapi_document_id
           AND sprint_phase = 4
         RETURNING * INTO ev4;

        -- обновить конец спринта
        UPDATE sprints
           SET date_end = phase4_end
         WHERE strapi_document_id = in_sprint_strapi_document_id;

        msg := format(
          'CUSTOM: длительность каждой фазы %s мин.; ' ||
          'Фазы: 1) %s — %s; 2) %s — %s; 3) %s — %s; 4) %s — %s; ' ||
          'Новый конец спринта: %s.',
          in_duration_minutes,
          phase1_start, phase1_end,
          phase2_start, phase2_end,
          phase3_start, phase3_end,
          phase4_start, phase4_end,
          phase4_end
        );

        RETURN json_build_object(
          'result','success',
          'message', msg
        );

    ELSE
        RETURN json_build_object(
          'result','error',
          'message', 'Режим должен быть NORMAL или CUSTOM.'
        );
    END IF;

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
      'result','error',
      'message', SQLERRM
    );
END;
