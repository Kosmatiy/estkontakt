DECLARE
  rec RECORD;
BEGIN
  -- Шапка лога
  PERFORM log_message(format(
    '=== analyze_user_task_review_assignments started for sprint=%s ===',
    p_sprint_strapi_document_id
  ));

  /*
    Логика:
    1) Соединяем user_task_to_review -> tasks -> lectures, чтобы убедиться,
       что task принадлежит нужному спринту (sprint_strapi_document_id).
    2) Сгруппируем по (reviewer_user_strapi_document_id, task_strapi_document_id).
    3) Считаем COUNT(*), сколько проверок назначено.
    4) Для каждой группы выводим запись в логи.
  */
  FOR rec IN
    SELECT
      r.reviewer_user_strapi_document_id AS reviewer,
      r.task_strapi_document_id          AS task,
      COUNT(*)                           AS total_checks
    FROM user_task_to_review r
      JOIN tasks t
        ON t.strapi_document_id = r.task_strapi_document_id
      JOIN lectures l
        ON l.strapi_document_id = t.lecture_strapi_document_id
    WHERE l.sprint_strapi_document_id = p_sprint_strapi_document_id
    GROUP BY
      r.reviewer_user_strapi_document_id,
      r.task_strapi_document_id
    ORDER BY
      r.reviewer_user_strapi_document_id,
      r.task_strapi_document_id
  LOOP
    -- Выводим в distribution_logs
    PERFORM log_message(format(
      'Reviewer=%s, Task=%s, AssignedChecks=%s',
      rec.reviewer,
      rec.task,
      rec.total_checks
    ));
  END LOOP;

  -- Итоговое сообщение
  PERFORM log_message(format(
    '=== analyze_user_task_review_assignments DONE for sprint=%s ===',
    p_sprint_strapi_document_id
  ));
END;
