DECLARE
  v_cleaned text;
BEGIN
  IF _username IS NULL OR length(trim(_username)) = 0 THEN
    -- Если пустой ввод, возвращаем NULL или, по желанию, выбрасываем ошибку
    RETURN NULL;
  END IF;

  -- 1) Приводим к нижнему регистру
  v_cleaned := lower(_username);

  -- 2) Удаляем все ведущие символы '@' (если их несколько)
  v_cleaned := regexp_replace(v_cleaned, '^@+', '');

  -- 3) Добавляем ровно одну @ в начало
  v_cleaned := '@' || v_cleaned;

  RETURN v_cleaned;
END;
