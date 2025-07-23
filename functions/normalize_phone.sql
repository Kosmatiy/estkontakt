DECLARE
  v_cleaned text;
BEGIN
  -- 1) Убираем все символы, кроме цифр
  v_cleaned := regexp_replace(_phone, '[^0-9]', '', 'g');
  
  -- 2) Если первая цифра '8', заменяем её на '7'
  IF length(v_cleaned) >= 1 AND left(v_cleaned,1) = '8' THEN
    v_cleaned := '7' || substring(v_cleaned FROM 2);
  END IF;

  -- 3) Проверяем длину и первый символ
  IF length(v_cleaned) != 11 OR left(v_cleaned,1) <> '7' THEN
    -- В зависимости от логики, можно выбросить ошибку или вернуть NULL
    RAISE EXCEPTION 'Неверный формат телефона: %', v_cleaned;
  END IF;

  RETURN v_cleaned;
END;
