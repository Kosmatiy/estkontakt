BEGIN
  -- Приводим значение в поле telegram_username к нижнему регистру
  NEW.telegram_username := lower(NEW.telegram_username);

  -- Если есть другие поля, которые тоже нужно приводить к нижнему регистру, добавьте их здесь
  -- Например:
  -- NEW.some_other_field := lower(NEW.some_other_field);

  RETURN NEW;
END;
