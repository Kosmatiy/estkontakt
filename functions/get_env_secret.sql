DECLARE
  v_key text;
BEGIN
  v_key := current_setting('myapp.secretkey', true);
  IF v_key IS NULL THEN
    RAISE EXCEPTION 'Secret not found in config!';
  END IF;
  RETURN v_key;
END;
