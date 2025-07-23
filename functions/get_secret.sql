DECLARE
  v_value text;
BEGIN
  SELECT key_value
    INTO v_value
    FROM _db_private
   WHERE key_name = _key
   LIMIT 1;

  IF v_value IS NULL THEN
    RAISE EXCEPTION 'No secret found for key: %', _key;
  END IF;

  RETURN v_value;
END;
