BEGIN
    RETURN floor(random() * max_shift + 1)::int;
END;
