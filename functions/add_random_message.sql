DECLARE
    random_message TEXT;
    random_messages TEXT[] := ARRAY[
        'Hello, world!',
        'This is a random message.',
        'SQL is fun!',
        'Have a great day!',
        'Keep learning and growing!'
    ];
BEGIN
    -- Выбираем случайное сообщение из массива
    random_message := random_messages[floor(random() * array_length(random_messages, 1) + 1)::int];

    -- Добавляем новую запись в таблицу
    INSERT INTO admin_messages (message_text)
    VALUES (random_message);
END;
