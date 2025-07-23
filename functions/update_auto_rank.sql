begin
    -- Если запись удалена, пересчитываем ранги для удалённого stream_id
    if (tg_op = 'DELETE') then
        -- Удаляем старые ранги для удалённого stream_id
        delete from _auto_rank where stream_id = old.stream_id;

        -- Пересчитываем ранги для оставшихся пользователей
        insert into _auto_rank (user_id, stream_id, rank)
        select
            user_id,
            stream_id,
            dense_rank() over (partition by stream_id order by score desc) as rank
        from
            _auto_score
        where
            stream_id = old.stream_id;

    -- Если запись добавлена или обновлена, пересчитываем ранги
    elsif (tg_op = 'INSERT' or tg_op = 'UPDATE') then
        -- Удаляем старые ранги для текущего stream_id
        delete from _auto_rank where stream_id = new.stream_id;

        -- Вставляем новые ранги для текущего stream_id
        insert into _auto_rank (user_id, stream_id, rank)
        select
            user_id,
            stream_id,
            dense_rank() over (partition by stream_id order by score desc) as rank
        from
            _auto_score
        where
            stream_id = new.stream_id;
    end if;

    return null;
end;
