declare
  rec        record;
  cols_text  text;
  rec_json   jsonb;
begin
  for rec in
    select table_name
    from information_schema.tables
    where table_schema = 'public'
      and table_type   = 'BASE TABLE'
    order by table_name
  loop
    -- Собираем список колонок с их типами
    select string_agg(column_name || ' ' || data_type, ', ' order by ordinal_position)
    into   cols_text
    from   information_schema.columns
    where  table_schema = 'public'
      and  table_name   = rec.table_name;

    -- Берём первый пример записи в JSONB
    execute format(
      'select to_jsonb(t) from public.%I t limit 1',
      rec.table_name
    )
    into rec_json;

    -- Заполняем выходные поля и возвращаем строку
    tbl_name          := rec.table_name;
    columns_and_types := cols_text;
    example_record    := rec_json;
    return next;
  end loop;
end;
