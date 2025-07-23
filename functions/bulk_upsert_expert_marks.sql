begin
    insert into expert_team_events_marks(
        expert_strapi_document_id,
        user_strapi_document_id,
        team_event_strapi_document_id,
        mark,
        attended,
        played
    )
    select  expert_id,
            elem ->> 'user_id',
            event_id,
            nullif(elem->>'mark','')::int,
            (elem->>'attended')::boolean,
            (elem->>'played')::boolean
    from    jsonb_array_elements(payload) elem
    on conflict on constraint ux_expert_user_event
    do update set
        mark     = excluded.mark,
        attended = excluded.attended,
        played   = excluded.played;
end;
