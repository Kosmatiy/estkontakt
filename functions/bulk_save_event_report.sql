declare
    no_attend text[] := coalesce(
        array(select jsonb_array_elements_text(players_no_attend)), '{}');
    no_play   text[] := coalesce(
        array(select jsonb_array_elements_text(players_no_play)),   '{}');
    losers    text[] := coalesce(
        array(select jsonb_array_elements_text(teams_lost)),        '{}');
begin
    /* 1. флаги attended / played ---------------------------------------- */
    insert into expert_team_events_marks(
        expert_strapi_document_id,
        user_strapi_document_id,
        team_event_strapi_document_id,
        mark,
        attended,
        played)
    select  expert_id,
            uid,
            event_id,
            null,
            (uid <> all(no_attend))                                   as attended,
            (uid <> all(no_attend) and uid <> all(no_play))           as played
    from jsonb_array_elements_text(players_all) u(uid)
    on conflict on constraint ux_expert_user_event
    do update set
        attended = excluded.attended,
        played   = excluded.played;

    /* 2. результат команд (won / lost) ---------------------------------- */
    insert into expert_team_event_results(
        expert_strapi_document_id,
        team_strapi_document_id,
        team_event_strapi_document_id,
        won)
    select  expert_id,
            tid,
            event_id,
            (tid <> all(losers))                                       as won
    from jsonb_array_elements_text(teams_all) t(tid)
    on conflict on constraint ux_expert_team_event
    do update set
        won = excluded.won;

    /* 3. пересчёт mark (+20 / –5 / NULL) -------------------------------- */
    update expert_team_events_marks m
    set    mark = case
                     when m.attended and m.played then
                          case when r.won then 20 else -5 end
                     else
                          null
                  end
    from   users u
    join   expert_team_event_results r
           on  r.expert_strapi_document_id      = expert_id
           and r.team_event_strapi_document_id  = event_id
           and r.team_strapi_document_id        = u.team_strapi_document_id
    where  m.expert_strapi_document_id          = expert_id
      and  m.team_event_strapi_document_id      = event_id
      and  m.user_strapi_document_id            = u.strapi_document_id;

    /* -------- OK ------------------------------------------------------- */
    return json_build_object(
        'result' , 'success',
        'message', 'отзыв успешно записан'
    );

exception
    when others then
        return json_build_object(
            'result' , 'error',
            'message', sqlerrm
        );
end;
