declare
    v_log         text[] := array[]::text[];
    v_rows        int;
    v_bad_pairs   jsonb;
    v_over_quota  jsonb;
begin
/*────────────────── 1. очистка (режим CLEANSLATE) ──────────────────────*/
    if upper(p_mode) = 'CLEANSLATE' then
        delete from user_duel_to_review t
        using duels d
        where d.sprint_strapi_document_id = p_sprint_uuid
          and t.duel_strapi_document_id   = d.strapi_document_id;
        get diagnostics v_rows = row_count;
        v_log := v_log || format('cleanup:%s',v_rows);
    end if;

/*────────────────── 2. входные данные спринта ──────────────────────────*/
    /* 2-A. дуэли */
    create temp table tmp_duels on commit drop as
    select strapi_document_id duel_id, type
    from   duels
    where  sprint_strapi_document_id = p_sprint_uuid;

    /* 2-B. допущенные пользователи (без dismissed / strikes) */
    create temp table tmp_users on commit drop as
    select u.strapi_document_id user_id,
           u.team_strapi_document_id team_id
    from   users u
    where  u.stream_strapi_document_id = (
              select stream_strapi_document_id
              from   sprints where strapi_document_id = p_sprint_uuid )
      and  u.dismissed_at is null
      and  not exists ( select 1
                        from   strikes s
                        where  s.user_strapi_document_id   = u.strapi_document_id
                          and  s.sprint_strapi_document_id = p_sprint_uuid );

    /* 2-C. последняя запись каждого игрока в паре */
    create temp table tmp_answers_raw on commit drop as
    with ranked as (
        select a.*,
               row_number() over (
                   partition by a.pair_id, a.user_strapi_document_id
                   order by     a.created_at desc) rn
        from   user_duel_answers a
        join   tmp_duels d  on d.duel_id = a.duel_strapi_document_id
        join   tmp_users u  on u.user_id = a.user_strapi_document_id)
    select pair_id,
           user_strapi_document_id       as user_id,
           rival_user_strapi_document_id as rival_id,
           duel_strapi_document_id       as duel_id,
           hash
    from   ranked
    where  rn = 1;

    /* 2-D. пары (по одной строке) */
    create temp table tmp_pairs on commit drop as
    select pair_id,
           min(hash)    as hash,     -- представитель пары (детерминизм)
           duel_id,
           min(user_id) as player_a,
           max(user_id) as player_b
    from   tmp_answers_raw
    group  by pair_id, duel_id;

/*────────────────── 3. квоты (3 × answers_cnt) ─────────────────────────*/
    create temp table tmp_quota on commit drop as
    select u.user_id,
           coalesce(a.cnt,0)*3 quota,
           coalesce(a.cnt,0)*3 remain
    from   tmp_users u
    left   join (select user_id,count(*) cnt
                 from tmp_answers_raw group by user_id) a using(user_id);

    delete from tmp_quota where quota = 0;

/*────────────────── 4. подготовка round-robin колец ────────────────────*/
    /* 4-A. шесть слотов на каждого рецензента */
    create temp table tmp_reviewers on commit drop as
    select user_id
    from   tmp_quota
    cross  join generate_series(1,6);

    /* 4-B. «кольцо» слотов r_slots */
    create temp table r_slots on commit drop as
    select row_number() over () - 1 as rn_slot,
           user_id                   as reviewer_id
    from   tmp_reviewers;

    /* 4-C. «кольцо» пар p_slots : 6 позиций на пару */
    create temp table p_slots on commit drop as
    select row_number() over () - 1 as rn_pair,
           pair_id, hash, duel_id, player_a, player_b
    from   tmp_pairs
    cross  join generate_series(1,6);

/*────────────────── 5. round-robin + доукомплектовка ───────────────────*/
    /* 5-A. сдвиг (TRAINING) */
    create temp table tmp_offsets on commit drop as
    with const as (select count(*) reviewers_total from r_slots)
    select p.hash,
           p.duel_id,
           case when d.type='TRAINING'
                then (abs(hashtext(p.hash))::bigint/100)
                     % (select reviewers_total from const)
                else 0
           end as rr_shift
    from   tmp_pairs p
    join   tmp_duels d using(duel_id);

    /* 5-B. первичный round-robin */
    create temp table tmp_assignments on commit drop as
    with tot as (select count(*) n from p_slots)
    select r.reviewer_id, p.hash, p.duel_id
    from   r_slots r
    join   p_slots p
      on ( r.rn_slot +
           (select rr_shift from tmp_offsets
            where hash=p.hash and duel_id=p.duel_id) )
         % (select n from tot) = p.rn_pair
    where  r.reviewer_id not in (p.player_a,p.player_b);

    /* 5-C. вычитаем использованную квоту */
    update tmp_quota q
    set    remain = remain - a.used
    from  (select reviewer_id,count(*) used
           from   tmp_assignments
           group  by reviewer_id) a
    where q.user_id = a.reviewer_id;

    /* 5-D. пары с нехваткой ревьюеров */
    create temp table need_pairs on commit drop as
    select hash, duel_id,
           6 - count(*) as need_left
    from   tmp_assignments
    group  by hash,duel_id
    having count(*) < 6;

    /* 5-E. цикл доукомплектовки */
    while exists (select 1 from need_pairs where need_left > 0) loop
        with cand as (
            select n.hash,
                   n.duel_id,
                   n.need_left,                        -- ← нужно
                   q.user_id          as reviewer_id,
                   row_number() over (partition by n.hash,n.duel_id
                                      order by q.remain desc,q.user_id) rn
            from   need_pairs n
            join   tmp_pairs  p using(hash,duel_id)
            join   tmp_quota  q on q.remain > 0
            where  q.user_id not in (p.player_a,p.player_b)
              and  not exists (select 1
                               from tmp_assignments a
                               where a.hash       = n.hash
                                 and a.duel_id    = n.duel_id
                                 and a.reviewer_id = q.user_id))
        insert into tmp_assignments(reviewer_id,hash,duel_id)
        select reviewer_id,hash,duel_id
        from   cand
        where  rn <= need_left                   -- ← теперь поле видно
        on conflict do nothing;

        /* пересчёт остатков */
        update tmp_quota q
        set    remain = quota - a.used
        from  (select reviewer_id,count(*) used
               from   tmp_assignments
               group  by reviewer_id) a
        where q.user_id = a.reviewer_id;

        /* пересчёт дыр */
        update need_pairs np
        set    need_left = 6 - a.cnt
        from  (select hash,duel_id,count(*) cnt
               from   tmp_assignments
               group  by hash,duel_id) a
        where np.hash=a.hash and np.duel_id=a.duel_id;
    end loop;

/*────────────────── 6. вставка строк в user_duel_to_review ─────────────*/
    insert into user_duel_to_review(
        reviewer_user_strapi_document_id,
        duel_strapi_document_id,
        user_strapi_document_id,
        hash)
    select a.reviewer_id,
           a.duel_id,
           r.user_id,
           a.hash
    from   tmp_assignments a
    join   tmp_answers_raw r using(hash,duel_id)
    on conflict do nothing;

    get diagnostics v_rows = row_count;
    v_log := v_log || format('persist:%s',v_rows);

/*────────────────── 7. расширенная валидация + лог ─────────────────────*/
    /* 7-A. пары с недобором */
    with bad as (
        select p.hash,p.duel_id,count(u.hash) c
        from   tmp_pairs p
        left   join user_duel_to_review u
               on u.hash=p.hash and u.duel_strapi_document_id=p.duel_id
        group  by p.hash,p.duel_id
        having count(u.hash) <> 12)
    select jsonb_agg(to_jsonb(bad) order by bad.hash,bad.duel_id)
    into   v_bad_pairs
    from   bad limit 20;

    if v_bad_pairs is not null then
        v_log := v_log || format('bad_pairs(sample≤20):%s',v_bad_pairs);
        raise exception 'Validation: pair without 12 reviews';
    end if;

    /* 7-B. превышение квоты */
    with over as (
        select r.reviewer_user_strapi_document_id reviewer_id,
               count(*) used,
               q.quota
        from   user_duel_to_review r
        join   tmp_quota q on q.user_id=r.reviewer_user_strapi_document_id
        where  r.duel_strapi_document_id in (select duel_id from tmp_duels)
        group  by r.reviewer_user_strapi_document_id,q.quota
        having count(*) > q.quota)
    select jsonb_agg(to_jsonb(over) order by over.reviewer_id)
    into   v_over_quota
    from   over limit 20;

    if v_over_quota is not null then
        v_log := v_log || format('over_quota(sample≤20):%s',v_over_quota);
        raise exception 'Validation: quota exceeded';
    end if;

    v_log := v_log || 'validate:OK';

/*────────────────── 8. итоговый JSON ───────────────────────────────────*/
    return jsonb_build_object(
             'run_id', gen_random_uuid(),
             'status','OK',
             'steps', v_log);
end;
