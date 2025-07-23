DECLARE
    /* ── спринт и stream ────────────────────────────────────── */
    v_sprint        sprints%ROWTYPE;
    v_stream_id     TEXT;

    /* ── счётчики ───────────────────────────────────────────── */
    pairs_total     INT;
    skip_pairs      INT;
    rows_inserted   INT := 0;

    /* ── лог пропущенных ───────────────────────────────────── */
    skipped_hashes  TEXT;

    /* ── пул видео-ссылок (укорочен для примера) ───────────── */
    video_urls TEXT[] := ARRAY[
     'https://drive.google.com/file/d/1CfifFK1EbUIRbj885fkkqVnj2G0QtIaQ/preview?usp=drivesdk',
'https://drive.google.com/file/d/13sqa88vZsSBc3C50n_vKH5Ty1F5XM0L_/preview?usp=drivesdk',
'https://drive.google.com/file/d/1nuHBREz0npZaw9mPUEYTQMp8F3XIGQIg/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Bs9tZ0YSazga1e79lNGQtKMsBRTXx2Uh/preview?usp=drivesdk',
'https://drive.google.com/file/d/1ILZy4szN1r4S-KS6F1ewZXzxGs4syamo/preview?usp=drivesdk',
'https://drive.google.com/file/d/1XYzhCqDcC4iOfMdxGo-fk5RdlZm21SUW/preview?usp=drivesdk',
'https://drive.google.com/file/d/1bo283mWIdYQn8W8bZDZVmnAAYPjA0lyN/preview?usp=drivesdk',
'https://drive.google.com/file/d/161-KZcm9qh14AQZxAn_TddzECevBmyi7/preview?usp=drivesdk',
'https://drive.google.com/file/d/1VDWhRIwmcpoTnszCldvCu6Dsln6cjKT0/preview?usp=drivesdk',
'https://drive.google.com/file/d/148zx2B0gJZ1YezE_6Xccb_j0o0JkuKhn/preview?usp=drivesdk',
'https://drive.google.com/file/d/1g00P0dcvtWmUGjRWj2YSAjV_kOY8CFPP/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Bkv2MJTNE1pCHAUWz0G98l3MeZIp0ZYm/preview?usp=drivesdk',
'https://drive.google.com/file/d/1disdWlOf0qg6pJVPsyIW8s2gAU7rEZSs/preview?usp=drivesdk',
'https://drive.google.com/file/d/1FiH6mrhbRn-Tle8JTF0LY6XzcYJZYIpj/preview?usp=drivesdk',
'https://drive.google.com/file/d/1PorBCyn0CbwDReIG8jP6Osi3feoiGwUI/preview?usp=drivesdk',
'https://drive.google.com/file/d/1qzltThohmEvTHYpp2HgR54IUCSFiEmUz/preview?usp=drivesdk',
'https://drive.google.com/file/d/1aCrdyJcPY5OclNI5itzSDDCtWKIfL9A0/preview?usp=drivesdk',
'https://drive.google.com/file/d/1KTUYXPFFgxESLrX7M84fDuj4zCqWdJ_u/preview?usp=drivesdk',
'https://drive.google.com/file/d/1JeyEtxro7AXytOl-a82cWeDXJEjYAv8c/preview?usp=drivesdk',
'https://drive.google.com/file/d/1WMINF5O-e8JjYNcuaUgIo79MNbi403mp/preview?usp=drivesdk',
'https://drive.google.com/file/d/1opzyPOXF2RtZV-1W6btcXfL80UsmSCD_/preview?usp=drivesdk',
'https://drive.google.com/file/d/1dgnakzalrgptiCK5yVd1TmtxXrdCocaS/preview?usp=drivesdk',
'https://drive.google.com/file/d/15MAQ4wBsW97jMgc7hBedjn1J6mWWgJNN/preview?usp=drivesdk',
'https://drive.google.com/file/d/1qqPBJ4F9oZmMbJJ3t6XN7UCHlm5v-mjy/preview?usp=drivesdk',
'https://drive.google.com/file/d/1_RrTIcXOOJ5eWYmMNmvEaQOuxbyOaT1W/preview?usp=drivesdk',
'https://drive.google.com/file/d/1laBFnTGM69-Vy7WFjTjNFErh6QfJx2AZ/preview?usp=drivesdk',
'https://drive.google.com/file/d/17SXNJ-9_eqyAL9tnWSobkFwT-fAOXAoN/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Yem1TLsUST-H5or6Vt0FoyVMgLs3Rryl/preview?usp=drivesdk',
'https://drive.google.com/file/d/13VkNjvpHbvgsoow8ivK2xQT-XEQFopmw/preview?usp=drivesdk',
'https://drive.google.com/file/d/1RTuAVOSi_f5sc4idrdMKTUv2HFCvmJPD/preview?usp=drivesdk',
'https://drive.google.com/file/d/13ctB6MclVE0PLRprF3RjWc9bOfZbb-Iw/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Lj7ROBpUJyJVx2NYKORsL2bxg7NoioOM/preview?usp=drivesdk',
'https://drive.google.com/file/d/1qgy9hT12dt_hxFdyokWuKRlOzx4Fqi7U/preview?usp=drivesdk',
'https://drive.google.com/file/d/1oH-E0ShF4KMJEeZLMM9jfD50BYl_jW6I/preview?usp=drivesdk',
'https://drive.google.com/file/d/1mcZHtYro3F5VmSOlz1jlE9VvS8lKZDIp/preview?usp=drivesdk',
'https://drive.google.com/file/d/1xvQzaYZAINx1NDwg5JppsAJJr5e5b9Yr/preview?usp=drivesdk',
'https://drive.google.com/file/d/15kO4glV-683jNGK_-VV-TABokBxv2zwX/preview?usp=drivesdk',
'https://drive.google.com/file/d/15_qz3U_lVxdsJ75TO1MIofOXplSRuE8C/preview?usp=drivesdk',
'https://drive.google.com/file/d/1KawvTUrXz6fZfK9bH07zfje5gHNFujcq/preview?usp=drivesdk',
'https://drive.google.com/file/d/1HPLf3LUNuaj969TCBFi_QufbKSa-pEuj/preview?usp=drivesdk',
'https://drive.google.com/file/d/1LHv_EODwc-ixHSETKFnawEM_hnXuYeOp/preview?usp=drivesdk',
'https://drive.google.com/file/d/1R7hvEg-RZ73h_GuMWL0ELZZSm0wbmcTv/preview?usp=drivesdk',
'https://drive.google.com/file/d/1UoLaSzM5sXkToTsfoKgen_sXB1ZpfO28/preview?usp=drivesdk',
'https://drive.google.com/file/d/1TW_ht8B3isDwXotvnYq6W84zDGwY26Mb/preview?usp=drivesdk',
'https://drive.google.com/file/d/1BD2xhVxscgekDjMiII2mUlkS_j_kj38n/preview?usp=drivesdk',
'https://drive.google.com/file/d/1npfqFwRsAeLyZR7MDLvjrtt4ES4Kqcnb/preview?usp=drivesdk',
'https://drive.google.com/file/d/1XyHcTfWVkLAiWFfwME7FEL_QyxzkXdrq/preview?usp=drivesdk',
'https://drive.google.com/file/d/1JRS4T1i6jOhndIQHZ_4VXBaAKmVf0OQY/preview?usp=drivesdk',
'https://drive.google.com/file/d/1yZ-GBtKBn25b1GMOOdRXhhTHuD0QH17B/preview?usp=drivesdk',
'https://drive.google.com/file/d/1c8vTJR74tCsXkf3_YN1oBl5QX-C_oXeY/preview?usp=drivesdk',
'https://drive.google.com/file/d/1xrz7hApktt-2v3X5AQ335XZWh8aaM-ZN/preview?usp=drivesdk',
'https://drive.google.com/file/d/1gDX8cnYR4P-KbZ8_wl80kHC4jh4c6IEQ/preview?usp=drivesdk',
'https://drive.google.com/file/d/1tjRJ7IMJUUmNnw46vJJZR7uf2lZrTLwY/preview?usp=drivesdk',
'https://drive.google.com/file/d/1L2J5vKKE3_fOwd3kmiOHd9UOSL1y5dbO/preview?usp=drivesdk',
'https://drive.google.com/file/d/1QjFK7AB3MCyby5XJb9MHQ_g2nXxwW6Xt/preview?usp=drivesdk',
'https://drive.google.com/file/d/11WaYal_IN1euJ5dxFL__rhRdDf9NL5s8/preview?usp=drivesdk',
'https://drive.google.com/file/d/1yVbusL2iOm0H-lN0xtKgs6YoM8VZTItc/preview?usp=drivesdk',
'https://drive.google.com/file/d/1hWYf8Ugp_2MFdK6IrIEn91_EM_I__dFJ/preview?usp=drivesdk',
'https://drive.google.com/file/d/1vBMEwxB-qGQvbEqqqCKmc8z8H9qNLG4t/preview?usp=drivesdk',
'https://drive.google.com/file/d/1iJmk7K0EnUdr0yxaa8ozESh39aZyTNZj/preview?usp=drivesdk',
'https://drive.google.com/file/d/1HfwZ7n16A1TOY8CriKVkLeegL3br6qiV/preview?usp=drivesdk',
'https://drive.google.com/file/d/1YyFPJPtu3zN2w-19hHvD4u1aEhX-9pwC/preview?usp=drivesdk',
'https://drive.google.com/file/d/1GK6z_hbLFslxGqy9n2VI0cwm9AOpAotz/preview?usp=drivesdk',
'https://drive.google.com/file/d/1PxPdm1o4qzFQ-Ea-qrc4hWBvzvvR7uva/preview?usp=drivesdk',
'https://drive.google.com/file/d/1uyqf_hrgg6tP6kgPWSuMVrR6hlSUO4cm/preview?usp=drivesdk',
'https://drive.google.com/file/d/1H_85JOu2EY9vi0bMysBbtqbMlM3Q9It6/preview?usp=drivesdk',
'https://drive.google.com/file/d/1-_uHUOJRIylOkMMgKzr-c6uMJgiVnILG/preview?usp=drivesdk',
'https://drive.google.com/file/d/12MU0uRq2gRg2x0hwLvoOvWBg_cL60Jy2/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Gg24eXR4Vj5xVOvoMwHaaXv4ruEsqrcS/preview?usp=drivesdk',
'https://drive.google.com/file/d/1LyIrx3e4NREEtxNY24zBGA8FRuhb4s6n/preview?usp=drivesdk',
'https://drive.google.com/file/d/1LGUQ6lPl7Zz7-8HE4FGJVHtHRDuQQRVp/preview?usp=drivesdk',
'https://drive.google.com/file/d/1fnFUF5HR7dQA7E5H2uiwBpnfmh8aKbCq/preview?usp=drivesdk',
'https://drive.google.com/file/d/1PWs05Nk29h1ycepzUaUnEaITTmz-XZNg/preview?usp=drivesdk',
'https://drive.google.com/file/d/1TIhdty5x67mpm1sarMTxyYCeu8X04YgF/preview?usp=drivesdk',
'https://drive.google.com/file/d/11OLOhjTNoaMA27APMsSVTx98N2yMhE6v/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Mm35orhF74aLynLF8wFRC21G4cy54GGa/preview?usp=drivesdk',
'https://drive.google.com/file/d/1davVaO5H69NUEoUzsBx_tER3OX96y_-6/preview?usp=drivesdk',
'https://drive.google.com/file/d/1YD4grtlTY6r2lkPbsQr-J5RyEpeoid5N/preview?usp=drivesdk',
'https://drive.google.com/file/d/1eL1NXZHc1qodgJmbsB_TjDlU5C4R4tzb/preview?usp=drivesdk',
'https://drive.google.com/file/d/1tk0k9D_l6juaH9za_uyzGCy9NGD0CZFQ/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Fmli2Zon9iL90zQ1O_mBU5vRPxRlhtD4/preview?usp=drivesdk',
'https://drive.google.com/file/d/1liXr8lttIZ6rMbptCe7HtD4XKQwqiz2s/preview?usp=drivesdk',
'https://drive.google.com/file/d/17yCrrcQ3hxWGq0J0N18i38gzJoYbbxKe/preview?usp=drivesdk',
'https://drive.google.com/file/d/1T908vKimLZNFln1EgZ1F8u7qXPQEnhea/preview?usp=drivesdk',
'https://drive.google.com/file/d/1nXiIKo41c0jKtYR42TAmd4qMomURZG8G/preview?usp=drivesdk',
'https://drive.google.com/file/d/1DJx8F0hzhC0yjahbAQn_4bgw-DYURjI8/preview?usp=drivesdk',
'https://drive.google.com/file/d/1QqhLehg8EfWLOSg9Mn7gpq6nhiIR9_sL/preview?usp=drivesdk',
'https://drive.google.com/file/d/1oTthbmKELsfXgbQqUCuoTiwYV7z1zTxn/preview?usp=drivesdk',
'https://drive.google.com/file/d/1ZzIHriCmO0GztwUHNInQL6iTd6h3WZcj/preview?usp=drivesdk',
'https://drive.google.com/file/d/1qRE9QUqFZeQZS2EUfP2DbFJkOJ-8inHL/preview?usp=drivesdk',
'https://drive.google.com/file/d/1EHCFqjTzXcSpcCvzUy_YJKFQeHBOp15X/preview?usp=drivesdk',
'https://drive.google.com/file/d/1sJeScS7q00HhAccW0JpfuWvTkeJvGkC_/preview?usp=drivesdk',
'https://drive.google.com/file/d/1v50bSW-98hKETZ_8EYKYhoWhyY1h4Dia/preview?usp=drivesdk',
'https://drive.google.com/file/d/1R2Ao1DUJM2BK6NR_BljoNSg8_gP6e7VY/preview?usp=drivesdk',
'https://drive.google.com/file/d/1uasGXcGF_EIOeyQrZ4MwZoQbd6vfs7iq/preview?usp=drivesdk',
'https://drive.google.com/file/d/1pkGLq2vW5w1oNfI8WTtiDYsANbp9x-ZB/preview?usp=drivesdk',
'https://drive.google.com/file/d/1lYpjMnZbPWVGfwaV8JT8usOJ68Gw6Wkg/preview?usp=drivesdk',
'https://drive.google.com/file/d/1mbMPbjdmVTln101o5x7SrspdpD9E5KZv/preview?usp=drivesdk',
'https://drive.google.com/file/d/1Js3T4FRMKQh0xvswFVtxVuFctIUZ0W2G/preview?usp=drivesdk',
'https://drive.google.com/file/d/1iCSAgmSkOl2H3DTg4-zj5PzYJcp7gUS4/preview?usp=drivesdk'
    ];
BEGIN
    /* 0. Проверяем спринт -------------------------------------------------- */
    SELECT * INTO v_sprint
      FROM sprints
     WHERE strapi_document_id = in_sprint_document_id;

    IF NOT FOUND THEN
        RETURN json_build_object(
            'result' ,'error',
            'message', format('Спринт %s не найден', in_sprint_document_id)
        );
    END IF;
    v_stream_id := v_sprint.stream_strapi_document_id;

    /* 1. Очистка ----------------------------------------------------------- */
    IF in_mode = 'CLEANSLATE' THEN
        DELETE FROM user_duel_answers uda
        USING duels d
        WHERE uda.duel_strapi_document_id = d.strapi_document_id
          AND d.sprint_strapi_document_id = v_sprint.strapi_document_id;
    END IF;

    /* 2. Собираем пары этого спринта -------------------------------------- */
    CREATE TEMP TABLE _pairs ON COMMIT DROP AS
    SELECT
        dd.hash,
        dd.duel_strapi_document_id     AS duel_id,
        MIN(dd.id)                     AS pair_id,
        MIN(dd.user_strapi_document_id) AS user_a,
        MAX(dd.user_strapi_document_id) AS user_b
    FROM   duel_distributions dd
    JOIN   duels d  ON d.strapi_document_id = dd.duel_strapi_document_id
    JOIN   users u  ON u.strapi_document_id = dd.user_strapi_document_id
    WHERE  d.sprint_strapi_document_id = v_sprint.strapi_document_id
      AND  u.stream_strapi_document_id = v_stream_id
    GROUP  BY dd.hash, dd.duel_strapi_document_id
    HAVING COUNT(*) = 2;                              -- строго две строки в паре

    SELECT COUNT(*) INTO pairs_total FROM _pairs;

    IF pairs_total = 0 THEN
        RETURN json_build_object(
            'result' ,'error',
            'message','Подходящих пар duel_distributions не найдено'
        );
    END IF;

    /* 3. Вычисляем, сколько пропустить ------------------------------------ */
    skip_pairs := CEIL(pairs_total * in_fail_percent / 100.0);

    CREATE TEMP TABLE _skip(hash TEXT PRIMARY KEY) ON COMMIT DROP;
    INSERT INTO _skip(hash)
    SELECT hash FROM _pairs ORDER BY random() LIMIT skip_pairs;

    SELECT string_agg(hash, ', ') INTO skipped_hashes FROM _skip;

    /* 4. Вставляем ответы для остальных пар ------------------------------- */
    INSERT INTO user_duel_answers(
        created_at,
        video_url,
        comment,
        user_strapi_document_id,
        rival_user_strapi_document_id,
        pair_id,
        duel_strapi_document_id,
        hash,
        answer_part,
        status,
        video_url_from_user
    )
    /* часть 1 – первый игрок */
    SELECT
        now(),
        video_urls[ CEIL(random()*array_length(video_urls,1)) ],
        'Test auto comment',
        p.user_a,
        p.user_b,
        p.pair_id,
        p.duel_id,
        p.hash,
        1,
        'ok',
        video_urls[ CEIL(random()*array_length(video_urls,1)) ]
    FROM _pairs p
    WHERE NOT EXISTS (SELECT 1 FROM _skip s WHERE s.hash = p.hash)

    UNION ALL

    /* часть 2 – второй игрок */
    SELECT
        now(),
        video_urls[ CEIL(random()*array_length(video_urls,1)) ],
        'Test auto comment',
        p.user_b,
        p.user_a,
        p.pair_id,
        p.duel_id,
        p.hash,
        2,
        'ok',
        video_urls[ CEIL(random()*array_length(video_urls,1)) ]
    FROM _pairs p
    WHERE NOT EXISTS (SELECT 1 FROM _skip s WHERE s.hash = p.hash)
    ON CONFLICT DO NOTHING;           -- если уникальный индекс уже существует

    GET DIAGNOSTICS rows_inserted = ROW_COUNT;

    /* 5. JSON-ответ ------------------------------------------------------- */
    RETURN json_build_object(
    'result'          , 'success',
    'generated_records', rows_inserted,
    'skipped_pairs'   , COALESCE(skipped_hashes,'нет'),
    'message'         , format(
        'Ответы на дуэли: спринт «%s». Пар всего %s, пропущено %s (%s%%), создано строк %s, режим %s.',
        v_sprint.sprint_name,
        pairs_total,
        skip_pairs,
        round(in_fail_percent)::INT,      -- ←  округлили заранее
        rows_inserted,
        in_mode
    )
);

EXCEPTION WHEN OTHERS THEN
    RETURN json_build_object(
        'result' ,'error',
        'message', SQLERRM
    );
END;
