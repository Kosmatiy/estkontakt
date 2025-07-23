DECLARE
    duel_record RECORD;
    player_mapping JSONB;
    duel_mapping JSONB;
BEGIN
    -- Очищаем таблицу для данного спринта
    DELETE FROM user_duel_to_review udr
    WHERE udr.duel_strapi_document_id IN (
        SELECT d.strapi_document_id 
        FROM duels d
        WHERE d.sprint_strapi_document_id = input_sprint_strapi_document_id
    );

    -- Для каждого дуэля в спринте
    FOR duel_record IN 
        SELECT d.strapi_document_id as duel_id
        FROM duels d
        WHERE d.sprint_strapi_document_id = input_sprint_strapi_document_id
    LOOP
        -- Получаем нумерацию игроков и схваток для данного дуэля
        WITH player_duel_hashes AS (
            SELECT
                combined.duel_strapi_document_id,
                combined.player_id,
                COUNT(DISTINCT combined.hash) AS distinct_hash_count,
                ARRAY_AGG(DISTINCT combined.hash) AS player_hashes
            FROM (
                SELECT uda.duel_strapi_document_id, uda.user_strapi_document_id AS player_id, uda.hash
                FROM user_duel_answers uda
                WHERE uda.duel_strapi_document_id = duel_record.duel_id
                UNION ALL
                SELECT uda.duel_strapi_document_id, uda.rival_user_strapi_document_id AS player_id, uda.hash
                FROM user_duel_answers uda
                WHERE uda.duel_strapi_document_id = duel_record.duel_id
            ) AS combined
            GROUP BY combined.duel_strapi_document_id, combined.player_id
        ),
        two_match_players AS (
            SELECT
                pdh.duel_strapi_document_id,
                pdh.player_id,
                pdh.player_hashes,
                ROW_NUMBER() OVER (PARTITION BY pdh.duel_strapi_document_id ORDER BY pdh.player_id) AS player_number
            FROM player_duel_hashes pdh
            WHERE pdh.distinct_hash_count >= 2
        ),
        selected_main_matches AS (
            SELECT
                tmp.duel_strapi_document_id,
                tmp.player_id,
                tmp.player_number,
                (tmp.player_hashes)[1 + floor(random() * array_length(tmp.player_hashes, 1))::int] AS selected_hash
            FROM two_match_players tmp
        ),
        two_match_opponents AS (
            SELECT DISTINCT
                smp.duel_strapi_document_id,
                smp.player_id as two_match_player,
                smp.player_number,
                CASE
                    WHEN uda.user_strapi_document_id = smp.player_id
                    THEN uda.rival_user_strapi_document_id
                    ELSE uda.user_strapi_document_id
                END as opponent_id
            FROM selected_main_matches smp
            JOIN user_duel_answers uda ON smp.selected_hash = uda.hash
                AND smp.duel_strapi_document_id = uda.duel_strapi_document_id
        ),
        single_match_duels_raw AS (
            SELECT DISTINCT
                uda.duel_strapi_document_id,
                uda.hash,
                uda.user_strapi_document_id,
                uda.rival_user_strapi_document_id
            FROM user_duel_answers uda
            WHERE uda.duel_strapi_document_id = duel_record.duel_id
            AND uda.hash NOT IN (
                SELECT smm.selected_hash
                FROM selected_main_matches smm
                WHERE smm.duel_strapi_document_id = uda.duel_strapi_document_id
            )
            AND uda.user_strapi_document_id NOT IN (
                SELECT tmp.player_id
                FROM two_match_players tmp
                WHERE tmp.duel_strapi_document_id = uda.duel_strapi_document_id
            )
            AND uda.rival_user_strapi_document_id NOT IN (
                SELECT tmp.player_id
                FROM two_match_players tmp
                WHERE tmp.duel_strapi_document_id = uda.duel_strapi_document_id
            )
        ),
        single_match_duels AS (
            SELECT DISTINCT ON (smdr.duel_strapi_document_id, smdr.hash)
                smdr.duel_strapi_document_id,
                smdr.hash,
                smdr.user_strapi_document_id,
                smdr.rival_user_strapi_document_id,
                ROW_NUMBER() OVER (PARTITION BY smdr.duel_strapi_document_id ORDER BY smdr.hash) as match_order
            FROM single_match_duels_raw smdr
        ),
        duel_stats AS (
            SELECT
                combined.duel_strapi_document_id,
                COUNT(DISTINCT combined.hash) as N,
                COUNT(DISTINCT CASE WHEN combined.source = 'two_match' THEN combined.hash END) as two_match_count
            FROM (
                SELECT smm.duel_strapi_document_id, smm.selected_hash as hash, 'two_match' as source
                FROM selected_main_matches smm
                UNION ALL
                SELECT smd.duel_strapi_document_id, smd.hash, 'single_match' as source
                FROM single_match_duels smd
            ) combined
            GROUP BY combined.duel_strapi_document_id
        ),
        single_match_pairs AS (
            SELECT
                smd.duel_strapi_document_id,
                smd.hash,
                smd.match_order,
                CASE
                    WHEN smd.user_strapi_document_id < smd.rival_user_strapi_document_id
                    THEN smd.user_strapi_document_id
                    ELSE smd.rival_user_strapi_document_id
                END as lower_player,
                CASE
                    WHEN smd.user_strapi_document_id < smd.rival_user_strapi_document_id
                    THEN smd.rival_user_strapi_document_id
                    ELSE smd.user_strapi_document_id
                END as upper_player
            FROM single_match_duels smd
        ),
        additional_matches AS (
            SELECT DISTINCT
                tmp.duel_strapi_document_id,
                tmp.player_id,
                tmp.player_number,
                unnest(tmp.player_hashes) as match_hash
            FROM two_match_players tmp
        ),
        additional_matches_filtered AS (
            SELECT
                am.duel_strapi_document_id,
                am.player_id,
                am.player_number,
                am.match_hash
            FROM additional_matches am
            WHERE am.match_hash NOT IN (
                SELECT smm.selected_hash
                FROM selected_main_matches smm
                WHERE smm.duel_strapi_document_id = am.duel_strapi_document_id
            )
        ),
        additional_opponents AS (
            SELECT DISTINCT
                amf.duel_strapi_document_id,
                amf.player_id as two_match_player,
                amf.player_number,
                amf.match_hash,
                CASE
                    WHEN uda.user_strapi_document_id = amf.player_id
                    THEN uda.rival_user_strapi_document_id
                    ELSE uda.user_strapi_document_id
                END as opponent_id
            FROM additional_matches_filtered amf
            JOIN user_duel_answers uda ON amf.match_hash = uda.hash
                AND amf.duel_strapi_document_id = uda.duel_strapi_document_id
        ),
        all_players_raw AS (
            SELECT
                tmp.duel_strapi_document_id,
                tmp.player_id,
                tmp.player_number,
                'two_match' as player_type
            FROM two_match_players tmp
            
            UNION ALL
            
            SELECT
                smp.duel_strapi_document_id,
                smp.lower_player as player_id,
                smp.match_order + ds.two_match_count as player_number,
                'single_match_lower' as player_type
            FROM single_match_pairs smp
            JOIN duel_stats ds ON smp.duel_strapi_document_id = ds.duel_strapi_document_id
            
            UNION ALL
            
            SELECT
                tmo.duel_strapi_document_id,
                tmo.opponent_id as player_id,
                ds.N + tmo.player_number as player_number,
                'two_match_opponent' as player_type
            FROM two_match_opponents tmo
            JOIN duel_stats ds ON tmo.duel_strapi_document_id = ds.duel_strapi_document_id
            
            UNION ALL
            
            SELECT
                smp.duel_strapi_document_id,
                smp.upper_player as player_id,
                ds.N + ds.two_match_count + smp.match_order as player_number,
                'single_match_upper' as player_type
            FROM single_match_pairs smp
            JOIN duel_stats ds ON smp.duel_strapi_document_id = ds.duel_strapi_document_id
            
            UNION ALL
            
            SELECT
                ao.duel_strapi_document_id,
                ao.opponent_id as player_id,
                (ds.N * 2) + ao.player_number as player_number,
                'additional_opponent' as player_type
            FROM additional_opponents ao
            JOIN duel_stats ds ON ao.duel_strapi_document_id = ds.duel_strapi_document_id
        ),
        all_players AS (
            SELECT DISTINCT ON (apr.duel_strapi_document_id, apr.player_id)
                apr.duel_strapi_document_id,
                apr.player_id,
                MIN(apr.player_number) as player_number,
                MIN(apr.player_type) as player_type
            FROM all_players_raw apr
            GROUP BY apr.duel_strapi_document_id, apr.player_id
        ),
        final_numbered_players AS (
            SELECT
                ap.duel_strapi_document_id,
                ap.player_id,
                ROW_NUMBER() OVER (
                    PARTITION BY ap.duel_strapi_document_id
                    ORDER BY
                        CASE ap.player_type
                            WHEN 'two_match' THEN 1
                            WHEN 'single_match_lower' THEN 2
                            WHEN 'two_match_opponent' THEN 3
                            WHEN 'single_match_upper' THEN 4
                            WHEN 'additional_opponent' THEN 5
                        END,
                        ap.player_number
                ) as final_player_number
            FROM all_players ap
        ),
        all_duels AS (
            SELECT
                smp.duel_strapi_document_id,
                smp.selected_hash as hash,
                smp.player_number as match_order
            FROM selected_main_matches smp
            
            UNION ALL
            
            SELECT
                smd.duel_strapi_document_id,
                smd.hash,
                ROW_NUMBER() OVER (PARTITION BY smd.duel_strapi_document_id ORDER BY smd.hash) +
                (SELECT COUNT(*) FROM selected_main_matches smm WHERE smm.duel_strapi_document_id = smd.duel_strapi_document_id) as match_order
            FROM single_match_duels smd
            
            UNION ALL
            
            SELECT
                amf.duel_strapi_document_id,
                amf.match_hash as hash,
                ds.N + amf.player_number as match_order
            FROM additional_matches_filtered amf
            JOIN duel_stats ds ON amf.duel_strapi_document_id = ds.duel_strapi_document_id
        )
        
        -- Сохраняем маппинги игроков и схваток
        SELECT 
            json_object_agg(fnp.final_player_number::text, fnp.player_id) as players,
            (SELECT json_object_agg(ad.match_order::text, ad.hash) FROM all_duels ad WHERE ad.duel_strapi_document_id = duel_record.duel_id) as duels
        INTO player_mapping, duel_mapping
        FROM final_numbered_players fnp
        WHERE fnp.duel_strapi_document_id = duel_record.duel_id;

        -- Применяем алгоритм распределения проверок
        PERFORM apply_review_distribution(duel_record.duel_id, player_mapping, duel_mapping);
        
    END LOOP;

    -- Возвращаем результаты
    RETURN QUERY
    SELECT 
        udr.reviewer_user_strapi_document_id,
        udr.duel_strapi_document_id,
        udr.user_strapi_document_id,
        udr.hash
    FROM user_duel_to_review udr
    WHERE udr.duel_strapi_document_id IN (
        SELECT d.strapi_document_id 
        FROM duels d
        WHERE d.sprint_strapi_document_id = input_sprint_strapi_document_id
    );

END;
