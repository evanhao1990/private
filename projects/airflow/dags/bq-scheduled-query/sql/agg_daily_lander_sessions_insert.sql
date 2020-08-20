-- #!{"id":"bq-daily-lander-sessions-insert", "interval":"*/30 6-10 * * *"}!#
insert into alg-hn-insights.Zz_Test_Airflow.agg_daily_ga_sessions_partitioned

select
    main.timestamp,
    CAST(timestamp(DATETIME(TIMESTAMP_SECONDS(main.timestamp)))  AS DATE) AS utc_date,
    main.fullVisitorID,
    main.session_id,
    main.hostname,
    -- device
    main.deviceCategory, main.browser,  main.mobileDeviceInfo,
    -- traffic source
    main.medium,main.source, main.channelGrouping,
    -- geo
    main.country, main.region, main.city, main.latitude, main.longitude,
    main.minutes_on_site,
    main.distinct_page_views,
    main.pi, main.pe, main.pt, main.lander_name, main._lander_static, main._lander_gtl, main._lander_bg,
    case when main.lander_number like '%pre%' then -1 else main._is_lander_session end as is_lander_session, -- -1 tags sessions only on pre-landers
    main.success_number,
    main.lander_number,
    case when main._is_lander_session = 1 and main.success_number is null then null else sub.is_doi end                          as is_doi,
    case when main._is_lander_session = 1 and main._lander_category is null then 'default' else main._lander_category end        as lander_category,
    case when main._is_lander_session = 1 and main._lander_sub_category is null then 'default'else main._lander_sub_category end as lander_sub_category,
    case when main._is_lander_session = 1 and main._lander_source is null then 'default' else main._lander_source end            as lander_source,
    main.lander_step,

    -- 1 tags this lander session is from a pre-lander
    max(case when main.lander_number like '%pre%' then 1 else 0 end) over (partition by main.session_id) as is_prelander_session

from(
        SELECT
            visitStartTime                                                                                         as timestamp,
            fullVisitorID,
            concat(cast(visitid as string), cast(fullVisitorID as string))                                         as session_id,
            -- device
            device.deviceCategory, device.browser,  device.mobileDeviceInfo,
            -- traffic source
            trafficSource.medium, trafficSource.source, channelGrouping,
            -- geo
            geoNetwork.country, geoNetwork.region, geoNetwork.city, geoNetwork.latitude, geoNetwork.longitude,

            round(totals.timeOnSite/60,1)                                                                          as minutes_on_site,
            h.page.hostname                                                                                        as hostname,
            REGEXP_EXTRACT(h.page.pagePath, r"/landing([0-9]+[a-z]{0,3})")                                         as lander_number,
            max(REGEXP_EXTRACT(h.page.pagePath, r"/(landing[0-9]+.*?)\?.*"))                                       as lander_name,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]pi=([0-9]+)[^0-9]*"))                                      as pi,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]pe=([0-9]+)[^0-9]*"))                                      as pe,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]pt=([0-9]+)[^0-9]*"))                                      as pt,

            count(distinct h.page.pagePath)                                                                        as distinct_page_views,

            max(REGEXP_EXTRACT(h.page.pagePath, r"/landing[0-9]+/([0-9]*)"))                                       as lander_step,
            max(REGEXP_EXTRACT(h.page.pagePath, r"/landing/success/landing([0-9]+)[^0-9]*"))                       as success_number,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]source=(\w+[.]?\w+?\w*)"))                                 as _lander_source,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]cat=(\w+[-+=]?\w*)"))                                      as _lander_category,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]sub=(\w+)"))                                               as _lander_sub_category,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]static=(\w+)"))                                            as _lander_static,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]style=(.?\w+)"))                                           as _lander_style,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]gtl=(\d+)"))                                               as _lander_gtl,
            max(REGEXP_EXTRACT(h.page.pagePath, r".+[?&]bg=(\d+)"))                                                as _lander_bg,
            max(case when REGEXP_CONTAINS(h.page.pagePath , r"/landing") then 1 else 0 end)                        as _is_lander_session

        FROM `196983973.ga_sessions_*`,
            unnest(hits) h
        where _TABLE_SUFFIX between format_date('%Y%m%d', date_add(date(current_timestamp(), "Europe/Amsterdam"),interval -1 day)) and format_date('%Y%m%d', date_add(date(current_timestamp(), "Europe/Amsterdam"),interval -1 day)) -- suffix is based on ams timezone

        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) main
left join (
        SELECT
            fullVisitorID,
            1 as is_doi
        FROM `196983973.ga_sessions_*`, unnest(hits) h
        WHERE REGEXP_CONTAINS(h.page.pagepath, r"/activate/")
        and  _TABLE_SUFFIX >= format_date('%Y%m%d', date_add(date(current_timestamp(), "Europe/Amsterdam"),interval -1 day)) -- today + yesterday

        GROUP BY 1,2
) sub on main.fullVisitorID = sub.fullVisitorID
where main.session_id not in (select distinct session_id from alg-hn-insights.Zz_Test_Airflow.agg_daily_ga_sessions_partitioned sp where sp.utc_date >= date_add(date(current_timestamp,'UTC'),interval -2 day))
and main._is_lander_session = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33