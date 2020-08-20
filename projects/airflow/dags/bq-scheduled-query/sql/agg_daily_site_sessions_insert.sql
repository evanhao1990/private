-- #!{"id":"bq-daily-site-sessions-insert", "interval":"*/30 6-10 * * *"}!#
insert into alg-hn-insights.Zz_Test_Airflow.agg_daily_site_sessions

select
main.*,
case when main.nr_msg_click > 0 and (main.nr_payment_click + main.nr_quick_credits_click) > 0 then 1 -- 'message and payment'
     when main.nr_msg_click > 0 and (main.nr_payment_click + main.nr_quick_credits_click) = 0 then 2 -- 'message, no payment'
     when main.nr_msg_click = 0 and (main.nr_payment_click + main.nr_quick_credits_click) > 0 then 3 -- 'no message, payment'
     when main.nr_msg_click = 0 and (main.nr_payment_click + main.nr_quick_credits_click) = 0 then 4 -- 'no message, no payment'
     else -1 end as engagement_clicks,

max(main.visitNumber) over (partition by main.utc_date, main.fullvisitorid) as max_visit

from(

SELECT
    visitid                                                                                         as timestamp,
    CAST(timestamp(DATETIME(TIMESTAMP_SECONDS(visitid)))  AS DATE)                                  AS utc_date,
    fullVisitorID,
    concat( cast(fullVisitorID as string),cast(visitid as string))                                         as session_id,
    (select cd.value from h.customDimensions as cd where cd.index = 1) as account_id,
    -- device
    device.deviceCategory, device.browser,  device.mobileDeviceInfo,
    -- traffic source
    trafficSource.medium, trafficSource.source, channelGrouping,
    -- geo
    geoNetwork.country, geoNetwork.region, geoNetwork.city, geoNetwork.latitude, geoNetwork.longitude,

    round((max(h.time)-min(h.time))/1000/60,1)                                                                             as minutes_on_site,
    min(visitNumber) as visitNumber,
    h.page.hostname                                                                                                        as hostname,
    count(h.page.pagePath)                                                                                                 as page_viewed,
    count(distinct h.page.pagePath)                                                                                        as distinct_page_viewed,
    count(distinct REGEXP_EXTRACT(h.page.pagePath, r"/profile/([a-z]*)"))                                                  as profiles_viewed,
    count(distinct REGEXP_EXTRACT(h.page.pagePath, r"/conversation/([0-9]*)"))                                             as conversation_viewed,
    max((select cd.value from h.customDimensions as cd where cd.index = 4))                                                as site_country,
    max((select cd.value from h.customDimensions as cd where cd.index = 3))                                                as platform,
    sum(case when h.eventInfo.eventCategory = 'Click' and h.eventInfo.eventAction ='Send Message' then 1 else 0 end)       as nr_msg_click,
    sum(case when h.eventInfo.eventCategory = 'Click' and h.eventInfo.eventAction ='Splash Register' then 1 else 0 end)    as nr_splash_register_click,
    sum(case when h.eventInfo.eventCategory = 'Click' and h.eventInfo.eventAction ='Payment' then 1 else 0 end)            as nr_payment_click,
    sum(case when h.eventInfo.eventCategory = 'Click' and h.eventInfo.eventAction ='Buy Quick Credits' then 1 else 0 end)  as nr_quick_credits_click,
    max(case when REGEXP_CONTAINS(h.page.pagePath , r"/members|/profile|/conversation|/messages|/credits|/favourites|/visitors|/settings|/my-profile") then 1 else 0 end) as is_login_session,
    max(case when REGEXP_CONTAINS(h.page.pagePath , r"/landing") then 1 else 0 end)                        as is_lander_session,

FROM `196983973.ga_sessions_*`,
    unnest(hits) h
where _TABLE_SUFFIX = format_date('%Y%m%d', date_add(date(current_timestamp(), "Europe/Amsterdam"),interval -1 day))

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,19
) main
where main.account_id is not null and main.account_id != '' and main.is_login_session = 1
and main.session_id not in (select distinct adss.session_id from alg-hn-insights.Zz_Test_Airflow.agg_daily_site_sessions adss where adss.utc_date >= date_add(date(current_timestamp(),"UTC"),interval -2 day))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
having is_lander_session = 0;