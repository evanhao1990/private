select
    dd.dimdateid,
    dd.dimdate as date,
    dc.countrycode,
    pro.sitebrand_label as brandcode,
    cat.device_category as device,
    cat.channel_grouping,
    cat.source,
    cat.medium,
    cat.campaign,
    id.adwords_campaign_id,
    sum(cat.sessions) as sessions,
    sum(cat.transactions) as transactions
from mart.fact_ua_categories cat
    join mart.dim_date dd on dd.dimdateid = cat.dim_register_date_id and dd.dimdate between {start_date} and {end_date}
    join mart.dim_ua_profile pro on pro.dim_ua_profile_id = cat.dim_ua_profile_id
    join mart.dim_country dc on dc.dimcountryid = pro.dim_country_id
left join (select ads.campaign, ads.adwords_campaign_id
                    from mart.fact_ua_adwords_campaign ads
                    join mart.dim_date dd on dd.dimdateid = ads.dim_date_id and dd.dimdate between {start_date} and {end_date}
                    where ads.campaign != '(not set)'
                    group by 1,2) id on cat.campaign = id.campaign and cat.source = 'google' and cat.medium = 'cpc'
group by 1,2,3,4,5,6,7,8,9,10
