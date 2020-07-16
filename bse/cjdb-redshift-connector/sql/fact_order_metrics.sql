select

       cus_order.dimcustomerid,
       cus_order.dimorderlinedateid                        as dimorderdateid,
       cus_order.dimdate                                   as date,
       cus_order.referenceorder,
       cus_order.countrycode,
       cus_order.site_brand as brandcode,
       nvl(web.device_category, '(unknown)')  as device,
       nvl(web.channel_grouping, '(unknown)') as channel_grouping,
       nvl(web.source, '(unknown)')  as source,
       nvl(web.medium, '(unknown)')  as medium,
       nvl(web.campaign, '(unknown)') as campaign,
       id.adwords_campaign_id,
       cus_order.is_sovendus_order,
       case when web.transaction_id is not null then 1 else 0 end as is_ga_tracked,
       cus_order.gs0,
       cus_order.gs1,
       cus_order.gis0,
       cus_order.gis1,
       cus_order.gsiibc,
       cus_order.gsii,
       cus_order.gr2,
       cus_order.gir,
       cus_order.gsii - cus_order.gr2 as ns,
       cus_order.gis1 - cus_order.gir as nis

from (
         -- from fact_orderline get customerid, from fact_order get checkout scope and sales numbers
         select fo.dimcustomerid,
                fo.referenceorder,
                fo.dimorderlinedateid,
                dd.dimdate,
                dc.countrycode,
                cs.brand_code                                                                               as site_brand,
                case when dp.dimpromotionid is not null then 1 else 0 end as is_sovendus_order,
                SUM(case when fo.dimorderlinestateid not in (3, 10, 16, 18) then (fo.Quantity * fo.ItemSalesEUR) else 0 end) as gsiibc,
                SUM(case when fo.dimorderlinestateid not in (3, 10, 16, 18, 4,17) then (fo.Quantity * fo.ItemSalesEUR) else 0 end) as gsii,
                SUM(case when fo.dimorderlinestateid not in (3, 10, 16, 18) then fo.quantity else 0 end)    AS gis0,
                SUM(case when fo.dimorderlinestateid not in (3, 10, 16, 18,4,17) then fo.quantity else 0 end)    AS gis1,
                SUM(case when fo.dimorderlinestateid in (3, 10, 16, 18) then (fo.Quantity * -1.0 * fo.ItemSalesEUR) else 0 end) as gr2,
                sum(case when fo.dimorderlinestateid in (3, 10, 16, 18) then fo.quantity * -1.0 else 0 end) as gir,
                sum(case when fo.DimOrderLineStateID not in (3, 10, 16, 18) then (fo.Quantity * (fo.ItemRetailPriceEUR / (1 + fo.VATRate))) else 0 end) as gs0,
                sum(case when fo.DimOrderLineStateID not in (3, 10, 16, 18,4,17) then (fo.Quantity * (fo.ItemRetailPriceEUR / (1 + fo.VATRate))) else 0 end) as gs1
         from mart.fact_orderline fo
                  join mart.fact_order fo2 on fo2.reference_order = fo.referenceorder
                  join mart.dim_checkout_scope cs on cs.dim_checkout_scope_id = fo2.dim_checkout_scope_id
                  join mart.dim_country dc on dc.dimcountryid = fo.dimshiptocountryid
                  left join mart.dim_promotion dp on dp.dimpromotionid = fo.dimpromotionid and lower(dp.promotion) like '%sovendus%'
                  join mart.dim_date dd on dd.dimdateid = fo.dimorderlinedateid and dd.dimdate between {start_date} and {end_date}
         where fo.dimchannelid in (2, 3, 4, 5)
           and fo.dim_business_model_id = 1
           and fo.DimOrderLineTypeID = 1
         group by 1, 2, 3, 4, 5, 6, 7
     ) cus_order

    left join
     (   -- please change the logic after BI fix duplicated transaction ids in fact_ua_transactions
         -- from ua_transactions get web data
                  
         select
            tr.transaction_id,
            max(tr.device_category)  as device_category,
            max(tr.channel_grouping) as channel_grouping,
            max(tr.source) as source,
            max(tr.medium) as medium,
            max(tr.campaign) as campaign
         from mart.fact_ua_transactions tr
         join mart.dim_date dd on dd.dimdateid = tr.dim_register_date_id and dd.dimdate between {start_date} and {end_date}

         group by 1
     ) web on cus_order.referenceorder = web.transaction_id
    left join (
            -- please change this logic after BI implement adwords campaign id in fact_ua_tables
            -- get campaign id from fact_adwords_campaign the since in athena we only have most updated name
            select
                ads.campaign,
                max(ads.adwords_campaign_id) as adwords_campaign_id -- there are 0.3% campaign names have 2 campaign_ids
            from mart.fact_ua_adwords_campaign ads
            join mart.dim_date dd on dd.dimdateid = ads.dim_date_id and dd.dimdate between {start_date} and {end_date}
            where ads.campaign != '(not set)'
            group by 1
            ) id on web.campaign = id.campaign and web.source = 'google' and web.medium = 'cpc'

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17,18,19,20,21,22