select
     (select distinct dd.fiscalyearquartername as cq from mart.dim_date dd where dd.dimdate = current_date) as cq
    ,'2016-08-01' as start_date
    ,(select dd.dimdate from mart.dim_date dd where dd.dimdate = last_day(add_months(current_date,-1))) as end_date
into #tempvar
;
drop table if exists sandbox_ana.cft_sales;
create table sandbox_ana.cft_sales as
(
select

     main.brand||main.business_area||main.product_category||main.fy||main.fq as key_q
    ,main.brand||main.business_area||main.product_category||main.fy||main.fm as key_m
    ,main.brand
    ,main.business_area
    ,main.product_category
    ,main.fy
    ,main.fq
    ,main.fm as month
    ,main.gsii_m
    ,main.disc_val_m
    ,main.discount_m
    ,main.markup_ivat_m
    ,main.placeholder
    ,main.noos_m
    ,main.gsii_q
    ,main.discount_q
    ,main.net_sales_ed_q / nullif(main.net_cogs_ed_q,0) * 1.19 as markup_ivat_q
    ,sub.trans_rr
    ,main.noos_q
    ,main.gsii_m_3year / nullif(main.gsii_q_3year,0) as gsii_share
    ,main.disc_val_m_3year / nullif(main.disc_val_q_3year,0) as dis_val_share

from
(
    select

        /*attributes*/

         case when sb.ibrandlabel = 'NM' then 'VM' else sb.ibrandlabel end     as brand
        ,case when dc.dimchannelid in (2,3,6) then 'BS.com'
              when dc.dimchannelid in (4,5) then 'BRAND.com'
              when dc.dimchannelid = 7 then 'PARTNER.com' else 'NA' end        as business_area
        ,case when brand = 'VM' and sty.productcategory in ('T-SHIRTS','TOPS')
		      then 'T-SHIRTS & TOPS' else sty.productcategory end              as product_category
        ,right(dd.fiscalyearname,5)                                            as fy
        ,right(dd.fiscalyearquartername,2)                                     as fq
        ,dd.shortmonthname                                                     as fm
        ,dd.fiscalyearquartername                                              as quarter_name
        ,te.cq                                                                 as current_quarter

        /*KPIs monthly*/

        ,sum(ao.gs2)                                                           as gsii_m
        ,sum(ao.disc_value)                                                    as disc_val_m
        ,sum(ao.disc_value) / nullif(sum(ao.gs1),0)                            as discount_m
        ,sum(ao.gs1) - sum(ao.gr1_fd)                                          as net_sales_ed_m
        ,sum(ao.cogs) - sum(ao.cogr_fd)                                        as net_cogs_ed_m
        ,net_sales_ed_m / nullif(net_cogs_ed_m,0) * 1.19                       as markup_ivat_m
        ,0                                                                     as placeholder
        ,sum(case when sty.noos = 1 then ao.gs2 else 0 end)                    as gsii_noos_m
        ,gsii_noos_m / nullif(gsii_m,0)                                        as noos_m

        /*KPIs quarterly. Monthly KPIs are sumed up on quarterly*/

        ,sum(gsii_m)     over (partition by brand,business_area,product_category,fy,fq)                            as gsii_q
        ,sum(disc_val_m) over (partition by brand,business_area,product_category,fy,fq)                            as disc_val_q
        ,disc_val_q / nullif((gsii_q + disc_val_q),0)                                                              as discount_q
        ,sum(net_sales_ed_m ) over (partition by brand,business_area,product_category,fy,fq) as net_sales_ed_q
        ,sum(net_cogs_ed_m)   over (partition by brand,business_area,product_category,fy,fq) as net_cogs_ed_q
        ,sum(gsii_noos_m)     over (partition by brand,business_area,product_category,fy,fq) / nullif(gsii_q,0)    as noos_q

        /*KPIs to calculate historical monthly split.*/

        ,sum(case when quarter_name <> current_quarter then gsii_m     else 0 end) over (partition by brand,business_area,product_category,fm) as gsii_m_3year
        ,sum(case when quarter_name <> current_quarter then disc_val_m else 0 end) over (partition by brand,business_area,product_category,fm) as disc_val_m_3year
        ,sum(case when quarter_name <> current_quarter then gsii_m     else 0 end) over (partition by brand,business_area,product_category,fq) as gsii_q_3year
        ,sum(case when quarter_name <> current_quarter then disc_val_m else 0 end) over (partition by brand,business_area,product_category,fq) as disc_val_q_3year

    from mart.agg_orderline_day ao
        join mart.dim_sku sku     on sku.dimskuid = ao.dim_sku_id
        join mart.dim_subbrand sb on sb.dimsubbrandid = ao.dim_subbrand_id
        join mart.dim_style sty   on sty.dimstyleid = ao.dim_style_id
        join mart.dim_date dd     on dd.dimdateid = ao.dim_date_id
        join mart.dim_channel dc  on dc.dimchannelid = ao.dim_channel_id
        join #tempvar te on true

    where dd.dimdate between te.start_date and te.end_date
        and sty.productcategory not in ('MARKETING','RESELLABLE MARKETING','GIFT CARDS')
        and sb.dimbrandid not in (0,10,17,20)
        and sb.subbrand not in ('Vero Moda Curve','Junarose by VM')

    group by 1,2,3,4,5,6,7,8
) main

join

( /*transactional return rate rolling 12 months*/
    select
         case when sb.ibrandlabel = 'NM' then 'VM' else sb.ibrandlabel end         as brand
        ,case when ao.dim_channel_id in (2,3,6) then 'BS.com'
             when ao.dim_channel_id in (4,5)   then 'BRAND.com'
             when ao.dim_channel_id = 7        then 'PARTNER.com' else 'NA' end   as business_area
        ,case when brand = 'VM' and sty.productcategory in ('T-SHIRTS','TOPS')
		      then 'T-SHIRTS & TOPS' else sty.productcategory end                 as product_category
        ,sum(ao.gr1) / nullif(sum(ao.gs1),0)                                      as trans_rr

    from mart.agg_orderline_day ao
        join mart.dim_date dd on dd.dimdateid = ao.dim_date_id
        join mart.dim_subbrand sb on sb.dimsubbrandid = ao.dim_subbrand_id
        join mart.dim_style sty on sty.dimstyleid = ao.dim_style_id

    where dd.dimdate between add_months(current_date,-13) and add_months(current_date,-2) --rolling 12 months starting from current month -2 month
    and sb.subbrand not in ('Vero Moda Curve','Junarose by VM')
    group by 1,2,3
) sub on main.brand = sub.brand and main.business_area = sub.business_area and main.product_category = sub.product_category
order by 3,4,5,6,7,8

)