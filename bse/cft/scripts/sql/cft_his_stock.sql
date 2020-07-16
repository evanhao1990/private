select
     main.*
    ,sub.net_cogs_ed
from

(
    select /*stock value eoh*/

        /*attributes*/

         case when sty.noos = 1 then 'NOOS'
              when sty.noos = 0 then 'NON-NOOS' else 'NA' end                  as noos
        ,case when sb.ibrandlabel = 'NM' then 'VM' else sb.ibrandlabel end     as brand
        ,case when brand = 'VM' and sty.productcategory in ('T-SHIRTS','TOPS')
		      then 'T-SHIRTS & TOPS' else sty.productcategory end              as product_category
        ,right(dd.fiscalyearname,5)                                            as fy
        ,right(dd.fiscalyearquartername,2)                                     as fq
        ,dd.calendaryearmonth                                                  as cm
        ,dd.shortmonthname                                                     as month

        ,sum(asd.stock_qty * sku.costpriceeur)                                 as eoh_val

    from mart.agg_stock_day asd
        join mart.dim_date dd on dd.dimdateid = asd.dim_date_id
        join mart.dim_style sty on sty.dimstyleid = asd.dim_style_id
        join mart.dim_sku sku on sku.dimskuid = asd.dim_sku_id
        join mart.dim_subbrand sb on sb.dimsubbrandid = asd.dim_subbrand_id
        join #tempvar te on true

    where asd.dim_date_id in (select /*last day in the month*/
                                t0.dimdateid
                              from
                                (select
                                    dd.calendaryearmonth,
                                    max(dd.dimdateid) as dimdateid
                                from mart.dim_date dd join #tempvar te on true
                                where dd.dimdate between te.start_date and te.end_date group by 1) t0)

        and sty.productcategory not in ('MARKETING','RESELLABLE MARKETING','GIFT CARDS')
        and sb.dimbrandid not in (0,10,17,20)
        and sb.subbrand not in ('Vero Moda Curve','Junarose by VM')
    group by 1,2,3,4,5,6,7

) main

join

(
    select /*net cogs ed*/
         case when sty.noos = 1 then 'NOOS'
              when sty.noos = 0 then 'NON-NOOS' else 'NA' end                  as noos
        ,case when sb.ibrandlabel = 'NM' then 'VM' else sb.ibrandlabel end     as brand
        ,case when brand = 'VM' and sty.productcategory in ('T-SHIRTS','TOPS')
		      then 'T-SHIRTS & TOPS' else sty.productcategory end              as product_category
        ,right(dd.fiscalyearname,5)                                            as fy
        ,right(dd.fiscalyearquartername,2)                                     as fq
        ,dd.calendaryearmonth                                                  as cm
        ,dd.shortmonthname                                                     as fm

        ,sum(ao.cogs) - sum(ao.cogr_fd)                                        as net_cogs_ed

    from mart.agg_orderline_day ao
            join mart.dim_sku sku     on sku.dimskuid = ao.dim_sku_id
            join mart.dim_subbrand sb on sb.dimsubbrandid = ao.dim_subbrand_id
            join mart.dim_style sty   on sty.dimstyleid = ao.dim_style_id
            join mart.dim_date dd     on dd.dimdateid = ao.dim_date_id
            join mart.dim_channel dc  on dc.dimchannelid = ao.dim_channel_id
            join #tempvar te on true
    where dd.dimdate between te.start_date and te.end_date
    and sb.subbrand not in ('Vero Moda Curve','Junarose by VM')
    group by 1,2,3,4,5,6,7

) sub on main.noos = sub.noos and main.brand = sub.brand and main.product_category = sub.product_category and main.cm = sub.cm