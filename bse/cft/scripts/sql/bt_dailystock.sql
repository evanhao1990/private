drop table if exists sandbox_ana.bt_dailystock;
create table sandbox_ana.bt_dailystock as
(
    select
         case when sb.ibrandlabel = 'NM' then 'VM' else sb.ibrandlabel end     as brand
        ,case when brand = 'VM' and sty.productcategory in ('T-SHIRTS','TOPS')
              then 'T-SHIRTS & TOPS' else sty.productcategory end              as productcategory
        ,case when sty.noos = 1 then 'NOOS' else 'NON-NOOS' end                 as noos
        ,sum(sku.costpriceeur * sd.stock_qty) * 1.0                             as stock_val_eoh
    from mart.agg_stock_day sd
        join mart.dim_sku sku on sku.dimskuid = sd.dim_sku_id
        join mart.dim_subbrand sb on sb.dimsubbrandid = sd.dim_subbrand_id
        join mart.dim_style sty on sty.dimstyleid = sd.dim_style_id
        join mart.dim_date dd on dd.dimdateid = sd.dim_date_id
    where dd.dimdate = current_date - 1
    and sty.productcategory  Not in ('GIFT CARDS','MARKETING','RESELLABLE MARKETING')
    and sb.subbrand not in ('Vero Moda Curve','Junarose by VM')
    group by 1,2,3
    having stock_val_eoh > 0 and brand != ' '
    order by 1,2,3
)