
create table #tempvar as
(
    select
        dd.dimdateid - 28      as cy_d1
       ,dd.dimdateid - 1       as cy_d2
       ,dd.dimdateid - 28 -365 as ly_d1
       ,dd.dimdateid - 1 -365  as ly_d2
    from dwh.mart.dim_date dd
    where dd.dimdate = current_date
);


--Sales

select

    case when dd.dimdateid between te.cy_d1 and te.cy_d2 then 'CURRENT'
         when dd.dimdateid between te.ly_d1 and te.ly_d2 then 'LY' else 'NA' end                         as TIME,
			 
    case when od.dim_channel_id in (2,3,6) then 'BS.com'
         when od.dim_channel_id IN (4,5)   then 'BRAND.com'
         when od.dim_channel_id IN (7)     then 'PARTNER.com' else 'NA' end                              as BUSINESS_AREA,
    su.IBrandLabel,
    upper(sty.ProductCategory)                                                                           as ProductCategory,
    case when sty.NOOS IN (0) then 'Non-NOOS' else 'NOOS' end                                            as NOOS,
    case when da.age_cat_styleoption_last_indelivery in (1,2) then 'A. New (0-8w)'
         when da.age_cat_styleoption_last_indelivery in (3,4) then 'B. Mature (8-16w)'
         when da.age_cat_styleoption_last_indelivery in (5)   then 'C. Old (>16w)' else 'UNKOWN' end     as AGE_CATEGORY,
    sum(od.gs1)                                                                                          as GS_I_OD,
    sum(od.gs2)                                                                                          as GS_II_OD,
    sum(od.disc_value)                                                                                   as DISC_VAL,
    sum(od.gis1)                                                                                         as GIS_I,
    sum(od.cogs) - sum(od.cogr_fd)                                                                       as NET_COGS_ED

from mart.agg_orderline_day od
    join mart.dim_date dd on dd.dimdateid = od.dim_date_id
    join mart.dim_subbrand su on su.dimsubbrandid = od.dim_subbrand_id
    join mart.dim_style sty on sty.dimstyleid = od.dim_style_id
    join mart.agg_product_age_day da on da.dim_sku_id = od.dim_sku_id and da.dim_date_id = od.dim_date_id
    join #tempvar te on true
where ((dd.dimdateid between te.cy_d1 and te.cy_d2) or (dd.dimdateid between te.ly_d1 and te.ly_d2))
    and sty.productcategory not in ('MARKETING','RESELLABLE MARKETING','GIFT CARDS')

group by 1,2,3,4,5,6
;


select
    case when dd.dimdateid = te.cy_d2 then 'CURRENT'
         when dd.dimdateid = te.ly_d2 then 'LY' else 'NA' end                                            as TIME,
    su.IBrandLabel,
    upper(sty.ProductCategory)                                                                           as ProductCategory,
    case when sty.NOOS IN (0) then 'Non-NOOS' ELSE 'NOOS' END                                            as NOOS,
    case when da.age_cat_styleoption_last_indelivery in (1,2) then 'A. New (0-8w)'
	     when da.age_cat_styleoption_last_indelivery in (3,4) then 'B. Mature (8-16w)'
	     when da.age_cat_styleoption_last_indelivery in (5)   then 'C. Old (>16w)' else 'UNKOWN' end     as AGE_CATEGORY,
    case when st.mov_cat_styleoption = 'A. Fast Movers' then 'A. Fast Movers'
         when st.mov_cat_styleoption = 'B. Medium Movers' then 'B. Medium Movers'
         when st.mov_cat_styleoption = 'C. Slow Movers' then 'C. Slow-Not Movers'
         when st.mov_cat_styleoption = 'D. Non Movers' then 'C. Slow-Not Movers'
         when st.mov_cat_styleoption = 'E. Not Moving' then 'C. Slow-Not Movers' else 'UNKOWN' end       as MOVERS_CAT_SO,
    sum(st.stock_qty)                                                                                    as STOCK_QTY_EOH,
    sum(st.stock_qty * sk.costpriceeur)                                                                  as STOCK_VAL_EOH,
    sum(st.gis1_7d)                                                                                      as GIS_7d

from mart.agg_stock_day st
    join mart.dim_date dd on dd.dimdateid = st.dim_date_id
    join mart.dim_sku sk on sk.dimskuid = st.dim_sku_id
    join mart.agg_product_age_day da on da.dim_sku_id = st.dim_sku_id and da.dim_date_id = st.dim_date_id
    join mart.dim_subbrand su on su.dimsubbrandid = st.dim_subbrand_id
    join mart.dim_style sty on sty.dimstyleid = st.dim_style_id
    join #tempvar te on true											
where ((dd.dimdateid = te.cy_d2) or (dd.dimdateid = te.ly_d2))
    and st.stock_qty not in (0)
    AND sty.ProductCategory NOT IN ('MARKETING','RESELLABLE MARKETING','GIFT CARDS') 
	
group by 1,2,3,4,5,6