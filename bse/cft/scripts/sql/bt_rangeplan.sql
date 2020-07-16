drop table if exists sandbox_ana.bt_rangeplan;
create table sandbox_ana.bt_rangeplan as 
(
    select
         sb.ibrandlabel as brand
        ,case when brand = 'VM' and sty.productcategory in ('T-SHIRTS','TOPS')
              then 'T-SHIRTS & TOPS' else sty.productcategory end              as product_category
        ,case when sty.noos = 1 then 'NOOS' else 'NON-NOOS' end                as noos
        ,dd.calendaryearmonth
        ,count(distinct(sty.itemname /*|| opt.colour*/))                       as nr_style
        ,sum(fs.stockathandquantity) as qty_indelivered
        ,sum(fs.stockathandquantity * sku.costpriceeur)                        as val_indelivered
    from dwh.mart.fact_stockmovements fs
        join dwh.mart.dim_sku sku on sku.dimskuid = fs.dimskuid
        join dwh.mart.dim_subbrand sb on sb.dimsubbrandid = sku.dimsubbrandid
        join dwh.mart.dim_styleoption opt on opt.dimstyleoptionid = sku.dimstyleoptionid
        join dwh.mart.dim_style sty on sty.dimstyleid = opt.dimstyleid
        join dwh.mart.dim_date dd on dd.dimdateid = fs.dimordercompilationdateid
        join #tempvar te on true
    where fs.dimstockmovementsreasoncodeid in (1,14,30)
    and dd.dimdate between te.start_date and te.end_date
    group by 1,2,3,4
    order by 1,2,3,4
)