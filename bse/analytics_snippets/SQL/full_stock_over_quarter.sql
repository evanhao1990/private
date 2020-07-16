-- get all styleoptions that had stock in Q1
--------------------------------------------
drop table if exists sandbox_ana.rory_vm_styleoptions;
create table sandbox_ana.rory_vm_styleoptions as (
select distinct
sb.ibrandlabel,
fs.dimstyleoptionid,
ds.noos,
dd.calendaryear
from mart.fact_stock fs
  join mart.dim_date dd on dd.dimdateid = fs.dimdateid
  join mart.dim_subbrand sb on sb.dimsubbrandid = fs.dimsubbrandid
  join mart.dim_style ds on ds.dimstyleid = fs.dimstyleid
where sb.ibrandlabel in ('ON', 'VL', 'VM')
  and ((dd.dimdate between '2017-08-01' and '2017-10-31') or (dd.dimdate between '2018-08-01' and '2018-10-31'))
  and fs.qty > 0);

-- stock quantity at start of quarter
---------------------------------------
drop table if exists sandbox_ana.rory_vm_stock_start;
create table sandbox_ana.rory_vm_stock_start as (
select
dd.dimdate,
dd.calendaryear,
fs.dimstyleoptionid,
sum(fs.qty) as stock_boh
from mart.fact_stock fs
  join mart.dim_date dd on dd.dimdateid = fs.dimdateid
  join sandbox_ana.rory_vm_styleoptions so on so.dimstyleoptionid = fs.dimstyleoptionid and so.calendaryear = dd.calendaryear
where (dd.dimdate = '2017-08-01' or dd.dimdate = '2018-08-01')
group by 1, 2, 3);

-- stock quantity purchased over the season
-------------------------------------------
drop table if exists sandbox_ana.rory_vm_indeliveries;
create table sandbox_ana.rory_vm_indeliveries as (
select
dd.calendaryear,
so.dimstyleoptionid,
max(dd.dimdate) as last_indelivery_date,
sum(fsm.stockathandquantity) as indeliveries

from mart.fact_stockmovements fsm
  join mart.dim_date dd on dd.dimdateid = fsm.dimordercompilationdateid
  join mart.dim_stockmovementreasoncode smr on smr.dimstockmovementsreasoncodeid = fsm.dimstockmovementsreasoncodeid
  join mart.dim_sku sku on sku.sku = fsm.sku
  join sandbox_ana.rory_vm_styleoptions so on so.dimstyleoptionid = sku.dimstyleoptionid and so.calendaryear = dd.calendaryear

where ((dd.dimdate between '2017-08-01' and '2017-10-31') or (dd.dimdate between '2018-08-01' and '2018-10-31'))
  and smr.reasoncode in (510, 526, 615)
group by 1, 2);

-- join together start and deliveries
-------------------------------------
drop table if exists sandbox_ana.rory_vm_full_stock;
create table sandbox_ana.rory_vm_full_stock as (
select
coalesce(ss.calendaryear, id.calendaryear) as calendaryear,
coalesce(ss.dimstyleoptionid, id.dimstyleoptionid) as dimstyleoptionid,
nvl(ss.stock_boh, 0) as stock_boh,
nvl(id.indeliveries, 0) as indeliveries,
nvl(ss.stock_boh, 0) + nvl(id.indeliveries, 0) as total_stock
from sandbox_ana.rory_vm_stock_start ss
  full outer join sandbox_ana.rory_vm_indeliveries id on id.dimstyleoptionid = ss.dimstyleoptionid and id.calendaryear = ss.calendaryear);

-- sales metrics across the two periods
---------------------------------------------------------
drop table if exists sandbox_ana.rory_vm_sales;
create table sandbox_ana.rory_vm_sales as (
select
dd.calendaryear,
db.brandcode,
so.dimstyleoptionid,
so.noos,
sum(fo.Quantity * fo.ItemSalesEUR) as gsii,
sum(fo.Quantity * (fo.ItemRetailPriceEUR / (1 +fo.VATRate))) as gsi,
sum(fo.Quantity) as gis

from mart.fact_orderline fo
  join mart.dim_date dd on dd.dimdateid = fo.dimorderlinedateid
  join mart.dim_brand db on db.dimbrandid = fo.dimbrandid
  join sandbox_ana.rory_vm_styleoptions so on so.dimstyleoptionid = fo.dimstyleoptionid and so.calendaryear = dd.calendaryear

where ((dd.dimdate between '2017-08-01' and '2017-10-31') or (dd.dimdate between '2018-08-01' and '2018-10-31'))
  and db.brandcode in ('ON', 'VL', 'VM')
  and fo.dimshiptocountryid = 56
  and fo.dimchannelid in (4, 5)
  and fo.dimorderlinestateid = 2
  and fo.dimorderlinetypeid = 1

group by 1, 2, 3, 4);

-- Traffic data from those styleoptions
--------------------------------------------------------
drop table if exists sandbox_ana.rory_vm_traffic;
create table sandbox_ana.rory_vm_traffic as (
select
dd.calendaryear,
db.brandcode,
fsa.dim_style_option_id,
so.noos,
sum(fsa.detail_views) as PDPv,
sum(fsa.cart_adds) as items_to_cart

from mart.fact_ua_style_actions fsa
  join mart.dim_date dd on dd.dimdateid = fsa.dim_register_date_id
  join mart.dim_ua_profile dup on dup.dim_ua_profile_id = fsa.dim_ua_profile_id
  join mart.dim_country dc on dc.dimcountryid = dup.dim_country_id
  join mart.dim_style ds on ds.dimstyleid = fsa.dim_style_id
  join mart.dim_brand db on db.dimbrandid = ds.dimbrandid
  join sandbox_ana.rory_vm_styleoptions so on so.dimstyleoptionid = fsa.dim_style_option_id and so.calendaryear = dd.calendaryear

where dup.sitebrand_label in ('ON', 'VL', 'VM')
  and ((dd.dimdate between '2017-08-01' and '2017-10-31') or (dd.dimdate between '2018-08-01' and '2018-10-31'))
  and db.brandcode in ('ON', 'VL', 'VM')
  and dc.countrycode = 'DE'
group by 1, 2, 3, 4);

-- join together
select
fs.*,
ds.noos,
db.brandcode,
nvl(sales.gis, 0) as gis,
nvl(sales.gsi, 0) as gsi,
nvl(sales.gsii, 0) as gsii,
nvl(traffic.pdpv, 0) as pdpv,
nvl(traffic.items_to_cart, 0) as items_to_cart

from sandbox_ana.rory_vm_full_stock fs
  left join sandbox_ana.rory_vm_sales sales on sales.dimstyleoptionid = fs.dimstyleoptionid and sales.calendaryear = fs.calendaryear
  left join sandbox_ana.rory_vm_traffic traffic on traffic.dim_style_option_id = fs.dimstyleoptionid and traffic.calendaryear = fs.calendaryear
  join mart.dim_styleoption dso on dso.dimstyleoptionid = fs.dimstyleoptionid
  join mart.dim_style ds on ds.dimstyleid = dso.dimstyleid
  join mart.dim_brand db on db.dimbrandid = ds.dimbrandid;



