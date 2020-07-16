-- which payment method was used?
drop table if exists sandbox_ana.bba1278_payment_method;
create table sandbox_ana.bba1278_payment_method as (
select
fp.referenceorder,
listagg(distinct pi.paymentinstrumentname, ',') within group (order by pi.paymentinstrumentname) as paymentinstrumentname,
count(1) as counts
from mart.fact_payment fp
  join mart.dim_paymentinstrument pi on pi.dimpaymentinstrumentid = fp.dimpaymentinstrumentid
  join mart.dim_paymentstate ps on ps.dimpaymentstateid = fp.dimpaymentstateid
  join mart.dim_date dd on dd.dimdateid = fp.dimdatecreatedid
where dd.dimdate between '2017-08-01' and '2018-10-31'
group by 1
having counts = 1);