# fact_stock and agg_stock_day have 1 day difference
# here there's one example on how to match the tables

use mart;
 
select agg.dim_sku_id, sum(agg.stock_qty) as agg_stock_qty, sum(fact.Qty) as fact_stock_qty
from agg_stock_day agg
inner join fact_stock partition (P201802) fact
on agg.dim_date_id = fact.DimDateID - 1
and agg.dim_sku_id = fact.DimSKUID
where agg.dim_date_id = 4071
group by dim_sku_id
having sum(agg.stock_qty) <> sum(fact.Qty); 
