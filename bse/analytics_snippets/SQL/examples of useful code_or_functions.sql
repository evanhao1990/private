## Tricks and ideas
############################################
## Join tables on column
select
* 
from (
select t1.* from t1
UNION
select t2.* from t2
)

################################################
## Totals/subtotals
select
a_month
a_quarter
brand
sum(gsi)
from fo
group by 1,2,3 with rollup

##################################################
## Utilities
SET sql_mode = ''; 																								## resets sql mode - no need for full group by

SET @season_end_date = '2017-07-31';  																		# create variable
CREATE INDEX idx_1 ON sandbox.customer_order_id (first_order_id(15), DimBrandID);   		# create individual or grouped indexes

# SET variable with multiple 'sub variables' to then filter the table
SET @brands = (select GROUP_CONCAT(db.DimBrandID) from mart.dim_brand db where db.BrandCode in ('PC', 'MM')); 
SELECT ...
FROM  bla
WHERE FIND_IN_SET(ol.DimBrandID, @brands)

# basics
count(distinct ...)
sum(case when ... then ... else ... end)
min, max, avg
datediff(t1.DimDate, t2.DimDate)
case when ... then ...
	when ... then ...
	else ... end

set session group_concat_max_len = 10000			
group_concat(distinct orders) 																				## creates list with orders

substring_index(group_concat(distinct orders order by dates asc), ',', 2) 						## gets 2nd order
INSTR(dc.Campaign, 'JJ') = 0 																					## checks if 'JJ' is in campaign name
case when CustomerEmail like "%gmail%" then 1 else 0 end as isGoogle								# checks if email string has 'gmail'
FIND_IN_SET(ol.DimBrandID, @brands)																			## checks if values are in list
WHERE FIND_IN_SET(fo.DimSubBrandID,@brands)																## filter for brand ids in @brands list
IFNULL(X,y)																											## IF X is null then y
NULLIF(X, y)																										## IF X is y then null
IF(A,B,C)																											# IF A then B eLse C

DROP TABLE IF EXISTS blabla;
CREATE TABLE blabla ( ..... )
