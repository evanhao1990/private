## Conclusion: do not include damaged goods' cost in CM1 query

drop table if exists sandbox.damaged_categories;
create table sandbox.damaged_categories(
	SELECT
		ds.ProductCategory, 
		sum(fo.TotalSalesEUR * -1) as GRII,
		sum(fo.Quantity * -1) as QTY,
		fst.dam_qty as nb_damaged,
		fst.dam_qty / sum(fo.Quantity * -1) as share_damaged
	
	
	FROM mart.fact_orderline fo
	
	JOIN mart.dim_date dt on dt.DimDateID = fo.DimReturnDateID
	JOIN mart.dim_style ds on ds.DimStyleID = fo.DimStyleID
	
	LEFT JOIN(
		select
			ds.ProductCategory,
			sum(fs.StockAtHandQuantity * -1) as dam_qty
		from mart.fact_stockmovements fs 
		
		JOIN mart.dim_date dt on dt.DimDateID = fs.DimOrderCompilationDateID
		JOIN mart.dim_style ds on ds.DimStyleID = fs.DimStyleID
		
		where dt.DimDate between '2016-08-01' and '2017-08-31'
			and fs.DimStockMovementsReasonCodeID in (12,13,48,32)
			
		group by 1
		
		) fst on fst.ProductCategory = ds.ProductCategory
	
	
	where dt.DimDate between '2016-08-01' and '2017-08-31'
		and fo.DimOrderLineTypeID = 1
		and fo.DimOrderLineStateID in (3,10,16)
	
	group by 1,4
	limit 0
)
;
insert into sandbox.damaged_categories (
	SELECT
		ds.ProductCategory, 
		sum(fo.TotalSalesEUR * -1) as GRII,
		sum(fo.Quantity * -1) as QTY,
		fst.dam_qty as nb_damaged,
		fst.dam_qty / sum(fo.Quantity * -1) as share_damaged
	
	
	FROM mart.fact_orderline fo
	
	JOIN mart.dim_date dt on dt.DimDateID = fo.DimReturnDateID
	JOIN mart.dim_style ds on ds.DimStyleID = fo.DimStyleID
	
	LEFT JOIN(
		select
			ds.ProductCategory,
			sum(fs.StockAtHandQuantity * -1) as dam_qty
		from mart.fact_stockmovements fs 
		
		JOIN mart.dim_date dt on dt.DimDateID = fs.DimOrderCompilationDateID
		JOIN mart.dim_style ds on ds.DimStyleID = fs.DimStyleID
		
		where dt.DimDate between '2016-08-01' and '2017-08-31'
			and fs.DimStockMovementsReasonCodeID in (12,13,48,32)
			
		group by 1
		
		) fst on fst.ProductCategory = ds.ProductCategory
	
	
	where dt.DimDate between '2016-08-01' and '2017-08-31'
		and fo.DimOrderLineTypeID = 1
		and fo.DimOrderLineStateID in (3,10,16)
	
	group by 1,4
)

select * from sandbox.damaged_categories