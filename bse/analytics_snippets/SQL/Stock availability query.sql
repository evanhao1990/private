########### Stock QtY & AR for the first 4 live weeks
SET @d1 = (select dt.dimdateid from mart.dim_date dt where dt.DimDate = '2017-01-01');
SET @d2 = (select dt.dimdateid from mart.dim_date dt where dt.DimDate = '2017-01-31');

SELECT
	stk_data.DimStyleOptionID,
	avg(stk_data.stock_qty) as Stock_Qty,
	avg(stk_data.AR) as SA

FROM (			# stock availability & qty per day per SO - for the first 5 live days
		SELECT
			dt.DimDate,
			full_sizes.DimStyleOptionID,
			sum(agg.stock_qty) as stock_qty,
			count(distinct agg.dim_sku_id) / full_sizes.total_number_SKU as AR
		
		FROM (  	# total number of sku's & GoLiveDay per SO
					SELECT
						sku.DimStyleOptionID,
						min(sku.GoLiveDate) as GoLiveDate,
						min(sku.DimGoLiveDateID) as GoLiveDateID,
						min(sku.DimGoLiveDateID) + 5 as after_5_days,
						COUNT(DISTINCT sku.DimSKUID) AS total_number_SKU
					FROM	mart.dim_sku sku
					WHERE sku.DimBrandID = 6					# VL
					GROUP BY 1
			) full_sizes
		
		JOIN mart.agg_stock_day agg ON full_sizes.DimStyleOptionID = agg.dim_styleoption_id
		JOIN mart.dim_date dt on dt.DimDateID=agg.dim_date_id
			
		WHERE full_sizes.GoLiveDateID BETWEEN @d1 AND @d2
			and agg.dim_date_id BETWEEN @d1 AND @d2
			and agg.dim_brand_id = 6								# VL
		
		GROUP BY 1,2) stk_data
		
GROUP BY 1


