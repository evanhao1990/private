SET @d1 = (SELECT dd.DimDateID -1 FROM mart.dim_date dd WHERE dd.DimDate = CURDATE()); 
SET @d2 = (SELECT dd.DimDateID -28 FROM mart.dim_date dd WHERE dd.DimDate = CURDATE());
SET @d3 = (SELECT dd.DimDateID -1 -365 FROM mart.dim_date dd WHERE dd.DimDate = CURDATE()); 
SET @d4 = (SELECT dd.DimDateID -28 -365 FROM mart.dim_date dd WHERE dd.DimDate = CURDATE());

#Sales

SELECT
CASE 	WHEN dd.DimDateID BETWEEN @d2 AND @d1 THEN 'CURRENT'
		WHEN dd.DimDateID BETWEEN @d4 AND @d3 THEN 'LY' ELSE 'NA' END AS 'TIME',
CASE 	WHEN od.dim_channel_id IN (2,3,6) THEN 'BS.com' 
		WHEN od.dim_channel_id IN (4,5) THEN 'BRAND.com' 	
		WHEN od.dim_channel_id IN (7) THEN 'PARTNER.com' ELSE 'NA' END AS BUSINESS_AREA,
su.IBrandLabel,
upper(sty.ProductCategory) as ProductCategory,
CASE WHEN sty.NOOS IN (0) THEN 'Non-NOOS' ELSE 'NOOS' END AS NOOS,
CASE	WHEN ag.age_category = 'A. New (0-4w)' THEN 'A. New (0-8w)' 	
		WHEN ag.age_category = 'B. Current (4-8w)' THEN 'A. New (0-8w)'
		WHEN ag.age_category = 'C. Mature (8-12w)' THEN 'B. Mature (8-16w)'
		WHEN ag.age_category = 'D. Old (12-16w)' THEN 'B. Mature (8-16w)'
		WHEN ag.age_category = 'E. Late (>16w)' THEN 'C. Old (>16w)' ELSE 'UNKOWN' END AS AGE_CATEGORY,	
SUM(od.gs1) AS GS_I_OD,
SUM(od.gs2) AS GS_II_OD,
SUM(od.disc_value) AS DISC_VAL,
SUM(od.gis1) AS GIS_I,
SUM(od.cogs) - SUM(od.cogr_fd) AS NET_COGS_ED

FROM mart.agg_orderline_day od
	JOIN mart.dim_date dd ON dd.DimDateID = od.dim_date_id
	JOIN mart.dim_subbrand su ON su.DimSubBrandID = od.dim_subbrand_id
	JOIN mart.dim_style sty ON sty.DimStyleID = od.dim_style_id
	JOIN mart.agg_product_age_day da ON da.dim_sku_id = od.dim_sku_id
												AND da.dim_date_id = od.dim_date_id
	JOIN mart.vw_dim_age_category ag ON ag.age_category_id = da.age_cat_styleoption_last_indelivery
	
WHERE ((dd.DimDateID BETWEEN @d2 AND @d1)
or (dd.DimDateID BETWEEN @d4 AND @d3))
AND sty.ProductCategory NOT IN ('MARKETING','RESELLABLE MARKETING','GIFT CARDS') 

GROUP BY 1,2,3,4,5,6
;

#Stock

SELECT
CASE 	WHEN dd.DimDateID = @d1 THEN 'CURRENT'
		WHEN dd.DimDateID = @d3 THEN 'LY' ELSE 'NA' END AS 'TIME',
su.IBrandLabel,
sty.ProductCategory as ProductCategory,
CASE WHEN sty.NOOS IN (0) THEN 'Non-NOOS' ELSE 'NOOS' END AS NOOS,
CASE	WHEN ag.age_category = 'A. New (0-4w)' THEN 'A. New (0-8w)' 	
		WHEN ag.age_category = 'B. Current (4-8w)' THEN 'A. New (0-8w)'
		WHEN ag.age_category = 'C. Mature (8-12w)' THEN 'B. Mature (8-16w)'
		WHEN ag.age_category = 'D. Old (12-16w)' THEN 'B. Mature (8-16w)'
		WHEN ag.age_category = 'E. Late (>16w)' THEN 'C. Old (>16w)' ELSE 'UNKOWN' END AS AGE_CATEGORY,	
CASE	WHEN st.mov_cat_styleoption = 'A. Fast Movers' THEN 'A. Fast Movers'
		WHEN st.mov_cat_styleoption = 'B. Medium Movers' THEN 'B. Medium Movers'
		WHEN st.mov_cat_styleoption = 'C. Slow Movers' THEN 'C. Slow-Not Movers'
		WHEN st.mov_cat_styleoption = 'D. Non Movers' THEN 'C. Slow-Not Movers'
		WHEN st.mov_cat_styleoption = 'E. Not Moving' THEN 'C. Slow-Not Movers' ELSE 'UNKOWN' END AS MOVERS_CAT_SO,
SUM(st.stock_qty) AS STOCK_QTY_EOH,
SUM(st.stock_qty * sk.CostPriceEUR) AS STOCK_VAL_EOH,
SUM(st.gis1_7d) AS GIS_7d

FROM mart.agg_stock_day st
	JOIN mart.dim_date dd ON dd.DimDateID = st.dim_date_id
	JOIN mart.dim_sku sk ON sk.DimSKUID = st.dim_sku_id
	JOIN mart.agg_product_age_day da ON da.dim_sku_id = st.dim_sku_id
												AND da.dim_date_id = st.dim_date_id
	JOIN mart.vw_dim_age_category ag ON ag.age_category_id = da.age_cat_styleoption_last_indelivery
	JOIN mart.dim_subbrand su ON su.DimSubBrandID = st.dim_subbrand_id
	JOIN mart.dim_style sty ON sty.DimStyleID = st.dim_style_id
												
WHERE ((dd.DimDateID = @d1)
OR (dd.DimDateID =@d3))
AND st.stock_qty NOT IN (0)
AND sty.ProductCategory NOT IN ('MARKETING','RESELLABLE MARKETING','GIFT CARDS') 
	
GROUP BY 1,2,3,4,5,6