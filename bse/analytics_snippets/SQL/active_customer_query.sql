
/* Info */
-- This query is made to run in redshift

-- there are 3 main parts: first order info, all order info and the output (from combining both tables)
-- things to check: dim tables for joins (did anything change ?)
-- things to change: date in temporary table
-- A customer is set as combination of 3 dimensions: CustomerID, Brand and Country


/* Settings - select the maximum data */
CREATE TEMPORARY TABLE dates as (select dt.DimDateID from mart.dim_date dt where dt.DimDate <= '2018-07-31');

/* Get Customer first order date and reference order  - 44 minutes */
DROP TABLE IF EXISTS sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrderID;
CREATE TABLE sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrderID AS (
SELECT
	fo.DimCustomerID,
	fo.DimBrandID,
	fo.DimShipToCountryID,
	min(fo.DimOrderLineDateID) as first_order_date_id,
	SPLIT_PART(LISTAGG(distinct fo.ReferenceOrder,',') within group (order by fo.DimOrderLineDateID asc),',',1) as first_order_id

FROM mart.fact_orderline fo

-- filter join - dates
JOIN dates dt on dt.DimDateID = fo.dimorderlinedateid

WHERE fo.DimCustomerID <> 0
	and fo.ReferenceOrder <> ''
	and fo.DimOrderLineTypeID = 1
	and fo.DimChannelID not in (0,6,7)
	and fo.TotalRetailPriceEUR <> 0 
	and fo.TotalSalesEUR <> 0
	
GROUP BY 1,2,3
); 

/* Get firstOrder info per customer  */
DROP TABLE IF EXISTS sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrder;
CREATE TABLE sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrder AS (
SELECT
	dc.DimCustomerID,
	fo.DimShipToCountryID,
	fo.DimBrandID,
	dc.first_order_id as first_order_id,
	dc.first_order_date_id,
	dts.season as FirstOrderYearSeason,
	dts.season_id as FirstOrderYearSeasonNo,
	sum(case when fo.DimOrderLineStateID = 2 then fo.TotalSalesEUR else 0 end) as GSII_FirstOrder,
	sum(case when fo.DimOrderLineStateID = 2 then fo.TotalVATEUR else 0 end) as VAT_FirstOrder,
	sum(case when fo.DimOrderLineStateID = 2 then fo.TotalRetailPriceEUR else 0 end) as GSI_FirstOrder,
	sum(case when fo.DimOrderLineStateID = 2 then fo.Quantity else 0 end) as GIS_FirstOrder,
	sum(case when fo.DimOrderLineStateID IN (3,10,16) then -fo.Quantity else 0 end) as GIR_FirstOrder,
	cast(NVL(1-((sum(case when fo.DimOrderLineStateID = 2 then fo.TotalSalesEUR else 0 end) + sum(case when fo.DimOrderLineStateID= 2 then fo.TotalVATEUR else 0 end)))
		/ NULLIF(sum(case when fo.DimOrderLineStateID= 2 then fo.TotalRetailPriceEUR else 0 end),0),0) as decimal(20, 2)) as DR_FirstOrder

FROM mart.fact_orderline fo
   JOIN sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrderID dc 
		on dc.first_order_id = fo.ReferenceOrder
		and dc.DimBrandID = fo.DimBrandID
	JOIN sandbox_ana.ana_dim_date dts on dts.DimDateID = dc.first_order_date_id -- we added season and season id to dim_date

-- filter join - dates
JOIN dates dt on dt.DimDateID = fo.dimorderlinedateid

WHERE fo.DimOrderLineTypeID = 1
	and fo.DimChannelID not in (0,6,7)
	and fo.TotalRetailPriceEUR <> 0 
	and fo.TotalSalesEUR <> 0

GROUP BY 1,2,3,4,5,6,7
); 

/* Get the full order history per customer */
DROP TABLE IF EXISTS sandbox_ana.ActiveCustomer_Brands_CustomerOrders;
CREATE TABLE sandbox_ana.ActiveCustomer_Brands_CustomerOrders AS (
SELECT 
	dc.DimCustomerID,
	dc.DimBrandID,
	dc.DimShipToCountryID,
	fo.ReferenceOrder,
	dts.season as OrderYearSeason,
	dts.season_id as OrderYearSeasonNo,
	sum(case when fo.DimOrderLineStateID=2 then fo.TotalSalesEUR else 0 end) as GSII_InSeason,
	sum(case when fo.DimOrderLineStateID=2 then fo.Quantity else 0 end) as GIS_InSeason,
	count(distinct fo.ReferenceOrder) AS TotalOrders_InSeason,
	sum(case when fo.DimOrderLineStateID=2 then fo.TotalVATEUR else 0 end) as VAT_InSeason,
	sum(case when fo.DimOrderLineStateID=2 then fo.TotalRetailPriceEUR else 0 end) as GSI_InSeason,
	sum(case when fo.DimOrderLineStateID IN (3,10,16) then -fo.TotalSalesEUR else 0 end) as GRII_InSeason,
	sum(case when fo.DimOrderLineStateID IN (3,10,16) then -fo.Quantity else 0 end) as GIR_InSeason
FROM	mart.fact_orderline fo 

JOIN sandbox_ana.ana_dim_date dts on fo.DimOrderLineDateID=dts.DimDateID
JOIN sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrderID dc 
	on dc.DimCustomerID = fo.DimCustomerID
	and dc.DimBrandID = fo.DimBrandID
	and dc.DimShipToCountryID = fo.DimShipToCountryID

-- filter join - dates
JOIN dates dt on dt.DimDateID = fo.dimorderlinedateid

WHERE fo.ReferenceOrder <> ''
	and fo.DimOrderLineTypeID = 1
	and fo.DimChannelID not in (0,6,7)
	and fo.DimMarketID <> 0 
	and fo.TotalRetailPriceEUR <> 0 
	and fo.TotalSalesEUR <> 0

GROUP BY 1,2,3,4,5,6
); 

/* Get the Customer Type history per customer per season */
DROP TABLE IF EXISTS sandbox_ana.ActiveCustomer_Brands_Analysis;
CREATE TABLE sandbox_ana.ActiveCustomer_Brands_Analysis AS (
SELECT
	t1.DimCustomerID, 
	t1.DimBrandID,
	t1.DimShipToCountryID,
	t1.OrderYearSeasonNo,
	case 
		when (t1.OrderYearSeasonNo = t3.FirstOrderYearSeasonNo) then 'NC'
		when (t1.OrderYearSeasonNo - 1 = t3.FirstOrderYearSeasonNo) then 'NC-1'
		when (t1.OrderYearSeasonNo - 2 = t3.FirstOrderYearSeasonNo and POSITION((t1.OrderYearSeasonNo-1) IN cl.ordList) = 0 ) then 'NC-2'
		when (t1.OrderYearSeasonNo -  t3.FirstOrderYearSeasonNo > 1 and POSITION((t1.OrderYearSeasonNo-1) IN cl.ordList) > 0) then 'AC-1'
		when ((t1.OrderYearSeasonNo -  t3.FirstOrderYearSeasonNo > 2 and POSITION((t1.OrderYearSeasonNo-1) IN cl.ordList) = 0) 
			and POSITION((t1.OrderYearSeasonNo-2) IN cl.ordList) > 0) then 'AC-2'
	else 'REST' end as CustomerType

FROM sandbox_ana.ActiveCustomer_Brands_CustomerOrders t1
	LEFT JOIN sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrder t3
		on t1.DimCustomerID = t3.DimCustomerID
		and t1.DimShipToCountryID = t3.DimShipToCountryID
		and t1.DimBrandID = t3.DimBrandID
	JOIN (
			SELECT
				t1.DimCustomerID,
				t1.DimBrandID,
				t1.DimShipToCountryID,
				LISTAGG(distinct t1.OrderYearSeasonNo,',') as ordList
			
			FROM sandbox_ana.ActiveCustomer_Brands_CustomerOrders t1
			
			GROUP BY 1,2,3) cl
				on cl.DimCustomerID = t3.DimCustomerID
				and cl.DimShipToCountryID = t3.DimShipToCountryID
				and cl.DimBrandID = t3.DimBrandID
	
GROUP BY 1,2,3,4,5
); 

/* Combine first order info with full customer history */
DROP TABLE IF EXISTS sandbox_ana.ActiveCustomer_Brands_Output;
CREATE TABLE sandbox_ana.ActiveCustomer_Brands_Output AS (
SELECT
	tg.DimBrandID,
	tg.DimShipToCountryID,
	tg.CustomerType,
	tg.OrderYearSeason,
	tg.OrderYearSeasonNo,
	AVG(tg.DR_FirstOrder) as avg_DR_FirstOrder,
	count(distinct tg.DimCustomerID) as Customers_InSeason,
	sum(tg.GSII_InSeason) as GSII_InSeason,
	sum(tg.TotalOrders_InSeason) as TotalOrders_InSeason,
	sum(tg.GIS_InSeason) as GIS_InSeason,
	sum(tg.VAT_InSeason) as VAT_InSeason,
	sum(tg.GSI_InSeason) as GSI_Inseason,
	sum(tg.GRII_InSeason) as GRII_Inseason,
	sum(tg.GIR_InSeason) as GIR_Inseason

FROM (
		SELECT
			t1.*,
			t2.FirstOrderYearSeasonNo,
			t2.GSII_FirstOrder,
			t2.VAT_FirstOrder,
			t2.GSI_FirstOrder,
			t2.GIS_FirstOrder,
			t2.GIR_FirstOrder,
			t2.DR_FirstOrder,
			ti.CustomerType
		
		FROM sandbox_ana.ActiveCustomer_Brands_CustomerOrders t1
			LEFT JOIN sandbox_ana.ActiveCustomer_Brands_Analysis ti 
				on ti.DimCustomerID = t1.DimCustomerID
				and ti.DimBrandID = t1.DimBrandID
				and ti.DimShipToCountryID = t1.DimShipToCountryID
				and ti.OrderYearSeasonNo = t1.OrderYearSeasonNo
			JOIN sandbox_ana.ActiveCustomer_Brands_CustomerFirstOrder t2 
				on t2.DimCustomerID = t1.DimCustomerID
				and t2.DimBrandID = t1.DimBrandID
				and t2.DimShipToCountryID = t1.DimShipToCountryID
		) tg
	
GROUP BY 1,2,3,4,5
);

SELECT
DISTINCT
db.BrandCode,
dc.CountryCode,
ac.CustomerType,
ac.OrderYearSeason,
ac.OrderYearSeasonNo,
ac.avg_DR_FirstOrder,
ac.Customers_InSeason,
ac.GSII_InSeason,
ac.TotalOrders_InSeason,
ac.GIS_InSeason,
ac.VAT_InSeason,
ac.GSI_Inseason,
ac.GRII_Inseason,
ac.GIR_Inseason

FROM sandbox_ana.ActiveCustomer_Brands_Output ac 
JOIN mart.dim_brand db ON db.DimBrandID = ac.DimBrandID
JOIN mart.dim_country dc ON dc.DimCountryID = ac.DimShipToCountryID

WHERE dc.CountryCode is not null
and db.BrandCode is not null
;
