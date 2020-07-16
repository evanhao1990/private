/*
Example of New customer dataset

Customer: CustomerID (or email), country and DimBrandID

*/

-- 1:  first ReferenceOrder and date per customer

DROP TABLE IF EXISTS sandbox.customer_order_id;
CREATE TABLE sandbox.customer_order_id AS (
	SELECT DISTINCT
		fo.CustomerEmail,
		dc.DimCustomerID,
		fo.DimBrandID,
		fo.DimShipToCountryID,
		min(fo.DimOrderLineDateID) as first_order_date_id,
		SUBSTRING_INDEX(GROUP_CONCAT(distinct fo.ReferenceOrder ORDER BY fo.DimOrderLineDateID ASC), ',', 1) as first_order_id	
		
		FROM mart.dim_customer dc 
		
		JOIN mart.fact_orderline fo on dc.CustomerEmail = fo.CustomerEmail
		
		WHERE fo.CustomerEmail <> ''
			and fo.ReferenceOrder <> ''
		GROUP BY 1,2,3,4
);
CREATE INDEX idx_1 ON sandbox.customer_order_id (first_order_id(15), DimBrandID);
CREATE INDEX idx_2 ON sandbox.customer_order_id (CustomerEmail, DimShipToCountryID, DimBrandID);

-- 2: Assign customer type for every order in time range 

DROP TABLE IF EXISTS sandbox.customer_type;
CREATE TABLE sandbox.customer_type AS (
	SELECT DISTINCT
		co.DimCustomerID,
		co.DimBrandID,
		co.DimShipToCountryID,
		
		fo.ReferenceOrder,
		CASE WHEN fo.ReferenceOrder = co.first_order_id THEN 'NC' ELSE 'RC' END AS customer_type
		
		FROM mart.fact_orderline fo
		JOIN mart.dim_date dt ON dt.DimDateID = fo.DimOrderLineDateID
		
		JOIN sandbox.customer_order_id co ON co.CustomerEmail = fo.CustomerEmail
				AND co.DimBrandID = fo.DimBrandID
				AND co.DimShipToCountryID = fo.DimShipToCountryID
				
		WHERE dt.DimDate BETWEEN '2017-08-01' AND '2018-01-31'
			and fo.CustomerEmail <> ''
			and fo.ReferenceOrder <> ''
		GROUP BY 1,2,3,4,5
);

