/*
  **** Calculate CM1 *************

 The main approch consists in joining two tables:
 	- item level, ic -> per SKU in every order (metrics and costs which which are needed in item level, eg. intake cost)
 	- order level, oc -> per distinct order (metrics and costs which which are needed in order level, eg. shipping cost)
Thus, order costs are converted into item level

Instructions:
- set filters
- select attributes
- run query
 */

# Filters
SET @first_date = (SELECT dt.DimDateID FROM mart.dim_date dt WHERE dt.DimDate = '2017-07-01');
SET @last_date = (SELECT dt.DimDateID FROM mart.dim_date dt WHERE dt.DimDate = '2017-07-31');
SET @brands = (SELECT GROUP_CONCAT(sb.DimSubBrandID) FROM mart.dim_subbrand sb WHERE sb.IBrandLabel IN ('VL'));
SET @countries = (SELECT GROUP_CONCAT(ct.DimCountryID) FROM mart.dim_country ct WHERE ct.CountryCode IN ('DE'));
SET @channels = (SELECT GROUP_CONCAT(ch.DimChannelID) FROM mart.dim_channel ch WHERE ch.DimChannelID IN (2, 3, 4, 5));
# SET @marketplaces = (SELECT GROUP_CONCAT(mk.DimMarketplaceID) FROM mart.dim_marketplace mk WHERE mk.MarketplaceCode in ('ZA'));


DROP TABLE IF EXISTS sandbox.cm1_query;
CREATE TABLE sandbox.cm1_query(
	SELECT
		# Attributes
		dt.CalendarMonth,
		pre_cm1.DimBrandID,
		# pre_cm1.DimMarketplaceID,
		# pre_cm1.DimChannelID,
		# pre_cm1.DimShipToCountryID,

		SUM(pre_cm1.GSII_eV) AS GSII_eV,
		
		SUM(pre_cm1.NS) AS NS,
		
		-SUM(pre_cm1.COGS_net) AS COGS_net,
		SUM(pre_cm1.sales_service) AS sales_service,
		-(SUM(pre_cm1.fixed_paym_cost) + SUM(pre_cm1.var_paym_cost) + SUM(pre_cm1.return_fixed_cost)) AS payment_cost,
		-SUM(pre_cm1.warehouse_costs) AS warehousing_cost,
		-SUM(pre_cm1.distribution_cost) AS distribution_cost,

		SUM(pre_cm1.NS)
		- SUM(pre_cm1.COGS_net)
		- SUM(pre_cm1.warehouse_costs)
		- SUM(pre_cm1.distribution_cost)
		- (SUM(pre_cm1.fixed_paym_cost) + SUM(pre_cm1.var_paym_cost) + SUM(pre_cm1.return_fixed_cost))
		+ SUM(pre_cm1.sales_service) AS estimated_CM1

	FROM ( # Item and Order level metrics are combined and converted in SKU per ReferenceOrder metrics
		SELECT
			oc.DimOrderLineDateID,
			ic.ReferenceOrder,
			ic.DimBrandID,
			ic.DimSubBrandID,
			ic.DimShipToCountryID,
			ic.DimChannelID,
			ic.DimMarketplaceID,
			ic.SKU,
			ic.DimSKUID,
			ic.ProductCategory,
			ic.DimStyleOptionID,
			ic.DimStyleID,
			
			# packaging is a cost per order - needs to be assgined the same % cost to each item in the order
			ic.GIS*(wh1.cost_value + ic.pick_pack_item + oc.packaging_order/oc.GIS + ic.custom_item) + ic.GIR*(wh4.cost_value + ic.cleaning_item) AS warehouse_costs,

			# shipping/returning cost is a cost per order - needs to be assigned the same % cost to each item in the order/return
			ic.GIS*(oc.shipping_cost / oc.GIS) + ic.GIR*(CASE WHEN oc.GIR > 0 THEN oc.returning_cost / oc.GIR ELSE 0 END) AS distribution_cost,

			# Payment fixed fee is apportioned by GIS
			ic.GIS*(IFNULL(dpp.Paym_fixed_fee,0) / oc.GIS) AS fixed_paym_cost,

			# Payment variable fee is apportioned according to the share of GSII of each item in the order
			(IFNULL(dpp.Paym_var_fee,0) * fp.AmountEUR)*ic.GSII_eV/oc.GSII_eV AS var_paym_cost,

			# Refund fixed fee is apportioned equally between the number of returned items in the order
			ic.GIR*(CASE WHEN oc.GIR > 0 THEN IFNULL(dpp.Refund_fixed_fee,0) / oc.GIR ELSE 0 END) AS return_fixed_cost,

         # Sales service costs are apportioned equally between items returned and dispatched. Dispatch costs and Return costs are summed.
			ic.GIS*((oc.sales_service_shipping + oc.sales_service_payment) / oc.GIS) +
					ic.GIR*(CASE WHEN oc.GIR > 0 THEN (oc.sales_service_return+oc.refund_sales_service_shipping+oc.refund_sales_service_payment)/oc.GIR ELSE 0 END) AS sales_service,

			ic.COGS_net,

			SUM(ic.GIS) AS GIS,
			SUM(ic.GIR) AS GIR,
			SUM(ic.GSII_eV) AS GSII_eV,
			SUM(ic.NS) AS NS

		FROM (	# Item level - per SKU in ReferenceOrder item costs and sales metrics are calculated
				SELECT
					fo.ReferenceOrder,
					fo.SKU,
					fo.DimBrandID,
					fo.DimSubBrandID,
					ds.ProductCategory,
					fo.DimChannelID,
					fo.DimMarketplaceID,
					fo.DimShipToCountryID,
					fo.DimSKUID,
					fo.DimStyleID,
					fo.DimStyleOptionID,

					# cost1 and cost4 are added so we can join with the sub costs with no drivers (intake and returning)
					1 AS cost1,
					4 AS cost4,

					# intake has 2 types of drivers (Brand if DIRECT channels, Marketplace IF NOT)
					CASE WHEN fo.DimMarketplaceID = 0 THEN wh2a.cost_value ELSE wh2b.cost_value END AS pick_pack_item,

					IFNULL(wh5.cost_value, 0) AS cleaning_item,
					IFNULL(wh7.cost_value, 0) AS custom_item,
		
				# GIS
					SUM(CASE WHEN fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) THEN fo.Quantity ELSE 0 END) AS GIS,
				# GIR
					SUM(CASE WHEN fo.DimOrderLineStateID IN (3,10,16,18) THEN fo.Quantity ELSE 0 END * -1.0) AS GIR,
				# GSI
					SUM(CASE WHEN fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) AND fo.DimOrderLineTypeID=1 THEN (fo.Quantity * (fo.ItemRetailPriceEUR / (1 +fo.VATRate))) ELSE 0 END) AS GSI,
				# GSII										
					SUM(CASE WHEN fo.DimMarketplaceID NOT IN (1,9) AND fo.DimChannelID<>6 AND fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) THEN (fo.Quantity * fo.ItemSalesEUR)
						WHEN (fo.DimMarketplaceID IN (1,9) OR fo.DimChannelID=6) AND fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) THEN (fo.Quantity * (fo.ItemRetailPriceEUR / (1+fo.VATRate)))
						ELSE 0 END) AS GSII_eV,
				# GRII
					(-(SUM(CASE WHEN fo.DimMarketplaceID NOT IN (1,9) AND fo.DimChannelID<>6 AND fo.DimOrderLineStateID IN (3,10,16,18) THEN (fo.Quantity * fo.ItemSalesEUR)
						WHEN (fo.DimMarketplaceID IN (1,9) OR fo.DimChannelID=6) AND fo.DimOrderLineStateID IN (3,10,16,18) THEN (fo.Quantity * (fo.ItemRetailPriceEUR / (1+fo.VATRate)))
						ELSE 0 END))) GRII,
				# NS
					SUM(CASE WHEN fo.DimMarketplaceID NOT IN (1,9) AND fo.DimChannelID<>6 AND fo.DimOrderLineStateID NOT IN (4,17) THEN (fo.Quantity * fo.ItemSalesEUR)
						WHEN (fo.DimMarketplaceID IN (1,9) OR fo.DimChannelID=6) AND fo.DimOrderLineStateID NOT IN (4,17) THEN (fo.Quantity * (fo.ItemRetailPriceEUR / (1+fo.VATRate)))
						ELSE 0 END) AS NS,
				# COGS (COGS + COGS returned)
					SUM(CASE WHEN fo.DimOrderLineStateID NOT IN (4,17) THEN (fo.Quantity * fo.ItemCostEUR) ELSE 0 END) AS COGS_net

				FROM mart.fact_orderline fo

				JOIN mart.dim_style ds ON ds.DimStyleID = fo.DimStyleID

				# FOR EVERY cost we have to join the sub subcost table and specify its dim_sub_cost_type_id
				# wh2a has the pick and pack cost for direct channels (marketplace = 0) - DimBrandID is the driver
				LEFT JOIN sandbox.dim_WH_costs wh2a ON wh2a.driver_id = fo.DimBrandID
					AND fo.DimMarketplaceID = 0	# DIRECT CHANNEL
					AND wh2a.dim_driver_type_id = 1
					AND wh2a.dim_sub_cost_type_id = 2

				# wh2b has the pick and pack cost for non direct channels (marketplace <> 0) - DimMarketplaceID is the driver
				LEFT JOIN sandbox.dim_WH_costs wh2b ON wh2b.driver_id = fo.DimMarketplaceID
					AND fo.DimMarketplaceID <> 0	# PARTNERS
					AND wh2b.dim_driver_type_id = 2
					AND wh2b.dim_sub_cost_type_id = 2

				# Cleaning Cost varies per ProductCategory
				LEFT JOIN sandbox.dim_WH_costs wh5 ON wh5.driver_id = ds.ProductCategory
					AND wh5.dim_sub_cost_type_id = 5

				# Custom cost varies per Country
				LEFT JOIN sandbox.dim_WH_costs wh7 ON wh7.driver_id = fo.DimShipToCountryID			# Custom Cost
					AND wh7.dim_sub_cost_type_id = 7

				WHERE fo.DimOrderLineDateID BETWEEN @first_date AND @last_date
					AND fo.DimOrderLineTypeID = 1
					AND FIND_IN_SET(fo.DimSubBrandID,@brands)
					AND FIND_IN_SET(fo.DimShipToCountryID, @countries)
					AND FIND_IN_SET(fo.DimChannelID, @channels)
					# AND FIND_IN_SET(fo.DimMarketplaceID, @marketplaces)
				GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
		) ic

		# Costs with no drivers - same cost for every item
		LEFT JOIN sandbox.dim_WH_costs wh1 ON wh1.dim_sub_cost_type_id = ic.cost1						# Intake
		LEFT JOIN sandbox.dim_WH_costs wh4 ON wh4.dim_sub_cost_type_id = ic.cost4						# Returning Processing

	  JOIN ( # Order level - per ReferenceOrder costs and sales metrics are culculated
				SELECT
					fo.DimOrderLineDateID,
					fo.ReferenceOrder,

					wh3.cost_value AS packaging_order,

					IFNULL(dp.ShippingCost, 0) AS shipping_cost,
					IFNULL(dp.ReturningCost, 0) AS returning_cost,
				
				# GIS
					SUM(CASE WHEN fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) AND fo.DimOrderLineTypeID=1 THEN fo.Quantity ELSE 0 END) AS GIS,
				# GIR
					SUM((CASE WHEN fo.DimOrderLineStateID IN (3,10,16,18) AND fo.DimOrderLineTypeID=1 THEN fo.Quantity ELSE 0 END * -1.0)) AS GIR,
				# GSII inc VAT (For payment cost calculation)
					SUM(CASE WHEN fo.DimMarketplaceID NOT IN (1,9) AND fo.DimChannelID<>6 AND fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) AND fo.DimOrderLineTypeID=1 THEN ((fo.Quantity * fo.ItemSalesEUR)*(1 +fo.VATRate))
						WHEN (fo.DimMarketplaceID IN (1,9) OR fo.DimChannelID=6) AND fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) AND fo.DimOrderLineTypeID=1 THEN (fo.Quantity * fo.ItemRetailPriceEUR)
						ELSE 0 END) AS GSII_iV,
				# GSII ex VAT (P/L)
					SUM(CASE WHEN fo.DimMarketplaceID NOT IN (1,9) AND fo.DimChannelID<>6 AND fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) AND fo.DimOrderLineTypeID=1 THEN (fo.Quantity * fo.ItemSalesEUR)
						WHEN (fo.DimMarketplaceID IN (1,9) OR fo.DimChannelID=6) AND fo.DimOrderLineStateID NOT IN (3,10,4,16,17,18) AND fo.DimOrderLineTypeID=1 THEN (fo.Quantity * (fo.ItemRetailPriceEUR / (1 +fo.VATRate)))
						ELSE 0 END) AS GSII_eV,
				# GRII
					(-(SUM(CASE WHEN fo.DimMarketplaceID NOT IN (1,9) AND fo.DimChannelID<>6 AND fo.DimOrderLineStateID IN (3,10,16,18) AND fo.DimOrderLineTypeID=1 THEN (fo.Quantity * fo.ItemSalesEUR)
						WHEN (fo.DimMarketplaceID IN (1,9) OR fo.DimChannelID=6) AND fo.DimOrderLineStateID IN (3,10,16,18) AND fo.DimOrderLineTypeID=1 THEN (fo.Quantity * (fo.ItemRetailPriceEUR / (1 +fo.VATRate)))
							ELSE 0 END))) AS GRII,

					SUM(CASE WHEN fo.DimOrderLineTypeID=2 AND fo.DimOrderLineStateID=2 THEN fo.TotalSalesEUR * (1+fo.VATRate) ELSE 0 END) AS sales_service_shipping,
					SUM(CASE WHEN fo.DimOrderLineTypeID=3 AND fo.DimOrderLineStateID=2 THEN fo.TotalSalesEUR * (1+fo.VATRate) ELSE 0 END) AS sales_service_payment,
					SUM(CASE WHEN fo.DimOrderLineTypeID=6 AND fo.DimOrderLineStateID=10 THEN fo.TotalSalesEUR * (1+fo.VATRate) ELSE 0 END) AS sales_service_return,
					SUM(CASE WHEN fo.DimOrderLineTypeID=2 AND fo.DimOrderLineStateID=10 THEN fo.TotalSalesEUR * (1+fo.VATRate) ELSE 0 END) AS refund_sales_service_shipping,
					SUM(CASE WHEN fo.DimOrderLineTypeID=3 AND fo.DimOrderLineStateID=10 THEN fo.TotalSalesEUR * (1+fo.VATRate) ELSE 0 END) AS refund_sales_service_payment

				FROM mart.fact_orderline fo

				JOIN ( # join with fact orderline - here we are filtering fact_orderline (it will look only at the orders we want to)
					SELECT DISTINCT ol.ReferenceOrder, ol.DimOrderLineDateID
					FROM mart.fact_orderline ol
					WHERE ol.DimOrderLineDateID BETWEEN @first_date AND @last_date
						AND FIND_IN_SET(ol.DimSubBrandID, @brands)
						AND FIND_IN_SET(ol.DimShipToCountryID, @countries)
						AND FIND_IN_SET(ol.DimChannelID, @channels)
						# AND FIND_IN_SET(ol.DimMarketplaceID, @marketplaces)
					) ol ON ol.ReferenceOrder = fo.ReferenceOrder

				# packaging has marketplace as driver
				JOIN sandbox.dim_WH_costs wh3 ON wh3.driver_id = fo.DimMarketplaceID										# Packaging Cost
					AND wh3.dim_sub_cost_type_id = 3

				# Distribution costs depends on a combination of country, carrier and shipping method
				LEFT JOIN sandbox.dim_DistributionProfile dp ON 																# Distribution Costs
					dp.DimCountryID = fo.DimShipToCountryID
					AND dp.DimCarrierID = fo.DimCarrierID
					AND dp.DimShippingMethodID = fo.DimShippingMethodID

				GROUP BY 1,2,3,4,5
				) oc ON oc.ReferenceOrder = ic.ReferenceOrder
						AND oc.GIS > 0
						AND oc.GSII_iV > 0

		# we join with fact_payment here because GSII per order has to be calculated beforehand in order to calculate payment costs
		JOIN mart.fact_payment fp ON fp.ReferenceOrder = oc.ReferenceOrder
		JOIN mart.dim_paymentinstrument pin ON pin.DimPaymentInstrumentID = fp.DimPaymentInstrumentID
		JOIN sandbox.dim_PaymentProfile dpp ON dpp.payment_intrument = pin.PaymentInstrumentName					# Payment costs

		GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
	) pre_cm1
	JOIN mart.dim_date dt ON dt.DimDateID = pre_cm1.DimOrderLineDateID
	GROUP BY 1,2#,3,4,5
);
SELECT * FROM sandbox.cm1_query