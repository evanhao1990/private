/* 
this script combines output of CFT with BMR data on the lowest common denominator. 
We will not take data from BMR for those brands that have forecast data in cft so be careful to make sure we control those brands which go into cft forecast data.
*/

set @forecast_month = ('2018-08');

	(
	select #BMR data
	
		 bmr.TS_Forecast
		,bmr.Brand
		,bmr.BSE_Year as FY
		,bmr.CalendarYear
		,bmr.FiscalQuarter
		,bmr.MonthNameShort
		,bmr.MonthName
		,bmr.CalendarYearMonth
		,case when bmr.Channel in ('BS.COM','Employee Shop') then 'BS.COM'
		      when bmr.Channel in ('Brand.com')              then 'BRAND.COM'
		                                                     else 'Partner.COM' end as channel
		,sum(bmr.GSI_ED)                     as GSI_ED
		,sum(bmr.GRI_ED)                     as GRI_ED
		,sum(bmr.GSII_ED)                    as GSII_ED
		,sum(bmr.GRII_ED)                    as GRII_ED
		,sum(bmr.Net_Sales_ED)               as Net_Sales_ED
		,sum(bmr.Net_COGS_ED)                as Net_COGS_ED
		,avg(bmr.Markup_eVAT)                as Markup_eVAT
		,sum(bmr.`Discount%`)                as 'Discount%'
		
		/* below columns are used by finance when a month is ended on weekend because we do not dispatch items during weekend. 
			they will move sales number on those two days to next month, as FD. 
			for now we leave these columns out because they require mannual adjustment which cannot be done in CFT. */
			
		/*	sum(bmr.GSI_FD),sum(bmr.GRI_FD),sum(bmr.GSII_FD),sum(bmr.GRII_FD),sum(bmr.Net_Sales_FD),sum(bmr.Net_COGS_FD),*/
		
		,sum(bmr.Bulk)                       as Bulk
		,sum(bmr.Max_Stock)                  as Max_Stock
		,sum(bmr.`Online Closing Stock`)     as `Online Closing Stock`
		,sum(bmr.Prj_Stock_Total)            as Prj_Stock_Total
		,sum(bmr.Buying)                     as Buying
	from sandbox.BCM_BMR_Master bmr
	
	where bmr.TS_Forecast = @forecast_month
	and bmr.Brand not in (select distinct cft.Brand from sandbox.BCM_CFT_Master cft where cft.TS_Forecast = @forecast_month)
	group by 1,2,3,4,5,6,7,8,9
)

union

(
	select # CFT data
	
		 cft.TS_Forecast
		,cft.Brand
		,concat('FY ', cft.FiscalYear)                                            as FY
		,left(cft.CalendarYearMonth,4)                                            as calendaryear
		,cft.FiscalQuarter
		,cft.MonthName
		,concat(cft.MonthName,'\'',mid(cft.CalendarYearMonth,3,2))                as 'MonthName'
		,cft.CalendarYearMonth
		,cft.Bussiness_area
		,sum(cft.GSI_M)                                                           as GSI_ED
		,sum(cft.GRI_M)                                                           as GRI_ED
		,sum(cft.GSII_M)                                                          as GSII_ED
		,sum(cft.GRII_M)                                                          as GRII_ED
		,sum(cft.NetSales_M)                                                      as Net_SALES_ED
		,sum(cft.NetCOGS_M_Noos) + sum(cft.NetCOGS_M_Nonnoos)                     as Net_COGS_ED
		,avg(cft.MarkUp_eVAT_Q)                                                   as Markup_eVAT
		,1 - sum(cft.GSII_M)/sum(cft.GSI_M)                                       as 'Discount%'
		,sum(cft.Bulk_Noos) + sum(cft.Bulk_Nonnoos)                               as Bulk
		,sum(cft.TargetStock_Total)                                               as Max_stock
		,(sum(cft.ProjectedStock_Noos) + sum(cft.ProjectedStock_Nonnoos)) 
		  - (sum(cft.OnHold_Noos)+sum(cft.OnHold_Nonnoos))                        as Online_Closing_Stock 
		,sum(cft.ProjectedStock_Noos) + sum(cft.ProjectedStock_Nonnoos)           as Prj_Stock_Total
		,sum(cft.PlanToBuy_Noos) + sum(cft.PlanToBuy_Nonnoos)                     as buying
	
	from sandbox.BCM_CFT_Master cft
	where cft.TS_Forecast = @forecast_month
	group by 1,2,3,4,5,6,7,8,9
)