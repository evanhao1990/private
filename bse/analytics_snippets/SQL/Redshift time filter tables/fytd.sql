create or replace view sandbox_reporting.time_fytd as 
(
	/*In short, step 1: for current fiscal year to month, get fiscal month number and nr_days past of current month
	            step 2: get the dates within this period for current + last 2 years
	*/
	with tempvar(current_fiscalyear) as (select d.fiscalyear from mart.dim_date d where d.dimdate = current_date - 1)
	select 
		d2.dimdateid,
        d2.calendarmonth,
        d2.calendaryear,
		case when d2.fiscalyear = current_fiscalyear    then 'cy'
			 when d2.fiscalyear = current_fiscalyear -1 then 'ly'
		     when d2.fiscalyear = current_fiscalyear -2 then 'l2y'
			 end as tag_year
	from mart.dim_date d2
		join tempvar on true
		join 
			(-- step 1:for current fiscal year to month, get fiscal month number and nr_days past of current month
				select 
					dd.fiscalmonth,
					case when dd.fiscalmonth = 7 
						 then max(dd.dayofmonth) + greatest(max(dd.dayofmonth) - 27,0) --to solve feb 29th problem: if within feb then max days else feb max days + 1
					     else max(dd.dayofmonth) end as max_days 
				from mart.dim_date dd, tempvar
				where dd.fiscalyear = current_fiscalyear and dd.dimdate < current_date
				group by 1
			) sub on d2.fiscalmonth = sub.fiscalmonth 
			   and d2.dayofmonth <= sub.max_days 
			   and d2.fiscalyear between current_fiscalyear - 2 and current_fiscalyear
	group by 1,2,3,4
)
with no schema binding