create view sandbox_reporting.time_past_future_4weeks as 
(
	select 
	    dd.calendaryearweek, 
	    case when dd.dimdate between current_date - 28       and current_date -7 then 'cy'
	         when dd.dimdate between current_date - 28 - 364 and current_date + 20 - 364 then 'ly'
			 when dd.dimdate between current_date - 28 - 728 and current_date + 20 - 728 then 'l2y'
	         else 'NA' end as tag_year
	    
	from mart.dim_date dd
	where (dd.dimdate between current_date - 28       and current_date -7) 
	   or (dd.dimdate between current_date - 28 - 364 and current_date + 20 - 364)
	   or (dd.dimdate between current_date - 28 - 728 and current_date + 20 - 728)
	   group by 1,2
)  
   with no schema binding;