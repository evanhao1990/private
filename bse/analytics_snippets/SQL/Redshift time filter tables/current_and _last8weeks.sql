create or replace view sandbox_reporting.time_current_and_last8weeks as
(
	select 
		left(dd.calendaryearweek, 4)                                    as calendaryear,
		dd.calendarweek,
		dd.calendaryearweek,
		case when dd.dimdate > current_date - 64           then 'cy'
			 when dd.dimdate > current_date - 64 - 364     then 'ly'
			 when dd.dimdate > current_date - 64 - 364 * 2 then 'l2y'
		     else 'na' end                                              as tag_year,
		max(dd.dayofweek)                                               as nr_days_in_wk,
		max(dd.dimdate)                                                 as week_ends_date,
		count(case when nr_days_in_wk = 7 then calendaryearweek else null end) 
		      over (partition by tag_year order by calendaryearweek desc rows unbounded preceding) as last_n_weeks
	from mart.dim_date dd
	where (dd.dimdate between current_date - 56 and current_date - 1)                     -- last 8 weeks
       or (dd.dimdate between current_date - 56 - 364 and current_date - 1 - 364)         --same 8 weeks LY
	   or (dd.dimdate between current_date - 56 - 364 * 2 and current_date - 1 - 364 * 2) -- same 8 weeks LY-1
	group by 1,2,3,4
) with no schema binding;

