CREATE temporary TABLE temp_seasons as (

select season, (row_number() over (order by min_date_id))-1 as season_id, min_date_id, max_date_id
from (
	SELECT 
	
		CASE WHEN dt.calendarmonth between 2 and 7 then concat('SS',SUBSTRING(dt.calendaryear, 3))
			WHEN dt.calendarmonth = 1 then concat('AW',SUBSTRING(dt.calendaryear-1, 3))
			ELSE concat('AW',SUBSTRING(dt.calendaryear, 3)) END AS season,
		
		min(dimdateid) as min_date_id,
		max(dimdateid) as max_date_id
	
	FROM mart.dim_date dt 
	GROUP BY season
	ORDER BY 2 asc
	)
); 

CREATE TABLE sandbox_ana.ana_dim_date as (
select
dt.*,
ts.season,
ts.season_id
from mart.dim_date dt 
join temp_seasons ts on dt.dimdateid between ts.min_date_id and ts.max_date_id
);