select
    dd.dimdate,
    dd.dayofweek,
    dd.shortdayname,
    dd.shortmonthname,
    dd.calendaryear,
    dd.calendarweek,
    dd.calendarmonth,
    dd.calendarquarter,
    dd.calendaryearmonth,
    dd.calendaryearquarter,
    dd.calendaryearweek,
    dd.fiscalyearname,
    dd.fiscalyearmonthname,
    dd.fiscalyearquartername,
    dd.season,
    dd.season_no
from sandbox_ana.dim_date_season dd
where dd.dimdate >= '2018-01-01'