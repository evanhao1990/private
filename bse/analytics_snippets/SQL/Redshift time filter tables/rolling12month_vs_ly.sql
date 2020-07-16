create or replace view sandbox_reporting.time_rolling12month_vs_ly as
    (
        select 'last 31 days'                                     as time_period,
               dd.dimdate,
               dd.dimdateid,

               cast(dd.dimdate as varchar)                        as time,

               right(dd.calendaryearweek, 3) + '-' + dd.dayofweek as time_index, --calendar week day
               right(dd.dimdate, 5)                               as axis_date,
               case
                   when dd.dimdate between add_months(current_date, -11) and current_date then 'cy'
                   else 'ly' end                                  as tag_year,
               dense_rank() over (partition by tag_year order by time desc ) as last_n

        from mart.dim_date dd

        where dd.dimdate between add_months(current_date, -23) and current_date
          -- calendarweek + day = the weekdays for past 14 days
          and time_index in (select right(t2.calendaryearweek, 3) + '-' + t2.dayofweek
                             from mart.dim_date t2
                             where t2.dimdate between current_date - 31 and current_date - 1
                             group by 1)
        group by 1, 2, 3, 4, 5, 6
    )
    union
    (
        select 'last 16 weeks'                      as time_period,
               dd.dimdate,
               dd.dimdateid,

               cast(dd.calendaryearweek as varchar) as time,

               right(dd.calendaryearweek, 3)        as time_index,
               right(dd.calendaryearweek, 3)        as axis_date,
               case
                   when dd.dimdate between add_months(current_date, -11) and current_date then 'cy'
                   else 'ly' end                    as tag_year,
               dense_rank() over (partition by tag_year order by time desc ) as last_n

        from mart.dim_date dd

        where dd.dimdate between add_months(current_date, -23) and current_date
          and dd.calendaryearweek in (select t2.calendaryearweek
                                      from mart.dim_date t2
                                      where (t2.dimdate between current_date - 112 and current_date - 7)             -- last 16 weeks
                                         or (t2.dimdate between current_date - 112 - 364 and current_date - 7 - 364) --same 16 weeks LY
                                      group by 1)

        group by 1, 2, 3, 4, 5, 6
    )
    union
    (
        select 'last 12 months'                      as time_period,
               dd.dimdate,
               dd.dimdateid,
               cast(dd.calendaryearmonth as varchar) as time,
               right(dd.calendaryearmonth, 2)        as time_index,
               dd.calendaryearmonth                  as axis_date,
               case
                   when dd.dimdate between last_day(add_months(current_date, -12)) + 1 and current_date then 'cy'
                   else 'ly' end                     as tag_year,
               dense_rank() over (partition by tag_year order by time desc ) as last_n

        from mart.dim_date dd
        where dd.dimdate between last_day(add_months(current_date, -24)) + 1 and current_date
        group by 1, 2, 3, 4, 5, 6
        order by dimdate desc
    )
    with no schema binding;
