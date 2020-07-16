-- BCM793 CRM Page Dataset
-- Additions:
	-- Avg Session Time -> I added the total per sessions for now
	-- hit.time and time on page -> I added entry and exit + difference
	-- remove hits.number
	-- PROBLEM: sfmc ID is not tracked in all pages, so I use a subquery to identify the session_code

--Extras that I thought make sense:
	-- data coded in YYYY-MM-DD format
	-- country changed into site_country
	-- date filter in dynamic format

				SELECT main.date,
                       main.site_brand,
                       main.session_code,
                       main.user_type,
                       main.channelgrouping,
                       main.page_url,
                       main.is_entry,
                       main.is_exit,
                       main.site_country,
                       main.page_type,
                       main.page_category,
                       sfmc.sfmc_id,
                       main.total_session_time,
                       ROUND(main.page_entry_time/1000,0) AS page_entry_time_seconds,
                       ROUND(main.page_exit_time/1000,0) AS page_exit_time_seconds,
                       ROUND(main.page_exit_time/1000,0) - ROUND(main.page_entry_time/1000,0) AS time_on_page 

                FROM (SELECT  concat(substr(date,1,4),"-",substr(date,5,2),"-",substr(date,7,2)) as date,
                              '{}' as site_brand,
                              CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
                              fullVisitorId, --need them to partition
                              visitId, --need them to partition
                              device.deviceCategory as user_type,

                              channelGrouping,
                              SPLIT(hits.page.pagepath, '?')[OFFSET(0)] as page_url,
                              IFNULL(hits.isEntrance,false) as is_entry,
                              IFNULL(hits.isexit,false) as is_exit,

                              max(if(cd.index = 1,cd.value,null)) as site_country,
                              MAX(CASE WHEN cd.index = 4 then cd.value end) as page_type,
                              MAX(CASE WHEN cd.index = {} then cd.value end) as page_category,

                              totals.timeonsite AS total_session_time, 
                              hits.time AS page_entry_time, 
                              IFNULL(LEAD(hits.time, 1) OVER (PARTITION BY fullVisitorId, visitId ORDER BY hits.time ASC),totals.timeonsite*1000) as page_exit_time

                        FROM `ga-360-bigquery-api.{}.ga_sessions_*`, 
                                  UNNEST(hits) AS hits,
                                  unnest(hits.customDimensions) as cd

                        WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))) 
                                and lower(device.browser) NOT LIKE '%app%'
                                and hits.type = 'PAGE'

                        GROUP BY 1,2,3,4,5,6,7,8,9,10,14,15) main

               
                LEFT JOIN (SELECT CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
                                    MAX(CASE WHEN cd.index = {} then cd.value END) as sfmc_id
                            FROM `ga-360-bigquery-api.{}.ga_sessions_*`, 
                                   UNNEST(hits) AS hits,
                                   unnest(hits.customDimensions) as cd
                            WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))) 
                                    and lower(device.browser) NOT LIKE '%app%'
                            GROUP BY 1
                            HAVING sfmc_id IS NOT NULL) sfmc ON main.session_code = sfmc.session_code

                WHERE sfmc.sfmc_id IS NOT NULL