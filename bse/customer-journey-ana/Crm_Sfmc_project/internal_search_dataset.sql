-- BCM-807: CRM internal Search Dataset
-- BS.COM / Last 30 days / exclude app

				SELECT main.*,
                       sub.internal_search_query
                FROM (SELECT concat(substr(date,1,4),"-",substr(date,5,2),"-",substr(date,7,2)) as date,
                            '{}' AS site_brand,
                             CONCAT(CAST(visitId AS STRING),CAST(fullVisitorId AS STRING)) AS session_code,
                             device.deviceCategory AS user_type,
                             MAX(IF(cd.index=1,cd.value,NULL)) AS site_country,
                             MAX(IF(cd.index={},cd.value,NULL)) AS sfmc_id,
                             channelGrouping,
                             trafficsource.campaign as campaign
                      FROM `ga-360-bigquery-api.{}.ga_sessions_*`, --BS.com
                             UNNEST(hits) as hits,
                             UNNEST(hits.customDimensions) AS cd
                      WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))) 
                             AND lower(device.browser) NOT LIKE '%app%' 
                             AND totals.pageviews > 0 
                      GROUP BY 1,2,3,4,7,8) main

                LEFT JOIN (SELECT CONCAT(CAST(visitId AS STRING),CAST(fullVisitorId AS STRING)) AS session_code,
                                  hits.page.searchkeyword  AS internal_search_query 
                           FROM `ga-360-bigquery-api.{}.ga_sessions_*`, --BS.com
                                  UNNEST(hits) as hits
                           WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))) 
                                  AND lower(device.browser) NOT LIKE '%app%' --excluding app
                                  AND totals.pageviews > 0 
                                  AND hits.type = 'PAGE'
                           GROUP BY 1,2
                           HAVING internal_search_query IS NOT NULL) sub ON main.session_code = sub.session_code

                WHERE sfmc_id IS NOT NULL
                      AND internal_search_query IS NOT NULL
