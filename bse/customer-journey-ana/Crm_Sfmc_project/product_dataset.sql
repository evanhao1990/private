-- BCM777 CRM Product dataset

-- ADDITIONS:
	-- exclude hit_number
	-- dynamic date filter
	-- country renamed site_country
	-- adjusted date format
	-- excluded APP as we did in the other two datasets ?
	-- added subquery for SMFC
	-- added style number stripped from URL 

				SELECT master.date,
                       master.site_brand,
                       master.session_code, 
                       master.user_type, 
                       master.site_country, 
                       master.channelGrouping, 
                       master.campaign, 
                       sfmc.sfmc_id,

                        case when (master.cat = 'wishlist' and master.act LIKE '%add%') then 'ATW' 
                             when (master.cat = 'Ecommerce' and master.act = 'Add to Cart') then 'ATB' 
                             when (master.page_type = 'pdp' and master.hit_type = 'PAGE') then 'PDPv'
                             END as hit_type,

                        master.sku, 

                        REGEXP_EXTRACT(master.url, r'([0-9]{})') AS style_no,
                        if(conversions.gis > 0,1,0) AS order_flag

                FROM (
                      SELECT concat(substr(date,1,4),"-",substr(date,5,2),"-",substr(date,7,2)) as date,
                             '{}' as site_brand,
                              CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
                              device.deviceCategory as user_type,      
                              trafficsource.campaign as campaign,
                              channelGrouping,
                              hits.type as hit_type,
                              hits.eventinfo.eventcategory as cat,
                              hits.eventinfo.eventaction act,

                              CONCAT(prod.productsku) as sku,
                              hits.page.pagePath AS url,
                              max(if(cd.index = 1,cd.value,null)) as site_country,
                              MAX(CASE WHEN cd.index = 4 then cd.value end) as page_type

                      FROM `ga-360-bigquery-api.{}.ga_sessions_*`, 
                                UNNEST(hits) AS hits,
                                unnest(hits.product) as prod,
                                unnest(hits.customDimensions) as cd

                      WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)))  
                            and lower(device.browser) NOT LIKE '%app%'

                      group by 1,2,3,4,5,6,7,8,9,10,11
                      having ((cat = 'wishlist' and act LIKE '%add%') or 
                              (cat = 'Ecommerce' and act = 'Add to Cart') or 
                                (page_type = 'pdp' and hit_type = 'PAGE'))) master

                --get all sessions with sfmc_id for the period of our choice
                LEFT JOIN (SELECT CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
                                    MAX(CASE WHEN cd.index = {} then cd.value END) as sfmc_id
                            FROM `ga-360-bigquery-api.{}.ga_sessions_*`, 
                                   UNNEST(hits) AS hits,
                                   unnest(hits.customDimensions) as cd
                            WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))) 
                                    and lower(device.browser) NOT LIKE '%app%'
                            GROUP BY 1
                            HAVING sfmc_id IS NOT NULL) sfmc ON master.session_code = sfmc.session_code

                -- check per each session_code and product sku if it was bought or not
                LEFT JOIN (SELECT CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
                                  prod.productsku AS sku,
                                  COUNT(hits.transaction.transactionid) AS gis

                          FROM `ga-360-bigquery-api.{}.ga_sessions_*`, 
                                    UNNEST(hits) AS hits,
                                    unnest(hits.product) as prod

                          WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))) 
                                and lower(device.browser) NOT LIKE '%app%'

                          group by 1,2) conversions ON conversions.session_code = master.session_code
                                                        AND conversions.sku=master.sku 

                WHERE sfmc.sfmc_id IS NOT NULL
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12