-- filter: ON // 2019-03
select 
    main.session_code,
    main.channelGrouping as cg,
    main.timeOnSite as ToS,
    main.pageviews as pages,
    sub.PLPs as plps,
    sub.PDPs as pdps,
    main.page_list,
    main.event_list

from 
  (
    SELECT 
      CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) AS session_code,
      channelGrouping,
      totals.timeOnSite,
      totals.pageviews,
      -- pages within a session in sequence
      string_agg(case when h.type='PAGE' and cd.index=4 then cd.value end ) AS page_list,  
      -- events within a session in sequence (manually picked and ignored some)
      string_agg(case when h.type='EVENT' and cd.index=4 and h.eventInfo.eventAction in ('Add to Cart','Purchase','PromotionClick') 
                        then h.eventInfo.eventAction
                      when h.type='EVENT' and cd.index=4 and h.eventInfo.eventAction<>'Load' and h.eventInfo.eventcategory not in ('Monetate' ,'Site speed','Ecommerce') 
                        then h.eventInfo.eventcategory end) as event_list
    FROM `ga-360-bigquery-api.113635108.ga_sessions_*`, 
      UNNEST(hits) AS h,
      UNNEST(h.customDimensions) AS cd
    WHERE _TABLE_SUFFIX BETWEEN '20190301' AND '20190330' 
      AND totals.pageviews > 0 
    group by 1,2,3,4
    order by 4
  ) main
join 
  (
    SELECT -- distinct count of plps and pdps
      CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) AS session_code,
      count(IF(h.eventInfo.eventAction ='Load' and h.eventinfo.eventcategory = 'Ecommerce' and p.productListName = 'main', p.productListPosition,null)) as PLPs,
      count(IF((h.type = 'PAGE' and p.productListName = 'packshot') or (h.eventInfo.eventAction ='Select' and h.eventinfo.eventcategory = 'Color' and p.productListName = 'packshot'), p.productListPosition, null)) as PDPs
    FROM `ga-360-bigquery-api.113635108.ga_sessions_*`,
      UNNEST(hits) AS h,
      unnest(h.product) as p
    WHERE _TABLE_SUFFIX BETWEEN '20190301' AND '20190330'
      AND totals.pageviews > 0 
    group by 1
  ) sub on main.session_code = sub.session_code
group by 1,2,3,4,5,6,7,8