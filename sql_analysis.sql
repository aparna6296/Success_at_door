--How many number of sessions are there? (When talking about sessions, I have included only those sessions which are interactive)

SELECT sum(visits) FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`

--How many sessions does each visitor create?

SELECT fullvisitorid, count(visits) as sessions_per_user FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` group by fullvisitorid order by sessions_per_user desc

--How much time does it take on average to reach the order_confirmation screen per session (in minutes)?

SELECT fullvisitorid, visitNumber, round((avg(h.time)/1000)/60,2) as avg_duration FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`,unnest(hit) as h where h.eventCategory like '%order_confirmation%' group by fullvisitorid, visitNumber order by avg_duration desc, fullvisitorid asc

--4th Question Analysis and Visualization
SELECT   Concat(trafficsource.source," / ",trafficsource.medium) AS source_medium, 
         -- Users 
         Count(DISTINCT fullvisitorid) AS users, 
         -- new users 
         Count(DISTINCT( 
         CASE 
                  WHEN totals.newvisits = 1 THEN fullvisitorid 
                  ELSE NULL 
         END )) AS new_users, 
         -- sessions 
         Count(DISTINCT Concat(fullvisitorid, Cast(visitstarttime AS STRING))) AS sessions, 
         --bounce rate 
         Round(Count(DISTINCT( 
         CASE 
                  WHEN totals.bounces = 1 THEN Concat(fullvisitorid, Cast(visitstarttime AS STRING))
                  ELSE NULL 
         END )) / Count(DISTINCT Concat(fullvisitorid, Cast(visitstarttime AS STRING))),2) AS bounce_rate,
         -- Pages/Sessions 
         Round(Sum(totals.pageviews) / Count(DISTINCT Concat(fullvisitorid, Cast(visitstarttime AS STRING))),2) AS pages_sessions,
         -- Average Session Duration 
         Round(Sum(totals.timeonsite) / Count(DISTINCT Concat(fullvisitorid, Cast(visitstarttime AS STRING))),2) AS avg_session_dur_seconds,
         -- Ecommerce Conv Rate 
         Round(Count(DISTINCT hits.TRANSACTION.transactionid) / Count(DISTINCT Concat(Cast(fullvisitorid AS STRING), Cast(visitstarttime AS STRING))),2) AS ecom_conv_rate,
         -- Transactions 
         Count(DISTINCT hits.TRANSACTION.transactionid) AS transactions, 
         -- Revenue 
         Sum(hits.TRANSACTION.transactionrevenue)/1000000 AS revenue 
FROM     `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, 
         unnest(hits) AS hits 
WHERE    totals.visits = 1 
GROUP BY 1 
ORDER BY 2 DESC

