--SELECT COUNT(*) as total_rows 
--FROM {{ ref('fct_monthly_zone_revenue') }}

-- 15579

--SELECT pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue
--FROM {{ ref('fct_monthly_zone_revenue') }}
--WHERE EXTRACT(YEAR FROM revenue_month) = 2020 AND service_type = 'Green'
--GROUP BY pickup_zone
--ORDER BY total_revenue DESC
--LIMIT 1

-- East Harlem North 

--SELECT SUM(total_monthly_trips) as total_trips
--FROM {{ ref('fct_monthly_zone_revenue') }}
--WHERE service_type = 'Green'
--  AND revenue_month = '2019-10-01'

-- 386424  

--SELECT COUNT(*) as total_rows 
--FROM {{ ref('stg_fhv_tripdata') }}

--43244693