-- mart_delay_cause_breakdown_monthly:
-- Monthly breakdown of delay causes per carrier. 
-- Answers "Is United's delay problem weather or operations?"

{{ config(materialized='table') }}

WITH delayed_flights AS (

    --Only flights that were actually delayed >= 15 min and not cancelled. 
    -- This is when DOT populates the 5 delay cause columns. 
    SELECT 
        f.carrier_key, 
        c.carrier_name, 
        d.year,
        d.month_num,
        d.month_name,
        f.delay_carrier_min,
        f.delay_weather_min, 
        f.delay_nas_min,
        f.delay_security_min,
        f.delay_late_aircraft_min

    FROM {{ ref('fact_flights') }} AS f
    LEFT JOIN {{ ref('dim_carrier') }} AS c ON f.carrier_key = c.carrier_key
    LEFT JOIN {{ ref('dim_date') }} AS d ON f.date_key = d.date_key
    WHERE f.arr_delayed_15 = 1 AND NOT f.is_cancelled

), 

aggregated AS (

    SELECT
        carrier_key,
        carrier_name,
        year,
        month_num,
        month_name,

        COUNT(*) AS delayed_flights_count,

        -- Total delay minutes by cause
        ROUND(SUM(delay_carrier_min), 0) AS total_carrier_delay_min,
        ROUND(SUM(delay_weather_min), 0) AS total_weather_delay_min,
        ROUND(SUM(delay_nas_min), 0) AS total_nas_delay_min,
        ROUND(SUM(delay_security_min), 0) AS total_security_delay_min,
        ROUND(SUM(delay_late_aircraft_min), 0) AS total_late_aircraft_delay_min,

        -- Grand total
        total_carrier_delay_min + total_weather_delay_min + 
        total_nas_delay_min + total_security_delay_min + 
        total_late_aircraft_delay_min AS total_delay_min, 

        -- Percentage breakdown
        ROUND(total_carrier_delay_min * 100.0 / NULLIF(total_delay_min, 0), 1) AS pct_carrier, 
        ROUND(total_weather_delay_min * 100.0 / NULLIF(total_delay_min, 0), 1) AS pct_weather,
        ROUND(total_nas_delay_min * 100.0 / NULLIF(total_delay_min, 0), 1) AS pct_nas,
        ROUND(total_security_delay_min * 100.0 / NULLIF(total_delay_min, 0), 1) AS pct_security,
        ROUND(total_late_aircraft_delay_min * 100.0 / NULLIF(total_delay_min, 0), 1) AS pct_late_aircraft

    FROM delayed_flights
    GROUP BY carrier_key, carrier_name, year, month_num, month_name

)

SELECT * FROM aggregated
ORDER BY carrier_key, year, month_num