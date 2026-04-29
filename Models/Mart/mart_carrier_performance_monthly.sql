-- mart_carrier_performance_monthly: 
-- Monthly performance summary per carrier.
-- Questions: "Which airline is most reliable?" "How does on-time rate trend over time?"  
{{ config(materialized = 'table') }}

WITH flights AS (

    SELECT 
        f.carrier_key, 
        f.date_key, 
        c.carrier_name,
        d.year,
        d.month_num,
        d.month_name,
        f.dep_delay_min,
        f.arr_delay_min,
        f.is_cancelled,
        f.is_diverted,
        f.arr_delayed_15

    FROM {{ ref('fact_flights') }} AS f
    LEFT JOIN {{ ref('dim_carrier') }} AS c
        ON f.carrier_key = c.carrier_key
    LEFT JOIN {{ ref('dim_date') }} AS d
        ON f.date_key = d.date_key

), 

aggregated AS (

    SELECT 
        carrier_key,
        carrier_name,
        year,
        month_num,
        month_name,

        COUNT(*) AS total_flights, -- Volume of flights

        -- Cancellations & Diversions
        SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS cancelled_count,
        ROUND(cancelled_count * 100.0 / total_flights, 2) AS cancelled_pct,

        SUM(CASE WHEN is_diverted THEN 1 ELSE 0 END) AS diverted_count,
        ROUND(diverted_count * 100.0 / total_flights, 2) AS diverted_pct,

        -- On-time performance ()FAA standard: <= 15 min late at arrival)
        SUM(CASE WHEN arr_delayed_15 = 0 AND NOT is_cancelled AND NOT is_diverted THEN 1 ELSE 0 END) AS on_time_count, 
        ROUND(on_time_count * 100.0 / total_flights, 2) AS on_time_pct, 

        -- Delay statistics (excluding cancelled flights)
        ROUND(AVG(CASE WHEN NOT is_cancelled THEN dep_delay_min END), 2) AS avg_dep_delay, 
        ROUND(AVG(CASE WHEN NOT is_cancelled THEN arr_delay_min END), 2) AS avg_arr_delay,

        -- Percentiles for arrival delay (excluding cancelled)
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY arr_delay_min), 2) AS p75_arr_delay,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY arr_delay_min), 2) AS p95_arr_delay

    FROM flights
    GROUP BY carrier_key, carrier_name, year, month_num, month_name

)

SELECT * FROM aggregated
ORDER BY carrier_key, year, month_num

