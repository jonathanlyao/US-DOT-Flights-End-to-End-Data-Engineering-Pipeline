-- dim_date: standard date dimenstion, generated from the date range in stg_flights. 

{{ config(materialized = 'table') }}

WITH date_spine AS (

    SELECT 
        DATEADD(DAY, seq4(), '2024-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 366)) -- 2024 is a leap year, therefore 366 days. 

), 

final AS (

    SELECT 
        TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key, 
        full_date,
        YEAR(full_date) AS year,
        QUARTER(full_date) AS quarter,
        MONTH(full_date) AS month_num,
        MONTHNAME(full_date) AS month_name,
        DAY(full_date) AS day_of_month,
        DAYOFWEEK(full_date) AS day_of_week,
        DAYNAME(full_date) AS day_name, 
        CASE
            WHEN DAYOFWEEK(full_date) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend

    FROM date_spine
    WHERE full_date <= (SELECT MAX(flight_date) FROM {{ ref('stg_flights') }})

)

SELECT * FROM final
