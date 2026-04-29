-- fact_flights: one row per scheduled flight leg. Grain = carrier + flight_number + flight_date + origin + dest. 

{{ config(materialized = 'table') }}

WITH flights AS (

    SELECT
        -- surrogate key
        MD5(
            COALESCE(carrier_code, '') || '-' ||
            COALESCE(CAST(flight_number AS VARCHAR), '') || '-' ||
            COALESCE(CAST(flight_date AS VARCHAR), '') || '-' ||
            COALESCE(origin, '') || '-' ||
            COALESCE(dest, '') || '-' ||
            COALESCE(tail_number, '')
        ) AS flight_key, 

        -- Foreign keys
        carrier_code AS carrier_key,
        origin AS origin_airport_key,
        dest AS dest_airport_key,
        TO_CHAR(flight_date, 'YYYYMMDD')::INTEGER AS date_key,

        -- Flight identifiers
        tail_number,
        flight_number,
        flight_date, 

        -- Departure
        crs_dep_time_raw, 
        dep_time_raw, 
        dep_delay_min, 
        dep_delayed_15, 

        -- Arrival 
        crs_arr_time_raw, 
        arr_time_raw,
        arr_delay_min,
        arr_delayed_15, 

        -- Status
        is_cancelled,
        is_diverted, 
        cancellation_code, 

        -- Duration
        crs_elapsed_time_min, 
        actual_elapsed_time_min, 
        air_time_min, 
        distance_miles, 
        taxi_out_min, 
        taxi_in_min, 

        -- Delay causes. (only populated when delayed >= 15 min and not cancelled)
        delay_carrier_min,
        delay_weather_min,
        delay_nas_min, 
        delay_security_min,
        delay_late_aircraft_min, 

        -- Metadata
        source_file, 
        loaded_at

    FROM {{ ref('stg_flights') }}
)

SELECT * FROM flights