-- models/staging/stg_flights.sql
-- staging model: type casting, basic cleaning, carrier filtering. 
-- Source: DOT_FLIGHTS.RAW.RAW_FLIGHTS (ALL VARCHAR)
-- Output: typed, cleaned, filtered to AA/UA/DL only 

{{ config(materialized = 'view') }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_flights') }}

),

filtered AS (
    SELECT *
    FROM source
    WHERE "Reporting_Airline" IN ('AA', 'UA', 'DL')
),

typed AS (

    SELECT
        -- === Date & Time === 
        CAST("Year" AS INTEGER) AS flight_year,
        CAST("Quarter" AS INTEGER) AS flight_quarter,
        CAST("Month" AS INTEGER) AS flight_month,
        CAST("DayofMonth" AS INTEGER) AS day_of_month,
        CAST("DayOfWeek" AS INTEGER) AS day_of_week,
        TO_DATE("FlightDate", 'YYYY-MM-DD') AS flight_date,

        -- === Carrier ===
        "Reporting_Airline" AS carrier_code,
        CAST("DOT_ID_Reporting_Airline" AS INTEGER) AS dot_id_carrier,
        "IATA_CODE_Reporting_Airline" AS iata_code_carrier,
        "Tail_Number" AS tail_number,
        CAST("Flight_Number_Reporting_Airline" AS INTEGER) AS flight_number,

        -- === Origin and Destination ===
        CAST("OriginAirportID" AS INTEGER) AS origin_airport_id,
        "Origin" AS origin,
        "OriginCityName" AS origin_city,
        "OriginState" AS origin_state,
        "OriginStateName" AS origin_state_name,

        CAST("DestAirportID" AS INTEGER) AS dest_airport_id,
        "Dest" AS dest,
        "DestCityName" AS dest_city,
        "DestState" AS dest_state,
        "DestStateName" AS dest_state_name,

        -- === Departure and Arrival Times ===
        -- CRSDepTime is stored as HHMM integer (e.g. 1435 = 14:35 local time). 
        -- 2400 is used for midnight but is not a valid time. Convert to 0000. 

        CASE 
            WHEN "CRSDepTime" = 2400 THEN '0000'
            ELSE TO_TIME(LPAD(CAST("CRSDepTime" AS STRING), 4, '0'), 'HH24MI')
        END AS crs_dep_time_raw,
        CASE 
            WHEN "DepTime" = 2400 THEN '0000'
            ELSE TO_TIME(LPAD(CAST("DepTime" AS STRING), 4, '0'), 'HH24MI')
        END AS dep_time_raw,

        CAST("DepDelay" AS FLOAT) AS dep_delay_min,
        CAST("DepDelayMinutes" AS FLOAT) AS dep_delay_min_positive,
        CAST("DepDel15" AS FLOAT) AS dep_delayed_15,

        CASE 
            WHEN "CRSArrTime" = 2400 THEN '0000'
            ELSE LPAD(CAST("CRSArrTime" AS STRING), 4, '0')
        END AS crs_arr_time_raw,

        CASE
            WHEN "ArrTime" = 2400 THEN '0000'
            ELSE LPAD(CAST("ArrTime" AS STRING), 4, '0')
        END AS arr_time_raw,

        CAST("ArrDelay" AS FLOAT) AS arr_delay_min,
        CAST("ArrDelayMinutes" AS FLOAT) AS arr_delay_min_positive,
        CAST("ArrDel15" AS FLOAT) AS arr_delayed_15,

        -- === Flight Status === 
        CASE 
            WHEN CAST("Cancelled" AS FLOAT) = 1.0 THEN TRUE
            ELSE FALSE
        END AS is_cancelled,

        "CancellationCode" AS cancellation_code,

        CASE 
            WHEN CAST("Diverted" AS FLOAT) = 1.0 THEN TRUE
            ELSE FALSE
        END AS is_diverted,

        -- === Duration === 

        CAST("CRSElapsedTime" AS FLOAT) AS crs_elapsed_time_min, 
        CAST("ActualElapsedTime" AS FLOAT) AS actual_elapsed_time_min,
        CAST("AirTime" AS FLOAT) AS air_time_min,
        CAST("Distance" AS FLOAT) AS distance_miles, 
        CAST("TaxiOut" AS FLOAT) AS taxi_out_min,
        CAST("TaxiIn" AS FLOAT) AS taxi_in_min, 

        -- === Delay Causes === 
        -- Only populated when flight is delayed >= 15 minutes AND is not cancelled.
        CAST("CarrierDelay" AS FLOAT) AS delay_carrier_min,
        CAST("WeatherDelay" AS FLOAT) AS delay_weather_min,
        CAST("NASDelay" AS FLOAT) AS delay_nas_min,
        CAST("SecurityDelay" AS FLOAT) AS delay_security_min,
        CAST("LateAircraftDelay" AS FLOAT) AS delay_late_aircraft_min, 

        -- === Metadata ===
        "_SOURCE_FILE" AS source_file, 
        "_LOAD_TIMESTAMP" AS loaded_at
    
    FROM filtered


)

SELECT * FROM typed