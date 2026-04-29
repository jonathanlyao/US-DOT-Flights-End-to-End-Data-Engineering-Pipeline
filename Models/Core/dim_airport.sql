-- dim_airport: one row per airport, derived from both origin and deistination. 

{{ config(materialized = 'table') }}

WITH origins AS (

    SELECT DISTINCT 
        origin AS airport_code, 
        origin_airport_id AS airport_id,
        origin_city AS city, 
        origin_state AS state, 
        origin_state_name AS state_name
    FROM {{ ref('stg_flights') }}

), 

destinations AS (

    SELECT DISTINCT 
        dest AS airport_code, 
        dest_airport_id AS airport_id,
        dest_city AS city, 
        dest_state AS state, 
        dest_state_name AS state_name
    FROM {{ ref('stg_flights') }}

), 

combined AS (

    SELECT * FROM origins
    UNION 
    SELECT * FROM destinations

), 

final AS (

    SELECT 
        airport_code AS airport_key, 
        airport_code, 
        airport_id, 
        city, 
        state,
        state_name, 
        CASE
            WHEN state IN ('CT','ME','MA','NH','NJ','NY','PA','RI','VT')
                THEN 'Northeast'
            WHEN state IN ('IL','IN','IA','KS','MI','MN','MO','NE','ND','OH','SD','WI')
                THEN 'Midwest'
            WHEN state IN ('AL','AR','DE','FL','GA','KY','LA','MD','MS','NC','OK','SC','TN','TX','VA','WV','DC')
                THEN 'South'
            WHEN state IN ('AK','AZ','CA','CO','HI','ID','MT','NV','NM','OR','UT','WA','WY')
                THEN 'West'
            ELSE 'Territory/Other'
        END                                         AS region

    FROM combined

)

SELECT * FROM final