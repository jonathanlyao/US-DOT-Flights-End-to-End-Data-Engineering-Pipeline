-- dim_carrier: one row per airline carrier. 

{{ config(materialized = 'table') }}

WITH carriers AS (

    SELECT 
        DISTINCT carrier_code,
        iata_code_carrier,
        dot_id_carrier
    FROM {{ ref('stg_flights') }}

),

enriched AS (

    SELECT 
        carrier_code AS carrier_key,
        carrier_code, 
        iata_code_carrier, 
        dot_id_carrier,
        CASE carrier_code
            WHEN 'AA' THEN 'American Airlines'
            WHEN 'UA' THEN 'United Airlines'
            WHEN 'DL' THEN 'Delta Air Lines'
            ELSE 'Other'
        END AS carrier_name, 
        CASE carrier_code
            WHEN 'AA' THEN 'Major'
            WHEN 'UA' THEN 'Major'
            WHEN 'DL' THEN 'Major'
            ELSE 'Other'
        END AS carrier_group

    FROM carriers

)

SELECT * FROM enriched