{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    MD5(
        CONCAT_WS(
            '|',
            COALESCE(HEATING_TYPE, 'UNKNOWN'),
            COALESCE(PARKING, 'UNKNOWN'),
            COALESCE(ENERGY_RATING, 'UNKNOWN')
        )
    ) AS FEATURE_KEY,

    HEATING_TYPE,
    PARKING,
    ENERGY_RATING

FROM {{ source('silver', 'silver_listings') }}