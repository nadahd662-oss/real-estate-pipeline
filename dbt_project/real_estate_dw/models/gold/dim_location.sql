{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    MD5(
        CONCAT_WS(
            '|',
            COALESCE(COUNTRY, 'UNKNOWN'),
            COALESCE(CITY, 'UNKNOWN'),
            COALESCE(NEIGHBORHOOD, 'UNKNOWN')
        )
    ) AS LOCATION_KEY,

    COUNTRY,
    CITY,
    NEIGHBORHOOD

FROM {{ source('silver', 'silver_listings') }}