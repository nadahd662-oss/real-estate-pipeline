{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    MD5(
        CONCAT_WS(
            '|',
            COALESCE(PROPERTY_TYPE, 'UNKNOWN'),
            COALESCE(TO_VARCHAR(YEAR_BUILT), 'UNKNOWN'),
            COALESCE(CONDITION, 'UNKNOWN')
        )
    ) AS PROPERTY_KEY,

    PROPERTY_TYPE,
    YEAR_BUILT,
    PROPERTY_AGE,
    CONDITION

FROM {{ source('silver', 'silver_listings') }}