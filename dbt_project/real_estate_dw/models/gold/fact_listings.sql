{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    S.LISTING_ID,

    L.LOCATION_KEY,
    P.PROPERTY_KEY,
    D.DATE_KEY,
    F.FEATURE_KEY,

    S.SURFACE_M2,
    S.NUM_ROOMS,
    S.NUM_BATHROOMS,
    S.FLOOR,

    S.PRICE,
    S.PRICE_PER_M2

FROM {{ source('silver', 'silver_listings') }} AS S

LEFT JOIN {{ ref('dim_location') }} AS L
    ON S.COUNTRY = L.COUNTRY
    AND S.CITY = L.CITY
    AND S.NEIGHBORHOOD = L.NEIGHBORHOOD

LEFT JOIN {{ ref('dim_property') }} AS P
    ON S.PROPERTY_TYPE = P.PROPERTY_TYPE
    AND S.YEAR_BUILT = P.YEAR_BUILT
    AND S.CONDITION = P.CONDITION

LEFT JOIN {{ ref('dim_date') }} AS D
    ON S.LISTING_DATE = D.FULL_DATE

LEFT JOIN {{ ref('dim_features') }} AS F
    ON S.HEATING_TYPE = F.HEATING_TYPE
    AND S.PARKING = F.PARKING
    AND S.ENERGY_RATING = F.ENERGY_RATING