WITH cleaned AS (

    SELECT

        listing_id,

        CASE
            WHEN LOWER(TRIM(property_type)) = 'apt'
                THEN 'apartment'
            ELSE LOWER(TRIM(property_type))
        END AS property_type,

        country,
        city,
        neighborhood,

        CASE 
            WHEN TRY_TO_NUMBER(surface_m2, 10, 2) BETWEEN 15 AND 2000 
                THEN TRY_TO_NUMBER(surface_m2, 10, 2)
            ELSE NULL
        END AS surface_m2,

        TRY_TO_NUMBER(num_rooms) AS num_rooms,
        TRY_TO_NUMBER(num_bathrooms) AS num_bathrooms,
        TRY_TO_NUMBER(floor) AS floor,
        TRY_TO_NUMBER(year_built) AS year_built,

        CASE 
            WHEN TRY_TO_NUMBER(REGEXP_REPLACE(price,'[^0-9.]',''), 15, 2) BETWEEN 10000 AND 5000000 
                THEN TRY_TO_NUMBER(REGEXP_REPLACE(price,'[^0-9.]',''), 15, 2)
            ELSE NULL
        END AS price,

        COALESCE(
            TRY_TO_DATE(listing_date,'DD.MM.YYYY'),
            TRY_TO_DATE(listing_date,'DD/MM/YYYY'),
            TRY_TO_DATE(listing_date,'YYYY/MM/DD'),
            TRY_TO_DATE(listing_date,'YYYY-MM-DD'),
            TRY_TO_DATE(listing_date,'MM-DD-YYYY')
        ) AS listing_date,

        LOWER(TRIM(condition)) AS condition,
        LOWER(TRIM(heating_type)) AS heating_type,

        CASE
            WHEN LOWER(TRIM(parking)) IN ('yes','1')
                THEN 'yes'
            WHEN LOWER(TRIM(parking)) IN ('no','0')
                THEN 'no'
            ELSE NULL
        END AS parking,

        CASE 
            WHEN UPPER(TRIM(energy_rating)) IN ('A','B','C','D','E','F','G') 
                THEN UPPER(TRIM(energy_rating))
            ELSE NULL
        END AS energy_rating,

        _loaded_at

    FROM {{ source('bronze', 'listings_raw') }}

    QUALIFY 
    () OVER (
        PARTITION BY listing_id
        ORDER BY _loaded_at DESC
    ) = 1
)

SELECT
    *,
    ROUND(price / NULLIF(surface_m2,0),2) AS price_per_m2,
    YEAR(CURRENT_DATE()) - year_built AS property_age
FROM cleaned