import pandas as pd
import snowflake.connector

# Connection to Snowflake
conn = snowflake.connector.connect(
    account="azgcerm-yy35268",
    user="nada",
    password="Snowflake@Nada21",
    warehouse="dev_wh",
    database="real_estate_dw",
    schema="bronze",
    role="ACCOUNTADMIN"
)

# SQL Query
query = """
SELECT *
FROM BRONZE.LISTINGS_RAW
"""
# PROFILING
# 1 nombre de lignes
query = """
SELECT COUNT(*) AS nb_lignes
FROM BRONZE.LISTINGS_RAW
"""
nb_lignes = pd.read_sql(query, conn)
print(nb_lignes)
query = """
DESCRIBE TABLE BRONZE.LISTINGS_RAW
"""
noms_colonne = pd.read_sql(query, conn)
print(noms_colonne)
# 2 Nombres Des Nans 
query ="""
SELECT 
COUNT_IF(LISTING_ID IS NULL) AS NULL_LISTING_ID ,
ROUND(
COUNT_IF(LISTING_ID IS NULL) *100.0/COUNT(*), 
2
)AS prsc_nan_LISTING_ID ,
COUNT_IF(PROPERTY_TYPE IS NULL) AS NULL_PROPERTY_TYPE ,
ROUND(
COUNT_IF (PROPERTY_TYPE IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_PROPERTY_TYPE ,
COUNT_IF(COUNTRY IS NULL) AS COUNTRY ,
ROUND(
COUNT_IF (COUNTRY IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_COUNTRY ,
COUNT_IF(CITY IS NULL) AS  NULL_CITY ,
ROUND(
COUNT_IF (CITY IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_CITY ,
COUNT_IF(NEIGHBORHOOD IS NULL) AS NULL_NEIGHBORHOOD ,
ROUND(
COUNT_IF (NEIGHBORHOOD IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_NEIGHBORHOOD ,
COUNT_IF(SURFACE_M2 IS NULL) AS NULL_SURFACE_M2 ,
ROUND(
COUNT_IF (SURFACE_M2 IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_SURFACE_M2 ,
COUNT_IF(NUM_ROOMS IS NULL) AS NULL_NUM_ROOMS ,
ROUND(
COUNT_IF (NUM_ROOMS IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_SURFACE ,
COUNT_IF(NUM_BATHROOMS IS NULL) AS NUM_BATHROOMS ,
ROUND(
COUNT_IF (NUM_BATHROOMS IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_NUM_BATHROOMS ,
COUNT_IF(FLOOR IS NULL) AS NULL_FLOOR ,
ROUND(
COUNT_IF (FLOOR IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_FLOOR ,
COUNT_IF(YEAR_BUILT IS NULL) AS NULL_YEAR_BUILT ,
ROUND(
COUNT_IF (YEAR_BUILT IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_YEAR_BUILT ,
COUNT_IF(PRICE IS NULL) AS NULL_PRICE ,
ROUND(
COUNT_IF (PRICE IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_PRICE ,
COUNT_IF(LISTING_DATE IS NULL) AS NULL_LISTING_DATE ,
ROUND(
COUNT_IF (LISTING_DATE IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_LISTING_DATE ,
COUNT_IF(CONDITION IS NULL) AS NULL_CONDITION ,
ROUND(
COUNT_IF (CONDITION IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_CONDITION ,
COUNT_IF(HEATING_TYPE IS NULL) AS NULL_HEATING_TYPE ,
ROUND(
COUNT_IF (HEATING_TYPE IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_HEATING_TYPE ,
COUNT_IF(PARKING IS NULL) AS NULL_PARKING ,
ROUND(
COUNT_IF (PARKING IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_PARKING ,
COUNT_IF(ENERGY_RATING IS NULL) AS NULL_ENERGY_RATING ,
ROUND(
COUNT_IF (ENERGY_RATING IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan_ENERGY_RATING ,
COUNT_IF(_LOADED_AT IS NULL) AS NULL_LOADED_AT ,
ROUND(
COUNT_IF (_LOADED_AT IS NULL) *100.0/COUNT(*),
2
) AS prcs_nan__LOADED_AT 
FROM BRONZE.LISTINGS_RAW ;
"""
nombres_nan = pd.read_sql(query, conn)
print(nombres_nan)
# 3 Number Of Unique Values
query = """
SELECT
 COUNT (DISTINCT PROPERTY_TYPE ) AS PROPERTY_TYPE,
 COUNT (DISTINCT COUNTRY ) AS nb_COUNTRY,
 COUNT (DISTINCT CITY ) AS nb_CITY,
 COUNT (DISTINCT NEIGHBORHOOD ) AS nb_NEIGHBORHOOD,
 COUNT (DISTINCT SURFACE_M2 ) AS nb_SURFACE_M2,
 COUNT (DISTINCT NUM_ROOMS ) AS nb_NUM_ROOMS,
 COUNT (DISTINCT NUM_BATHROOMS ) AS nb_NUM_BATHROOMS,
 COUNT (DISTINCT FLOOR ) AS nb_FLOOR,
 COUNT (DISTINCT YEAR_BUILT ) AS nb_YEAR_BUILT,
 COUNT (DISTINCT LISTING_DATE ) AS nb_LISTING_DATE,
 COUNT (DISTINCT CONDITION ) AS nb_CONDITION,
 COUNT (DISTINCT HEATING_TYPE ) AS nb_HEATING_TYPE,
 COUNT (DISTINCT PARKING ) AS nb_PARKING,
 COUNT (DISTINCT ENERGY_RATING ) AS nb_ENERGY_RATING
FROM BRONZE.LISTINGS_RAW;
"""
nb_unique_values = pd.read_sql(query, conn)
print(nb_unique_values)
# 4 Unique Values 
columns = ["PROPERTY_TYPE","COUNTRY","CONDITION","HEATING_TYPE","PARKING","ENERGY_RATING"]
for col in columns:
    query = f"""
    SELECT DISTINCT {col}
    FROM BRONZE.LISTINGS_RAW
    """
    df = pd.read_sql(query, conn)
    print(f"\n=== {col} ===")
    print(df)
# 5 Doublons
query = """
SELECT LISTING_ID, COUNT(*) AS nb_occurrences
FROM BRONZE.LISTINGS_RAW
GROUP BY LISTING_ID
HAVING COUNT(*) > 1
"""
doublons = pd.read_sql(query, conn)
print(f"\n=== DOUBLONS (listing_id répétés) ===")
print(doublons)
print(f"Nombre de listing_id dupliqués: {len(doublons)}")
# 6 Price - valeurs non numériques
query = """
SELECT DISTINCT PRICE
FROM BRONZE.LISTINGS_RAW
WHERE TRY_CAST(PRICE AS FLOAT) IS NULL
"""
price_invalide = pd.read_sql(query, conn)
print(f"\n=== PRICE - valeurs non convertibles en nombre ===")
print(price_invalide)
# 7 Listing_date formats
query = """
SELECT DISTINCT LISTING_DATE
FROM BRONZE.LISTINGS_RAW
ORDER BY LISTING_DATE
LIMIT 30
"""
dates_sample = pd.read_sql(query, conn)
print(f"\n=== LISTING_DATE - échantillon des formats ===")
print(dates_sample)
# 8 Valeurs aberrantes - price et surface
query = """
SELECT 
    MIN(TRY_CAST(PRICE AS FLOAT)) AS min_price,
    MAX(TRY_CAST(PRICE AS FLOAT)) AS max_price,
    AVG(TRY_CAST(PRICE AS FLOAT)) AS avg_price,
    MIN(TRY_CAST(SURFACE_M2 AS FLOAT)) AS min_surface,
    MAX(TRY_CAST(SURFACE_M2 AS FLOAT)) AS max_surface,
    AVG(TRY_CAST(SURFACE_M2 AS FLOAT)) AS avg_surface
FROM BRONZE.LISTINGS_RAW
"""
stats_aberrantes = pd.read_sql(query, conn)
print(f"\n=== STATS PRICE & SURFACE (min/max/avg) ===")
print(stats_aberrantes)
# 9 Vérifier format listing_date avec le point (.)
query = """
SELECT LISTING_DATE
FROM BRONZE.LISTINGS_RAW
WHERE LISTING_DATE LIKE '%.%'
AND TRY_CAST(SPLIT_PART(LISTING_DATE, '.', 1) AS INT) > 12
"""
verif_point = pd.read_sql(query, conn)
print(f"\n=== Dates avec point où le 1er nombre > 12 (donc = jour, format DD.MM) ===")
print(verif_point)
print(f"Nombre de cas: {len(verif_point)}")
# 10 Vérifier format listing_date avec le slash (/)
query = """
SELECT LISTING_DATE
FROM BRONZE.LISTINGS_RAW
WHERE LISTING_DATE LIKE '%/%'
AND TRY_CAST(SPLIT_PART(LISTING_DATE, '/', 1) AS INT) > 12
"""
verif_slash = pd.read_sql(query, conn)
print(f"\n=== Dates avec slash où le 1er nombre > 12 (donc = jour, format DD/MM) ===")
print(verif_slash)
print(f"Nombre de cas: {len(verif_slash)}")
# 11 Vérifier format listing_date avec le tiret (-)
query = """
SELECT LISTING_DATE
FROM BRONZE.LISTINGS_RAW
WHERE LISTING_DATE LIKE '%-%'
AND TRY_CAST(SPLIT_PART(LISTING_DATE, '-', 2) AS INT) > 12
"""
verif_tiret = pd.read_sql(query, conn)
print(f"\n=== Dates avec tiret où le 2ème nombre > 12 (donc = jour, format MM-DD) ===")
print(verif_tiret)
print(f"Nombre de cas: {len(verif_tiret)}")
# 12 Vérifier les dates qui échouent tous les formats connus
query = """
SELECT DISTINCT LISTING_DATE
FROM BRONZE.LISTINGS_RAW
WHERE COALESCE(
    TRY_TO_DATE(LISTING_DATE,'DD.MM.YYYY'),
    TRY_TO_DATE(LISTING_DATE,'DD/MM/YYYY'),
    TRY_TO_DATE(LISTING_DATE,'YYYY/MM/DD'),
    TRY_TO_DATE(LISTING_DATE,'MM-DD-YYYY')
) IS NULL
LIMIT 30
"""
dates_echec = pd.read_sql(query, conn)
print(f"\n=== Dates qui échouent tous les formats ===")
print(dates_echec)
# 13 Country - vérifier formats/espaces
query = """
SELECT DISTINCT COUNTRY, LENGTH(COUNTRY) AS longueur
FROM BRONZE.LISTINGS_RAW
WHERE COUNTRY IS NOT NULL
ORDER BY COUNTRY
"""
country_check = pd.read_sql(query, conn)
print(f"\n=== COUNTRY - vérification espaces/casse ===")
print(country_check)
# 14 Condition - vérifier variantes cachées
query = """
SELECT DISTINCT CONDITION, LENGTH(CONDITION) AS longueur
FROM BRONZE.LISTINGS_RAW
WHERE CONDITION IS NOT NULL
ORDER BY CONDITION
"""
condition_check = pd.read_sql(query, conn)
print(f"\n=== CONDITION - vérification espaces/casse ===")
print(condition_check)
# 15 Year_built - vérifier valeurs aberrantes
query = """
SELECT 
    MIN(TRY_TO_NUMBER(YEAR_BUILT)) AS min_year,
    MAX(TRY_TO_NUMBER(YEAR_BUILT)) AS max_year
FROM BRONZE.LISTINGS_RAW
"""
year_check = pd.read_sql(query, conn)
print(f"\n=== YEAR_BUILT - min/max ===")
print(year_check)


















