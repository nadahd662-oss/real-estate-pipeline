import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os

# --- Snowflake connection details ---
conn = snowflake.connector.connect(
    account="azgcerm-yy35268",      # your Snowflake account
    user="nada",            # replace with your actual username
    password="Snowflake@Nada21",        # replace with your actual password
    warehouse="dev_wh",
    database="real_estate_dw",
    schema="bronze",
    role="ACCOUNTADMIN"
)

# --- Load CSV exactly as-is (all columns as strings) ---
csv_path = os.path.join("data", "real-estate-raw-6a3e627c94437783777235.csv")
df = pd.read_csv(csv_path, dtype=str)  # dtype=str forces raw, untouched values

# --- Add the metadata column ---
df["_LOADED_AT"] = datetime.utcnow()

# --- Uppercase column names to match Snowflake convention ---
df.columns = [col.upper() for col in df.columns]

# --- Write to Snowflake Bronze table ---
success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name="LISTINGS_RAW",
    schema="BRONZE",
    database="REAL_ESTATE_DW"
)

print(f"Loaded {nrows} rows into bronze.listings_raw — success: {success}")

conn.close()