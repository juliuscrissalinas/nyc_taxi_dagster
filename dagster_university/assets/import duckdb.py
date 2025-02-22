import duckdb
import pandas as pd

# Path to the Parquet file
parquet_file_path = '/Users/juliuscrissalinas/Documents/MSDS/ML_Ops/dagster/dagster_university/data/raw/taxi_trips_2023-03.parquet'

# Connect to DuckDB in-memory database
conn = duckdb.connect()

# Read the Parquet file
df = conn.execute(f"SELECT * FROM '{parquet_file_path}'").fetch_df()

# Convert the pickup_datetime column to datetime
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

# Filter for dates in the year 2023
df_2023 = df[df['tpep_pickup_datetime'].dt.year == 2023]

# Extract unique dates and sort them
unique_dates_2023 = sorted(df_2023['tpep_pickup_datetime'].dt.date.unique())

# Print the unique dates
print("Unique pickup dates in the Parquet file for the year 2023:")
for date in unique_dates_2023:
    print(date)
