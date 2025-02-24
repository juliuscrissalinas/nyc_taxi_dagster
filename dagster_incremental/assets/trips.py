import requests
from dagster_duckdb import DuckDBResource
from . import constants
from dagster import asset, AssetExecutionContext
from dagster._utils.backoff import backoff
from ..partitions import monthly_partition
import pandas as pd


@asset(partitions_def=monthly_partition)
def taxi_trips_file(context: AssetExecutionContext) -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]  # remove the day from the partition key

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)


@asset
def taxi_zones_file() -> None:
    """
    This asset will contain a unique identifier and name for each part of NYC as a distinct taxi zone.
    """
    raw_taxi_zones = requests.get(
        f"https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_taxi_zones.content)


@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition
)
def taxi_trips_partitioned(
    context: AssetExecutionContext, database: DuckDBResource
) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database, partitioned by month.
    """

    partition_date_str = (
        context.partition_key
    )  # partition_key is a parameter that specifies the partition key for the asset
    month_to_fetch = partition_date_str[:-3]

    query = f"""
      create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
      );

      delete from trips where partition_date = '{month_to_fetch}';

      insert into trips
      select
        VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
        tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
      from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)


@asset(deps=["taxi_trips_file"])
def taxi_trips_full_load(database: DuckDBResource) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database, not partitioned.
    """
    start_date = pd.to_datetime(constants.START_DATE)
    end_date = pd.to_datetime(constants.END_DATE)
    date_range = pd.date_range(start=start_date, end=end_date, freq="MS")

    query = """
      create table if not exists trips_full (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double
      );

      delete from trips_full;
    """

    for date in date_range:
        month_to_fetch = date.strftime("%Y-%m")
        query += f"""
          insert into trips_full
          select
            VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
            tpep_pickup_datetime, trip_distance, passenger_count, total_amount
          from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
        """

    with database.get_connection() as conn:
        conn.execute(query)


@asset(deps=["taxi_zones_file"])
def taxi_zones(database: DuckDBResource) -> None:
    """
    Use the taxi_zones_file file to create a table called zones in your DuckDB database
    """
    query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)
