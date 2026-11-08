"""Repeat-safe BTS monthly flight pipeline for Databricks serverless jobs."""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from typing import Any

MONTH_RE = re.compile(r"source_month=(\d{4}-\d{2})")
DEFAULT_RAW_ROOT = "/Volumes/workspace/airline_ops/raw_files/bts/on_time"


def source_month_from_path(path: str) -> str:
    """Get and validate the YYYY-MM partition from a BTS landing path."""
    match = MONTH_RE.search(path.replace("\\", "/"))
    if not match:
        raise ValueError(f"No source_month=YYYY-MM segment in path: {path}")
    month = match.group(1)
    datetime.strptime(month, "%Y-%m")
    return month


def percent(numerator: float, denominator: float) -> float:
    """Return a percentage rounded to two decimals; empty groups return zero."""
    if denominator == 0:
        return 0.0
    return round(100.0 * numerator / denominator, 2)


def _spark_functions() -> Any:
    # Import Spark only inside the job so small helper tests run on any laptop.
    from pyspark.sql import functions as F

    return F


def _as_double(column_name: str) -> Any:
    F = _spark_functions()
    safe_name = column_name.replace("`", "``")
    return F.expr(f"try_cast(`{safe_name}` AS DOUBLE)")


def _table_names(catalog: str, schema: str) -> dict[str, str]:
    prefix = f"{catalog}.{schema}"
    return {
        "bronze": f"{prefix}.bronze_flights",
        "silver": f"{prefix}.silver_flights",
        "airline": f"{prefix}.gold_airline_monthly",
        "airport": f"{prefix}.gold_airport_monthly",
        "route": f"{prefix}.gold_route_monthly",
    }


def _write_month(df: Any, table_name: str, month: str, spark: Any) -> None:
    writer = df.write.format("delta").option("mergeSchema", "true")
    if spark.catalog.tableExists(table_name):
        (
            writer.mode("overwrite")
            .option("replaceWhere", f"_source_month = '{month}'")
            .saveAsTable(table_name)
        )
    else:
        (writer.mode("overwrite").partitionBy("_source_month").saveAsTable(table_name))


def _list_month_files(dbutils: Any, raw_root: str) -> list[tuple[str, str]]:
    files: list[tuple[str, str]] = []
    for folder in dbutils.fs.ls(raw_root):
        if not folder.isDir():
            continue
        try:
            month = source_month_from_path(folder.path)
        except ValueError:
            continue
        for candidate in dbutils.fs.ls(folder.path):
            if candidate.name.lower() == "flights.csv":
                files.append((month, candidate.path))
                break
    return sorted(files)


def _make_silver(bronze: Any) -> Any:
    F = _spark_functions()
    flight_count = _as_double("Flights")
    cancelled_value = _as_double("Cancelled")
    diverted_value = _as_double("Diverted")
    arrival_delay = _as_double("ArrDelay")
    dep_delay = _as_double("DepDelay")
    arrival_15 = _as_double("ArrDel15")

    silver = bronze.select(
        F.col("_source_month"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.to_date(F.col("FlightDate")).alias("flight_date"),
        F.trim(F.col("Reporting_Airline")).alias("airline_code"),
        F.trim(F.col("Tail_Number")).alias("tail_number"),
        F.trim(F.col("Origin")).alias("origin_airport_code"),
        F.trim(F.col("Dest")).alias("destination_airport_code"),
        flight_count.alias("flight_count"),
        _as_double("Distance").alias("distance_miles"),
        dep_delay.alias("departure_delay_signed_minutes"),
        _as_double("DepDelayMinutes").alias("departure_delay_minutes"),
        arrival_delay.alias("arrival_delay_signed_minutes"),
        _as_double("ArrDelayMinutes").alias("arrival_delay_minutes"),
        arrival_15.alias("arrival_delay_15_flag"),
        _as_double("DepDel15").alias("departure_delay_15_flag"),
        F.when(cancelled_value > 0, F.lit(1)).otherwise(F.lit(0)).alias("cancelled_flag"),
        F.when(diverted_value > 0, F.lit(1)).otherwise(F.lit(0)).alias("diverted_flag"),
        _as_double("CarrierDelay").alias("carrier_delay_minutes"),
        _as_double("WeatherDelay").alias("weather_delay_minutes"),
        _as_double("NASDelay").alias("nas_delay_minutes"),
        _as_double("SecurityDelay").alias("security_delay_minutes"),
        _as_double("LateAircraftDelay").alias("late_aircraft_delay_minutes"),
    )

    required_ok = (
        F.col("flight_date").isNotNull()
        & F.col("airline_code").isNotNull()
        & (F.col("airline_code") != "")
        & F.col("origin_airport_code").isNotNull()
        & F.col("destination_airport_code").isNotNull()
        & (F.coalesce(F.col("flight_count"), F.lit(0.0)) > 0)
    )
    return silver.withColumn(
        "_row_quality", F.when(required_ok, F.lit("valid")).otherwise(F.lit("invalid"))
    )


def _metrics(group_columns: list[str]) -> Any:
    F = _spark_functions()
    count = F.coalesce(F.col("flight_count"), F.lit(0.0))
    complete = (F.col("cancelled_flag") == 0) & (F.col("diverted_flag") == 0)
    scheduled = F.sum(count)
    completed = F.sum(F.when(complete, count).otherwise(F.lit(0.0)))
    on_time = F.sum(
        F.when(complete & (F.col("arrival_delay_15_flag") == 0), count).otherwise(F.lit(0.0))
    )
    cancelled = F.sum(F.when(F.col("cancelled_flag") == 1, count).otherwise(F.lit(0.0)))
    diverted = F.sum(F.when(F.col("diverted_flag") == 1, count).otherwise(F.lit(0.0)))

    return [
        scheduled.alias("scheduled_flights"),
        F.round(F.when(completed > 0, 100.0 * on_time / completed).otherwise(0.0), 2).alias(
            "on_time_arrival_pct"
        ),
        F.round(F.when(scheduled > 0, 100.0 * cancelled / scheduled).otherwise(0.0), 2).alias(
            "cancellation_pct"
        ),
        F.round(F.when(scheduled > 0, 100.0 * diverted / scheduled).otherwise(0.0), 2).alias(
            "diversion_pct"
        ),
        F.round(F.avg(F.when(complete, F.col("arrival_delay_signed_minutes"))), 2).alias(
            "avg_arrival_delay_signed_minutes"
        ),
    ]


def _build_gold(spark: Any, silver_table: str, tables: dict[str, str]) -> None:
    F = _spark_functions()
    valid = spark.table(silver_table).where(F.col("_row_quality") == "valid")

    airline = valid.groupBy("_source_month", "airline_code").agg(*_metrics([]))
    airport = valid.groupBy(
        "_source_month", F.col("origin_airport_code").alias("airport_code")
    ).agg(*_metrics([]))
    route = valid.groupBy("_source_month", "origin_airport_code", "destination_airport_code").agg(
        *_metrics([])
    )
    for frame, table_name in (
        (airline, tables["airline"]),
        (airport, tables["airport"]),
        (route, tables["route"]),
    ):
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )


def run_pipeline(
    spark: Any,
    dbutils: Any,
    raw_root: str = DEFAULT_RAW_ROOT,
    catalog: str = "workspace",
    schema: str = "airline_ops",
) -> None:
    F = _spark_functions()
    tables = _table_names(catalog, schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    monthly_files = _list_month_files(dbutils, raw_root)
    if not monthly_files:
        raise RuntimeError(f"No source_month=YYYY-MM/flights.csv files found under {raw_root}")

    for month, file_path in monthly_files:
        raw = spark.read.option("header", "true").option("inferSchema", "false").csv(file_path)
        if "Unnamed: 109" in raw.columns:
            raw = raw.withColumnRenamed("Unnamed: 109", "source_extra_109")
        raw = (
            raw.withColumn("_source_month", F.lit(month))
            .withColumn("_source_file", F.lit(file_path))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        _write_month(raw, tables["bronze"], month, spark)

        bronze_month = spark.table(tables["bronze"]).where(F.col("_source_month") == month)
        silver_month = _make_silver(bronze_month)
        _write_month(silver_month, tables["silver"], month, spark)
        quality = silver_month.groupBy("_row_quality").count().orderBy("_row_quality")
        print(f"Silver quality counts for {month}:")
        quality.show(truncate=False)

    _build_gold(spark, tables["silver"], tables)
    for label in ("airline", "airport", "route"):
        count = spark.table(tables[label]).count()
        print(f"{tables[label]} rows: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest and summarize BTS monthly flight CSVs")
    parser.add_argument("--raw-root", default=DEFAULT_RAW_ROOT)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="airline_ops")
    args = parser.parse_args()

    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_pipeline(spark, DBUtils(spark), args.raw_root, args.catalog, args.schema)


if __name__ == "__main__":
    main()
