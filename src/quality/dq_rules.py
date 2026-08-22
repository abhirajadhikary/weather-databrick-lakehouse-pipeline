from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

def run_data_quality_checks(spark: SparkSession, batch_id: str):
    spark.sql("CREATE SCHEMA IF NOT EXISTS weather_catalog.observability")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS weather_catalog.observability.data_quality_results (
            check_name STRING,
            target_table STRING,
            status STRING,
            failed_records LONG,
            execution_timestamp TIMESTAMP,
            batch_id STRING
        ) USING DELTA     
    """)

    # 1. Check Silver Invalid Records Volume
    invalid_count = spark.read.table("weather_catalog.silver.slv_invalid_records") \
        .filter(F.col("batch_id") == batch_id).count()

    invalid_status = "PASSED" if invalid_count == 0 else "FAILED"

    # 2. Check Fact Duplicate Primary Keys
    fact_dups = spark.read.table("weather_catalog.gold.fact_weather_observations") \
        .groupBy("location_key", "observation_epoch") \
        .count() \
        .filter(F.col("count") > 1).count()

    dup_status = "PASSED" if fact_dups == 0 else "FAILED"

    # Save DQ Log Entries
    dq_records = [
        ("quarantine_record_check", "weather_catalog.silver.slv_invalid_records", invalid_status, invalid_count, datetime.now(), batch_id),
        ("fact_uniqueness_check", "weather_catalog.gold.fact_weather_observations", dup_status,
         fact_dups, datetime.now(), batch_id)
    ]

    dq_df = spark.createDataFrame(dq_records, ["check_name", "target_table", "status", "failed_records", "execution_timestamp", "batch_id"])
    dq_df.write.format("delta").mode("append").saveAsTable("weather_catalog.observability.data_quality_results")