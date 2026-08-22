from pyspark.sql import SparkSession
from datetime import datetime

def log_pipeline_metrics(spark: SparkSession, batch_id: str, status: str = "SUCCESS"):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS weather_catalog.observability.pipeline_runs (
            batch_id STRING,
            bronze_count LONG,
            silver_count LONG,
            gold_fact_count LONG,
            status STRING,
            execution_timestamp TIMESTAMP
        ) USING DELTA
    """)

    b_count = spark.read.table("weather_catalog.bronze.brz_weather_observations").filter(f"batch_id = '{batch_id}'").count()
    s_count = spark.read.table("weather_catalog.silver.slv_weather_observations").filter(f"batch_id = '{batch_id}'").count()
    g_count = spark.read.table("weather_catalog.gold.fact_weather_observations").filter(f"batch_id = '{batch_id}'").count()

    metrics = [(batch_id, b_count, s_count, g_count, status, datetime.now())]
    df = spark.createDataFrame(metrics, ["batch_id", "bronze_count", "silver_count", "gold_fact_count", "status", "execution_timestamp"])
    df.write.format("delta").mode("append").saveAsTable("weather_catalog.observability.pipeline_runs")