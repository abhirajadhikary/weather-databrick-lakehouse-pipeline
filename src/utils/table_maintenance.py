from pyspark.sql import SparkSession

def optimize_and_vacuum_tables(spark: SparkSession):
    tables = [
        "weather_catalog.bronze.brz_weather_observations",
        "weather_catalog.silver.slv_weather_observations",
        "weather_catalog.gold.fact_weather_observations",
        "weather_catalog.gold.gld_daily_weather_metrics"
    ]

    for tbl in tables:
        # Compact small files
        spark.sql(f"OPTIMIZE {tbl}")

        spark.sql(f"VACUUM {tbl} RETAIN 168 HOURS")
