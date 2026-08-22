from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_bronze_to_silver(spark: SparkSession):
    spark.sql("CREATE SCHEMA IF NOT EXISTS weather_catalog.silver")
    bronze_df = spark.read.table("weather_catalog.bronze.brz_weather_observations")

    flattened_df = bronze_df.select(
        # Weather Observation - temp(c/f), feelslike(c/f), humidity, cloud, uv, visibility
        F.col("current.temp_c").alias("temp_celsius"),
        F.col("current.feelslike_c").alias("feels_like_celsius"),
        F.col("current.humidity").alias("humidity"),
        F.col("current.cloud").alias("cloud"),
        F.col("current.uv").alias("uv_index"),
        F.col("current.vis_km").alias("visibility"),

        # condition, condition code
        F.col("current.condition.text").alias("condition"),
        F.col("current.condition.code").alias("condition_code"),
        
        # Precipitation - precipitation(mm), chances of rain/snow, will it rain/snow
        F.col("current.precip_mm").alias("precipitation_mm"),
        F.col("current.chance_of_rain").alias("chances_of_rain"),
        F.col("current.will_it_rain").alias("will_it_rain"),
        F.col("current.chance_of_snow").alias("chances_of_snow"),
        F.col("current.will_it_snow").alias("will_it_snow"),
        
        # Wind - wind direction, wind degree, wind speed, gust
        F.col("current.wind_dir").alias("wind_direction"),
        F.col("current.wind_degree").alias("wind_degree"),
        F.col("current.wind_kph").alias("wind_speed"),
        F.col("current.gust_kph").alias("gust_speed"),

        # Pressure and atmospheric - pressure, dew point, wet bulb
        F.col("current.pressure_mb").alias("pressure"),
        F.col("current.dewpoint_c").alias("dew_point"),
        F.col("current.wetbulb_c").alias("wet_bulb"),
        
        # Solar radiation - diffuse radiation, direct normal irradiance, global total irradiance, shortwave radiation
        F.col("current.diff_rad").alias("solar_radiation"),
        F.col("current.dni").alias("direct_normal_irradiance"),
        F.col("current.gti").alias("global_total_irradiance"),
        F.col("current.short_rad").alias("solar_shortwave_radiation"),
        
        # Time information of last update
        F.col("current.last_updated").alias("last_updated"),
        F.col("current.last_updated_epoch").alias("last_updated_epoch"),

        # Location - country, lat, localtime, localtime_epoch, name, region, timezone
        F.col("location.country").alias("country"),
        F.col("location.lat").alias("latitude"),
        F.col("location.localtime").alias("localtime"),
        F.col("location.localtime_epoch").alias("localtime_epoch"),
        F.col("location.lon").alias("longitude"),
        F.col("location.name").alias("name"),
        F.col("location.region").alias("region"),
        F.col("location.tz_id").alias("timezone_id"),

        # Ingestion metadata -
        F.col("ingestion_timestamp").alias("ingestion_timestamp"),
        F.col("batch_id").alias("batch_id"),
        F.col("source_system").alias("source_system")
    )

    valid_condition = (
        F.col("temp_celsius").isNotNull()
        & F.col("humidity").between(0, 100)
        & F.col("cloud").between(0, 100)
        & (F.col("uv_index") >= 0)
        & (F.col("visibility") >= 0)

        & F.col("condition").isNotNull()
        & F.col("condition_code").isNotNull()

        & (F.col("precipitation_mm") >= 0)
        & F.col("chances_of_rain").between(0, 100)
        & F.col("chances_of_snow").between(0, 100)
        & F.col("will_it_rain").isin(0, 1)
        & F.col("will_it_snow").isin(0, 1)

        & F.col("wind_direction").isNotNull()
        & F.col("wind_degree").between(0, 360)
        & (F.col("wind_speed") >= 0)
        & (F.col("gust_speed") >= 0)

        & (F.col("pressure") > 0)
        & F.col("dew_point").isNotNull()
        & F.col("wet_bulb").isNotNull()

        & (F.col("solar_radiation") >= 0)
        & (F.col("direct_normal_irradiance") >= 0)
        & (F.col("global_total_irradiance") >= 0)
        & (F.col("solar_shortwave_radiation") >= 0)

        & F.col("last_updated").isNotNull()
        & F.col("last_updated_epoch").isNotNull()

        & F.col("country").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & F.col("name").isNotNull()
        & F.col("region").isNotNull()
        & F.col("timezone_id").isNotNull()

        & F.col("ingestion_timestamp").isNotNull()
        & F.col("batch_id").isNotNull()
        & F.col("source_system").isNotNull()
    )

    valid_df = flattened_df.filter(F.coalesce(valid_condition, F.lit(False)))
    invalid_df = flattened_df.filter(~F.coalesce(valid_condition, F.lit(False)))

    invalid_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("weather_catalog.silver.slv_invalid_records")

    dedup_df = valid_df.dropDuplicates(["name", "last_updated_epoch"])
    dedup_df.createOrReplaceTempView("staged_silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS weather_catalog.silver.slv_weather_observations
        USING DELTA
        AS SELECT * FROM staged_silver WHERE 1=0
    """)

    spark.sql("""
        MERGE INTO weather_catalog.silver.slv_weather_observations AS t
        USING staged_silver AS s
        ON t.name = s.name AND t.last_updated_epoch = s.last_updated_epoch
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    