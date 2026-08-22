from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_silver_to_gold(spark: SparkSession):
    spark.sql("CREATE SCHEMA IF NOT EXISTS weather_catalog.gold")

    # Enabling Change Data Feed on Silver table for incremnetal downstream tracking
    spark.sql("""
        ALTER TABLE weather_catalog.silver.slv_weather_observations
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    silver_df = spark.read.table("weather_catalog.silver.slv_weather_observations")

    # 1. Dimension Table: dim_location
    dim_location = silver_df.select(
        F.md5(F.concat_ws("||", F.col("name"), F.col("region"), F.col("country"))).alias("location_key"),
        F.col("name").alias("city"),
        F.col("region"),
        F.col("country"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("timezone_id")
    ).dropDuplicates(["location_key"])

    dim_location.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("weather_catalog.gold.dim_location")

    # 2. Dimension Table: dim_weather_condition
    dim_condition = silver_df.select(
        F.col("condition_code"),
        F.col("condition").alias("condition_text")
    ).dropDuplicates(["condition_code"])

    dim_condition.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("weather_catalog.gold.dim_weather_condition")

    # 3. Fact Table: fact_weather_observations
    fact_df = silver_df.select(
        F.md5(F.concat_ws("||", F.col("name"), F.col("region"), F.col("country"))).alias("location_key"),
        F.col("condition_code"),
        F.col("last_updated_epoch").alias("observation_epoch"),
        F.to_date(F.col("last_updated")).alias("observation_date"),
        F.to_timestamp(F.col("last_updated")).alias("observation_timestamp"),
        F.col("temp_celsius"),
        F.col("feels_like_celsius"),
        F.col("humidity"),
        F.col("cloud"),
        F.col("visibility"),
        F.col("chances_of_rain"),
        F.col("chances_of_snow"),
        F.col("will_it_rain"),
        F.col("will_it_snow"),
        F.col("dew_point"),
        F.col("solar_radiation"),
        F.col("direct_normal_irradiance"),
        F.col("global_total_irradiance"),
        F.col("solar_shortwave_radiation"),
        F.col("wet_bulb"),
        F.col("wind_degree"),
        F.col("gust_speed"),
        F.col("precipitation_mm"),
        F.col("wind_speed"),
        F.col("wind_direction"),
        F.col("pressure"),
        F.col("uv_index"),
        F.col("batch_id"),
        F.col("ingestion_timestamp")
    )

    fact_df.createOrReplaceTempView("staged_fact_observations")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS weather_catalog.gold.fact_weather_observations
        USING DELTA
        AS SELECT * FROM staged_fact_observations WHERE 1=0    
    """)
    spark.sql("""
        MERGE INTO weather_catalog.gold.fact_weather_observations as t
        USING staged_fact_observations AS s
        ON t.location_key = s.location_key AND t.observation_epoch = s.observation_epoch
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *    
    """)

    # 4. Gold Aggregate Product: gld_daily_weather_metrics
    daily_metrics = spark.sql("""
        SELECT 
            location_key,
            observation_date,
            
            -- Temperature Metrics
            ROUND(AVG(temp_celsius), 2) AS avg_temp_celsius,
            MAX(temp_celsius) AS max_temp_celsius,
            MIN(temp_celsius) AS min_temp_celsius,
            ROUND(AVG(feels_like_celsius), 2) AS avg_feels_like_celsius,
            
            -- Atmospheric & Humidity
            ROUND(AVG(humidity), 2) AS avg_humidity,
            ROUND(AVG(pressure), 2) AS avg_pressure_mb,
            ROUND(AVG(dew_point), 2) AS avg_dew_point_celsius,
            ROUND(AVG(wet_bulb), 2) AS avg_wet_bulb_celsius,
            
            -- Precipitation & Cloud
            ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
            ROUND(AVG(cloud), 2) AS avg_cloud_cover_pct,
            MAX(will_it_rain) AS rained_flag,
            MAX(will_it_snow) AS snowed_flag,
            
            -- Wind & Solar
            ROUND(MAX(wind_speed), 2) AS max_wind_speed_kph,
            ROUND(MAX(gust_speed), 2) AS max_gust_speed_kph,
            ROUND(AVG(solar_radiation), 2) AS avg_solar_radiation,
            ROUND(MAX(uv_index), 2) AS max_uv_index,
            
            -- Row count tracking
            COUNT(1) AS total_observations
        FROM weather_catalog.gold.fact_weather_observations
        GROUP BY location_key, observation_date
    """)

    daily_metrics.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("weather_catalog.gold.gld_daily_weather_metrics")