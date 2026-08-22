from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json
import uuid
from datetime import datetime

def save_raw_json_to_bronze(spark: SparkSession, raw_payloads: list, table_name: str, batch_id: str):
    if not raw_payloads:
        return
    
    json_list = [json.dumps(p) for p in raw_payloads]
    string_df = spark.createDataFrame([(j,) for j in json_list], ["json_str"])
    
    # Infer schema from JSON strings using schema_of_json_agg
    schema = string_df.selectExpr("schema_of_json_agg(json_str)").collect()[0][0]
    
    # Parse JSON using from_json with inferred schema
    df = string_df.select(F.from_json(F.col("json_str"), schema).alias("data")).select("data.*")

    bronze_df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
                  .withColumn("batch_id", F.lit(batch_id)) \
                  .withColumn("source_system", F.lit("WeatherAPI"))

    bronze_df.write.format("delta") \
             .mode("append") \
             .option("mergeSchema", "true") \
             .saveAsTable(table_name)