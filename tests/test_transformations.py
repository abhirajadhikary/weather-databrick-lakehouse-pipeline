import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("weather-lakehouse-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

def test_silver_schema_validation(spark):
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("humidity", LongType(), True)
    ])
    data = [("London", 15.5, 80), ("New York", 20.0, 65), ("Paris", 18.3, 70)]
    df = spark.createDataFrame(data, schema)
    
    assert df.count() == 3
    assert "temp_c" in df.columns
    assert df.schema["temp_c"].dataType == DoubleType()

def test_quarantine_filter_logic(spark):
    data = [
        ("London", 15.5, 80),
        ("BadCity", -99.0, -10)
    ]
    columns = ["city", "temp_c", "humidity"]
    df = spark.createDataFrame(data, columns)

    valid_df = df.filter((df.temp_c > -80) & (df.humidity >= 0))
    invalid_df = df.filter((df.temp_c <= -80) | (df.humidity < 0))

    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    assert valid_df.first()["city"] == "London"