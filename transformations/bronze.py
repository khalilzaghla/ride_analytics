import dlt
from pyspark.sql import functions as F 
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    IntegerType,
    BooleanType
)
LANDING_PATH = spark.conf.get("landing_volume_path")



RIDE_SCHEMA = StructType([
    StructField("ride_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("status", StringType(), True),
    StructField("city", StringType(), True),
    StructField("pickup_area", StringType(), True),
    StructField("drop_area", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_rating", DoubleType(), True),
    StructField("rider_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("duration_mins", IntegerType(), True),
    StructField("base_fare", DoubleType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("final_fare", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("is_peak_hour", BooleanType(), True)
])

 # to create the matrialzed view !! 
@dlt.table
def bronze_ride_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(RIDE_SCHEMA)
        .load(LANDING_PATH)
        .withColumn("_ingest_time", F.current_timestamp())
    )


