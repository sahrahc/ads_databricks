# Databricks notebook source
# read kafka messages NOT OPERATIONAL YET
# need to set up AWS kafka
kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "ads_events")
        .option("startingOffsets", "latest")
        .load()
)

# convert kafka value to json
from pyspark.sql.functions import col

json_df = kafka_df.select(
    col("timestamp"),
    col("value").cast("string").alias("json_payload")
)

# schema for synthetic ad events
from pyspark.sql.types import *

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("campaign_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("geo", StringType()),
    StructField("device", StringType()),
    StructField("price_paid", DoubleType()),
    StructField("conversion_value", DoubleType())
])

# parse json into structured data
from pyspark.sql.functions import from_json

parsed_df = json_df.select(
    from_json(col("json_payload"), event_schema).alias("data")
).select("data.*")

# write to bronze delta table
bronze_query = (
    parsed_df.writeStream
        .format("delta")
        .option("checkpointLocation", "/Volumes/streaming_ads/raw_ads/raw/checkpoints/ads_bronze")
        .outputMode("append")
        .start("/Volumes/streaming_ads/raw_ads/raw/delta/ads_bronze")
)

# COMMAND ----------

# query status 
bronze_query.status

# COMMAND ----------

# DBTITLE 1,Verify Stream Status and Checkpoint
# simple alternative: spark.streams.active
# Check detailed stream status
for stream in spark.streams.active:
    print(f"Stream ID: {stream.id}")
    print(f"Status: {stream.status}")
    print(f"Recent Progress:")
    print(stream.recentProgress)
    print("\n" + "="*50 + "\n")

# COMMAND ----------

# DBTITLE 1,Query the bronze table
# query the bronze table (will only exist after data flows from Kafka)
try:
    df = spark.read.format("delta").load("/Volumes/ingestion/raw_ads/raw/delta/ads_bronze")
    print(f"Total records: {df.count()}")
    display(df)
except Exception as e:
    if "PATH_NOT_FOUND" in str(e):
        print("⚠️ Delta table doesn't exist yet.")
        print("This is expected - the table will be created once the stream receives data from Kafka.")
        print("\nFix the Kafka connection issue above, then wait for data to flow.")
    else:
        raise e
