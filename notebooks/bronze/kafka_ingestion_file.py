# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# DBTITLE 1,Option 2: Streaming Read with Auto Loader
# Streaming read - monitors directory for new files
# Auto Loader automatically infers schema and handles new files

# Directory to monitor (where JSON files will be dropped)
input_path = "/Volumes/ingestion/raw_ads/raw/json_input/"

# Read streaming with Auto Loader
file_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/streaming_ads/raw_ads/raw/checkpoints/file_schema")
        .load(input_path)
)

# Apply same schema as Kafka events (if needed)
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

# Input does not have nested JSON, using Auto Loader to handle schema
from pyspark.sql.functions import col, from_json

# The file is already structured JSON, using it directly:
parsed_file_df = file_stream_df

# Write to bronze delta table (different path from Kafka to keep separate)
file_bronze_query = (
    parsed_file_df.writeStream
        .format("delta")
        .option("checkpointLocation", "/Volumes/streaming_ads/raw_ads/raw/checkpoints/ads_bronze_file")
        .outputMode("append")
        .start("/Volumes/streaming_ads/raw_ads/raw/delta/ads_bronze_file")
)

# TODO: Convert the Kafka payload to string to store raw payload plus metadata so it can replayed or debugged later
# This JSON parsing to be moved to Silver

# COMMAND ----------

# DBTITLE 1,Check File Stream Status
# Check status of file-based stream
file_bronze_query.status

# COMMAND ----------

# DBTITLE 1,Query File-Based Bronze Table
# Checking that the code worked
try:
    file_bronze_df = spark.read.format("delta").load("/Volumes/ingestion/raw_ads/raw/delta/ads_bronze_file")
    print(f"Total records from file ingestion: {file_bronze_df.count()}")
    display(file_bronze_df)
except Exception as e:
    if "PATH_NOT_FOUND" in str(e):
        print("⚠️ Delta table doesn't exist yet.")
        print("Upload JSON files to /Volumes/ingestion/raw_ads/raw/json_input/ and the stream will process them.")
    else:
        raise e
