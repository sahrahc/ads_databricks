# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %md
# MAGIC Dedup in this stage, set data types including time normalization

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./config_silver

# COMMAND ----------

# DBTITLE 1,Transform and Write Silver Table
# data types to cast columns to
from pyspark.sql.functions import col, to_timestamp

# Cast timestamp and floats

silver_df = bronze_stream.select(
    col("event_id"),
    col("event_type"),
    to_timestamp("event_time").alias("event_time"),
    col("campaign_id"),
    col("user_id"),
    col("session_id"),
    col("geo"),
    col("device"),
    col("price_paid").cast("double"),
    col("conversion_value").cast("double")
)

# dedup events, up to 10 minutes late
silver_df = (
    silver_df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

# write silver table
silver_query = (
    silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_silver_path}/stg_ads_event_file")
        .trigger(availableNow=True)
        .start(f"{silver_path}/stg_ads_event_file")
)



# COMMAND ----------

# DBTITLE 1,Verify
# Verify data in silver
try:
    file_silver_df = spark.read.format("delta").load(f"{silver_path}/stg_ads_event_file")
    print(f"Total records from silver processing: {file_silver_df.count()}")
    display(file_silver_df.limit(20))
except Exception as e:
    if "PATH_NOT_FOUND" in str(e):
        print("⚠️ Delta table doesn't exist yet.")
        print("Upload JSON files to /Volumes/streaming_ads/raw_ads/raw/json_input/ and the stream will process them.")
    else:
        raise e

