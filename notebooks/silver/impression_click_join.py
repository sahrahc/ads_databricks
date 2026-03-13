# Databricks notebook source
# MAGIC %run ./config_silver

# COMMAND ----------

# DBTITLE 1,Join
# streaming join of impressions and clicks for funnel analytics
# depends on parse_events.py to read into bronze

from pyspark.sql.functions import expr, col, to_timestamp
from delta.tables import DeltaTable

ts_format = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"

impressions = (
    bronze_stream
    .filter(col("event_type") == "ad_impression")
    .withColumn("event_time", to_timestamp(col("event_time"), ts_format))
    .withWatermark("event_time", "10 minutes")
)

clicks = (
    bronze_stream
    .filter(col("event_type") == "ad_click")
    .withColumn("event_time", to_timestamp(col("event_time"), ts_format))
    .withWatermark("event_time", "10 minutes")
)

# 30 minutes cutoff between impression and click
joined = (
    impressions.alias("i")
    .join(
        clicks.alias("c"),
        expr("""
            i.session_id = c.session_id AND
            c.event_time >= i.event_time AND
            c.event_time <= i.event_time + interval 30 minutes
        """),
        "left"
    )
    .select(
        col("i.event_id").alias("impression_id"),
        col("i.campaign_id"),
        col("i.user_id"),
        col("i.session_id"),
        col("i.event_time").alias("impression_time"),
        col("c.event_time").alias("click_time")
    )
)

def upsert(batch_df, batch_id):
    output_path = f"{silver_path}/stg_impression_click"

    # first run: table doesn't exist yet, create it
    if not DeltaTable.isDeltaTable(spark, output_path):
        batch_df.write.format("delta").save(output_path)
    else:
        delta_table = DeltaTable.forPath(spark, output_path)
        (
            delta_table.alias("t")
            .merge(
                batch_df.alias("s"),
                "t.impression_id = s.impression_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

query = (
    joined.writeStream
        .foreachBatch(upsert)
        .option("checkpointLocation", f"{checkpoint_silver_path}/stg_impression_click")
        .trigger(availableNow=True)
        .start()
)

# COMMAND ----------

# DBTITLE 1,Verify
# Verify data in impressions+click funnel
# Verify data in silver
try:
    file_silver_df = spark.read.format("delta").load(f"{silver_path}/stg_impression_click")
    print(f"Total records from impressions+click join: {file_silver_df.count()}")
    display(file_silver_df.limit(20))
except Exception as e:
    if "PATH_NOT_FOUND" in str(e):
        print("⚠️ Delta table doesn't exist yet.")
        print("Upload JSON files to /Volumes/streaming_ads/raw_ads/raw/json_input/ and the stream will process them.")
    else:
        raise e
