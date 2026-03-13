# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %run ./config_gold

# COMMAND ----------

from pyspark.sql.functions import count, col

# measure ads per playback session
#  - Business metrics ads_served

session_ads = (
    silver_batch_df
    .filter(col("event_type") == "ad_impression")
    .groupBy("session_id")
    .agg(count("*").alias("ads_served"))
)

# write gold table 4: session ad load
session_query = (
    session_ads.write
        .format("delta")
        .mode("append")
        .save(f"{gold_path}/fct_session_ad_load")
)

