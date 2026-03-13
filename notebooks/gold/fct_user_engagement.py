# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %run ./config_gold

# COMMAND ----------

# measure engagement
from pyspark.sql.functions import when, col, countDistinct, sum

user_metrics = (
    silver_batch_df
    .groupBy("user_id")
    .agg(
        countDistinct("session_id").alias("sessions"),
        sum(when(col("event_type") == "ad_impression", 1).otherwise(0)).alias("ads_seen"),
        sum(when(col("event_type") == "conversion", 1).otherwise(0)).alias("conversions")
    )
)

# write gold table 5: user metrics
user_query = (
    user_metrics.write
        .format("delta")
        .mode("append")
        .save(f"{gold_path}/fct_user_engagement")
)
