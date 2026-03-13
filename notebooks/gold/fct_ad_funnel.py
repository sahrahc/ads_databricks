# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %run ./config_gold

# COMMAND ----------

from pyspark.sql.functions import when, col, sum, try_divide
# Ad/Conversion Funnel

#  - Base Business metrics: impressions, clicks, conversions
#  - Derived metrics: ctr, cvr
# 
# compute conversion funnel
funnel_metrics = (
    silver_batch_df
    .groupBy("campaign_id")
    .agg(
        sum(when(col("event_type") == "ad_impression", 1).otherwise(0)).alias("impressions"),
        sum(when(col("event_type") == "ad_click", 1).otherwise(0)).alias("clicks"),
        sum(when(col("event_type") == "conversion", 1).otherwise(0)).alias("conversions")
    )
    .withColumn("ctr", try_divide(col("clicks"), col("impressions")))
    .withColumn("cvr", try_divide(col("conversions"), col("clicks")))
)

# write gold table 2: Ad Funnel Metrics
funnel_query = (
    funnel_metrics.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/fct_ad_funnel")
)
