# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %run ./config_gold

# COMMAND ----------

# Campaign Performance Fact Table
#  - Business metrics: impressions, clicks, conversions, spend, revenue, roas
# 
from pyspark.sql.functions import to_date, when, col, sum, try_divide

campaign_metrics = (
    silver_batch_df
    .withColumn("date", to_date("event_time"))
    .groupBy("campaign_id", "date")
    .agg(
        sum(when(col("event_type") == "ad_impression", 1).otherwise(0)).alias("impressions"),
        sum(when(col("event_type") == "ad_click", 1).otherwise(0)).alias("clicks"),
        sum(when(col("event_type") == "conversion", 1).otherwise(0)).alias("conversions"),
        sum("price_paid").alias("spend"),
        sum("conversion_value").alias("revenue"),
        # ROAS: revenue / spend
        try_divide(sum("conversion_value"), sum("price_paid")).alias("roas")
    )
)

campaign_query = (
    campaign_metrics.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/fct_campaign_daily")
)
