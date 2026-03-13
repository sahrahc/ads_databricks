# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %run ./config_gold

# COMMAND ----------

from pyspark.sql.functions import hour, to_date, sum

hourly_spend = (
    silver_batch_df
    .withColumn("hour", hour("event_time"))
    .withColumn("date", to_date("event_time"))
    .groupBy("campaign_id", "date", "hour")
    .agg(sum("price_paid").cast("double").alias("hourly_spend"))
)

pacing_query = (
    hourly_spend.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/fct_budget_hourly")
)
