# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %run ./config_gold

# COMMAND ----------

from pyspark.sql.functions import expr, to_date, col

# attribution is at the campaign level where only 7 days after impression is considered
# Base metrics: 
attribution = (
    silver_batch_df.alias("i").filter("event_type = 'ad_click'")
    .join(
        silver_batch_df.alias("c").filter("event_type = 'conversion'"),
        expr("""
            i.session_id = c.session_id AND
            c.event_time BETWEEN i.event_time AND i.event_time + INTERVAL 7 DAYS
        """),
        "left"
    )
    .withColumn("date", to_date(col("i.event_time")))
    .groupBy("i.campaign_id", "date")
    .agg(
        expr("count(1) as impressions"),
        expr("count(distinct c.event_id) as conversions"),
        expr("sum(c.conversion_value) as revenue")
    )
)

attribution_query = (
    attribution.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/fct_attribution")
)
