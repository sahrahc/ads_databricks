# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# DBTITLE 1,Set Up
# Configure paths
silver_path = "/Volumes/streaming_ads/raw_ads/raw/delta/stg_ads_event_file" # use stg_ads_event for prod
gold_path = "/Volumes/streaming_ads/dw/delta"

# read silver in batch
silver_batch_df = (
    spark.read
        .format("delta")
        .load(silver_path)
)

