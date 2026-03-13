# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
silver_path = "/Volumes/streaming_ads/raw_ads/raw/delta"
checkpoint_silver_path = "/Volumes/streaming_ads/raw_ads/raw/checkpoints"

# Read the bronze stream
bronze_stream = (
    spark.readStream
        .format("delta")
        .load("/Volumes/streaming_ads/raw_ads/raw/delta/ads_bronze_file")
)
