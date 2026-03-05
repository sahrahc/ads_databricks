# Databricks Streaming Ads Analytics Pipeline

This project implements a **streaming ads analytics pipeline** using **Apache Spark Structured Streaming in Databricks**.  
The pipeline ingests ad events from Kafka, processes them through a **Bronze → Silver → Gold architecture**, and produces analytics-ready datasets for reporting and experimentation.

The goal is to simulate a realistic **ads data platform** similar to those used at streaming platforms, marketplaces, and ad-tech companies.

---

# Architecture

The pipeline follows a modern streaming data architecture.

Synthetic Ads Generator
↓
Apache Kafka
↓
Databricks Spark Streaming
↓
Bronze Tables (raw events)
↓
Silver Tables (cleaned events)
↓
Gold Tables (analytics metrics)


Bronze and Silver layers run as **streaming ingestion pipelines**, while Gold produces **business metrics and aggregations**.

---

# Technologies

| Component | Purpose |
|----------|---------|
| Databricks | Distributed Spark compute |
| Apache Kafka | Event ingestion |
| Spark Structured Streaming | Streaming processing |
| Delta Lake | Storage layer |
| Python / PySpark | Transformation logic |

---

# Databricks Environment Setup

1. Log into your Databricks workspace.
2. Navigate to **Compute**.
3. Create a cluster.

Recommended cluster settings:

| Setting | Example |
|-------|--------|
| Runtime | Latest Databricks Runtime |
| Worker nodes | 1–2 |
| Node type | Small instance |
| Auto terminate | 30–60 minutes |

Once the cluster is running, attach notebooks to it.

---

# Kafka Streaming Source

Databricks reads Kafka messages using Spark Structured Streaming.

Example:

```python
kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "ads_events")
        .option("startingOffsets", "latest")
        .load()
)

Kafka records include:
| Field     | Description       |
| --------- | ----------------- |
| key       | Kafka message key |
| value     | Event payload     |
| topic     | Kafka topic       |
| partition | Topic partition   |
| offset    | Kafka offset      |
| timestamp | Message timestamp |

# Bronze Layer

The Bronze layer stores raw Kafka events with minimal transformation.

Bronze principles:

- raw immutable data
- append-only
- no business logic
- debugging and replay support

Example Bronze ingestion:

bronze_df = kafka_df.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as kafka_timestamp",
    "partition",
    "offset"
)

Write to delta:

bronze_query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/mnt/checkpoints/ads_bronze")
        .start("/mnt/delta/ads_bronze")
)

Bronze table schema:

| Column          | Description            |
| --------------- | ---------------------- |
| raw_json        | Original event payload |
| kafka_timestamp | Kafka ingestion time   |
| partition       | Kafka partition        |
| offset          | Kafka offset           |

# Silver Layer

The Silver layer transforms raw events into clean structured records.

Responsibilities:

- JSON parsing
- schema enforcement
- deduplication
- timestamp normalization

Example transformation:

silver_df = bronze_stream.select(
    from_json(col("raw_json"), event_schema).alias("data")
).select("data.*")

Deduplicate events:

silver_df = (
    silver_df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

Write Silver table:

silver_query = (
    silver_df.writeStream
        .format("delta")
        .option("checkpointLocation", "/mnt/checkpoints/ads_silver")
        .start("/mnt/delta/ads_silver")
)

Silver table contains structured events.

# Gold Layer

Gold tables provide analytics-ready datasets.

They compute business metrics used by dashboards and data science workflows.

Campaign Performance

Metrics:
- impressions
- clicks
- conversions
- spend
- revenue

Example aggregation:

campaign_metrics = (
    silver_df
    .groupBy("campaign_id", "date")
    .agg(
        sum(when(col("event_type") == "ad_impression", 1).otherwise(0)).alias("impressions"),
        sum(when(col("event_type") == "ad_click", 1).otherwise(0)).alias("clicks"),
        sum("price_paid").alias("spend"),
        sum("conversion_value").alias("revenue")
    )
)

# Gold Tables
| Table                 | Description                |
| --------------------- | -------------------------- |
| gold_campaign_metrics | Campaign-level performance |
| gold_funnel_metrics   | CTR and conversion funnel  |
| gold_roas             | Return on ad spend         |
| gold_session_ads      | Ads served per session     |
| gold_user_metrics     | User engagement metrics    |


These tables power business dashboards and analytics queries.

# Streaming Checkpoints

Each streaming query uses a checkpoint directory.

Example:
/mnt/checkpoints/ads_bronze
/mnt/checkpoints/ads_silver
/mnt/checkpoints/gold_campaign

Checkpoint data stores:
- processed Kafka offsets
- streaming state
- query progress metadata

Checkpoints enable fault tolerance and exactly-once processing.

# Running the Pipeline

1. Start Kafka and the synthetic event generator.
2. Start the Bronze ingestion notebook.
3. Start the Silver transformation notebook.
4. Start Gold aggregation jobs.

Verify data flow using:
  spark.streams.active

# Example Analytics Queries

Campaign performance:

SELECT campaign_id, impressions, clicks
FROM gold_campaign_metrics
ORDER BY impressions DESC

Return on ad spend:

SELECT campaign_id, revenue / spend AS roas
FROM gold_campaign_metrics
ORDER BY roas DESC

# Future Enhancements

Potential improvements:
- streaming attribution modeling
- conversion lag analysis
- campaign pacing monitoring
- anomaly detection
- ad fatigue modeling
- ML feature generation

# Summary

This project demonstrates a realistic streaming analytics pipeline for ad events using Databricks and Kafka.

Key concepts demonstrated:
- streaming ingestion
- Bronze / Silver / Gold architecture
- schema enforcement and deduplication
- real-time campaign analytics
- scalable event processing

The pipeline can serve as a foundation for experimentation, analytics, and data platform design.
