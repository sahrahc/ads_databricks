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


Bronze and Silver layers run as **streaming ingestion pipelines**, while Gold produces **business metrics and aggregations** in batch.

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

# Streaming Source

1. Using S3 compute for Spark Structured Programming
2. Used JSON file consumed from Kafka stream (see https://github.com/sahrahc/synthetic_ads_json) for development due to cost of setting up streaming server in AWS

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

- JSON parsing (TODO: change to raw immutable data and move JSON parsing to Silver)
- append-only
- no business logic
- debugging and replay support

# Silver Layer

The Silver layer transforms raw events into clean structured records.

Responsibilities:

- schema (data types) enforcement
- deduplication
- timestamp normalization

Silver table contains structured events.

# Gold Layer

Gold tables provide analytics-ready datasets.

They compute business metrics used by dashboards and data science workflows.

Campaign Performance

Metrics:
- impressions
- clicks
- conversions 
- attributed (7-day) conversions
- spend
- revenue

| Table                 | Description                                     |
| --------------------- | ----------------------------------------------- |
| fct_campaign_daily    | Campaign-level performance and ROAS             |
| fct_ad_funnel         | CTR and conversion funnel                       |
| fct_attribution       | Attributed conversion if happened within 7 days |
| fct_session_ad_load   | Ads served per session                          |
| fct_user_engagement   | User engagement metrics                         |


These tables power business dashboards and analytics queries.

# Jobs

COMING SOON

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
