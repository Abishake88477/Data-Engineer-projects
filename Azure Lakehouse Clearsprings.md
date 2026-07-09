# 01 · Azure Lakehouse Pipeline — Clearsprings Ready Homes

**Client:** Nationwide Accommodation Ltd (Client: Clearsprings Ready Homes, London, United Kingdom)
**Role:** Azure Data Engineer (Remote) · Jun 2024 → Present
**Scale:** 3+ TB of enterprise data ingested daily · 50M+ records transformed

## Overview

An end-to-end Azure Lakehouse pipeline consolidating housing and property management data from three distinct source classes into a Medallion (Bronze → Silver → Gold) architecture consumed by BI, Property Management, Housing Support, Maintenance and Compliance teams.

### Sources
- Azure Data Lake Storage (ADLS Gen2)
- 5 × Snowflake databases (tenancy, occupancy, maintenance, compliance, reference)
- 4 × Web APIs (property, resident, ticketing, compliance)

### Sinks
- ADLS Gen2 — Silver layer (curated, validated)
- ADLS Gen2 — Gold layer (BI-ready, denormalised)

## Architecture

```
    +-----------+     +-----------+     +-----------+
    |   ADLS    |     | Snowflake |     |  Web APIs |
    +-----+-----+     +-----+-----+     +-----+-----+
          |                 |                 |
          +--------+--------+--------+--------+
                   |                 |
              [ Azure Data Factory ] (25+ parameterised pipelines)
                   |                 |
              [ Azure Databricks ]  (PySpark ingestion jobs)
                   |
       +-----------+-----------+-----------+
       |  Bronze   |  Silver   |   Gold    |
       +-----------+-----------+-----------+
                                   |
                            BI · KPI · Ops
```

## Key components

- **Ingestion jobs (×3)** — cleanse / validate / transform tenancy, occupancy, maintenance & compliance data into Silver layer tables. Improved batch efficiency by **40%**.
- **Master PySpark job** — consolidates Silver → Gold curated datasets. See project 03.
- **Metadata-driven framework** — reduces new pipeline onboarding time by **45%**. See project 02.
- **Reconciliation** — Spark SQL validation scripts + automated source-to-target reconciliation → **99.8% data accuracy**.
- **Optimisation** — partitioning, caching and broadcast joins reduce job execution time by **50%**.

## Orchestration schedule
Databricks notebooks scheduled by Azure Data Factory to run **daily at 09:00 ET**.

## Representative PySpark ingestion (sample)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date

spark = SparkSession.builder.appName("bronze_to_silver_tenancy").getOrCreate()

bronze = (
    spark.read.format("parquet")
    .load("abfss://bronze@stgadls.dfs.core.windows.net/tenancy/date=*/")
)

silver = (
    bronze
    .filter(col("tenancy_id").isNotNull())
    .withColumn("start_date", to_date(col("start_date")))
    .withColumn("end_date", to_date(col("end_date")))
    .dropDuplicates(["tenancy_id"])
    .withColumn("ingested_at", current_timestamp())
)

(
    silver.write.format("delta")
    .mode("overwrite")
    .partitionBy("ingested_date")
    .save("abfss://silver@stgadls.dfs.core.windows.net/tenancy/")
)
```

## Business impact

| Metric | Before | After |
|--------|--------|-------|
| Daily volume | ~1 TB | **3+ TB** |
| Data accuracy | ~96% | **99.8%** |
| Pipeline onboarding | baseline | **-45%** |
| Job execution time | baseline | **-50%** |
| Batch processing efficiency | baseline | **+40%** |
