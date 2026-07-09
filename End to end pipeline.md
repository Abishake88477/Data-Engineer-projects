# 07 · Clearsprings Ready Homes — End-to-End Azure Data Pipeline

**Client:** Nationwide Accommodation Ltd (Client: Clearsprings Ready Homes, London, UK)
**Role:** Azure Data Engineer (Remote) · Jun 2024 → Present

## Overview

An end-to-end **Azure Cloud** data pipeline supporting housing and property management operations for **Clearsprings Ready Homes**. The platform consolidates data from multiple business systems, processes it through **Azure Databricks**, and delivers curated Lakehouse tables for reporting, operational monitoring, and downstream business applications.

> _Another team within the organization uses Azure Synapse for reporting and analytical workloads — outside the scope of this repository._

## Azure services used

- **Azure Data Lake Storage (ADLS)** — landing zone + Silver + Gold layers
- **Azure Databricks** — PySpark compute for ingestion, transformation & consolidation
- **Azure Data Factory (ADF)** — orchestration + scheduling

## Data sources

The pipeline ingests from **three primary source classes**:

| # | Source | Contents |
|---|--------|----------|
| 1 | **Azure Data Lake Storage** | Tenant change requests, document uploads, property updates, historical operational data |
| 2 | **Snowflake** (5 different databases) | Property details, tenancy records, repair history, contractor information, compliance records, financial transactions |
| 3 | **Web APIs** (4 external / internal) | Tenant information, property occupancy, maintenance requests, inspection updates, contractor status |

## Data processing — Silver Layer

Three PySpark jobs run on Azure Databricks to extract data from the source systems and produce **Silver Layer** tables. Each job performs a mix of:

- Filtering
- Cleansing (nulls, trims, standard casts)
- Aggregations
- Window functions
- Joins
- Data validation (schema, referential integrity, business-rule checks)
- Business-rule implementation

The processed Silver Layer data is written into **Databricks Lakehouse tables** — these intermediate datasets are also consumed by other internal teams for their own reporting.

```
   +----------+     +-----------+     +----------+
   |   ADLS   |     | Snowflake |     | Web APIs |
   +----+-----+     +-----+-----+     +----+-----+
        |                 |                 |
        v                 v                 v
   [ PySpark Ingest Job 1 ]                 |
   [ PySpark Ingest Job 2 ] ----------------+
   [ PySpark Ingest Job 3 ]
        |
        v
   +---------------------------+
   |   Silver Layer Lakehouse  |
   +---------------------------+
```

## Master processing — Gold Layer

One **master PySpark job** reads all the Silver Layer tables, applies additional business transformations, and produces the **final curated dataset** which is written into Gold Lakehouse tables that serve as the primary source for downstream consumers.

```
   +---------------------------+
   |   Silver Layer Lakehouse  |
   +-------------+-------------+
                 |
                 v
   [ Master PySpark Consolidation Job ]
                 |
                 v
   +---------------------------+
   |    Gold Layer Lakehouse   |   ← downstream teams consume here
   +---------------------------+
```

## Downstream consumers

- **Business Intelligence & Reporting**
- **Property Management Operations**
- **Housing Support Teams**
- **Maintenance & Repairs Teams**
- **Compliance & Audit Teams**

These teams use the curated datasets for dashboards, operational reporting, KPI monitoring and business analytics.

## Orchestration

The entire pipeline is orchestrated using **Azure Data Factory**. Databricks notebooks are executed via ADF pipelines and scheduled to run **daily at 09:00 AM Eastern Time**, ensuring downstream systems always receive the latest operational data before the business day begins.

## Repository layout

```
07-clearsprings-ready-homes-detailed/
├── README.md                       ← this file
├── notebooks/
│   ├── 01_ingest_adls_to_silver.py       (Job 1 · ADLS → Silver)
│   ├── 02_ingest_snowflake_to_silver.py  (Job 2 · Snowflake → Silver)
│   ├── 03_ingest_webapi_to_silver.py     (Job 3 · Web APIs → Silver)
│   └── 04_master_silver_to_gold.py       (Master · Silver → Gold)
├── adf/
│   └── pl_clearsprings_daily_9am_et.json (ADF pipeline definition)
└── config/
    └── source_config.yaml                (source connections + tables)
```

## Getting started (local dev / notebooks)

```bash
pip install pyspark==3.5.0 delta-spark==3.1.0 requests snowflake-connector-python
```

Then open the notebooks in `notebooks/` in Databricks (or run locally against a Spark session).

## Business impact

| Metric | Value |
|--------|-------|
| Daily volume | **3+ TB** |
| Ingestion PySpark jobs | 3 |
| Master consolidation job | 1 |
| Silver Layer tables produced | dozens (tenancy, occupancy, maintenance, compliance, property, resident, ticketing, reference, …) |
| Downstream teams served | 5 |
| Schedule | Daily · 09:00 AM Eastern Time |
