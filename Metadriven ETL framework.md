# 02 · Metadata-Driven ETL Framework

**Client:** Nationwide Accommodation Ltd (Client: Clearsprings Ready Homes, London, UK)
**Role:** Azure Data Engineer · Jun 2024 → Present

## Overview

A configuration-driven ETL framework built on Azure Data Factory + Databricks. New pipelines are onboarded by editing metadata rows (source, target, schema, transform rules) — not by writing new pipeline code — reducing onboarding time by **45%** and development effort by **35%**.

## What lives in "metadata"

A control table (`etl.pipeline_config`) stores everything needed to run a pipeline:

| Column | Purpose |
|--------|---------|
| pipeline_id | Unique identifier |
| source_type | `snowflake` \| `api` \| `adls` |
| source_config | Connection / query / endpoint |
| target_layer | `bronze` \| `silver` \| `gold` |
| target_table | Delta table name |
| schema_json | Expected schema |
| transform_rules | JSON DSL — filters, dedup keys, casts |
| dependencies | Upstream pipeline_ids |
| schedule_cron | Optional override |
| enabled | 0/1 |

## Reusable Databricks notebooks

- `nb_ingest_generic` — reads any source using `source_config`
- `nb_validate_generic` — runs schema + null / range checks from `transform_rules`
- `nb_write_delta_generic` — writes to Bronze / Silver / Gold with SCD-2 support

## Reusable ADF pipeline template

A single parameterised pipeline `pl_metadata_driven_runner` is invoked with `pipeline_id`. It:
1. Reads metadata for `pipeline_id`
2. Resolves dependencies
3. Executes the correct Databricks notebook via `DatabricksNotebook` activity
4. Emits a run-log record to `etl.pipeline_runs`

## Impact

- **25+** parameterised ADF pipelines deployed across Dev / QA / Prod using this framework
- Pipeline onboarding time reduced by **45%**
- Development effort reduced by **35%**
- Standardised error handling, logging and reconciliation across every job

## Sample config row

```json
{
  "pipeline_id": "silver.tenancy.snowflake",
  "source_type": "snowflake",
  "source_config": {
    "database": "housing_prod",
    "schema": "tenancy",
    "query": "SELECT * FROM v_tenancy_daily"
  },
  "target_layer": "silver",
  "target_table": "silver.tenancy",
  "transform_rules": {
    "not_null": ["tenancy_id"],
    "dedup_keys": ["tenancy_id"],
    "casts": { "start_date": "date", "end_date": "date" }
  },
  "dependencies": [],
  "enabled": 1
}
```
