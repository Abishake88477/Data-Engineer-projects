# 04 · Enterprise ADF Pipelines — Stay Belvedere Hotels

**Client:** Marketplace Direct Limited (Client: Stay Belvedere Hotels Ltd, London, UK)
**Role:** Cloud Data Engineer · May 2022 → Jun 2024

## Overview

Twenty-plus enterprise Azure Data Factory pipelines powering business reporting and analytics across the hotel-operations estate. Pipelines are dynamic and parameter-driven, supporting **15+ data sources** and multiple deployment environments (Dev / QA / Prod).

## Scale

- **20+** enterprise ADF pipelines
- **2+ TB** of structured & semi-structured data processed
- **100+** recurring Databricks jobs monitored
- **10,000+** CSV / JSON / Parquet files landed into curated Data Lake layers
- Manual scheduling effort reduced by **80%**

## Design principles

- **Parameter-driven**: pipeline logic reused across environments; environment-specific values (connection strings, container names, watermark tables) live in ADF global parameters / Linked Services.
- **Reusable PySpark blocks**: see project 05 for the component library.
- **Optimised Spark**: partitioning, caching, broadcast joins → **+45%** overall performance.

## Representative ADF pipeline shape

```
pl_hotel_daily_ingest
├── LookupActivity    → watermark from Azure SQL
├── ForEachActivity   → over source tables from control table
│   ├── CopyActivity   → SQL → ADLS Bronze (parquet)
│   ├── DatabricksNotebook → transform Bronze → Silver
│   └── DatabricksNotebook → publish Silver → Gold
└── StoredProcedure   → update watermark on success
```

## Impact

- Reliable overnight data delivery to reporting stakeholders
- Manual scheduling / babysitting effort reduced by **80%**
- Development time per new source dropped **30%** thanks to reusable components
