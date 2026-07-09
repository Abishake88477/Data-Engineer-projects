# 05 · Reusable PySpark Component Library

**Client:** Marketplace Direct Limited (Client: Stay Belvedere Hotels Ltd, London, UK)
**Role:** Cloud Data Engineer · May 2022 → Jun 2024

## Overview

A library of production-hardened PySpark components (transformations, IO helpers, validation utilities) that let engineers assemble new ETL jobs like Lego blocks. Combined with parameter-driven ADF pipelines, this cut development time by **30%** across multiple ETL projects.

## Component groups

| Package | What it does |
|---------|--------------|
| `io.readers` | Uniform readers for CSV / JSON / Parquet / Delta / Snowflake |
| `io.writers` | Delta writers with SCD-2, partition-pruning, retry & idempotency |
| `transforms.cleanse` | Trim / null-normalise / cast / dedup helpers |
| `transforms.enrich` | Join / broadcast / window helpers, watermark logic |
| `validation.rules` | Rule-runner for schema, uniqueness, range, referential checks |
| `logging.spark_run` | Structured run-log writer (`etl.pipeline_runs`) |

## Sample — SCD-2 upsert helper

```python
from pyspark.sql import DataFrame, functions as F

def upsert_scd2(
    src: DataFrame,
    target_path: str,
    keys: list[str],
    change_cols: list[str],
    valid_from: str = "valid_from",
    valid_to: str = "valid_to",
    is_current: str = "is_current",
) -> None:
    """Type-2 slowly-changing dimension upsert into a Delta table."""
    from delta.tables import DeltaTable

    now = F.current_timestamp()
    stg = src.withColumn(valid_from, now) \
             .withColumn(valid_to, F.lit(None).cast("timestamp")) \
             .withColumn(is_current, F.lit(True))

    tgt = DeltaTable.forPath(spark, target_path)

    merge_cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    change_cond = " OR ".join([f"t.{c} <> s.{c}" for c in change_cols])

    # Expire changed current rows
    (
        tgt.alias("t")
           .merge(stg.alias("s"), f"{merge_cond} AND t.{is_current} = true AND ({change_cond})")
           .whenMatchedUpdate(set={valid_to: "current_timestamp()", is_current: "false"})
           .execute()
    )

    # Insert new versions & brand-new keys
    (
        stg.alias("s").join(tgt.alias("t"),
                           on=[F.col(f"s.{k}") == F.col(f"t.{k}") for k in keys],
                           how="left_anti")
    ).write.format("delta").mode("append").save(target_path)
```

## Sample — Rule runner

```python
def run_rules(df, rules: dict) -> dict:
    """
    rules = {
      "not_null": ["id", "email"],
      "unique":   ["id"],
      "range":    {"age": [0, 120]}
    }
    """
    report = {}
    for col in rules.get("not_null", []):
        report[f"nulls.{col}"] = df.filter(df[col].isNull()).count()
    for col in rules.get("unique", []):
        report[f"dupes.{col}"] = df.groupBy(col).count().filter("count > 1").count()
    for col, (lo, hi) in rules.get("range", {}).items():
        report[f"range.{col}"] = df.filter((df[col] < lo) | (df[col] > hi)).count()
    return report
```

## Impact

- Development time reduced by **30%** across ETL projects
- Consistent quality — every job uses the same validation, logging and SCD-2 primitives
- Dynamic ADF pipelines supporting **15+** data sources with no bespoke code
