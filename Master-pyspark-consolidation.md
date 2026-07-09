# 03 · Master PySpark Consolidation Job

**Client:** Nationwide Accommodation Ltd (Client: Clearsprings Ready Homes, London, UK)
**Role:** Azure Data Engineer · Jun 2024 → Present

## Purpose

Consolidate the Silver-layer tables (tenancy, occupancy, maintenance, compliance, property, resident, ticketing, reference) into curated Gold-layer datasets that BI dashboards and downstream operations consume directly.

Consumers:
- BI team (Power BI dashboards)
- Property Management team
- Housing Support team
- Maintenance team
- Compliance team

## Approach

- Transformed **50+ million records** using PySpark joins, aggregations and window functions
- Sources unified: Snowflake + ADLS + API-sourced Silver tables
- Partitioning by `region` and `snapshot_date`, broadcast joins on small reference tables → **-50%** execution time
- Caching for shared intermediate DataFrames

## Sample transform (window + aggregation)

```python
from pyspark.sql import functions as F, Window

# Latest tenancy record per property
w = Window.partitionBy("property_id").orderBy(F.col("effective_date").desc())

latest_tenancy = (
    silver_tenancy
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)

# Occupancy KPI per region / month
occupancy_kpi = (
    silver_occupancy
    .withColumn("month", F.trunc("event_date", "MM"))
    .groupBy("region", "month")
    .agg(
        F.sum("occupied_units").alias("occupied"),
        F.sum("available_units").alias("available"),
    )
    .withColumn("occupancy_rate", F.col("occupied") / F.col("available"))
)

gold_property_360 = (
    latest_tenancy
    .join(F.broadcast(dim_property), "property_id", "left")
    .join(silver_maintenance, "property_id", "left")
    .join(silver_compliance, "property_id", "left")
)

(
    gold_property_360.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("region")
    .saveAsTable("gold.property_360")
)
```

## Impact

- Powers the **KPI dashboards** for 5 business teams
- Reduced end-to-end job execution time by **50%** via optimisation
- Zero manual intervention — orchestrated by Azure Data Factory at 09:00 ET daily
