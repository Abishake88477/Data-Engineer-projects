# Databricks notebook — Job 1
# ADLS raw operational data  →  Silver Layer Lakehouse tables
#
# Reads tenant change requests, document uploads, property updates and
# historical operational data landed in ADLS, cleanses & validates them,
# then writes to Silver Layer Delta tables in the Databricks Lakehouse.

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType, IntegerType,
)

spark = SparkSession.builder.appName("clearsprings_adls_to_silver").getOrCreate()

# ------------------------------------------------------------------
# 1. CONFIG — in production these come from the metadata control table
# ------------------------------------------------------------------
ADLS_BRONZE_BASE = "abfss://bronze@clearsprings.dfs.core.windows.net"
SILVER_CATALOG   = "lakehouse.silver"

sources = {
    "tenant_change_requests": {
        "path": f"{ADLS_BRONZE_BASE}/tenant_change_requests/date=*",
        "target": f"{SILVER_CATALOG}.tenant_change_requests",
        "key_cols": ["change_request_id"],
    },
    "document_uploads": {
        "path": f"{ADLS_BRONZE_BASE}/document_uploads/date=*",
        "target": f"{SILVER_CATALOG}.document_uploads",
        "key_cols": ["document_id"],
    },
    "property_updates": {
        "path": f"{ADLS_BRONZE_BASE}/property_updates/date=*",
        "target": f"{SILVER_CATALOG}.property_updates",
        "key_cols": ["property_id", "update_ts"],
    },
    "operational_history": {
        "path": f"{ADLS_BRONZE_BASE}/operational_history/date=*",
        "target": f"{SILVER_CATALOG}.operational_history",
        "key_cols": ["event_id"],
    },
}


# ------------------------------------------------------------------
# 2. HELPERS
# ------------------------------------------------------------------
def cleanse(df):
    """Standard cleanse: trim strings, normalise nulls, cast timestamps."""
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))),
            )
    return df


def dedup_latest(df, keys, order_col="ingested_at"):
    """Keep the latest record per key using a window function."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def validate(df, not_null_cols):
    """Business-rule validation — filter out invalid rows and count rejects."""
    invalid = df
    for c in not_null_cols:
        invalid = invalid.filter(F.col(c).isNull())
    reject_count = invalid.count()
    if reject_count:
        print(f"[warn] rejecting {reject_count} rows failing not-null on {not_null_cols}")
    for c in not_null_cols:
        df = df.filter(F.col(c).isNotNull())
    return df


# ------------------------------------------------------------------
# 3. RUN
# ------------------------------------------------------------------
for name, cfg in sources.items():
    print(f"\n=== processing source: {name} ===")

    raw = spark.read.format("parquet").load(cfg["path"])

    cleansed  = cleanse(raw)
    validated = validate(cleansed, not_null_cols=cfg["key_cols"])
    deduped   = dedup_latest(validated, keys=cfg["key_cols"])

    silver = (
        deduped
        .withColumn("source_system", F.lit("adls"))
        .withColumn("silver_ingested_at", F.current_timestamp())
    )

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(cfg["target"])
    )
    print(f"[ok] wrote {silver.count()} rows → {cfg['target']}")
