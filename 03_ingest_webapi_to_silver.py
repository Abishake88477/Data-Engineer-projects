# Databricks notebook — Job 3
# Web APIs (4 external / internal)  →  Silver Layer Lakehouse tables
#
# Pulls tenant information, property occupancy, maintenance requests,
# inspection updates and contractor status from 4 Web APIs, applies
# transformations, then writes to Silver Layer Delta tables.

import time
import requests
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("clearsprings_webapi_to_silver").getOrCreate()

# ------------------------------------------------------------------
# 1. CONFIG
# ------------------------------------------------------------------
API_BASE = "https://api.clearsprings.internal"

apis = [
    {
        "name":    "tenants",
        "url":     f"{API_BASE}/tenants",
        "silver":  "lakehouse.silver.tenants_api",
        "key":     ["tenant_id"],
    },
    {
        "name":    "occupancy",
        "url":     f"{API_BASE}/occupancy",
        "silver":  "lakehouse.silver.occupancy_api",
        "key":     ["property_id", "as_of_date"],
    },
    {
        "name":    "maintenance_requests",
        "url":     f"{API_BASE}/maintenance/requests",
        "silver":  "lakehouse.silver.maintenance_requests_api",
        "key":     ["request_id"],
    },
    {
        "name":    "contractor_status",
        "url":     f"{API_BASE}/contractors/status",
        "silver":  "lakehouse.silver.contractor_status_api",
        "key":     ["contractor_id"],
    },
]

TOKEN = dbutils.secrets.get("kv-clearsprings", "api-bearer-token")   # noqa: F821
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}


# ------------------------------------------------------------------
# 2. HELPERS
# ------------------------------------------------------------------
def fetch_paginated(url):
    """Simple paginated fetch with retry and exponential backoff."""
    all_rows, page, backoff = [], 1, 1
    while True:
        for attempt in range(3):
            try:
                r = requests.get(url, headers=HEADERS, params={"page": page, "size": 1000}, timeout=30)
                r.raise_for_status()
                data = r.json()
                break
            except requests.RequestException as e:
                print(f"[warn] {url} page={page} attempt={attempt}: {e}")
                time.sleep(backoff)
                backoff *= 2
        else:
            raise RuntimeError(f"failed to fetch {url} page {page}")

        rows = data.get("items", [])
        all_rows.extend(rows)
        if not data.get("next"):
            break
        page += 1
    return all_rows


def dedup_latest(df, keys, order_col="silver_ingested_at"):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ------------------------------------------------------------------
# 3. RUN
# ------------------------------------------------------------------
for api in apis:
    print(f"\n=== API: {api['name']} ===")

    rows = fetch_paginated(api["url"])
    print(f"[info] pulled {len(rows)} rows from {api['name']}")
    if not rows:
        continue

    df = spark.createDataFrame(rows)
    silver = (
        df
        .withColumn("source_system", F.lit(f"webapi:{api['name']}"))
        .withColumn("silver_ingested_at", F.current_timestamp())
    )
    silver = dedup_latest(silver, keys=api["key"])

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(api["silver"])
    )
    print(f"[ok] wrote {silver.count()} rows → {api['silver']}")
