# Databricks notebook — Master Job
# Silver Layer Lakehouse  →  Gold Layer curated datasets
#
# Reads every Silver Layer table, applies additional business
# transformations, and writes the final curated Gold dataset consumed by:
#   • Business Intelligence & Reporting
#   • Property Management Operations
#   • Housing Support Teams
#   • Maintenance & Repairs Teams
#   • Compliance & Audit Teams

from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("clearsprings_master_silver_to_gold").getOrCreate()

# ------------------------------------------------------------------
# 1. READ ALL SILVER LAYER TABLES
# ------------------------------------------------------------------
silver = {
    # from ADLS ingest (Job 1)
    "tenant_change_requests":  spark.table("lakehouse.silver.tenant_change_requests"),
    "document_uploads":        spark.table("lakehouse.silver.document_uploads"),
    "property_updates":        spark.table("lakehouse.silver.property_updates"),
    "operational_history":     spark.table("lakehouse.silver.operational_history"),

    # from Snowflake ingest (Job 2)
    "property_details":        spark.table("lakehouse.silver.property_details"),
    "tenancy_records":         spark.table("lakehouse.silver.tenancy_records"),
    "repair_history":          spark.table("lakehouse.silver.repair_history"),
    "contractor_info":         spark.table("lakehouse.silver.contractor_info"),
    "compliance_records":      spark.table("lakehouse.silver.compliance_records"),
    "financial_transactions":  spark.table("lakehouse.silver.financial_transactions"),

    # from Web APIs (Job 3)
    "tenants_api":             spark.table("lakehouse.silver.tenants_api"),
    "occupancy_api":           spark.table("lakehouse.silver.occupancy_api"),
    "maintenance_requests":    spark.table("lakehouse.silver.maintenance_requests_api"),
    "contractor_status":       spark.table("lakehouse.silver.contractor_status_api"),
}


# ------------------------------------------------------------------
# 2. GOLD · property_360   (used by BI + Property Management)
# ------------------------------------------------------------------
latest_tenancy_w = Window.partitionBy("PROPERTY_ID").orderBy(F.col("UPDATED_AT").desc())
latest_tenancy = (
    silver["tenancy_records"]
    .withColumn("_rn", F.row_number().over(latest_tenancy_w))
    .where("_rn = 1").drop("_rn")
)

repair_agg = (
    silver["repair_history"]
    .groupBy("PROPERTY_ID")
    .agg(
        F.count("*").alias("REPAIR_COUNT_LIFETIME"),
        F.max("REPAIR_DATE").alias("LAST_REPAIR_DATE"),
    )
)

gold_property_360 = (
    silver["property_details"].alias("p")
    .join(F.broadcast(latest_tenancy).alias("t"), on="PROPERTY_ID", how="left")
    .join(repair_agg.alias("r"), on="PROPERTY_ID", how="left")
    .join(silver["compliance_records"].alias("c"), on="PROPERTY_ID", how="left")
    .join(silver["occupancy_api"].alias("o"), on="PROPERTY_ID", how="left")
    .withColumn("GOLD_REFRESHED_AT", F.current_timestamp())
)


# ------------------------------------------------------------------
# 3. GOLD · maintenance_ops   (used by Maintenance & Repairs)
# ------------------------------------------------------------------
gold_maintenance_ops = (
    silver["maintenance_requests"].alias("m")
    .join(silver["contractor_status"].alias("cs"), on="CONTRACTOR_ID", how="left")
    .join(F.broadcast(silver["contractor_info"]).alias("ci"), on="CONTRACTOR_ID", how="left")
    .join(silver["property_details"].alias("p"), on="PROPERTY_ID", how="left")
    .withColumn(
        "SLA_STATUS",
        F.when(F.col("m.due_date") < F.current_timestamp(), F.lit("BREACHED"))
         .otherwise(F.lit("OK")),
    )
    .withColumn("GOLD_REFRESHED_AT", F.current_timestamp())
)


# ------------------------------------------------------------------
# 4. GOLD · compliance_audit   (used by Compliance & Audit)
# ------------------------------------------------------------------
gold_compliance_audit = (
    silver["compliance_records"].alias("c")
    .join(silver["property_details"].alias("p"), on="PROPERTY_ID", how="left")
    .join(silver["tenant_change_requests"].alias("tcr"), on="PROPERTY_ID", how="left")
    .withColumn(
        "COMPLIANCE_STATUS",
        F.when(F.col("EXPIRY_DATE") < F.current_date(), F.lit("EXPIRED"))
         .when(F.datediff(F.col("EXPIRY_DATE"), F.current_date()) <= 30, F.lit("EXPIRING_SOON"))
         .otherwise(F.lit("VALID")),
    )
    .withColumn("GOLD_REFRESHED_AT", F.current_timestamp())
)


# ------------------------------------------------------------------
# 5. GOLD · housing_support   (used by Housing Support)
# ------------------------------------------------------------------
gold_housing_support = (
    silver["tenants_api"].alias("t")
    .join(F.broadcast(latest_tenancy).alias("tr"), on="TENANT_ID", how="left")
    .join(silver["occupancy_api"].alias("o"), on="PROPERTY_ID", how="left")
    .join(silver["document_uploads"].alias("d"), on="TENANT_ID", how="left")
    .withColumn("GOLD_REFRESHED_AT", F.current_timestamp())
)


# ------------------------------------------------------------------
# 6. WRITE GOLD LAKEHOUSE TABLES
# ------------------------------------------------------------------
gold_writes = [
    (gold_property_360,     "lakehouse.gold.property_360",     ["REGION"]),
    (gold_maintenance_ops,  "lakehouse.gold.maintenance_ops",  ["REGION"]),
    (gold_compliance_audit, "lakehouse.gold.compliance_audit", ["REGION"]),
    (gold_housing_support,  "lakehouse.gold.housing_support",  ["REGION"]),
]

for df, target, part in gold_writes:
    print(f"[gold] writing → {target}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*part)
        .saveAsTable(target)
    )
    print(f"[gold] {target} rows = {df.count()}")

print("\n[done] master consolidation complete — Gold Lakehouse refreshed.")
