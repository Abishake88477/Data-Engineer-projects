# Databricks notebook — Job 2
# Snowflake (5 databases)  →  Silver Layer Lakehouse tables
#
# Extracts property details, tenancy records, repair history,
# contractor information, compliance records and financial transactions
# from 5 Snowflake databases, applies transformations, then writes to
# Silver Layer Delta tables in the Databricks Lakehouse.

from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("clearsprings_snowflake_to_silver").getOrCreate()

# ------------------------------------------------------------------
# 1. CONFIG
# ------------------------------------------------------------------
SF_OPTIONS_BASE = {
    "sfURL":       dbutils.secrets.get("kv-clearsprings", "snowflake-url"),           # noqa: F821
    "sfUser":      dbutils.secrets.get("kv-clearsprings", "snowflake-user"),          # noqa: F821
    "sfPassword":  dbutils.secrets.get("kv-clearsprings", "snowflake-password"),      # noqa: F821
    "sfWarehouse": "COMPUTE_WH",
    "sfRole":      "DATA_ENGINEER",
}

# 5 Snowflake databases → 6 tables total
snowflake_sources = [
    {"db": "PROPERTY_DB",     "schema": "CORE",   "table": "PROPERTY_DETAILS",       "silver": "lakehouse.silver.property_details"},
    {"db": "TENANCY_DB",      "schema": "CORE",   "table": "TENANCY_RECORDS",        "silver": "lakehouse.silver.tenancy_records"},
    {"db": "REPAIR_DB",       "schema": "CORE",   "table": "REPAIR_HISTORY",         "silver": "lakehouse.silver.repair_history"},
    {"db": "CONTRACTOR_DB",   "schema": "CORE",   "table": "CONTRACTOR_INFO",        "silver": "lakehouse.silver.contractor_info"},
    {"db": "COMPLIANCE_DB",   "schema": "CORE",   "table": "COMPLIANCE_RECORDS",     "silver": "lakehouse.silver.compliance_records"},
    {"db": "FINANCE_DB",      "schema": "CORE",   "table": "FINANCIAL_TRANSACTIONS", "silver": "lakehouse.silver.financial_transactions"},
]


# ------------------------------------------------------------------
# 2. HELPERS
# ------------------------------------------------------------------
def read_snowflake(db, schema, table):
    opts = {
        **SF_OPTIONS_BASE,
        "sfDatabase": db,
        "sfSchema":   schema,
        "dbtable":    f"{schema}.{table}",
    }
    return (
        spark.read.format("net.snowflake.spark.snowflake").options(**opts).load()
    )


def apply_business_rules(df, tablename):
    """Table-specific business rules & transformations."""
    if tablename == "TENANCY_RECORDS":
        # Only keep active or historical tenancies with valid dates
        df = df.filter(F.col("START_DATE").isNotNull())
        w = Window.partitionBy("TENANCY_ID").orderBy(F.col("UPDATED_AT").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

    if tablename == "REPAIR_HISTORY":
        # Aggregate repeat repairs per property
        agg = (
            df.groupBy("PROPERTY_ID")
              .agg(F.count("*").alias("REPAIR_COUNT_LIFETIME"))
        )
        df = df.join(agg, "PROPERTY_ID", "left")

    if tablename == "FINANCIAL_TRANSACTIONS":
        # Business rule — filter reversed / cancelled txns
        df = df.filter(F.col("STATUS").isin("POSTED", "SETTLED"))

    return df


def cleanse(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))),
            )
    return df


# ------------------------------------------------------------------
# 3. RUN
# ------------------------------------------------------------------
for src in snowflake_sources:
    print(f"\n=== {src['db']}.{src['schema']}.{src['table']} ===")

    raw = read_snowflake(src["db"], src["schema"], src["table"])
    print(f"[info] read {raw.count()} rows from Snowflake")

    df = cleanse(raw)
    df = apply_business_rules(df, src["table"])

    silver = (
        df
        .withColumn("source_system", F.lit(f"snowflake:{src['db']}"))
        .withColumn("silver_ingested_at", F.current_timestamp())
    )

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(src["silver"])
    )
    print(f"[ok] wrote {silver.count()} rows → {src['silver']}")
