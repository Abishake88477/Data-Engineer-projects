# 06 · Healthcare ETL Testing Framework — NHS Trust

**Client:** The Princess Alexandra Hospital NHS Trust, Harlow, United Kingdom
**Role:** ETL Testing Engineer · Mar 2019 → May 2022

## Overview

An SQL-first testing and reconciliation framework used to certify healthcare data migrations and integrations across **50+ releases** — achieving **99.9% data accuracy** and resolving **300+ defects** over the engagement.

## Scope

- Source-to-target reconciliation across **20+ million** records per release cycle
- **800+** SQL validation scripts (schema, referential integrity, row-count, checksum, business-rule)
- Hive-based auxiliary storage on Hadoop with HBase / Impala integrations for large-scale comparisons
- Defect intake / triage → **300+** ETL defects raised and closed

## Framework building blocks

```
etl-testing-framework/
├── validators/
│   ├── row_counts.sql
│   ├── column_null_ratios.sql
│   ├── checksum_by_partition.sql
│   ├── referential_integrity.sql
│   └── business_rules/
├── reconciliation/
│   ├── source_vs_target_join.sql
│   └── delta_report.py
├── hive/
│   ├── ddl/  (managed, external, partitioned)
│   └── udfs/
└── defect_reports/
```

## Sample — row-level reconciliation

```sql
WITH src AS (
  SELECT patient_id, MD5(CONCAT_WS('|', dob, gender, postcode)) AS h FROM source.patient
),
tgt AS (
  SELECT patient_id, MD5(CONCAT_WS('|', dob, gender, postcode)) AS h FROM target.patient
)
SELECT
  COALESCE(src.patient_id, tgt.patient_id) AS patient_id,
  CASE
    WHEN src.patient_id IS NULL THEN 'MISSING_IN_SOURCE'
    WHEN tgt.patient_id IS NULL THEN 'MISSING_IN_TARGET'
    WHEN src.h <> tgt.h        THEN 'HASH_MISMATCH'
  END AS defect_type
FROM src FULL OUTER JOIN tgt USING (patient_id)
WHERE src.h IS DISTINCT FROM tgt.h;
```

## Hive integration

- Managed, external and partitioned Hive tables used to stage large source dumps for comparison
- Integrated with Hadoop / HBase / Impala for high-throughput scans

## Impact

| Metric | Result |
|--------|--------|
| Release cycles validated | **50+** |
| Data accuracy achieved | **99.9%** |
| SQL validation scripts | **800+** |
| Records reconciled | **20M+** |
| Defects raised & resolved | **300+** |
| Production data-quality lift | **+30%** |
| Testing effort reduction | **-25%** |
