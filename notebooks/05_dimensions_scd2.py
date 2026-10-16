# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 - Dimensions (incl. SCD Type 2 on `dim_zone`)
# MAGIC
# MAGIC | Dimension | Type | Why |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | 0 (static) | calendar never changes |
# MAGIC | `dim_vendor`, `dim_payment_type`, `dim_rate_code` | 1 (overwrite) | tiny code lists, history is worthless |
# MAGIC | `dim_zone` | **2 (history)** | a zone can be renamed or move service zone; old trips must keep the label they were booked under |
# MAGIC
# MAGIC SCD2 columns: `zone_key` (surrogate), `location_id` (natural), `hash_diff` (change detector),
# MAGIC `effective_from` / `effective_to` / `is_current` / `version_no`.

# COMMAND ----------

# MAGIC %run ./1_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 5.1 dim_date

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T_DIM_DATE} AS
WITH d AS (
  SELECT explode(sequence(DATE'2024-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS full_date
)
SELECT
  CAST(date_format(full_date,'yyyyMMdd') AS INT) AS date_key,
  full_date,
  year(full_date)                       AS year,
  quarter(full_date)                    AS quarter,
  month(full_date)                      AS month,
  date_format(full_date,'MMMM')         AS month_name,
  date_format(full_date,'yyyy-MM')      AS year_month,
  weekofyear(full_date)                 AS week_of_year,
  dayofmonth(full_date)                 AS day_of_month,
  dayofweek(full_date)                  AS day_of_week,
  date_format(full_date,'EEEE')         AS day_name,
  dayofweek(full_date) IN (1,7)         AS is_weekend
FROM d
""")

# COMMAND ----------

# MAGIC %md ## 5.2 SCD Type 1 code dimensions
# MAGIC MERGE (not overwrite) so the surrogate keys stay stable for the fact table.

# COMMAND ----------

def scd1(table, schema, rows, key_col):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({schema}) USING DELTA")
    src = spark.createDataFrame(rows, schema)
    src.createOrReplaceTempView("src_scd1")
    cols = [f.name for f in src.schema.fields]
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in cols if c != key_col])
    spark.sql(f"""
      MERGE INTO {table} t USING src_scd1 s ON t.{key_col} = s.{key_col}
      WHEN MATCHED THEN UPDATE SET {set_clause}
      WHEN NOT MATCHED THEN INSERT *
    """)

scd1(T_DIM_VENDOR, "vendor_key INT, vendor_name STRING, vendor_desc STRING", [
    (1, "Creative Mobile Technologies", "CMT"),
    (2, "Curb Mobility / VeriFone",     "VTS"),
    (6, "Myle Technologies",            "MYLE"),
    (7, "Helix",                        "HELIX"),
    (-1, "Unknown",                     "Late arriving / missing vendor"),
], "vendor_key")

scd1(T_DIM_PAYMENT, "payment_key INT, payment_name STRING, is_electronic BOOLEAN", [
    (1, "Credit card", True), (2, "Cash", False), (3, "No charge", False),
    (4, "Dispute", False), (5, "Unknown", False), (6, "Voided trip", False),
    (-1, "Unknown", False),
], "payment_key")

scd1(T_DIM_RATE, "rate_code_key INT, rate_code_name STRING", [
    (1, "Standard rate"), (2, "JFK flat fare"), (3, "Newark"),
    (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride"),
    (99, "Unknown"), (-1, "Unknown"),
], "rate_code_key")

for t in [T_DIM_VENDOR, T_DIM_PAYMENT, T_DIM_RATE]:
    print(t, spark.table(t).count(), "rows")

# COMMAND ----------

# MAGIC %md ## 5.3 dim_zone - SCD Type 2

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_DIM_ZONE} (
  zone_key       BIGINT  NOT NULL COMMENT 'surrogate key - one per version of a zone',
  location_id    INT     NOT NULL COMMENT 'natural/business key from TLC',
  zone_name      STRING,
  borough        STRING,
  service_zone   STRING,
  hash_diff      STRING  COMMENT 'sha2 of tracked attributes - drives change detection',
  effective_from TIMESTAMP,
  effective_to   TIMESTAMP,
  is_current     BOOLEAN,
  version_no     INT,
  created_at     TIMESTAMP,
  updated_at     TIMESTAMP
) USING DELTA
CLUSTER BY (location_id)
COMMENT 'SCD2 taxi zone dimension'
""")

# unknown member so the fact table never loses a row to a missing lookup
spark.sql(f"""
MERGE INTO {T_DIM_ZONE} t
USING (SELECT -1L AS zone_key, -1 AS location_id, 'Unknown' AS zone_name,
              'Unknown' AS borough, 'Unknown' AS service_zone) s
ON t.zone_key = s.zone_key
WHEN NOT MATCHED THEN INSERT (zone_key, location_id, zone_name, borough, service_zone,
       hash_diff, effective_from, effective_to, is_current, version_no, created_at, updated_at)
  VALUES (s.zone_key, s.location_id, s.zone_name, s.borough, s.service_zone,
       'UNKNOWN', TIMESTAMP'1900-01-01', TIMESTAMP'9999-12-31', true, 1,
       current_timestamp(), current_timestamp())
""")

# COMMAND ----------

display(spark.table(T_DIM_ZONE))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The two-branch MERGE
# MAGIC A single `MERGE` cannot both close a row and insert its replacement, because one source row
# MAGIC can only touch one target row. The standard trick is to feed it the source **twice**:
# MAGIC
# MAGIC * **branch A** (`merge_key = location_id`) -> matches the current row, closes it out.
# MAGIC   If the zone is brand new nothing matches, so it falls through to `INSERT` - version 1.
# MAGIC * **branch B** (`merge_key = NULL`) -> can never match, so it always inserts. Only emitted
# MAGIC   for zones whose `hash_diff` actually moved - that is the new version.

# COMMAND ----------

base_key = spark.sql(f"SELECT coalesce(max(zone_key),0) AS k FROM {T_DIM_ZONE} WHERE zone_key > 0").first()["k"]

spark.sql(f"""
MERGE INTO {T_DIM_ZONE} AS t
USING (
  SELECT
    {base_key} + row_number() OVER (ORDER BY merge_key NULLS LAST, location_id) AS new_zone_key,
    u.*
  FROM (
      -- branch A: close out the current row, or insert version 1 for a brand new zone
      SELECT s.location_id AS merge_key, s.location_id, s.zone_name, s.borough,
             s.service_zone, s.hash_diff,
             coalesce(v.max_version, 0) + 1 AS next_version
      FROM {T_SILVER_ZONE_STG} s
      LEFT JOIN (SELECT location_id, max(version_no) AS max_version
                 FROM {T_DIM_ZONE} GROUP BY location_id) v
        ON v.location_id = s.location_id

      UNION ALL

      -- branch B: forced insert of the new version, only for genuinely changed zones
      SELECT CAST(NULL AS INT) AS merge_key, s.location_id, s.zone_name, s.borough,
             s.service_zone, s.hash_diff,
             coalesce(v.max_version, 0) + 1 AS next_version
      FROM {T_SILVER_ZONE_STG} s
      JOIN {T_DIM_ZONE} d
        ON d.location_id = s.location_id AND d.is_current = true
      LEFT JOIN (SELECT location_id, max(version_no) AS max_version
                 FROM {T_DIM_ZONE} GROUP BY location_id) v
        ON v.location_id = s.location_id
      WHERE d.hash_diff <> s.hash_diff
  ) u
) AS src
ON t.location_id = src.merge_key AND t.is_current = true

WHEN MATCHED AND t.hash_diff <> src.hash_diff THEN UPDATE SET
  t.is_current     = false,
  t.effective_to   = current_timestamp() - INTERVAL 1 SECOND,
  t.updated_at     = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
  zone_key, location_id, zone_name, borough, service_zone, hash_diff,
  effective_from, effective_to, is_current, version_no, created_at, updated_at)
VALUES (
  src.new_zone_key, src.location_id, src.zone_name, src.borough, src.service_zone, src.hash_diff,
  -- v1 opens at 1900 so already-loaded historical trips still fall inside a validity window
  CASE WHEN src.next_version = 1 THEN TIMESTAMP'1900-01-01 00:00:00' ELSE current_timestamp() END,
  TIMESTAMP'9999-12-31 23:59:59', true, src.next_version,
  current_timestamp(), current_timestamp())
""")

print("dim_zone rows:", spark.table(T_DIM_ZONE).count())

# COMMAND ----------

# MAGIC %md ## 5.4 Inspect the history
# MAGIC After you rerun notebook 03 with `zone_version = 2` and rerun this notebook, the four
# MAGIC changed zones will show two rows: one closed, one current.

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows, sum(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows, max(version_no) AS max_version FROM {T_DIM_ZONE}"))

# COMMAND ----------

display(spark.sql(f"""
SELECT location_id, version_no, zone_name, borough, service_zone,
       effective_from, effective_to, is_current
FROM {T_DIM_ZONE}
WHERE location_id IN (
  SELECT location_id FROM {T_DIM_ZONE} GROUP BY location_id HAVING count(*) > 1)
ORDER BY location_id, version_no
"""))

# COMMAND ----------

# integrity checks: exactly one current row per zone, no overlapping validity windows
display(spark.sql(f"""
SELECT
  (SELECT count(*) FROM (SELECT location_id FROM {T_DIM_ZONE} WHERE is_current
     GROUP BY location_id HAVING count(*) <> 1))                       AS zones_with_bad_current_flag,
  (SELECT count(*) FROM {T_DIM_ZONE} a JOIN {T_DIM_ZONE} b
     ON a.location_id = b.location_id AND a.zone_key <> b.zone_key
    AND a.effective_from < b.effective_to AND b.effective_from < a.effective_to) AS overlapping_versions,
  (SELECT count(DISTINCT zone_key) FROM {T_DIM_ZONE})                  AS distinct_surrogate_keys,
  (SELECT count(*) FROM {T_DIM_ZONE})                                  AS total_rows
"""))