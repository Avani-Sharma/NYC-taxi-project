# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 - Fact table: `fact_trips`
# MAGIC
# MAGIC * **Incremental** - a watermark table remembers the last `_processed_at` consumed from silver.
# MAGIC * **Star schema joins** - every natural key is swapped for a surrogate key.
# MAGIC * **SCD2-aware join** - `dim_zone` is joined on `location_id` **and** the trip timestamp
# MAGIC   falling inside `[effective_from, effective_to)`, so a trip keeps the zone attributes that
# MAGIC   were true when it happened, not today's.
# MAGIC * **MERGE, not append** - re-running is safe.
# MAGIC * **Liquid clustering** on `(pickup_date, pu_zone_key)`.

# COMMAND ----------

# MAGIC %run ./1_config

# COMMAND ----------

from pyspark.sql import functions as F
spark.sql(f"USE CATALOG {CATALOG}")
T_WATERMARK = f"{CATALOG}.{GOLD_SCHEMA}.etl_watermark"

# COMMAND ----------

# MAGIC %md ## 6.1 Watermark control table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_WATERMARK} (
  table_name STRING, last_processed_ts TIMESTAMP, rows_loaded BIGINT, updated_at TIMESTAMP
) USING DELTA COMMENT 'Incremental load bookmarks'
""")

wm = spark.sql(f"SELECT last_processed_ts FROM {T_WATERMARK} WHERE table_name = 'fact_trips'").collect()
last_ts = wm[0]["last_processed_ts"] if wm else None
print("watermark:", last_ts)

# COMMAND ----------

# MAGIC %md ## 6.2 Fact DDL

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_FACT_TRIPS} (
  trip_key        STRING NOT NULL,
  date_key        INT,
  pickup_ts       TIMESTAMP,
  dropoff_ts      TIMESTAMP,
  pickup_date     DATE,
  pickup_hour     INT,
  pu_zone_key     BIGINT,
  do_zone_key     BIGINT,
  vendor_key      INT,
  payment_key     INT,
  rate_code_key   INT,
  passenger_count INT,
  trip_distance   DOUBLE,
  trip_duration_min DOUBLE,
  avg_speed_mph   DOUBLE,
  fare_amount     DOUBLE,
  extra           DOUBLE,
  mta_tax         DOUBLE,
  tip_amount      DOUBLE,
  tolls_amount    DOUBLE,
  improvement_surcharge DOUBLE,
  congestion_surcharge  DOUBLE,
  airport_fee     DOUBLE,
  total_amount    DOUBLE,
  tip_pct         DOUBLE,
  is_airport_trip BOOLEAN,
  _loaded_at      TIMESTAMP
) USING DELTA
CLUSTER BY (pickup_date, pu_zone_key)
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
COMMENT 'Transactional fact - one row per completed taxi trip'
""")

# --- Alternative if you prefer classic Hive-style partitioning (see docs/DESIGN_DECISIONS.md) ---
# CREATE TABLE ... USING DELTA PARTITIONED BY (pickup_year_month)   -- ~1 partition per month
# then: OPTIMIZE fact_trips ZORDER BY (pu_zone_key, pickup_hour)

# COMMAND ----------

# MAGIC %md ## 6.3 Build the incremental slice and MERGE it in

# COMMAND ----------

where_clause = f"WHERE s._processed_at > TIMESTAMP'{last_ts}'" if last_ts else ""

src = spark.sql(f"""
SELECT
  s.trip_key,
  d.date_key,
  s.pickup_ts, s.dropoff_ts, s.pickup_date, s.pickup_hour,
  coalesce(pz.zone_key, -1)  AS pu_zone_key,
  coalesce(dz.zone_key, -1)  AS do_zone_key,
  coalesce(v.vendor_key, -1) AS vendor_key,
  coalesce(p.payment_key, -1) AS payment_key,
  coalesce(r.rate_code_key, -1) AS rate_code_key,
  s.passenger_count, s.trip_distance, s.trip_duration_min, s.avg_speed_mph,
  s.fare_amount, s.extra, s.mta_tax, s.tip_amount, s.tolls_amount,
  s.improvement_surcharge, s.congestion_surcharge, s.airport_fee,
  s.total_amount, s.tip_pct, s.is_airport_trip,
  current_timestamp() AS _loaded_at,
  s._processed_at
FROM {T_SILVER_TRIPS} s
LEFT JOIN {T_DIM_DATE} d
       ON d.full_date = s.pickup_date
-- SCD2 point-in-time lookups
LEFT JOIN {T_DIM_ZONE} pz
       ON pz.location_id = s.pu_location_id
      AND s.pickup_ts >= pz.effective_from AND s.pickup_ts < pz.effective_to
LEFT JOIN {T_DIM_ZONE} dz
       ON dz.location_id = s.do_location_id
      AND s.pickup_ts >= dz.effective_from AND s.pickup_ts < dz.effective_to
-- SCD1 lookups
LEFT JOIN {T_DIM_VENDOR}  v ON v.vendor_key    = s.vendor_id
LEFT JOIN {T_DIM_PAYMENT} p ON p.payment_key   = s.payment_type_id
LEFT JOIN {T_DIM_RATE}    r ON r.rate_code_key = s.rate_code_id
{where_clause}
""")

src.createOrReplaceTempView("fact_src")
new_rows = spark.sql("SELECT count(*) c, max(_processed_at) m FROM fact_src").first()
print(f"incoming rows: {new_rows['c']}")

# COMMAND ----------

if new_rows["c"] > 0:
    spark.sql(f"""
    MERGE INTO {T_FACT_TRIPS} t
    USING (SELECT * EXCEPT (_processed_at) FROM fact_src) s
      ON t.trip_key = s.trip_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    spark.sql(f"""
    MERGE INTO {T_WATERMARK} w
    USING (SELECT 'fact_trips' AS table_name,
                  TIMESTAMP'{new_rows['m']}' AS last_processed_ts,
                  {new_rows['c']} AS rows_loaded,
                  current_timestamp() AS updated_at) s
      ON w.table_name = s.table_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("fact_trips merged, watermark advanced")
else:
    print("nothing new to load")

# COMMAND ----------

# MAGIC %md ## 6.4 Referential integrity + a sample star join

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT count(*) FROM {T_FACT_TRIPS})                                AS fact_rows,
  (SELECT count(*) FROM {T_FACT_TRIPS} WHERE pu_zone_key = -1)         AS orphan_pickup_zone,
  (SELECT count(*) FROM {T_FACT_TRIPS} WHERE date_key IS NULL)         AS missing_date_key,
  (SELECT count(*) FROM (SELECT trip_key FROM {T_FACT_TRIPS}
      GROUP BY trip_key HAVING count(*) > 1))                          AS duplicate_trip_keys
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT z.borough, z.zone_name,
       count(*)                          AS trips,
       round(avg(f.trip_distance), 2)    AS avg_miles,
       round(sum(f.total_amount), 2)     AS revenue,
       round(avg(f.tip_pct), 1)          AS avg_tip_pct
FROM {T_FACT_TRIPS} f
JOIN {T_DIM_ZONE} z ON z.zone_key = f.pu_zone_key
JOIN {T_DIM_DATE} d ON d.date_key = f.date_key
WHERE d.is_weekend = false
GROUP BY z.borough, z.zone_name
ORDER BY revenue DESC
LIMIT 15
"""))