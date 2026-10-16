# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 07 - Gold marts for the dashboard
# MAGIC
# MAGIC Small, pre-aggregated tables rebuilt on every pipeline run. The AI/BI dashboard reads
# MAGIC these, so the moment the job finishes the dashboard reflects the new data.
# MAGIC
# MAGIC `mart_daily_kpis` is deliberately **`PARTITIONED BY (year_month)`** - a coarse, low
# MAGIC cardinality partition key on a table that is always filtered by date range. The big fact
# MAGIC table uses liquid clustering instead; the reasoning is in `docs/DESIGN_DECISIONS.md`.

# COMMAND ----------

# MAGIC %run ./1_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
G = f"{CATALOG}.{GOLD_SCHEMA}"

# COMMAND ----------

# MAGIC %md ## 7.1 Daily KPIs (partitioned)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {G}.mart_daily_kpis")
spark.sql(f"""
CREATE TABLE {G}.mart_daily_kpis
USING DELTA
PARTITIONED BY (year_month)
AS
SELECT
  d.full_date                              AS trip_date,
  d.year_month,
  d.day_name,
  d.is_weekend,
  count(*)                                 AS total_trips,
  round(sum(f.total_amount), 2)            AS total_revenue,
  round(avg(f.total_amount), 2)            AS avg_fare,
  round(sum(f.trip_distance), 1)           AS total_miles,
  round(avg(f.trip_distance), 2)           AS avg_distance,
  round(avg(f.trip_duration_min), 1)       AS avg_duration_min,
  round(avg(f.avg_speed_mph), 1)           AS avg_speed_mph,
  round(avg(f.tip_pct), 2)                 AS avg_tip_pct,
  sum(f.passenger_count)                   AS total_passengers,
  round(avg(f.passenger_count), 2)         AS avg_passengers,
  sum(CASE WHEN f.is_airport_trip THEN 1 ELSE 0 END) AS airport_trips,
  round(100.0 * sum(CASE WHEN f.payment_key = 1 THEN 1 ELSE 0 END) / count(*), 1) AS pct_card_payments
FROM {T_FACT_TRIPS} f
JOIN {T_DIM_DATE} d ON d.date_key = f.date_key
GROUP BY d.full_date, d.year_month, d.day_name, d.is_weekend
""")

# COMMAND ----------

# MAGIC %md ## 7.2 Hourly demand heatmap source

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.mart_hourly_demand AS
SELECT
  d.day_name,
  d.day_of_week,
  f.pickup_hour,
  count(*)                        AS trips,
  round(sum(f.total_amount), 2)   AS revenue,
  round(avg(f.avg_speed_mph), 1)  AS avg_speed_mph,
  round(avg(f.trip_duration_min), 1) AS avg_duration_min
FROM {T_FACT_TRIPS} f
JOIN {T_DIM_DATE} d ON d.date_key = f.date_key
GROUP BY d.day_name, d.day_of_week, f.pickup_hour
""")

# COMMAND ----------

# MAGIC %md ## 7.3 Zone performance (joins the SCD2 dimension on the *current* version)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.mart_zone_performance AS
SELECT
  z.location_id,
  z.zone_name,
  z.borough,
  z.service_zone,
  count(*)                                       AS pickups,
  round(sum(f.total_amount), 2)                  AS revenue,
  round(avg(f.total_amount), 2)                  AS avg_fare,
  round(avg(f.trip_distance), 2)                 AS avg_distance,
  round(avg(f.tip_pct), 2)                       AS avg_tip_pct,
  round(count(*) * 100.0 / sum(count(*)) OVER (), 3) AS pct_of_all_pickups,
  max(f.pickup_date)                             AS last_trip_date
FROM {T_FACT_TRIPS} f
JOIN {T_DIM_ZONE} z ON z.zone_key = f.pu_zone_key
GROUP BY z.location_id, z.zone_name, z.borough, z.service_zone
""")

# COMMAND ----------

# MAGIC %md ## 7.4 Payment + vendor scorecards

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.mart_payment_mix AS
SELECT p.payment_name, p.is_electronic,
       count(*)                      AS trips,
       round(sum(f.total_amount), 2) AS revenue,
       round(avg(f.tip_amount), 2)   AS avg_tip,
       round(avg(f.tip_pct), 2)      AS avg_tip_pct
FROM {T_FACT_TRIPS} f
JOIN {T_DIM_PAYMENT} p ON p.payment_key = f.payment_key
GROUP BY p.payment_name, p.is_electronic
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.mart_vendor_scorecard AS
SELECT v.vendor_name, r.rate_code_name,
       count(*)                           AS trips,
       round(sum(f.total_amount), 2)      AS revenue,
       round(avg(f.trip_distance), 2)     AS avg_distance,
       round(avg(f.trip_duration_min), 1) AS avg_duration_min
FROM {T_FACT_TRIPS} f
JOIN {T_DIM_VENDOR} v ON v.vendor_key    = f.vendor_key
JOIN {T_DIM_RATE}   r ON r.rate_code_key = f.rate_code_key
GROUP BY v.vendor_name, r.rate_code_name
""")

# COMMAND ----------

# MAGIC %md ## 7.5 Pipeline health view - powers the "last refreshed" tile

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.vw_pipeline_health AS
SELECT
  (SELECT count(*) FROM {T_BRONZE_TRIPS})                        AS bronze_rows,
  (SELECT count(*) FROM {T_SILVER_TRIPS})                        AS silver_rows,
  (SELECT count(*) FROM {T_SILVER_QUARANTINE})                   AS quarantined_rows,
  (SELECT count(*) FROM {T_FACT_TRIPS})                          AS fact_rows,
  (SELECT round(100.0 * count(*) /
      nullif((SELECT count(*) FROM {T_BRONZE_TRIPS}), 0), 2)
     FROM {T_SILVER_TRIPS})                                      AS pct_passed_dq,
  (SELECT count(*) FROM {T_DIM_ZONE})                            AS dim_zone_rows,
  (SELECT count(*) FROM {T_DIM_ZONE} WHERE is_current = false)   AS dim_zone_expired_versions,
  (SELECT max(pickup_date) FROM {T_FACT_TRIPS})                  AS latest_trip_date,
  (SELECT max(_loaded_at) FROM {T_FACT_TRIPS})                   AS last_refreshed_at
""")

display(spark.sql(f"SELECT * FROM {G}.vw_pipeline_health"))

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {G}"))