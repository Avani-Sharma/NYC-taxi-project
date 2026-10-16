# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 - Data quality gate
# MAGIC Runs after gold. Any FAIL raises, which makes the Lakeflow task go red and stops the run
# MAGIC from being reported as successful.

# COMMAND ----------

# MAGIC %run ./1_config

# COMMAND ----------

G = f"{CATALOG}.{GOLD_SCHEMA}"

CHECKS = [
    ("fact has rows",
     f"SELECT count(*) FROM {T_FACT_TRIPS}", lambda v: v > 0),
    ("no duplicate trip_key in fact",
     f"SELECT count(*) FROM (SELECT trip_key FROM {T_FACT_TRIPS} GROUP BY trip_key HAVING count(*) > 1)",
     lambda v: v == 0),
    ("no orphan pickup zone keys",
     f"SELECT count(*) FROM {T_FACT_TRIPS} f LEFT JOIN {T_DIM_ZONE} z ON z.zone_key = f.pu_zone_key WHERE z.zone_key IS NULL",
     lambda v: v == 0),
    ("no null date_key",
     f"SELECT count(*) FROM {T_FACT_TRIPS} WHERE date_key IS NULL", lambda v: v == 0),
    ("exactly one current row per zone",
     f"SELECT count(*) FROM (SELECT location_id FROM {T_DIM_ZONE} WHERE is_current GROUP BY location_id HAVING count(*) <> 1)",
     lambda v: v == 0),
    ("no overlapping SCD2 validity windows",
     f"""SELECT count(*) FROM {T_DIM_ZONE} a JOIN {T_DIM_ZONE} b
         ON a.location_id = b.location_id AND a.zone_key <> b.zone_key
        AND a.effective_from < b.effective_to AND b.effective_from < a.effective_to""",
     lambda v: v == 0),
    ("no negative revenue in fact",
     f"SELECT count(*) FROM {T_FACT_TRIPS} WHERE total_amount <= 0", lambda v: v == 0),
    ("DQ pass rate above 85%",
     f"SELECT round(100.0 * (SELECT count(*) FROM {T_SILVER_TRIPS}) / nullif((SELECT count(*) FROM {T_BRONZE_TRIPS}),0), 2)",
     lambda v: v is not None and v >= 85),
    ("gold marts are not empty",
     f"SELECT count(*) FROM {G}.mart_daily_kpis", lambda v: v > 0),
]

results, failed = [], []
for name, sql, ok in CHECKS:
    val = spark.sql(sql).first()[0]
    passed = bool(ok(val))
    results.append((name, float(val) if val is not None else None, "PASS" if passed else "FAIL"))
    if not passed:
        failed.append(name)

display(spark.createDataFrame(results, "check string, value double, status string"))

if failed:
    raise Exception("Data quality gate failed: " + "; ".join(failed))
print("All data quality checks passed.")