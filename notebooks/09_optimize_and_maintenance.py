# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 - Optimize and maintenance
# MAGIC Weekly maintenance is separate from the critical path. Liquid-clustered tables use OPTIMIZE;
# MAGIC VACUUM retains the safe 168-hour window; ANALYZE refreshes table statistics.

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
tables = [T_BRONZE_TRIPS, T_BRONZE_ZONES, T_SILVER_TRIPS, T_SILVER_QUARANTINE,
          T_SILVER_ZONE_STG, T_DIM_ZONE, T_FACT_TRIPS, T_DIM_DATE, T_DIM_VENDOR,
          T_DIM_PAYMENT, T_DIM_RATE]
for table_name in tables:
    if spark.catalog.tableExists(table_name):
        print(f"Maintaining {table_name}")
        spark.sql(f"OPTIMIZE {table_name}")
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
        # Keep Delta's safety check enabled and use the POC's 168-hour retention.
        spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")

# The daily mart is partitioned by year_month; all marts are small, but analyze their stats.
for mart in ["mart_daily_kpis", "mart_hourly_demand", "mart_zone_performance",
             "mart_payment_mix", "mart_vendor_scorecard"]:
    name = f"{CATALOG}.{GOLD_SCHEMA}.{mart}"
    if spark.catalog.tableExists(name):
        spark.sql(f"ANALYZE TABLE {name} COMPUTE STATISTICS")

spark.sql(f"DESCRIBE HISTORY {T_FACT_TRIPS}").show(10, truncate=False)