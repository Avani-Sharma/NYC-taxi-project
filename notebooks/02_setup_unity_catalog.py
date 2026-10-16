# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./01_config

# COMMAND ----------

spark.sql(f"create catalog if not exists {CATALOG}")
spark.sql(f"use catalog {CATALOG}")

# COMMAND ----------

catalogList = [LANDING_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]

# COMMAND ----------

print(LANDING_SCHEMA)
print(BRONZE_SCHEMA)
print(SILVER_SCHEMA)
print(GOLD_SCHEMA)

# COMMAND ----------

for c in catalogList:
    spark.sql(f"create schema if not exists {CATALOG}.{c}")

spark.sql(f"create volume if not exists {CATALOG}.{LANDING_SCHEMA}.{VOLUME}")

# COMMAND ----------

display(spark.sql(f"show schemas in {CATALOG}"))