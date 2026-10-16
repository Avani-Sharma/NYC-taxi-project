# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("catalogName", "nyc_taxi", "enter catalog name:")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalogName")
print(CATALOG)

# COMMAND ----------

USE_PREFIXED_SCHEMAS = (CATALOG == "workspace")

# COMMAND ----------

def _schema(name):
    if USE_PREFIXED_SCHEMAS:
        return f"nyc_{name}"
    else:
        return name

# COMMAND ----------

LANDING_SCHEMA = _schema("landing")
BRONZE_SCHEMA = _schema("bronze")
SILVER_SCHEMA = _schema("silver")
GOLD_SCHEMA = _schema("gold")

# COMMAND ----------

VOLUME = "files"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{VOLUME}"
RAW_TRIPS = f"{VOLUME_ROOT}/raw_trips"
RAW_ZONES = f"{VOLUME_ROOT}/raw_zones"

# COMMAND ----------

# table name of bronze (fully qualified)
T_BRONZE_TRIPS = f"{CATALOG}.{BRONZE_SCHEMA}.trips_raw"
T_BRONZE_ZONES = f"{CATALOG}.{BRONZE_SCHEMA}.zones_raw"

# COMMAND ----------

from datetime import date

# COMMAND ----------

# table names of silver (fully qualified)
T_SILVER_TRIPS      = f"{CATALOG}.{SILVER_SCHEMA}.trips"
T_SILVER_QUARANTINE = f"{CATALOG}.{SILVER_SCHEMA}.trips_quarantine"
T_SILVER_ZONE_STG   = f"{CATALOG}.{SILVER_SCHEMA}.zone_stage"

# silver streaming checkpoint
CKPT = f"{VOLUME_ROOT}/checkpoints/silver_trips_clean"

# COMMAND ----------

# Silver data-quality thresholds (min / max values used by the DQ rules)
DQ = {
    "min_duration_min":  1,
    "max_duration_min":  720,
    "min_trip_distance": 0.1,
    "max_trip_distance": 100,
    "max_passengers":    6,
    "min_total_amount":  0,
    "max_total_amount":  1000,
    "valid_from_date":   "2024-01-01",   
    "valid_to_date":     "2027-12-31",
}

# COMMAND ----------

print(f"catalog : {CATALOG}")
print(f"schemas : {LANDING_SCHEMA} / {BRONZE_SCHEMA} / {SILVER_SCHEMA} / {GOLD_SCHEMA}")
print(f"landing volume : {VOLUME_ROOT}")

# COMMAND ----------

# table names of gold (fully qualified)
T_DIM_DATE    = f"{CATALOG}.{GOLD_SCHEMA}.dim_date"
T_DIM_VENDOR  = f"{CATALOG}.{GOLD_SCHEMA}.dim_vendor"
T_DIM_PAYMENT = f"{CATALOG}.{GOLD_SCHEMA}.dim_payment_type"
T_DIM_RATE    = f"{CATALOG}.{GOLD_SCHEMA}.dim_rate_code"
T_DIM_ZONE    = f"{CATALOG}.{GOLD_SCHEMA}.dim_zone"


T_FACT_TRIPS  = f"{CATALOG}.{GOLD_SCHEMA}.fact_trips"
T_WATERMARK   = f"{CATALOG}.{GOLD_SCHEMA}.etl_watermark"