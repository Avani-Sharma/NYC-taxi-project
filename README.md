# 🚕 NYC Taxi End-to-End Data Engineering Lakehouse Pipeline

An production-grade **Lakehouse Data Engineering Pipeline** built on **Databricks** using the **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**. This project ingests NYC Yellow Taxi trip datasets, applies robust schema enforcement, handles **SCD Type 2 dimension tracking**, executes data quality validation checks, and creates aggregated analytical data marts ready for business dashboards.

---

## 🏗️ Architecture Overview

The pipeline processes raw Parquet data using Unity Catalog, PySpark, and Delta Lake format across three structured layers:

```text
[ Raw Parquet Source Data ]
         │
         ▼
[ Bronze Layer ]   ──► Ingestion & Storage (Unity Catalog Raw Delta Tables)
         │
         ▼
[ Silver Layer ]   ──► Data Cleansing, Schema Enforcement, SCD Type 2 Dimensions & Fact Tables
         │
         ▼
[ Gold Layer ]     ──► Aggregated Data Marts, KPIs & Data Quality Checks
         │
         ▼
[ Analytics & BI ] ──► Databricks Lakeview Dashboard & BI Reporting

---

## 🌟 Key Features

* **Unity Catalog Governance:** Complete governance and structured cataloging across catalogs, schemas, and volumes.
* **Medallion Architecture:** Clear separation of concerns with Bronze (Raw), Silver (Cleansed/Fact-Dim), and Gold (Marts).
* **Slowly Changing Dimensions (SCD Type 2):** Historical tracking for taxi zone updates and lookup metadata.
* **Data Quality Framework:** Explicit verification notebooks testing schema constraints, zero/negative metrics, and null handling.
* **Performance Optimization:** Delta table maintenance, Z-Ordering, VACUUM, and OPTIMIZE configurations.
* **CI/CD Automation:** GitHub Actions workflow (`ci-cd.yaml`) for automated testing and workspace deployment.

---

## 📁 Repository Structure

```text
NYC-taxi-project/
├── .github/
│   └── workflows/
│       └── ci-cd.yaml                   # GitHub Actions CI/CD Deployment Pipeline
├── notebooks/                           # Databricks Lakehouse Pipeline Notebooks
│   ├── 01_config.py                     # Centralized variables, catalog & schema paths
│   ├── 02_setup_unity_catalog.py        # Unity Catalog structure & volume setup
│   ├── 03_bronze_layer.ipynb            # Ingestion of raw yellow trip Parquet files
│   ├── 04_silver_layer.ipynb            # Cleansing, typecasting & null handling
│   ├── 05_dimensions_scd2.py            # Taxi Zone Dimension handling via SCD Type 2
│   ├── 06_fact_trips.py                 # Fact table construction & joins
│   ├── 07_gold_marts.py                 # Business KPI aggregations & summary tables
│   ├── 08_data_quality_checks.py        # Automated schema & metrics validation tests
│   └── 09_optimize_and_maintenance.py   # OPTIMIZE, Z-Order, and Delta VACUUM routines
├── workflows/
│   └── workflow-config.json             # Databricks Workflows (Jobs API) export
├── dashboard/
│   └── NYC Taxi Analytics Dashboard     # Exported Databricks Lakeview Dashboard
├── data/                                # Reference data & raw trip data samples
│   ├── taxi_zone_lookup.csv
│   ├── taxi_zone_lookup_v2.csv
│   └── yellow_tripdata_2024-01.parquet
├── POC/                                 # Proof-of-concept design & architecture documentation
│   └── NYC_Taxi_Lakehouse_POC.pdf
└── README.md

---

## 🚀 Pipeline Execution Flow

1. **`01_config` & `02_setup_unity_catalog`:** Initializes workspace configurations, catalogs, schemas, and external storage locations.
2. **`03_bronze_layer`:** Reads raw `yellow_tripdata_2024-01.parquet` into append-only Delta Lake tables.
3. **`04_silver_layer` & `05_dimensions_scd2`:** 
   - Cleanses invalid trip records (negative fares, invalid distances, invalid passenger counts).
   - Standardizes timestamps, duration, and lookup keys.
   - Manages lookup zone dimension changes using SCD Type 2 logic (`taxi_zone_lookup_v2.csv`).
4. **`06_fact_trips`:** Merges cleansed trip metrics with dimensional keys to form the core Fact table.
5. **`07_gold_marts`:** Builds business-level analytical views:
   - Daily & monthly trip counts, total revenue, and average trip distance.
   - Pickup and drop-off borough hotspot analysis.
   - Payment method breakdown and tip percentage trends.
6. **`08_data_quality_checks`:** Validates key data constraints and integrity assertions across all layers.
7. **`09_optimize_and_maintenance`:** Executes `OPTIMIZE` with `Z-ORDER BY` on frequently filtered keys and runs `VACUUM` to clean obsolete Delta files.

---

## ⚙️ CI/CD & Orchestration

* **Databricks Workflows:** Jobs orchestrated sequentially via `workflows/workflow-config.json` with retry handling and alerts.
* **GitHub Actions:** CI/CD pipeline configured under `.github/workflows/ci-cd.yaml` to deploy code seamlessly into Databricks workspaces upon commits.

---

## 🛠️ Tech Stack

* **Platform:** Databricks (Unity Catalog)
* **Compute / Processing:** Apache Spark, PySpark, Spark SQL
* **Storage Format:** Delta Lake
* **Orchestration:** Databricks Workflows
* **CI/CD:** GitHub Actions
* **Visualization:** Databricks Lakeview Dashboard
