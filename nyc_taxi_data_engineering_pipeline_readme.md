# NYC Taxi Data Engineering Pipeline

An end-to-end Data Engineering pipeline built on **Databricks** utilizing the **Medallion Architecture (Bronze -> Silver -> Gold)**. This project ingests raw NYC Yellow & Green Taxi trip data, cleanses and transforms it, enriches it with lookup information, and creates aggregated analytical views ready for business dashboards and reporting.

---

## 📐 Architecture Overview

The data pipeline processes high-volume trip data through three core layers:

```text
[ Raw Data Source ]
        │
        ▼
[ Bronze Layer ]   ──► Ingestion & Raw Storage (Parquet / Delta Format)
        │
        ▼
[ Silver Layer ]   ──► Data Cleansing, Schema Enforcement, Deduplication, & Enrichment
        │
        ▼
[ Gold Layer ]     ──► Aggregations, Metrics Calculation & KPI Tables
        │
        ▼
[ Analytics & BI ] ──► Databricks Lakeview Dashboards
```

---

## 🏗️ Medallion Pipeline Layers

### 1. 🥉 Bronze Layer (Raw Data Ingestion)
- Ingests raw trip record dataset (Yellow & Green Taxi trips).
- Preserves raw source schema without destructive transformations.
- Converts raw format into **Delta Lake** tables for ACID compliance and efficient query execution.

### 2. 🥈 Silver Layer (Data Cleansing & Enrichment)
- **Cleansing:** Filters out invalid trips (e.g., zero/negative distances, invalid passenger counts, negative fare amounts).
- **Data Standardization:** Casts timestamps, trip durations, and fare metrics to proper data types.
- **Enrichment:** Joins trip data with **Taxi Zone Lookup** datasets to map Location IDs (`PULocationID`, `DOLocationID`) to actual Boroughs and Zones.

### 3. 🥇 Gold Layer (Aggregations & Business KPIs)
Calculates key metrics to drive business decisions, including:
- **Daily & Monthly Summary:** Total trips, total revenue, average fare per trip, and average distance.
- **Borough Performance:** Pickup and drop-off hotspot analysis by Borough.
- **Payment Insights:** Payment method distributions (Credit Card vs. Cash) and tipping trends.

---

## 📁 Repository Structure

```text
NYC-taxi-project/
├── notebooks/               # Databricks PySpark & SQL Notebooks
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_aggregations.py
├── workflows/               # Databricks Job Workflows export
│   └── workflow-config.json
├── dashboards/              # Databricks Lakeview Dashboard export
│   └── nyc_taxi_analytics.lvdash.json
├── data/                    # Sample lookup files (e.g., taxi_zone_lookup.csv)
├── POC/                     # Proof-of-concept scripts & exploratory notebooks
├── .gitignore
└── README.md
```

---

## ⚙️ Databricks Workflow & Job Automation

The end-to-end pipeline is orchestrated using **Databricks Workflows (Jobs)**:
- Sequential task dependency management (`Bronze` ➔ `Silver` ➔ `Gold`).
- Automated retry mechanisms and email alert notifications on failure.
- Exported JSON configuration is available under `workflows/workflow-config.json` for CI/CD deployment or workspace replication.

---

## 📊 Dashboard & Key Metrics

The pipeline feeds into an interactive **Databricks Lakeview Dashboard** to monitor key operational metrics:
- **Total Revenue & Trip Counts** over time.
- **Peak Hours Analysis** (busiest times of day by pickup location).
- **Average Tip Percentage** broken down by trip distance and fare class.
- **Borough Distribution Heatmap**.

---

## 🛠️ Tech Stack & Tools

- **Storage & Processing Platform:** Databricks
- **Compute Framework:** Apache Spark / PySpark
- **Table Format:** Delta Lake
- **Language:** Python, PySpark, Spark SQL
- **Orchestration:** Databricks Workflows (Jobs API)
- **Visualization:** Databricks Lakeview Dashboards
- **Version Control:** Git & GitHub

---

## 🚀 How to Run

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/NYC-taxi-project.git
   ```
2. **Import Notebooks:** Import the scripts inside `notebooks/` into your Databricks Workspace.
3. **Upload Zone Lookup Data:** Place `taxi_zone_lookup.csv` in DBFS or Unity Catalog Volume.
4. **Deploy Workflow:** Import `workflows/workflow-config.json` in Databricks Workflows to create and run the automated job.