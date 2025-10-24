# GA4 E-Commerce Analytics — End-to-End PySpark + dbt + CI/CD Pipeline

## 🧠 Overview
This project implements a modern analytics pipeline for GA4 e-commerce event data.  
Data is extracted via **PySpark**, modeled and tested in **dbt**, and continuously integrated and deployed through **GitHub Actions**.  
Final marts are stored in **MotherDuck** and visualized in **Power BI**, demonstrating full data lifecycle coverage — from raw ingestion to BI-ready gold layers.

---

## ⚙️ Architecture
| Layer | Tool | Purpose |
|-------|------|----------|
| **Extraction** | PySpark + BigQuery Connector | Reads GA4 raw events and writes structured Parquet outputs to GCS / local for dbt ingestion |
| **Transformation** | dbt Core (DuckDB/MotherDuck) | Builds modular SQL pipelines with schema contracts, data tests, and lineage |
| **Storage** | MotherDuck (DuckDB Cloud) | Analytical warehouse for dev/prod environments |
| **Orchestration** | GitHub Actions | Automated CI/CD for dbt builds and promotion |
| **Visualization** | Power BI | Validates model accessibility and enables exploratory analytics |

---

## 🧩 ETL — PySpark Stage
**File:** `ga4-ecommerce-project-1-pyspark-etl.py`

- Connects to **BigQuery**: `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
- Transforms data into structured DataFrames (e.g., `dim_users`, `dim_sessions`, `fact_events`)
- Runs built-in **data quality checks** via `run_dq_check()`
- Outputs Parquet datasets to both:
  - **Google Cloud Storage** (`write_to_gcs_for_dbt`)
  - **Local directory** (`write_local_for_dbt`) for dbt consumption
- Uses Spark connectors:
  - `spark-bigquery-with-dependencies_2.12:0.37.0`
  - `gcs-connector:hadoop3-2.2.20`
  - `javax.inject:1`

This ensures reproducible, schema-consistent ingestion for downstream dbt modeling.

---

## 🧱 dbt Models

### Silver Layer
Transforms raw PySpark exports into standardized intermediate tables:
- Normalizes timestamps, identifiers, and session keys  
- Produces reusable models like `stg__conversion_events`, `int__acquisition_touchpoints`  
- Enforces data contracts and tests with `dbt_utils`

---

### 🧱 Gold Layer (Marts)
Final business-ready models designed for marketing, attribution, and behavioral analytics.  
Each enforces **dbt contracts**, **column-level constraints**, and **data integrity tests**.

| Model | Theme | Description |
|--------|--------|-------------|
| `mrt__conversion_funnel` | Conversion | Session-level aggregation showing drop-off at each funnel stage (view → cart → checkout → purchase). Validated via monotonic funnel logic test. |
| `mrt__first_touch_attribution` | Marketing Attribution | Attributes purchase revenue to the **first** recorded marketing touchpoint per user. Ensures causal timestamp ordering. |
| `mrt__last_touch_attribution` | Marketing Attribution | Attributes purchase revenue to the **last** marketing touch before conversion. Complements first-touch for channel comparison. |
| `mrt__first_touch_ltv` | LTV Analysis | Computes average and cumulative lifetime value segmented by the user’s first acquisition channel. |
| `mrt__campaign_performance_rev_metrics` | Campaign Performance | Aggregates revenue, users, and conversion metrics by campaign source/medium for marketing optimization. |
| `mrt__acquisition_device_cohorts` | Behavioral Cohorts | Groups users by device type and acquisition month to track engagement, retention, and spend trends. |
| `mrt__user_device_segments` | Behavioral Segmentation | Classifies users into device and spend quartiles for downstream personalization analysis. |
| `mrt__rfm` | Customer Value | Assigns recency, frequency, and monetary scores to users; supports customer segmentation and lifecycle modeling. |
| `mrt__multi_touch_pathing` | Attribution Pathing | Maps multi-touch journeys (source sequences) leading to purchase for advanced attribution modeling. |
| `mrt__purchase_summary_daily` | Revenue Summary | Daily revenue and purchase metrics rolled up by date, channel, and device; used for time-series visualizations. |
| `mrt__marketing_channel_efficiency` | ROI Metrics | Combines spend and attributed revenue to compute ROAS, CPA, and efficiency KPIs by channel. |

---

## 🔄 CI/CD Automation
This project uses **GitHub Actions** for continuous integration and deployment of dbt models.

### 🧪 CI Workflow — `ci.yml`
Triggered on pushes to `dev` or any `feature/**` branch.  
Validates code changes by:
- Spinning up a temporary **DuckDB/MotherDuck dev environment**  
- Running `dbt build --defer --state prod_state --select "state:modified+"`  
  (only builds modified models vs production state)
- Downloads production `manifest.json` from latest successful CD run for state comparison
- Ensures no regressions or test failures before merge

### 🚀 CD Workflow — `cd.yml`
Triggered on merge/push to `main`.  
Performs a full **production build and artifact upload**:
- Installs dependencies and configures MotherDuck prod profile  
- Runs `dbt build --target prod` to rebuild analytics marts  
- Uploads `manifest.json` and `run_results.json` as build artifacts (`prod-state`) for CI reuse

Together, these workflows enforce **deployment hygiene**, **schema consistency**, and **safe promotion** from `dev` → `prod`.

---

## 🧪 Data Validation
- **PySpark:** `run_dq_check()` to flag nulls, duplicates, or schema drift pre-dbt  
- **dbt:** model-level tests (`unique`, `not_null`, `expression_is_true`)  
- **Power BI:** manual validation confirming marts are queryable via ODBC connection

---

## 📊 Power BI Verification
A sample visualization (`purchase_revenue` by `touchpoint_medium` and `touchpoint_source`) verifies:
- dbt gold models are BI-ready  
- data lineage holds end-to-end from PySpark extraction through Power BI consumption  

---

## 🚀 How to Run Locally

> ⚠️ Prerequisite: This ETL assumes access to the GA4 sample dataset from BigQuery (`bigquery-public-data.ga4_obfuscated_sample_ecommerce`) and a configured local or remote GCS bucket for staging Parquet outputs.

### 1️⃣ Configure Environment
Set up your local `.env` or environment variables with the following keys:

```bash
# Google Cloud project and bucket configuration
GCP_PROJECT_ID="your-project-id"
GCS_BUCKET_PATH="gs://your-staging-bucket/dbt_inputs/"

# Spark BigQuery connector jar locations (if running locally)
SPARK_JARS="spark-bigquery-with-dependencies_2.12-0.37.0.jar,gcs-connector-hadoop3-2.2.20.jar"

# 2️⃣ Run PySpark ETL
spark-submit \
  --jars $SPARK_JARS \
  ga4-ecommerce-project-1-pyspark-etl.py


# 3️⃣ Build dbt models
dbt deps
dbt build

# 4️⃣ Run dbt tests and docs
dbt test
dbt docs generate && dbt docs serve


