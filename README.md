# 🚖 Chicago Taxi Data Engineering Project

## 📌 Overview
An end-to-end data engineering pipeline built using **Databricks, PySpark, Delta Lake, and dbt** to transform raw Chicago Taxi mobility data into an audit-ready analytical warehouse.

The project implements a robust **Medallion Architecture (Bronze → Silver → Gold)** with SCD Type 2 historical tracking, deterministic surrogate keys, and financial reconciliation validation.

---

## 🗺️ Architecture: Medallion (Bronze → Silver → Gold)

### 🥉 Bronze Layer (Raw)
- Ingests raw taxi trip and dimension CSV data into Delta tables.
- Preserves immutable source-of-truth data for full reprocessing capability.

### 🥈 Silver Layer (Cleaned & Integrated)
- **Change Data Capture (CDC):** Custom PySpark framework using Delta Lake `MERGE` for upserts.
- **Deduplication:** `ROW_NUMBER()` window logic to enforce correct data grain.
- **Schema Enforcement:** Explicit casting to `decimal(10,2)` and `timestamp` for financial and temporal accuracy.

### 🥇 Gold Layer (Curated & Modeled)
- Final **Star Schema** design (1 Fact table, 5 Dimension tables).
- **SCD Type 2 Snapshots:** Implemented via dbt using timestamp strategy.
- **SHA-256 Surrogate Keys:** Deterministic hashing to replace natural keys.
- Null handling via `COALESCE` for analytical consistency.

---

## 🛠️ Tech Stack

- **Databricks**
- **Apache Spark / PySpark**
- **Delta Lake**
- **dbt (Data Build Tool)**
- **Spark SQL**
- **SCD Type 2 Snapshots**
- **SHA-256 Surrogate Keys**
- **Git & GitHub**

---

## 📂 Repository Structure

| Folder / File | Description |
|---------------|------------|
| `macros/` | Reusable dbt macros |
| `models/` | Bronze, Silver, Gold dbt models |
| `snapshots/` | SCD Type 2 snapshot definitions |
| `source_data/` | Raw CSV datasets |
| `pyspark_dbt_project/` | Databricks ingestion & transformation notebooks |
| `dbt_project.yml` | dbt configuration |
| `lineage_graph.png` | dbt model lineage visualization |

---

## 🚀 Key Engineering Achievements

### 1️⃣ Resolved 2x Row Inflation
- Identified duplicate join keys inflating 1,000 trips to 2,000 rows.
- Applied `ROW_NUMBER()` partitioning logic to enforce correct dimensional grain.
- Restored accurate **1,000 unique trip records**.

### 2️⃣ Financial Reconciliation Audit
- Built validation checks across Silver and Gold layers.
- Achieved 1:1 parity in total revenue:

  **$51,122.35 (Silver) = $51,122.35 (Gold)** ✅

---
## 🏗️ Data Model (Star Schema)

```text
          [ dim_drivers ]       [ dim_vehicles ]
                 \                 /
                  \               /
[ dim_customers ] --- [ fact_trips ] --- [ dim_payments ]
                  /   (1,000 Rows)   \
                 /                   \
          [ dim_locations ]      ( $51,122.35 )
```

---
## ✅ Final Validation & Audit Results

| Layer | Total Records | Total Revenue | Status |
| :------------ | :------------ | :------------ | :--- |
| **Silver (Source)** | 1,000 | $51,122.35 | ✅ PASS |
| **Gold (Target)** | 1,000 | $51,122.35 | ✅ PASS |

## ▶️ How to Run

1. `dbt snapshot` – Initialize SCD Type 2 history  
2. `dbt run` – Build Bronze, Silver, Gold layers  
3. `dbt test` – Execute data quality checks  
4. Run reconciliation scripts to validate revenue parity

