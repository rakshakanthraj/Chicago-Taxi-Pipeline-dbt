# 🚖 Chicago Taxi Data Engineering Project

**Description:** An end-to-end dbt and Databricks pipeline transforming raw Chicago Taxi mobility data into an audit-ready Gold layer. This project implements a robust Medallion Architecture using PySpark and dbt, featuring SCD Type 2 tracking and a rigorous financial reconciliation audit.

---

## 🗺️ Architecture: Medallion (Bronze → Silver → Gold)

This project implements a multi-stage pipeline to transform raw urban mobility data into a production-ready analytical warehouse.

**1. Bronze (Raw):** - Ingestion of raw taxi trip data and dimension CSVs into Databricks Delta tables.  
- Preserves the **Source of Truth** for full re-processing capability.

**2. Silver (Cleaned & Integrated):** - **Python Framework:** Custom Python class handles CDC (Change Data Capture) using Delta Lake `MERGE` (Upserts).  
- **Deduplication:** Dynamic logic using `partitionBy` and `row_number()` to ensure a unique record grain.  
- **Schema Enforcement:** dbt casts raw strings into precise types (e.g., `decimal(10,2)`) for financial accuracy and `timestamp` for temporal analysis.

**3. Gold (Curated & Modeled):** - **Star Schema:** Final modeling into 1 Fact table and 5 Dimension tables for optimal analytical performance.  
- **Historical Tracking (SCD Type 2):** dbt Snapshots implemented using a timestamp strategy across all dimensions to track metadata changes over time.  
- **Integrity:** **SHA-256** Surrogate Keys via dbt replace natural keys; null values are handled through `COALESCE`.

---

## 🚀 Key Technical Wins

### **1. Fixed 2x Row Inflation**
- **The Problem:** Initial joins between trips and dimensions doubled the row count from 1,000 to 2,000 records due to duplicate keys in the source data.  
- **The Solution:** Implemented `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` logic to deduplicate dimension keys at the grain of the join.  
- **The Result:** Maintained a perfect grain of **1,000 unique trips**.

### **2. Financial Reconciliation Audit**
- **The Process:** Developed validation scripts to verify transformation logic against source data across all layers.  
- **The Final Metric:** Successfully reconciled total revenue to **$51,122.35** with 1:1 parity between the Silver and Gold layers.

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

---

## 📂 Repository Structure

* **macros/** 
* **models/** → dbt models (Bronze/Silver/Gold) 
* **snapshots/** → SCD Type 2 snapshots for dimensions 
* **source_data/** → Raw CSV files used for ingestion
* **PySpark_DBT_project/** → Databricks notebooks for Ingestion, Transformation, and Business logic 
* **audit/** → SQL validation and reconciliation scripts
* **PNG/lineage_graph.png** → Visual ER diagram and dbt lineage

---

## 🛠️ How to Run

1. **Snapshots:** `dbt snapshot` → Initialize SCD Type 2 history.
2. **Models:** `dbt run` → Build Bronze, Silver, and Gold layers. 
3. **Tests:** `dbt test` → Verify data quality constraints (Unique, Not Null).
4. **Audit:** Execute scripts in the `audit/` folder to verify final financial parity.


