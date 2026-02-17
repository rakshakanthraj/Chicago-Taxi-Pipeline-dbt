# 🚖 Chicago Taxi Data Engineering Project

### **Architecture: Medallion (Bronze -> Silver -> Gold)**
**Tools:** dbt Cloud, Databricks, Spark SQL, PySpark, Delta Lake, GitHub

---

## 🗺️ Data Lineage (The Flow)
This project implements a robust Medallion Architecture to transform raw urban mobility data into a production-ready analytical warehouse.

1. **Bronze (Raw)**:
    - Ingestion of raw taxi trip data and dimension CSVs into Databricks.
    - Preserves the "Source of Truth" for full re-processing capability.

2. **Silver (Cleaned & Integrated)**:
    - **Python Transformation Framework**: Developed a custom Python class to handle **CDC (Change Data Capture)** using Delta Lake `MERGE` (Upserts).
    - **Deduplication**: Implemented a dynamic `dedup` function using `partitionBy` and `row_number()` to ensure a unique record grain.
    - **Schema Enforcement**: Used dbt to cast raw strings into precise types, specifically `decimal(10,2)` for financial accuracy and `timestamp` for temporal analysis.

3. **Gold (Curated & Modeled)**:
    - **Star Schema**: Final modeling of 1 Fact table and 5 Dimension tables.
    - **Historical Tracking (SCD Type 2)**: Implemented **dbt Snapshots** using a `timestamp` strategy across all dimensions to track metadata changes over time.
    - **Integrity**: Generated **SHA2 Surrogate Keys** via dbt to replace natural keys and handle null values through `COALESCE`.



---

## 🚀 Key Technical Wins

### **1. Fixed 2x Row Inflation**
* **The Problem:** Initial joins between trips and dimensions caused the row count to jump from 1,000 to 2,000 records due to duplicate keys in the source.
* **The Solution:** Implemented `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` logic to deduplicate dimension keys at the grain of the join.
* **The Result:** Maintained a perfect grain of **1,000 unique trips**.

### **2. Financial Reconciliation Audit**
* **Audit Process:** Developed a validation script to verify the transformation logic against the source data.
* **Final Metric:** Successfully reconciled total revenue to **$51,122.35** with 1:1 parity between Silver and Gold layers.

---

## 🏗️ Data Model (Star Schema)
The project utilizes a Star Schema design to optimize for analytical performance and self-service BI.



```text
          [ dim_drivers ]       [ dim_vehicles ]
                 \                 /
                  \               /
[ dim_customers ] --- [ fact_trips ] --- [ dim_payments ]
                  /   (1,000 Rows)   \
                 /                   \
          [ dim_locations ]      ( $51,122.35 )



Gemini said

This is it—the definitive, senior-level README.md for your Chicago Taxi Pipeline. It showcases your Python skills, your dbt expertise, your data modeling theory, and your obsession with data quality.

Copy and paste this entire block:
Markdown

# 🚖 Chicago Taxi Data Engineering Project

### **Architecture: Medallion (Bronze -> Silver -> Gold)**
**Tools:** dbt Cloud, Databricks, Spark SQL, PySpark, Delta Lake, GitHub

---

## 🗺️ Data Lineage (The Flow)
This project implements a robust Medallion Architecture to transform raw urban mobility data into a production-ready analytical warehouse.

1. **Bronze (Raw)**:
    - Ingestion of raw taxi trip data and dimension CSVs into Databricks.
    - Preserves the "Source of Truth" for full re-processing capability.

2. **Silver (Cleaned & Integrated)**:
    - **Python Transformation Framework**: Developed a custom Python class to handle **CDC (Change Data Capture)** using Delta Lake `MERGE` (Upserts).
    - **Deduplication**: Implemented a dynamic `dedup` function using `partitionBy` and `row_number()` to ensure a unique record grain.
    - **Schema Enforcement**: Used dbt to cast raw strings into precise types, specifically `decimal(10,2)` for financial accuracy and `timestamp` for temporal analysis.

3. **Gold (Curated & Modeled)**:
    - **Star Schema**: Final modeling of 1 Fact table and 5 Dimension tables.
    - **Historical Tracking (SCD Type 2)**: Implemented **dbt Snapshots** using a `timestamp` strategy across all dimensions to track metadata changes over time.
    - **Integrity**: Generated **SHA2 Surrogate Keys** via dbt to replace natural keys and handle null values through `COALESCE`.



---

## 🚀 Key Technical Wins

### **1. Fixed 2x Row Inflation**
* **The Problem:** Initial joins between trips and dimensions caused the row count to jump from 1,000 to 2,000 records due to duplicate keys in the source.
* **The Solution:** Implemented `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` logic to deduplicate dimension keys at the grain of the join.
* **The Result:** Maintained a perfect grain of **1,000 unique trips**.

### **2. Financial Reconciliation Audit**
* **Audit Process:** Developed a validation script to verify the transformation logic against the source data.
* **Final Metric:** Successfully reconciled total revenue to **$51,122.35** with 1:1 parity between Silver and Gold layers.

---

## 🏗️ Data Model (Star Schema)
The project utilizes a Star Schema design to optimize for analytical performance and self-service BI.



```text
          [ dim_drivers ]       [ dim_vehicles ]
                 \                 /
                  \               /
[ dim_customers ] --- [ fact_trips ] --- [ dim_payments ]
                  /   (1,000 Rows)   \
                 /                   \
          [ dim_locations ]      ( $51,122.35 )
---

## ✅ Final Validation & Audit Results
The following results were certified through a Databricks SQL reconciliation audit:

| Layer | Total Records | Total Revenue | Status |
| :--- | :--- | :--- | :--- |
| **Silver (Source)** | 1,000 | $51,122.35 | ✅ PASS |
| **Gold (Target)** | 1,000 | $51,122.35 | ✅ PASS |

---

## 🛠️ How to Run
1. **Snapshots**: Run `dbt snapshot` to initialize SCD Type 2 history.
2. **Models**: Run `dbt run` to build the Medallion layers.
3. **Tests**: Run `dbt test` to verify data quality constraints (Unique, Not Null).
