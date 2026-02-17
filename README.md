# 🚖 Chicago Taxi Data Engineering Project

### **Architecture: Medallion (Bronze -> Silver -> Gold)**
**Tools:** dbt Cloud, Databricks, Spark SQL, GitHub

---

## 📌 Project Summary
This project transforms raw urban mobility data into a clean, audited **Star Schema**. The primary focus was ensuring data integrity and financial accuracy during the transition from the Silver transformation layer to the Gold analytical layer.

## 🚀 Key Technical Wins

### **1. Fixed 2x Row Inflation**
* **The Problem:** Joins between trips and dimensions initially caused the row count to jump from 1,000 to 2,000 records due to duplicate keys in the source.
* **The Solution:** Implemented `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` logic to deduplicate dimension keys before the join.
* **The Result:** Maintained a perfect grain of **1,000 unique trips** 

### **2. Financial Reconciliation Audit**
* **Audit Process:** Developed a reconciliation script to verify the transformation logic against the source data.
* **Final Metric:** Successfully reconciled total revenue to **$51,122.35** with 1:1 parity between Silver and Gold layers.

---

## 🏗️ Data Model (Star Schema)
The project utilizes a Star Schema design with a central Fact table and 5 Dimension tables to optimize for analytical performance.



```text
          [ dim_drivers ]       [ dim_vehicles ]
                 \                 /
                  \               /
[ dim_customers ] --- [ fact_trips ] --- [ dim_payments ]
                  /   (1,000 Rows)   \
                 /                   \
          [ dim_locations ]      ( $51,122.35 )
