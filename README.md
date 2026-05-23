# 🚖 Chicago Taxi Data Engineering Pipeline

## 📌 Overview
An end-to-end data engineering pipeline built using **Databricks, PySpark, Delta Lake, and dbt** to transform raw Chicago Taxi trip data into an analytics-ready warehouse.

The project implements a **Medallion Architecture (Bronze → Silver → Gold)** and focuses on:
- Data cleaning and transformation  
- Deduplication and data quality enforcement  
- Dimensional modeling using star schema  
- Historical tracking using SCD Type 2  
- Data reconciliation across layers  

---

## 🧠 Problem Statement
Raw taxi trip data is not analytics-ready due to:
- Duplicate records created during joins  
- Inconsistent schema and null values  
- No historical tracking of dimension changes  
- No validation across transformation layers  

This pipeline solves these issues by building a structured, reliable lakehouse architecture.

---

## 🏗️ Architecture: Medallion Framework

### 🥉 Bronze Layer (Raw Ingestion)
- Raw Chicago Taxi datasets ingested into Delta tables  
- Data stored in original format for traceability and replay  
- Serves as the source layer for all downstream transformations  

---

### 🥈 Silver Layer (Cleaned Data)
- Data cleaning and standardization using PySpark  
- Duplicate records removed using `ROW_NUMBER()` window function  
- Schema enforcement (timestamps, numeric precision, null handling)  
- Data prepared for analytical modeling  

---

### 🥇 Gold Layer (Analytics Layer)
Built using **dbt models**:

- Star schema design:
  - 1 Fact table (trip-level grain)
  - 5 Dimension tables (drivers, vehicles, customers, locations, payments)
- Fact table grain:
  - Each row represents a single taxi trip
    
- SCD Type 2 implementation using dbt snapshots:
  - Tracks historical changes in dimension attributes over time  

- Surrogate keys:
  - Generated using SHA-256 hashing for deterministic joins  

- Data standardization:
  - Null handling and type consistency for BI tools

---

## 🛠️ Tech Stack
- Databricks  
- Apache Spark / PySpark  
- Delta Lake  
- dbt (Data Build Tool)  
- Spark SQL  
- Git & GitHub  

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
| `lineage_graph.png` | Data lineage visualization |

---

## 🚀 Key Engineering Achievements

### 1️⃣ Resolved Row Duplication Issue (Data Grain Fix)
- Identified unexpected row inflation caused by incorrect join logic between fact and dimension datasets
- Root cause was a mismatch in join grain leading to duplicate trip-level records
- Applied `ROW_NUMBER()` window function to enforce uniqueness at the correct business grain
- Resulted in accurate trip-level dataset with consistent record counts across pipeline layers

---

### 2️⃣ Data Reconciliation & Financial Consistency Validation
- Built validation checks to compare aggregated metrics across Silver and Gold layers
- Detected and verified consistency in revenue calculations after transformations
- Ensured no data loss or duplication during modeling stages
- Final output confirmed consistent revenue totals across layers ($51,122.35), validating pipeline correctness
---

## 🏗️ Data Model (Star Schema)
        dim_drivers     dim_vehicles

dim_customers  →  fact_trips  ←  dim_payments

        dim_locations
## 📊 Validation & Reconciliation

### 🔍 Summary
- Fact table grain: 1 row = 1 trip
- Total records validated: 1,000
- Revenue consistency confirmed: $51,122.35

### ✅ Layer-wise Audit

| Layer | Total Records | Total Revenue | Status |
| :------------ | :------------ | :------------ | :--- |
| **Silver (Source)** | 1,000 | $51,122.35 | ✅ PASS |
| **Gold (Target)** | 1,000 | $51,122.35 | ✅ PASS |

## 🚀 Future Improvements

- Add orchestration using Databricks Workflows or Apache Airflow for automated pipeline execution  
- Implement incremental processing for large-scale data optimization  
- Add automated data quality tests using dbt tests or Great Expectations  
- Build BI dashboards using Power BI or Tableau for business insights  
- Containerize pipeline components for better deployment consistency  
