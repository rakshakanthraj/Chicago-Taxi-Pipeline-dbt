# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC --SHOW TABLES IN gold;
# MAGIC
# MAGIC DESCRIBE TABLE gold.fact_trips;
# MAGIC
# MAGIC
# MAGIC -- DESCRIBE TABLE gold.dim_customers;
# MAGIC -- DESCRIBE TABLE gold.dim_driver;
# MAGIC -- DESCRIBE TABLE gold.dim_locations;
# MAGIC -- DESCRIBE TABLE gold.dim_payments;
# MAGIC -- DESCRIBE TABLE gold.dim_vehicles;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS null_customer_sk
# MAGIC FROM gold.fact_trips
# MAGIC WHERE customer_sk IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete rows in fact_trips where customer_sk is NULL
# MAGIC DELETE FROM gold.fact_trips
# MAGIC WHERE customer_sk IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply NOT NULL constraint on customer_sk
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN customer_sk SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Check for any NULL customer_sk again (just to be sure)
# MAGIC SELECT COUNT(*) AS null_customer_sk
# MAGIC FROM gold.fact_trips
# MAGIC WHERE customer_sk IS NULL;
# MAGIC
# MAGIC -- 2. Set customer_sk, driver_sk, vehicle_sk, and trip_id to NOT NULL
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN trip_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN customer_sk SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN driver_sk SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN vehicle_sk SET NOT NULL;
# MAGIC
# MAGIC -- 3. Add a CHECK constraint to prevent negative values
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ADD CONSTRAINT positive_values
# MAGIC CHECK (distance_km >= 0 AND fare_amount >= 0);-- 1. Check for any NULL customer_sk again (just to be sure)
# MAGIC SELECT COUNT(*) AS null_customer_sk
# MAGIC FROM gold.fact_trips
# MAGIC WHERE customer_sk IS NULL;
# MAGIC
# MAGIC -- 2. Set customer_sk, driver_sk, vehicle_sk, and trip_id to NOT NULL
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN trip_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN customer_sk SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN driver_sk SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.fact_trips
# MAGIC ALTER COLUMN vehicle_sk SET NOT NULL;
# MAGIC
# MAGIC -- -- 3. Add a CHECK constraint to prevent negative values
# MAGIC -- ALTER TABLE gold.fact_trips
# MAGIC -- ADD CONSTRAINT positive_values
# MAGIC -- CHECK (distance_km >= 0 AND fare_amount >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold.fact_trips f
# MAGIC LEFT JOIN gold.dim_customers c
# MAGIC ON f.customer_sk = c.customer_sk
# MAGIC WHERE c.customer_sk IS NULL;
# MAGIC
# MAGIC -- Drivers
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold.fact_trips f
# MAGIC LEFT JOIN gold.dim_driver d
# MAGIC ON f.driver_sk = d.driver_sk
# MAGIC WHERE d.driver_sk IS NULL;
# MAGIC
# MAGIC -- Vehicles
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold.fact_trips f
# MAGIC LEFT JOIN gold.dim_vehicles v
# MAGIC ON f.vehicle_sk = v.vehicle_sk
# MAGIC WHERE v.vehicle_sk IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Verification Script 
# MAGIC -- FINAL RECONCILIATION: COMPARING SILVER TO GOLD

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FINAL AUDIT: Comparing Silver to Gold
# MAGIC -- We expect 1,000 trips and the total revenue of $51,122.35 [cite: 2026-02-15]
# MAGIC
# MAGIC SELECT 
# MAGIC     'SILVER_LAYER' AS layer,
# MAGIC     COUNT(trip_id) AS total_records,
# MAGIC     SUM(fare_amount) AS total_revenue
# MAGIC FROM silver.trips
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'GOLD_LAYER' AS layer,
# MAGIC     COUNT(*) AS total_records, -- Should be 1,000 [cite: 2026-02-15]
# MAGIC     SUM(fare_amount) AS total_revenue -- Should be 51,122.35 [cite: 2026-02-15]
# MAGIC FROM gold.fact_trips;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) as trip_count, 
# MAGIC     SUM(fare_amount) as total_fare
# MAGIC FROM silver.trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_locations' as tbl, COUNT(*) FROM gold.dim_locations; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_payments'  as tbl, COUNT(*) FROM gold.dim_payments;  
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_rows, SUM(fare_amount) as total_revenue 
# MAGIC FROM gold.fact_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(trip_id) as total_trips,
# MAGIC     COUNT(DISTINCT trip_id) as unique_trips,
# MAGIC     SUM(fare_amount) as total_revenue
# MAGIC FROM gold.fact_trips;