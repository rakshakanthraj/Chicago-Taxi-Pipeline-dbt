# Databricks notebook source
# MAGIC %md
# MAGIC ### How is our business doing overall?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(trip_id) AS total_trips,
# MAGIC     ROUND(SUM(fare_amount), 2) AS total_revenue,
# MAGIC     ROUND(AVG(fare_amount), 2) AS avg_fare_per_trip,
# MAGIC     ROUND(SUM(distance_km), 2) AS total_distance_covered
# MAGIC FROM gold.fact_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ### The "Top 5 Customers"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     c.fullname ,
# MAGIC     COUNT(f.trip_id) AS trip_count,
# MAGIC     ROUND(SUM(f.fare_amount), 2) AS total_spent
# MAGIC FROM gold.fact_trips f
# MAGIC JOIN gold.dim_customers c ON f.customer_sk = c.customer_sk
# MAGIC GROUP BY 1
# MAGIC ORDER BY total_spent DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which car category makes us the most money?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     hour(trip_start_time) AS trip_hour, 
# MAGIC     COUNT(*) AS trip_count
# MAGIC FROM gold.fact_trips
# MAGIC GROUP BY 1
# MAGIC ORDER BY trip_count DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     vehicle_sk, 
# MAGIC     SUM(fare_amount) AS revenue
# MAGIC FROM gold.fact_trips 
# MAGIC GROUP BY 1
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     f.vehicle_sk AS fact_id, 
# MAGIC     f.vehicle_sk AS fact_sk, 
# MAGIC     v.vehicle_sk AS dim_id, 
# MAGIC     v.vehicle_sk AS dim_sk,
# MAGIC     v.vehicle_type
# MAGIC FROM gold.fact_trips f
# MAGIC LEFT JOIN gold.dim_vehicles v ON f.vehicle_sk = v.vehicle_sk
# MAGIC WHERE f.vehicle_sk IS NOT NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Fact Hash
# MAGIC SELECT 'FACT' as source, vehicle_sk, count(*) 
# MAGIC FROM gold.fact_trips 
# MAGIC GROUP BY 1, 2 LIMIT 3;
# MAGIC
# MAGIC -- Check Dimension Hash
# MAGIC SELECT 'DIM' as source, vehicle_sk, vehicle_type 
# MAGIC FROM gold.dim_vehicles 
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'FACT' as source, vehicle_sk, count(*) 
# MAGIC FROM gold.fact_trips 
# MAGIC GROUP BY 1, 2 
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count(*) as total_trips,
# MAGIC     count(CASE WHEN driver_sk != sha2('-1', 256) THEN 1 END) as matched_drivers,
# MAGIC     (matched_drivers / total_trips) * 100 as match_rate_percent
# MAGIC FROM gold.fact_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 1: What do IDs look like in Trips?
# MAGIC SELECT driver_id, vehicle_id, payment_id FROM silver.trips LIMIT 5;
# MAGIC
# MAGIC -- Query 2: What do IDs look like in the Dimensions?
# MAGIC -- SELECT driver_id FROM silver.drivers LIMIT 5;
# MAGIC -- SELECT vehicle_id FROM silver.vehicles LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count(*) as total_trips,
# MAGIC     -- Count how many actually joined to a driver (not the 'Missing' hash)
# MAGIC     count(CASE WHEN driver_sk != sha2('-1', 256) THEN 1 END) as matched_drivers,
# MAGIC     -- Count how many actually joined to a payment method
# MAGIC     count(CASE WHEN payment_sk != sha2('-1', 256) THEN 1 END) as matched_payments,
# MAGIC     (matched_drivers / total_trips) * 100 as driver_match_pct,
# MAGIC     (matched_payments / total_trips) * 100 as payment_match_pct
# MAGIC FROM gold.fact_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count(*) as total_trips,
# MAGIC     -- Count how many actually joined to a driver (not the 'Missing' hash)
# MAGIC     count(CASE WHEN driver_sk != sha2('-1', 256) THEN 1 END) as matched_drivers,
# MAGIC     -- Count how many actually joined to a payment method
# MAGIC     count(CASE WHEN payment_sk != sha2('-1', 256) THEN 1 END) as matched_payments,
# MAGIC     (matched_drivers / total_trips) * 100 as driver_match_pct,
# MAGIC     (matched_payments / total_trips) * 100 as payment_match_pct
# MAGIC FROM gold.fact_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_id, count(*) 
# MAGIC FROM gold.fact_trips 
# MAGIC GROUP BY trip_id 
# MAGIC HAVING count(*) > 1 
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'Silver' as layer, SUM(fare_amount) as total_money, COUNT(*) as trip_count FROM silver.trips
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Gold' as layer, SUM(fare_amount) as total_money, COUNT(*) as trip_count FROM gold.fact_trips;