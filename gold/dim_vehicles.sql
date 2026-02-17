{{ config(materialized='table', schema='gold') }}
SELECT 
    sha2(trim(cast(vehicle_id as string)), 256) as vehicle_sk,
    * FROM silver.vehicles
QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY vehicle_id) = 1
