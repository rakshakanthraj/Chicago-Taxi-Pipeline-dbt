{{ config(materialized='table') }}

SELECT 
    sha2(trim(cast(driver_id as string)), 256) as driver_sk,
    * FROM silver.drivers
-- This ensures one row per driver_id before it ever touches the Fact table
QUALIFY ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY driver_id) = 1
