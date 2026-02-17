{{ config(materialized='table', schema='gold') }}
SELECT 
    sha2(trim(cast(location_id as string)), 256) as location_sk,
    * FROM silver.locations
QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY location_id) = 1
