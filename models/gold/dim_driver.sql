{{ config(materialized='table', schema='gold') }}

SELECT 
    sha2(trim(cast(driver_id as string)), 256) as driver_sk,
    fullname,
    phone_number,
    driver_rating,
    city
FROM {{ ref('DimDrivers') }}
WHERE dbt_valid_to IS NULL

UNION ALL

SELECT 
    sha2('Unknown', 256) as driver_sk, 
    'Unknown' as fullname, 
    'Unknown' as phone_number, 
    0.0 as driver_rating, 
    'Unknown' as city