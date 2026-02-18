{{ config(materialized='table', schema='gold') }}

SELECT 
    sha2(trim(cast(vehicle_id as string)), 256) as vehicle_sk,
    make,
    model,
    year,
    vehicle_type
FROM {{ ref('DimVehicles') }} -- Refers to the snapshot name in your YML
WHERE dbt_valid_to IS NULL    -- This removes duplicates

UNION ALL

SELECT 
    sha2('Unknown', 256) as vehicle_sk, 
    'Unknown', 'Unknown', -1, 'Unknown'