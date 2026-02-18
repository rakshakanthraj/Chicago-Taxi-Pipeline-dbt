{{ config(materialized='table', schema='gold') }}

SELECT 
    sha2(trim(cast(location_id as string)), 256) as location_sk,
    city,
    state,
    country
FROM {{ ref('DimLocations') }}
WHERE dbt_valid_to IS NULL

UNION ALL

SELECT 
    sha2('Unknown', 256) as location_sk, 
    'Unknown' as city, 
    'Unknown' as state, 
    'Unknown' as country