{{ config(materialized='table', schema='gold') }}

SELECT 
    sha2(trim(cast(customer_id as string)), 256) as customer_sk,
    fullname,
    email
FROM {{ ref('DimCustomers') }}
WHERE dbt_valid_to IS NULL

UNION ALL

SELECT 
    sha2('Unknown', 256) as customer_sk, 
    'Unknown', 'Unknown'