{{ config(materialized='table', schema='gold') }}

SELECT 
    sha2(trim(cast(payment_id as string)), 256) as payment_sk,
    payment_method,
    payment_status
FROM {{ ref('DimPayments') }}
WHERE dbt_valid_to IS NULL

UNION ALL

SELECT 
    sha2('Unknown', 256) as payment_sk, 
    'Unknown' as payment_method, 
    'Unknown' as payment_status