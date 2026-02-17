{{ config(materialized='table', schema='gold') }}
SELECT 
    sha2(trim(cast(customer_id as string)), 256) as customer_sk,
    * FROM silver.customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1
