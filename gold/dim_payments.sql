{{ config(materialized='table', schema='gold') }}
-- Note: Using silver.payments (the table you said exists in Databricks)
SELECT 
    sha2(trim(cast(payment_method as string)), 256) as payment_sk,
    * FROM silver.payments
QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_method ORDER BY payment_method) = 1
