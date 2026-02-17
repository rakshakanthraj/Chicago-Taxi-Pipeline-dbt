{{ config(materialized='table', schema='gold') }}

WITH trips_clean AS (
    SELECT * FROM {{ ref('trips') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY trip_start_time DESC) = 1
),

-- FIX: Added 'WHERE dbt_valid_to IS NULL' to every dimension
customers AS (
    SELECT customer_id, sha2(trim(cast(customer_id as string)), 256) as customer_sk 
    FROM {{ ref('DimCustomers') }}
    WHERE dbt_valid_to IS NULL 
),
drivers AS (
    SELECT driver_id, sha2(trim(cast(driver_id as string)), 256) as driver_sk 
    FROM {{ ref('DimDrivers') }}
    WHERE dbt_valid_to IS NULL
),
vehicles AS (
    SELECT vehicle_id, sha2(trim(cast(vehicle_id as string)), 256) as vehicle_sk 
    FROM {{ ref('DimVehicles') }}
    WHERE dbt_valid_to IS NULL
),
locations AS (
    SELECT location_id, sha2(trim(cast(location_id as string)), 256) as location_sk 
    FROM {{ ref('DimLocations') }}
    WHERE dbt_valid_to IS NULL
),
payments AS (
    SELECT payment_id, sha2(trim(cast(payment_id as string)), 256) as payment_sk 
    FROM {{ ref('DimPayments') }}
    WHERE dbt_valid_to IS NULL
)

SELECT
    t.*,
    COALESCE(c.customer_sk, sha2('-1', 256)) AS customer_sk,
    COALESCE(d.driver_sk,   sha2('-1', 256)) AS driver_sk,
    COALESCE(v.vehicle_sk,  sha2('-1', 256)) AS vehicle_sk,
    COALESCE(l.location_sk, sha2('-1', 256)) AS location_sk,
    COALESCE(p.payment_sk,  sha2('-1', 256)) AS payment_sk
FROM trips_clean t
LEFT JOIN customers c ON trim(cast(t.customer_id as string)) = trim(cast(c.customer_id as string))
LEFT JOIN drivers   d ON trim(cast(t.driver_id as string))   = trim(cast(d.driver_id as string))
LEFT JOIN vehicles  v ON trim(cast(t.vehicle_id as string))  = trim(cast(v.vehicle_id as string))
LEFT JOIN locations l ON trim(cast(t.pickup_location_id as string)) = trim(cast(l.location_id as string))
LEFT JOIN payments  p ON trim(cast(t.payment_id as string))  = trim(cast(p.payment_id as string))
