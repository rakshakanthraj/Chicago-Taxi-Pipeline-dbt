{{ config(materialized='table', schema='gold') }}

WITH base_trips AS (
    -- Force uniqueness on the source trips
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY trip_start_time DESC) as rn
        FROM {{ ref('trips') }}
    ) WHERE rn = 1
),

-- Deduplicate dimensions locally to ensure NO fan-out
customers as (SELECT DISTINCT customer_sk, fullname FROM {{ ref('dim_customers') }} WHERE customer_sk != sha2('Unknown', 256)),
drivers as (SELECT DISTINCT driver_sk, fullname FROM {{ ref('dim_driver') }} WHERE driver_sk != sha2('Unknown', 256)),
locations as (SELECT DISTINCT location_sk, city FROM {{ ref('dim_locations') }} WHERE location_sk != sha2('Unknown', 256)),
payments as (SELECT DISTINCT payment_sk, payment_method FROM {{ ref('dim_payments') }} WHERE payment_sk != sha2('Unknown', 256)),
vehicles as (SELECT DISTINCT vehicle_sk, make FROM {{ ref('dim_vehicles') }} WHERE vehicle_sk != sha2('Unknown', 256))

SELECT
    t.trip_id,
    COALESCE(c.customer_sk, sha2('Unknown', 256)) as customer_sk,
    COALESCE(dr.driver_sk, sha2('Unknown', 256)) as driver_sk,
    COALESCE(l.location_sk, sha2('Unknown', 256)) as location_sk,
    COALESCE(p.payment_sk, sha2('Unknown', 256)) as payment_sk,
    COALESCE(v.vehicle_sk, sha2('Unknown', 256)) as vehicle_sk,
    COALESCE(cast(date_format(t.trip_start_time, 'yyyyMMdd') as int), -1) as trip_date_sk,
    t.trip_start_time,
    t.fare_amount,
    t.distance_km
FROM base_trips t
LEFT JOIN customers c ON sha2(trim(cast(t.customer_id as string)), 256) = c.customer_sk
LEFT JOIN drivers dr ON sha2(trim(cast(t.driver_id as string)), 256) = dr.driver_sk
LEFT JOIN locations l ON sha2(trim(cast(t.pickup_location_id as string)), 256) = l.location_sk
LEFT JOIN payments p ON sha2(trim(cast(t.payment_id as string)), 256) = p.payment_sk
LEFT JOIN vehicles v ON sha2(trim(cast(t.vehicle_id as string)), 256) = v.vehicle_sk