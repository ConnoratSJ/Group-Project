{{ config(materialized='table', schema='DBT_TRANSFORMED')}}
WITH base AS (
    SELECT
        DATE,
        latitude,
        longitude,
        CURRENT_SPEED,
        FREE_SPEED,
        CURRENT_SPEED / NULLIF(FREE_SPEED, 0) AS speed_ratio,  -- Avoid division by zero
        FREE_SPEED / MIN(FREE_SPEED) OVER () AS normalized_speed  -- Calculating normalized speed
    FROM {{ ref('traffic_staging') }}
)

SELECT
    DATE,
    latitude,
    longitude,
    CURRENT_SPEED,
    FREE_SPEED,
    speed_ratio,
    normalized_speed
FROM base
