{{ config(materialized='view', schema='ADHOC') }}
SELECT
    DATE,
    LAT AS latitude,
    LONG AS longitude,
    CURRENT_SPEED,
    FREE_SPEED
FROM {{ source('raw_data_schema', 'TRAFFIC_DATA') }}
