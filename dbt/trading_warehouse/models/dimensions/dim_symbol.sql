{{ config(materialized='table', schema='marts') }}

WITH symbols AS (
    SELECT DISTINCT symbol FROM {{ ref('stg_trades') }}          WHERE symbol IS NOT NULL
    UNION
    SELECT DISTINCT symbol FROM {{ ref('stg_benchmark_prices') }} WHERE symbol IS NOT NULL
)

SELECT
    symbol,
    CASE
        WHEN symbol LIKE '%/%' THEN 'forex'
        ELSE 'crypto'
    END AS asset_class
FROM symbols
