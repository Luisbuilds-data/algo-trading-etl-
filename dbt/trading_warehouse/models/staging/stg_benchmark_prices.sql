{{ config(materialized='view', schema='staging') }}

SELECT
    date::date          AS price_date,
    symbol::text        AS symbol,
    close_price::numeric AS close_price,
    loaded_at
FROM {{ source('raw', 'benchmark_prices') }}
