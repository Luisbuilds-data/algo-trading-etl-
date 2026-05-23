{{ config(materialized='table', schema='marts') }}

SELECT
    price_date,
    symbol,
    close_price
FROM {{ ref('stg_benchmark_prices') }}
ORDER BY price_date, symbol
