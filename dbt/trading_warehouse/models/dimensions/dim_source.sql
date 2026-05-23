{{ config(materialized='table', schema='marts') }}

WITH sources AS (
    SELECT DISTINCT source
    FROM {{ ref('stg_trades') }}
    WHERE source IS NOT NULL
)

SELECT
    source,
    CASE
        WHEN lower(source) LIKE '%oanda%'  THEN 'OANDA Forex'
        WHEN lower(source) LIKE '%kraken%' THEN 'Kraken Crypto'
        ELSE initcap(source)
    END AS display_name
FROM sources
