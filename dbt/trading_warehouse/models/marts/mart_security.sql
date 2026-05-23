{{ config(materialized='table', schema='marts') }}

WITH by_ip AS (
    SELECT
        (alert_ts AT TIME ZONE 'UTC')::date AS date,
        severity_label,
        src_ip,
        COUNT(*)                             AS ip_count
    FROM {{ ref('fct_alerts') }}
    GROUP BY 1, 2, 3
),
ranked AS (
    SELECT
        date,
        severity_label,
        src_ip,
        ip_count,
        ROW_NUMBER() OVER (PARTITION BY date, severity_label ORDER BY ip_count DESC) AS rn
    FROM by_ip
),
totals AS (
    SELECT
        date,
        severity_label,
        SUM(ip_count) AS alert_count
    FROM by_ip
    GROUP BY 1, 2
)

SELECT
    t.date,
    t.severity_label,
    t.alert_count,
    r.src_ip AS top_src_ip
FROM totals t
LEFT JOIN ranked r ON r.date = t.date AND r.severity_label = t.severity_label AND r.rn = 1
ORDER BY t.date, t.severity_label
