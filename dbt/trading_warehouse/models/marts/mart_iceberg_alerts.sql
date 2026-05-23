{{ config(materialized='table', schema='marts') }}

SELECT
    rule_level,
    rule_description,
    COUNT(*)       AS alert_count,
    MIN(alert_ts)  AS first_seen,
    MAX(alert_ts)  AS last_seen
FROM {{ ref('stg_wazuh_alerts') }}
GROUP BY rule_level, rule_description
ORDER BY rule_level, alert_count DESC
