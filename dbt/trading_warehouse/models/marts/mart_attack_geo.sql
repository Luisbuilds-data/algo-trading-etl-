{{ config(materialized='table', schema='marts') }}

SELECT
    g.src_ip,
    g.country,
    g.country_code,
    g.city,
    g.latitude,
    g.longitude,
    g.isp,
    COUNT(a.id)                              AS alert_count,
    MIN(a.alert_ts)                          AS first_seen,
    MAX(a.alert_ts)                          AS last_seen
FROM raw.ip_geo g
JOIN {{ ref('stg_wazuh_alerts') }} a ON a.src_ip = g.src_ip
WHERE g.status = 'success'
  AND g.country IS NOT NULL
GROUP BY g.src_ip, g.country, g.country_code, g.city, g.latitude, g.longitude, g.isp
ORDER BY alert_count DESC
