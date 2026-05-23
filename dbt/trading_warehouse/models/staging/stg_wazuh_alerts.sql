{{ config(materialized='view', schema='staging') }}

SELECT
    id,
    alert_id,
    "timestamp" AT TIME ZONE 'UTC'      AS alert_ts,
    rule_id,
    rule_level,
    rule_description,
    agent_name,
    COALESCE(NULLIF(src_ip, ''), 'unknown') AS src_ip,
    full_log,
    raw,
    loaded_at
FROM {{ source('raw', 'wazuh_alerts') }}
