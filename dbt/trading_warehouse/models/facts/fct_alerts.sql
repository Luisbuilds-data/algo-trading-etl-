{{ config(materialized='table', schema='marts') }}

SELECT
    a.id,
    a.alert_id,
    a.alert_ts,
    a.rule_id,
    a.rule_level,
    a.rule_description,
    a.agent_name,
    a.src_ip,
    a.loaded_at,
    d.day_of_week,
    d.day_name,
    d.month,
    d.quarter,
    r.severity_label
FROM {{ ref('stg_wazuh_alerts') }} a
LEFT JOIN {{ ref('dim_date') }} d ON d.date_day = (a.alert_ts AT TIME ZONE 'UTC')::date
LEFT JOIN {{ ref('dim_rule') }} r ON r.rule_id  = a.rule_id
