{{ config(materialized='table', schema='marts') }}

SELECT DISTINCT
    rule_id,
    rule_level,
    rule_description,
    CASE
        WHEN rule_level BETWEEN 1 AND 5  THEN 'low'
        WHEN rule_level BETWEEN 6 AND 9  THEN 'medium'
        WHEN rule_level >= 10            THEN 'high'
        ELSE                                  'unknown'
    END AS severity_label
FROM {{ ref('stg_wazuh_alerts') }}
WHERE rule_id IS NOT NULL
