{{ config(materialized='table', schema='marts') }}

WITH date_spine AS (
    SELECT generate_series(
        '2026-01-01'::date,
        '2026-12-31'::date,
        '1 day'::interval
    )::date AS date_day
)

SELECT
    date_day,
    -- ISODOW: 1=Mon … 7=Sun → shift to 0=Mon … 6=Sun
    (EXTRACT(ISODOW FROM date_day)::int - 1)  AS day_of_week,
    TO_CHAR(date_day, 'Dy')                    AS day_name,
    EXTRACT(MONTH   FROM date_day)::int        AS month,
    EXTRACT(QUARTER FROM date_day)::int        AS quarter,
    EXTRACT(ISODOW  FROM date_day) >= 6        AS is_weekend
FROM date_spine
