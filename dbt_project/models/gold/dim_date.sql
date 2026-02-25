-- date spine from 2015-01-01 to two years from today
WITH date_spine AS (
    SELECT generate_series(
        '2015-01-01'::DATE,
        (CURRENT_DATE + INTERVAL '2 years')::DATE,
        '1 day'::INTERVAL
    )::DATE AS full_date
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date)::INTEGER AS year,
    EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
    EXTRACT(MONTH FROM full_date)::INTEGER AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(ISODOW FROM full_date)::INTEGER AS day_of_week,
    EXTRACT(ISODOW FROM full_date) >= 6 AS is_weekend
FROM date_spine
