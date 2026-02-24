-- ============================================================
-- RECRUITMENT ANALYTICS QUERIES
-- Database: recruitment (PostgreSQL 16)
-- Schema: public_gold (dbt-managed star schema)
-- ============================================================

-- 1. Currently open positions
SELECT
    f.title,
    d.department_name,
    l.location_name
FROM public_gold.fact_jobs f
JOIN public_gold.dim_department d ON d.department_key = f.department_key
JOIN public_gold.dim_location l ON l.location_key = f.location_key
WHERE f.is_open = true
ORDER BY d.department_name, f.title;

-- 2. Job count by department (all time)
SELECT
    d.department_name,
    COUNT(*) AS total_jobs
FROM public_gold.fact_jobs f
JOIN public_gold.dim_department d ON d.department_key = f.department_key
GROUP BY d.department_name
ORDER BY total_jobs DESC;

-- 3. Job count by location
SELECT
    l.location_name,
    COUNT(*) AS total_jobs
FROM public_gold.fact_jobs f
JOIN public_gold.dim_location l ON l.location_key = f.location_key
GROUP BY l.location_name
ORDER BY total_jobs DESC;

-- 4. AVERAGE TIME-TO-FILL by department
--    Key recruitment metric: how long does each department take to fill roles?
SELECT
    d.department_name,
    COUNT(*) FILTER (WHERE f.days_to_fill IS NOT NULL) AS filled_jobs,
    ROUND(AVG(f.days_to_fill), 1) AS avg_days_to_fill,
    MIN(f.days_to_fill) AS min_days,
    MAX(f.days_to_fill) AS max_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.days_to_fill)
        FILTER (WHERE f.days_to_fill IS NOT NULL) AS median_days_to_fill
FROM public_gold.fact_jobs f
JOIN public_gold.dim_department d ON d.department_key = f.department_key
WHERE f.days_to_fill IS NOT NULL
GROUP BY d.department_name
ORDER BY avg_days_to_fill DESC;

-- 5. Overall recruitment summary
SELECT
    COUNT(*) AS total_positions,
    COUNT(*) FILTER (WHERE is_open = true) AS open_positions,
    COUNT(*) FILTER (WHERE is_open = false) AS filled_positions,
    ROUND(AVG(days_to_fill), 1) AS avg_days_to_fill,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_fill)
        FILTER (WHERE days_to_fill IS NOT NULL) AS median_days_to_fill
FROM public_gold.fact_jobs;

-- 6. Seasonal hiring trends — jobs opened by quarter
SELECT
    dt.year,
    'Q' || dt.quarter AS quarter,
    COUNT(*) AS jobs_opened
FROM public_gold.fact_jobs f
JOIN public_gold.dim_date dt ON dt.date_key = f.open_date_key
GROUP BY dt.year, dt.quarter
ORDER BY dt.year, dt.quarter;

-- 7. Monthly hiring volume
SELECT
    dt.year,
    dt.month,
    TRIM(dt.month_name) AS month_name,
    COUNT(*) AS jobs_opened
FROM public_gold.fact_jobs f
JOIN public_gold.dim_date dt ON dt.date_key = f.open_date_key
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

-- 8. Department hiring velocity — fill rate analysis
SELECT
    d.department_name,
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (WHERE f.is_open = true) AS still_open,
    COUNT(*) FILTER (WHERE f.is_open = false) AS filled,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE f.is_open = false) / NULLIF(COUNT(*), 0),
        1
    ) AS fill_rate_pct
FROM public_gold.fact_jobs f
JOIN public_gold.dim_department d ON d.department_key = f.department_key
GROUP BY d.department_name
ORDER BY fill_rate_pct DESC;
