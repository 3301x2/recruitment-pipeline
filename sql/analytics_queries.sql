-- 1. jobs per department (all time)
SELECT
    d.department_name,
    COUNT(*) AS total_jobs,
    SUM(CASE WHEN f.is_open THEN 1 ELSE 0 END) AS open_jobs,
    SUM(CASE WHEN NOT f.is_open THEN 1 ELSE 0 END) AS closed_jobs
FROM gold.fact_jobs f
JOIN gold.dim_department d ON d.department_key = f.department_key
GROUP BY d.department_name
ORDER BY total_jobs DESC;

-- 2. avg days to fill by department
SELECT
    d.department_name,
    ROUND(AVG(f.days_to_fill)) AS avg_days_to_fill,
    MIN(f.days_to_fill) AS min_days,
    MAX(f.days_to_fill) AS max_days,
    COUNT(*) AS filled_jobs
FROM gold.fact_jobs f
JOIN gold.dim_department d ON d.department_key = f.department_key
WHERE f.days_to_fill IS NOT NULL
GROUP BY d.department_name
ORDER BY avg_days_to_fill DESC;

-- 3. hiring trend by year and quarter
SELECT
    dt.year,
    dt.quarter,
    COUNT(*) AS jobs_opened
FROM gold.fact_jobs f
JOIN gold.dim_date dt ON dt.date_key = f.open_date_key
GROUP BY dt.year, dt.quarter
ORDER BY dt.year, dt.quarter;

-- 4. jobs by location
SELECT
    l.location_name,
    COUNT(*) AS total_jobs,
    SUM(CASE WHEN f.is_open THEN 1 ELSE 0 END) AS currently_open
FROM gold.fact_jobs f
JOIN gold.dim_location l ON l.location_key = f.location_key
GROUP BY l.location_name
ORDER BY total_jobs DESC;

-- 5. currently open jobs with details
SELECT
    f.title,
    d.department_name,
    l.location_name,
    dt.full_date AS opened_on,
    f.source
FROM gold.fact_jobs f
JOIN gold.dim_department d ON d.department_key = f.department_key
JOIN gold.dim_location l ON l.location_key = f.location_key
LEFT JOIN gold.dim_date dt ON dt.date_key = f.open_date_key
WHERE f.is_open = TRUE
ORDER BY dt.full_date;
