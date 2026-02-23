SELECT
    ROW_NUMBER() OVER (ORDER BY department) AS department_key,
    department AS department_name
FROM (SELECT DISTINCT department FROM {{ ref('stg_jobs') }}) d
WHERE department IS NOT NULL
