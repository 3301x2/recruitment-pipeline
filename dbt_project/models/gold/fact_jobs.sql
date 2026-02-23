SELECT
    ROW_NUMBER() OVER () AS job_key,
    j.job_id,
    j.title,
    d.department_key,
    l.location_key,
    od.date_key AS open_date_key,
    cd.date_key AS close_date_key,
    j.close_date IS NULL AS is_open,
    CASE
        WHEN j.close_date IS NOT NULL
        THEN j.close_date - j.open_date
        ELSE NULL
    END AS days_to_fill,
    j.source
FROM {{ ref('stg_jobs') }} j
LEFT JOIN {{ ref('dim_department') }} d ON d.department_name = j.department
LEFT JOIN {{ ref('dim_location') }} l ON l.location_name = j.location
LEFT JOIN {{ ref('dim_date') }} od ON od.full_date = j.open_date
LEFT JOIN {{ ref('dim_date') }} cd ON cd.full_date = j.close_date
