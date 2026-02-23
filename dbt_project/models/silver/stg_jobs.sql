WITH api_jobs AS (
    SELECT
        id AS job_id,
        internal_job_id,
        title,
        absolute_url,
        INITCAP(COALESCE(NULLIF(departments->0->>'name', ''), 'Unknown')) AS department,
        INITCAP(COALESCE(NULLIF(location_name, ''), 'Unknown')) AS location,
        'OfferZen' AS company_name,
        updated_at::DATE AS open_date,
        NULL::DATE AS close_date,
        'api' AS source
    FROM {{ source('bronze', 'raw_jobs_api') }}
),

history_jobs AS (
    SELECT
        job_id,
        internal_job_id,
        title,
        absolute_url,
        INITCAP(COALESCE(NULLIF(TRIM(department), ''), 'Unknown')) AS department,
        INITCAP(COALESCE(NULLIF(TRIM(location), ''), 'Unknown')) AS location,
        company_name,
        CASE
            WHEN open_date ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(open_date, 'MM/DD/YYYY')
            WHEN open_date ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(open_date, 'YYYY-MM-DD')
            ELSE NULL
        END AS open_date,
        CASE
            WHEN close_date ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(close_date, 'MM/DD/YYYY')
            WHEN close_date ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(close_date, 'YYYY-MM-DD')
            ELSE NULL
        END AS close_date,
        'history' AS source
    FROM {{ source('bronze', 'raw_jobs_history') }}
)

SELECT * FROM history_jobs
UNION ALL
SELECT * FROM api_jobs
