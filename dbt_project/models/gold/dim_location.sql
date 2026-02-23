SELECT
    ROW_NUMBER() OVER (ORDER BY location) AS location_key,
    location AS location_name
FROM (SELECT DISTINCT location FROM {{ ref('stg_jobs') }}) l
WHERE location IS NOT NULL
