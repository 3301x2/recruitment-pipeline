1

i am building a data pipeline for greenhouse heres the public api:

'https://api.greenhouse.io/v1/boards/offerzen/jobs?content=true'

i have placed the historical data at : data/offerzen_jobs_history_raw.csv

here is the recomended stack : • Python for data processing and API integration
• PostgreSQL as the data warehouse
• DBT for data transformation and modeling
• Medallion Architecture (Bronze/Silver/Gold layers) for data
warehousing
• Star Schema for dimensional modeling

i have to ingest the data from : ◦ Ingest current job data from the Greenhouse API: curl -- location 'https://api.greenhouse.io/v1/boards/offerzen/ jobs?content=true'

process historical data, model,ochestrate and have analystics and insights have a read me  and a solutions md with explaining the achitercture and dessign decisions and trade offs 

create a boilerplate for such a project which files and nothing but psued code in the files


2

i need scripts/ingest.py for the bronze layer. it must load data from two sources into postgres:

1. greenhouse api — GET https://api.greenhouse.io/v1/boards/offerzen/jobs?content=true returns {"jobs": [...]} each job has id, internal_job_id, title, absolute_url, location.name, content, departments and offices arrays. load into bronze.raw_jobs_api, store departments and offices as JSONB

2. historical csv at data/offerzen_jobs_history_raw.csv — 316 rows with job_id, internal_job_id, absolute_url, title, department, location, company_name, open_date, close_date. load into bronze.raw_jobs_history

use psycopg2 and requests, load config from .env with python-dotenv. truncate and reload so its idempotent. print row counts after each load. keep it simple one file short comments

3

i need scripts/transform_silver.py. it reads from bronze.raw_jobs_api and bronze.raw_jobs_history, cleans the data and loads into silver.jobs

data quality issues to fix:
- department casing inconsistent: OPERATIONS, PRODUCT need to be title case
- location casing: "South africa" should be "South Africa"  
- 1 row has empty department, default to 'Unknown'
- some dates in MM/DD/YYYY format instead of YYYY-MM-DD, need to handle both
- api jobs dont have open_date/close_date, use updated_at as open_date and close_date stays null (still open)
- api jobs have department in a json array departments, extract first department name

silver.jobs columns: job_id, internal_job_id, title, absolute_url, department, location, company_name, open_date (DATE), close_date (DATE), source ('api' or 'history'), ingested_at

use psycopg2, truncate and reload, same db config pattern as ingest.py

4

im happy with the high level end to end pipeline working 
— bronze ingestion loads 4 api jobs + 316 csv rows, 
silver cleans and merges to 320 rows, 
gold builds the star schema with 10 departments, 
2 locations, 4383 dates, 
320 fact rows. 
all 9 data quality tests pass. 
now i want to enhance this with dbt for the silver and gold transformations instead of raw python. 

set up a dbt project with models for stg_jobs (silver), 
dim_department, dim_location, dim_date, fact_jobs (gold). include schema.yml with dbt tests (not_null, unique, relationships, accepted_values).
 the dbt models should do the same transformations as my python scripts — fix casing with INITCAP, handle mixed date formats, merge api + csv sources, build the star schema with dimensions and fact table