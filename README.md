# NYC Taxi Data Pipeline

End-to-end ELT data pipeline built with dbt, BigQuery and Apache Airflow.

## Tech Stack

| Tool | Purpose |
|------|---------|
| BigQuery | Cloud data warehouse |
| dbt Cloud | Data transformation and testing |
| Apache Airflow | Pipeline orchestration |
| Astronomer | Managed Airflow platform |
| GitHub | Version control and CI/CD |

## Architecture

<img width="630" height="554" alt="Screenshot 2026-07-06 at 2 25 29 PM" src="https://github.com/user-attachments/assets/8405b21e-a4f4-451e-8912-0bc516261d83" />



> End-to-end ELT pipeline: GitHub → Astronomer → Airflow → dbt Cloud → BigQuery

## dbt Models

| Model | Type | Description |
|-------|------|-------------|
| stg_taxi_trips | view | Cleans raw data, removes duplicates and corrupted dates |
| stg_vendors | view | Vendor reference data from seed file |
| int_trips_enriched | view | Adds tip percentage, trip duration, pickup hour |
| fct_trips_incremental | incremental | Fact table using MERGE — only new trips processed |
| mart_trips_by_location | table | Aggregated metrics by pickup location |
| mart_pipeline_monitoring | table | Daily metrics for observability |

## dbt Tests

| Test | What it checks |
|------|---------------|
| validate_fct_trips | Amounts, distances, percentages within valid ranges |
| validate_row_count | Row count above minimum threshold |
| validate_data_freshness | Data not stale |
| validate_date_range | No corrupted dates outside 2022 |

## Key Engineering Decisions

- Incremental model with MERGE strategy — processes only new trips instead of rebuilding 30 million rows daily
- Surrogate keys generated using dbt_utils since source data has no natural primary key
- ROW_NUMBER() deduplication to handle duplicate records in source data
- SCD Type 2 snapshot tracks vendor name changes over time
- Year filter in staging removes 451 corrupted records with dates from 2001-2009

## Data Quality Discoveries

Real issues found and fixed in the NYC Taxi public dataset:

| Issue | Records | Fix |
|-------|---------|-----|
| Duplicate trips | Multiple | ROW_NUMBER() deduplication |
| Negative tip amounts | Multiple | Filter in staging |
| Corrupted dates 2001-2009 | 451 rows | Year filter in staging |
| Zero fare trips | Multiple | Filter in staging |

## How to Run

### Prerequisites
- Astronomer Cloud account
- dbt Cloud account  
- Google Cloud BigQuery project

### Setup
1. Clone this repo
2. Connect to Astronomer Cloud
3. Add DBT_API_TOKEN as an Airflow Variable
4. Trigger the DAG or wait for daily schedule

### dbt Commands
```bash
dbt deps
dbt seed
dbt build
dbt test
```

## What I Learned

- Layered ELT pipelines — staging, intermediate, marts
- dbt incremental models with MERGE strategy
- Airflow sensors, operators and callbacks
- Data quality testing and observability
- GitOps deployment with Astronomer
- Debugging real data quality issues in production
