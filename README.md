# Airline Operations Reliability Lakehouse

A Databricks portfolio project that turns public monthly airline on-time records into reliable, reusable operations data.

The project answers practical questions such as: How often are flights on time? Which airports and carriers have changing delay or cancellation patterns? How complete and current is the latest data?

This is an analytics and data-engineering project. It does not predict flight delays or provide live operational guidance.

## Why this project

Data-engineering roles commonly ask for working pipelines, Spark and SQL transformations, Delta Lake, quality checks, orchestration, CI/CD, monitoring, and clear documentation. This project will demonstrate those skills on a real public dataset, with each result traceable back to its source period.

## Data source

- **U.S. airline on-time performance:** Bureau of Transportation Statistics (BTS) TranStats monthly flight records.
- **Reference data:** BTS carrier and airport lookup data where appropriate.
- **Access:** Public data from the official BTS site. The first five rows are only a schema preview. The project target is every flight record in each of the latest 24 available monthly files (currently September 2024 through August 2026), processed month by month. We will measure actual file sizes and Free Edition capacity before the full backfill.
- **Freshness:** BTS publishes historical monthly data with a delay. The pipeline will detect and process available periods rather than claim to be real time.

## What it will demonstrate

- Download or land monthly source files and record their source period
- Bronze, silver, and gold Delta tables
- Schema checks, valid-row checks, duplicate checks, and reconciliation counts
- Idempotent incremental processing, so rerunning a month does not duplicate its flights
- PySpark and SQL transformations, including a dimensional model for flight, airport, carrier, and date analysis
- A scheduled Databricks job with task dependencies, run history, error visibility, and recovery notes
- Automated code and data tests, Git-based CI/CD checks, and documented deployment steps
- Performance and cost improvements appropriate to the small Free Edition workspace
- Unity Catalog permissions, secret handling if credentials are needed, and clear governance notes
- An AI/BI dashboard with on-time rate, delay minutes, cancellations, airport/carrier comparisons, and data freshness

## Free Edition design notes

This project targets Databricks Free Edition for learning. Free Edition uses serverless compute, has usage quotas, and restricts outbound internet access to trusted domains. We will test whether the workspace can reach the BTS download site before choosing automated download or a file-upload landing step.

Free Edition is for non-commercial use and does not include every enterprise security or administration feature. We will demonstrate the governance and security features the workspace actually supports, and label production-only controls as design guidance rather than completed features.

## Licensing

The MIT License applies to this repository's code and documentation. It does not change the terms attached to BTS data or any other third-party material. The project will include source attribution and a note to check the source's current terms before reuse.

## Learning path

We will build one small part at a time:

1. Inspect a few rows from one monthly source file and learn what each row means.
2. Land the complete monthly file in bronze and record its source month.
3. Clean and validate records in silver.
4. Build gold tables for flight and operations analysis.
5. Add repeat-safe incremental processing and recovery behavior.
6. Schedule the job and inspect successful and failed runs.
7. Add CI checks, governance, performance work, and the dashboard.
8. Write setup instructions and demonstrate a complete run.

## Project status

Project direction selected. Databricks can reach the BTS site. The complete August 2026 source ZIP and CSV are stored in a Unity Catalog volume. `workspace.airline_ops.bronze_flights` contains 608,389 rows and 113 columns for that month; `workspace.airline_ops.silver_flights` contains the same 608,389 rows with cleaned fields and typed dates/numbers. All August rows passed the current basic required-field checks. Three Gold summaries are built: 12 airline-month rows, 352 airport-month rows, and 6,134 route-month rows. The target is a 24-month, full-record pipeline, subject to measured Free Edition capacity. Broader data-quality rules, dashboard, and scheduled runs remain to be completed.
