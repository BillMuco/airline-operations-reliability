# Airline Operations Reliability Lakehouse

A Databricks portfolio project that turns public monthly airline on-time records into reliable, reusable operations data.

The project answers practical questions such as: How often are flights on time? Which airports and carriers have changing delay or cancellation patterns? How complete and current is the latest data?

This is an analytics and data-engineering project. It does not predict flight delays or provide live operational guidance.

## Why this project

Data-engineering roles commonly ask for working pipelines, Spark and SQL transformations, Delta Lake, quality checks, orchestration, CI/CD, monitoring, and clear documentation. This project demonstrates those skills on a real public dataset, with each result traceable back to its source period.

## Data source

- **U.S. airline on-time performance:** Bureau of Transportation Statistics (BTS) TranStats monthly flight records.
- **Reference data:** BTS carrier and airport lookup data where appropriate.
- **Access:** Public data from the official BTS site. The first five rows are only a schema preview. The project target is every flight record in each of the latest 24 available monthly files (currently September 2024 through August 2026), processed month by month. We will measure actual file sizes and Free Edition capacity before the full backfill.
- **Freshness:** BTS publishes historical monthly data with a delay. The pipeline detects and processes available periods rather than claiming real-time data.

## Architecture

```text
BTS monthly CSVs in a Unity Catalog Volume
                  |
                  v
         Bronze Delta table
                  |
                  v
         Silver Delta table
                  |
                  v
       Monthly Gold summaries
                  |
                  v
       Databricks / Power BI
```

The Spark job scans folders named `source_month=YYYY-MM` under the raw Volume. For each `flights.csv`, it replaces that month in Bronze and Silver so a rerun does not add duplicate copies. It then rebuilds the compact Gold summaries. The monthly schedule is created paused so the first deployment cannot unexpectedly run; unpause it after the workspace connection and first run are verified.

## Tables

- `workspace.airline_ops.bronze_flights`: source columns preserved as strings, plus source file, month, and ingestion time.
- `workspace.airline_ops.silver_flights`: typed fields used for analysis and a basic valid/invalid row label.
- `workspace.airline_ops.gold_airline_monthly`: carrier reliability by source month.
- `workspace.airline_ops.gold_airport_monthly`: origin airport reliability by source month.
- `workspace.airline_ops.gold_route_monthly`: route reliability by source month.

The on-time arrival percentage counts completed, non-diverted flights that arrived less than 15 minutes late. Cancellation and diversion rates use scheduled flights as the denominator. These definitions should be reviewed against the source documentation before using the metrics for operational decisions.

## Target project outcomes

- Download or land monthly source files and record their source period
- Bronze, Silver, and Gold Delta tables
- Schema checks, valid-row checks, duplicate checks, and reconciliation counts
- Idempotent incremental processing, so rerunning a month does not duplicate its flights
- PySpark and SQL transformations, including a dimensional model for flight, airport, carrier, and date analysis
- A scheduled Databricks job with task dependencies, run history, error visibility, and recovery notes
- Automated code and data tests, Git-based CI/CD checks, and documented deployment steps
- Performance and cost improvements appropriate to the small Free Edition workspace
- Unity Catalog permissions, secret handling if credentials are needed, and clear governance notes
- An interactive dashboard with on-time rate, delay minutes, cancellations, airport/carrier comparisons, and data freshness

## CI/CD

GitHub Actions runs Ruff and unit tests on pushes and pull requests to `main`. After deployment settings are added, successful pushes to `main` also validate and deploy the Databricks bundle. The workflow uses the Databricks CLI and Declarative Automation Bundles; it does not include access tokens in the repository.

Repository settings required for deployment:

1. Add the repository variable `DATABRICKS_HOST` with your workspace URL.
2. Add the repository secret `DATABRICKS_TOKEN` with a Databricks personal access token. Create and enter the token directly in GitHub; never put it in this repository or chat.
3. Add the repository variable `DATABRICKS_DEPLOY_ENABLED` with the value `true` only after the two connection settings are present.

The first deployment creates a paused job. Verify it in Databricks, run it once, then unpause the monthly schedule.

## Free Edition design notes

This project targets Databricks Free Edition for learning. Free Edition uses serverless compute, has usage quotas, and restricts outbound internet access to trusted domains. Monthly files are landed in the Volume before the job processes them; the job does not depend on unverified external downloads at runtime.

Free Edition is for non-commercial use and does not include every enterprise security or administration feature. The project demonstrates the governance and security features the workspace supports, and labels production-only controls as design guidance rather than completed features.

## Licensing

The MIT License applies to this repository's code and documentation. It does not change the terms attached to BTS data or any other third-party material. The project includes source attribution and a note to check the source's current terms before reuse.

## Learning path

1. Inspect a few rows from one monthly source file and learn what each row means.
2. Land the complete monthly file in Bronze and record its source month.
3. Clean and validate records in Silver.
4. Build Gold tables for flight and operations analysis.
5. Add repeat-safe incremental processing and recovery behavior.
6. Schedule the job and inspect successful and failed runs.
7. Add CI checks, governance, performance work, and the dashboard.
8. Write setup instructions and demonstrate a complete run.

## Project status

Project direction selected. Databricks can reach the BTS site. The complete August 2026 source ZIP and CSV are stored in a Unity Catalog Volume. `workspace.airline_ops.bronze_flights` contains 608,389 rows and 113 columns for that month; `workspace.airline_ops.silver_flights` contains the same 608,389 rows with cleaned fields and typed dates/numbers. All August rows passed the current basic required-field checks. Three Gold summaries are built: 12 airline-month rows, 352 airport-month rows, and 6,134 route-month rows. The target is a 24-month, full-record pipeline, subject to measured Free Edition capacity. The current Python pipeline and bundle are being brought under CI/CD; the existing dashboard and data remain in the workspace.
