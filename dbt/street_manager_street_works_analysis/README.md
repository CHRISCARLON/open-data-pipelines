# Street Manager Street Works Analysis

dbt project for analysing street works data from Street Manager.

## Setup

Requires environment variables:
- `MOTHERDUCK_TOKEN`
- `MOTHERDB`

## Run

```bash
./run_dbt_jobs.sh
```

## Models

- **england_overview** - Impact scores and permit lists for all England
- **london_overview** - Impact scores and permit lists for London only
- **wellbeing_overview** - Wellbeing impact calculations (currently filtered to swa_code 4720)
- **uprn_usrn_counts** - Property counts per street
