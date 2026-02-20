# global-funds-data-pipeline

Data scraping and ETL pipeline for global fund datasets (Financial Times, Yahoo Finance, Stock Analysis).

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m playwright install chromium
```

## Run Examples

```bash
python3 src/sites/Financial_Times/financial_times_master_ticker_scraper.py
python3 src/sites/Financial_Times/financial_times_daily_nav_scraper.py
python3 src/sites/Financial_Times/financial_times_static_detail_scraper.py
python3 src/sites/Financial_Times/financial_times_holdings_scraper.py
python3 src/sites/Financial_Times/financial_times_sector_region_scraper.py
python3 src/sites/Yahoo_Finance/yahoo_finance_master_ticker_scraper.py
```

## Run With Prefect

```bash
python3 -m src.maintenance.prefect_pipeline
```

## Prefect Deployments (Daily + Weekly)

Use a Python 3.12 runtime (for this repo, `funds_pipeline_runner` container).

1. Install dependencies in runner:

```bash
docker exec -it funds_pipeline_runner bash -lc "pip install -r requirements.txt"
```

2. Start Prefect API server:

```bash
docker exec -d funds_pipeline_runner bash -lc "prefect server start --host 0.0.0.0 --port 4200"
```

3. Set API URL + register both deployments in one command:

```bash
docker exec -it funds_pipeline_runner bash -lc "export PREFECT_API_URL=http://127.0.0.1:4200/api && PYTHONPATH=. python -m src.maintenance.register_prefect_deployments"
```

Default schedules:
- `global-funds-daily`: `30 6 * * 1-5` (`America/New_York`)
- `global-funds-weekly-holdings`: `0 9 * * 6` (`America/New_York`)

Optional: run specific modules only.

```python
from src.maintenance.prefect_pipeline import run_data_pipeline

run_data_pipeline(
    run_financial_times=True,
    run_ft_daily_nav=False,
    run_ft_static_detail=True,
    run_ft_holdings=True,
    run_ft_sector_region=True,
    run_yahoo_finance=True,
    run_yf_master=True,
)
```

Holdings + Sector/Region can be set to run only on market-closed days (default: Saturday/Sunday in `Europe/London`):

```python
run_data_pipeline(
    holdings_weekly_on_market_closed=True,
    market_timezone="Europe/London",
)
```

Force run holdings on any day:

```python
run_data_pipeline(
    force_run_holdings=True,
)
```

## Output

- Validation files are written under `validation_output/`.
- Logs are written under `logs/`.

## Database Bootstrap (MySQL)

Project schema is versioned in:

- `db/init/001_schema.sql`

`docker-compose.yml` mounts `db/init` into MySQL init directory, so on a fresh MySQL volume tables are auto-created.

```bash
docker compose up -d mysql_db
```

Important:

- MySQL init scripts run only on first container initialization (new volume).
- If schema changes and you want re-init from scratch:

```bash
docker compose down -v
docker compose up -d mysql_db
```

## FX Backfill + NAV Data Mart

Backfill FX rates (example 90 days) and refresh NAV mart views:

```bash
PYTHONPATH=. python -m src.maintenance.fetch_daily_fx_rates --days 90 --target-currency USD
PYTHONPATH=. python -m src.maintenance.build_nav_data_mart
```

Views created:
- `vw_nav_unified` (FT + YF + SA NAV union)
- `vw_nav_usd` (NAV converted to USD via `daily_fx_rates`)
