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
