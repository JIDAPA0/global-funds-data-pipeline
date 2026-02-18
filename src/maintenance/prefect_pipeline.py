import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from prefect import flow, get_run_logger, task


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

SCRIPT_PATHS = {
    "ft_master": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_master_ticker_scraper.py",
    "ft_daily_nav": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_daily_nav_scraper.py",
    "ft_static_detail": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_static_detail_scraper.py",
    "ft_holdings": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_holdings_scraper.py",
    "ft_sector_region": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_sector_region_scraper.py",
    "yf_master": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_master_ticker_scraper.py",
}


def is_market_closed_day(market_timezone: str = "Europe/London", closed_weekdays: Tuple[int, ...] = (5, 6)) -> bool:
    if ZoneInfo is None:
        # Fallback: local server time if zoneinfo is unavailable.
        current = datetime.now()
    else:
        current = datetime.now(ZoneInfo(market_timezone))
    return current.weekday() in closed_weekdays


@task(retries=1, retry_delay_seconds=10, log_prints=True)
def run_python_script(script_key: str, args: Optional[List[str]] = None) -> None:
    logger = get_run_logger()
    script_path = SCRIPT_PATHS[script_key]

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    command = [sys.executable, str(script_path)]
    if args:
        command.extend(args)

    logger.info("Running script: %s", script_path)
    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )

    if result.stdout:
        logger.info("stdout (%s):\n%s", script_key, result.stdout)
    if result.stderr:
        logger.warning("stderr (%s):\n%s", script_key, result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Script failed ({script_key}) with exit code {result.returncode}")

    logger.info("Completed script: %s", script_key)


@flow(name="global-fund-data-pipeline")
def run_data_pipeline(
    run_financial_times: bool = True,
    run_yahoo_finance: bool = True,
    run_ft_daily_nav: bool = True,
    run_ft_static_detail: bool = True,
    run_ft_holdings: bool = True,
    run_ft_sector_region: bool = True,
    run_yf_master: bool = True,
    holdings_weekly_on_market_closed: bool = True,
    market_timezone: str = "Europe/London",
    force_run_holdings: bool = False,
) -> None:
    logger = get_run_logger()
    logger.info("Pipeline started")

    if run_financial_times:
        run_python_script("ft_master")

        if run_ft_daily_nav:
            run_python_script("ft_daily_nav")
        if run_ft_static_detail:
            run_python_script("ft_static_detail")

        should_run_holdings = True
        if holdings_weekly_on_market_closed and not force_run_holdings:
            should_run_holdings = is_market_closed_day(market_timezone=market_timezone)
            if not should_run_holdings:
                logger.info(
                    "Skipping FT holdings + sector/region: market is open (%s).",
                    market_timezone,
                )

        if should_run_holdings:
            if run_ft_holdings:
                run_python_script("ft_holdings")
            if run_ft_sector_region:
                run_python_script("ft_sector_region")

    if run_yahoo_finance and run_yf_master:
        run_python_script("yf_master")

    logger.info("Pipeline finished")


if __name__ == "__main__":
    run_data_pipeline()
