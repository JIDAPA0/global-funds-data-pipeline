import subprocess
import sys
from datetime import datetime
import os
from pathlib import Path
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from prefect import flow, get_run_logger, task
    PREFECT_AVAILABLE = True
except Exception:  # pragma: no cover
    import logging

    PREFECT_AVAILABLE = False

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    def get_run_logger():
        return logging.getLogger("pipeline")

    def flow(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    def task(*args, **kwargs):
        def decorator(func):
            return func

        return decorator


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

SCRIPT_PATHS = {
    "ft_master": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_master_ticker_scraper.py",
    "ft_daily_nav": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_daily_nav_scraper.py",
    "ft_static_detail": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_static_detail_scraper.py",
    "ft_avg_fund_return": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_avg_fund_return_scraper.py",
    "ft_holdings": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_holdings_scraper.py",
    "ft_sector_region": PROJECT_ROOT / "src/sites/Financial_Times/financial_times_sector_region_scraper.py",
    "yf_master": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_master_ticker_scraper.py",
    "yf_nav_etf": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_etf_nav_scraper.py",
    "yf_nav_fund": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_fund_nav_scraper.py",
    "yf_static_identity": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_static_identity_scraper.py",
    "yf_static_fees": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_static_fees_scraper.py",
    "yf_static_risk": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_static_risk_scraper.py",
    "yf_static_policy": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_static_policy_scraper.py",
    "yf_holdings": PROJECT_ROOT / "src/sites/Yahoo_Finance/yahoo_finance_holdings_scraper.py",
    "sa_master": PROJECT_ROOT / "src/sites/Stock_Analysis/stock_analysis_master_ticker_scraper.py",
    "sa_static_detail": PROJECT_ROOT / "src/sites/Stock_Analysis/stock_analysis_static_detail_scraper.py",
    "sa_holdings": PROJECT_ROOT / "src/sites/Stock_Analysis/stock_analysis_holdings_scraper.py",
    "sa_sector_country": PROJECT_ROOT / "src/sites/Stock_Analysis/stock_analysis_sector_country_scraper.py",
    "load_master_to_db": PROJECT_ROOT / "src/maintenance/load_master_lists_to_db.py",
    "merge_security_master": PROJECT_ROOT / "src/maintenance/merge_security_master_status.py",
    "merge_isin_master_priority": PROJECT_ROOT / "src/maintenance/merge_isin_master_priority.py",
    "load_daily_nav_to_db": PROJECT_ROOT / "src/maintenance/load_daily_nav_to_db.py",
    "load_static_to_db": PROJECT_ROOT / "src/maintenance/load_static_to_db.py",
    "load_ft_avg_fund_return_to_db": PROJECT_ROOT / "src/maintenance/load_ft_avg_fund_return_to_db.py",
    "load_holdings_to_db": PROJECT_ROOT / "src/maintenance/load_holdings_to_db.py",
    "fetch_daily_fx_rates": PROJECT_ROOT / "src/maintenance/fetch_daily_fx_rates.py",
    "build_nav_data_mart": PROJECT_ROOT / "src/maintenance/build_nav_data_mart.py",
    "create_ft_compat_views": PROJECT_ROOT / "src/maintenance/create_ft_compat_views.py",
    "create_canonical_views_3src": PROJECT_ROOT / "src/maintenance/create_canonical_views_3src.py",
    "publish_ready_isin_serving": PROJECT_ROOT / "src/maintenance/publish_ready_isin_serving.py",
    "fix_ft_data_quality_issues": PROJECT_ROOT / "src/maintenance/fix_ft_data_quality_issues.py",
    "data_quality_report": PROJECT_ROOT / "src/maintenance/data_quality_report.py",
}


def load_dotenv_vars() -> dict:
    if not ENV_FILE.exists():
        return {}

    out = {}
    for raw_line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            out[key] = value
    return out


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
    dotenv_env = load_dotenv_vars()
    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        env={
            **os.environ,
            **dotenv_env,
            "PYTHONPATH": f"{PROJECT_ROOT}:{os.environ.get('PYTHONPATH', '')}",
        },
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
    run_stock_analysis: bool = True,
    run_ft_daily_nav: bool = True,
    run_ft_static_detail: bool = True,
    run_ft_avg_fund_return: bool = True,
    run_ft_holdings: bool = True,
    run_ft_sector_region: bool = True,
    run_yf_master: bool = True,
    run_yf_nav_etf: bool = True,
    run_yf_nav_fund: bool = True,
    run_yf_static_identity: bool = True,
    run_yf_static_fees: bool = True,
    run_yf_static_risk: bool = True,
    run_yf_static_policy: bool = True,
    run_yf_holdings: bool = True,
    run_sa_master: bool = True,
    run_sa_static_detail: bool = True,
    run_sa_holdings: bool = True,
    run_sa_sector_country: bool = True,
    run_master_db_load: bool = True,
    run_security_master_merge: bool = True,
    run_isin_priority_merge: bool = True,
    run_daily_nav_db_load: bool = True,
    run_static_db_load: bool = True,
    run_ft_avg_return_db_load: bool = True,
    run_holdings_db_load: bool = True,
    run_fx_rates_load: bool = True,
    fx_backfill_days: int = 1,
    run_nav_data_mart_refresh: bool = True,
    run_ft_compat_views: bool = True,
    run_canonical_views_3src: bool = True,
    run_publish_ready_serving: bool = True,
    run_ft_dq_fix: bool = True,
    run_data_quality_report: bool = True,
    parallel_by_source: bool = True,
    holdings_weekly_on_market_closed: bool = True,
    market_timezone: str = "Europe/London",
    force_run_holdings: bool = False,
    smoke_test: bool = False,
    smoke_sample_size: int = 50,
) -> None:
    logger = get_run_logger()
    logger.info("Pipeline started")

    sample_args = ["--sample", str(smoke_sample_size)] if smoke_test else []
    nav_sample_args = ["--sample", str(smoke_sample_size)] if smoke_test else []

    def run_ft_source() -> None:
        if not run_financial_times:
            return
        if smoke_test:
            run_python_script(
                "ft_master",
                args=[
                    "--concurrency",
                    "20",
                    "--etf-mode",
                    "light",
                    "--sample-funds",
                    str(smoke_sample_size),
                    "--sample-etfs",
                    str(smoke_sample_size),
                ],
            )
        else:
            run_python_script("ft_master")

        if run_ft_daily_nav:
            run_python_script("ft_daily_nav", args=nav_sample_args)
        if run_ft_static_detail:
            run_python_script("ft_static_detail", args=(["--sample", "50"] if smoke_test else []))
        if run_ft_avg_fund_return:
            run_python_script("ft_avg_fund_return", args=sample_args)

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
                run_python_script("ft_holdings", args=sample_args)
            if run_ft_sector_region:
                run_python_script("ft_sector_region", args=sample_args)

    def run_yf_source() -> None:
        if not run_yahoo_finance:
            return
        if run_yf_master:
            run_python_script("yf_master")
        if run_yf_nav_etf:
            run_python_script("yf_nav_etf", args=sample_args)
        if run_yf_nav_fund:
            run_python_script("yf_nav_fund", args=sample_args)
        if run_yf_static_identity:
            run_python_script("yf_static_identity", args=sample_args)
        if run_yf_static_fees:
            run_python_script("yf_static_fees", args=sample_args)
        if run_yf_static_risk:
            run_python_script("yf_static_risk", args=sample_args)
        if run_yf_static_policy:
            run_python_script("yf_static_policy", args=sample_args)
        if run_yf_holdings:
            run_python_script("yf_holdings", args=sample_args)

    def run_sa_source() -> None:
        if not run_stock_analysis:
            return
        if run_sa_master:
            run_python_script("sa_master", args=(["--headless", "--sample", str(smoke_sample_size)] if smoke_test else ["--headless"]))
        if run_sa_static_detail:
            run_python_script("sa_static_detail", args=(["--headless"] if smoke_test else []))
        if run_sa_holdings:
            run_python_script("sa_holdings", args=sample_args)
        if run_sa_sector_country:
            run_python_script("sa_sector_country", args=sample_args)

    if parallel_by_source:
        logger.info("Running source pipelines in parallel mode (FT/YF/SA).")
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(run_ft_source),
                executor.submit(run_yf_source),
                executor.submit(run_sa_source),
            ]
            for future in futures:
                future.result()
    else:
        run_ft_source()
        run_yf_source()
        run_sa_source()

    # Master post-processing pipeline:
    # 1) clean/dedupe/load master lists into source staging tables
    # 2) merge source staging into consolidated stg_security_master with status lifecycle
    if run_master_db_load:
        run_python_script("load_master_to_db")
    if run_security_master_merge:
        run_python_script("merge_security_master")
    if run_daily_nav_db_load:
        run_python_script("load_daily_nav_to_db")
    if run_static_db_load:
        run_python_script("load_static_to_db")
    if run_ft_avg_return_db_load:
        run_python_script("load_ft_avg_fund_return_to_db")
    if run_isin_priority_merge:
        run_python_script("merge_isin_master_priority")
    if run_holdings_db_load:
        run_python_script("load_holdings_to_db")
    if run_fx_rates_load:
        run_python_script("fetch_daily_fx_rates", args=["--days", str(fx_backfill_days), "--target-currency", "USD"])
    if run_nav_data_mart_refresh:
        run_python_script("build_nav_data_mart")
    if run_ft_compat_views:
        run_python_script("create_ft_compat_views")
    if run_canonical_views_3src:
        run_python_script("create_canonical_views_3src")
    if run_publish_ready_serving:
        run_python_script("publish_ready_isin_serving")
    if run_ft_dq_fix:
        run_python_script("fix_ft_data_quality_issues")
    if run_data_quality_report:
        run_python_script("data_quality_report")

    logger.info("Pipeline finished")


if __name__ == "__main__":
    run_data_pipeline()
