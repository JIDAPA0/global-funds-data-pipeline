import argparse
import asyncio
import os
import random
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.browser_utils import get_random_user_agent, mimic_reading
from src.utils.logger import log_execution_summary, setup_logger
from src.utils.path_manager import VAL_SA_STATIC

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


logger = setup_logger("03_static_sa_detail")

LOGIN_URL = "https://stockanalysis.com/login"
SCREENER_URL = "https://stockanalysis.com/etf/screener/"

INFO_MAPPING = {
    "Symbol": "ticker",
    "Fund Name": "name",
    "ISIN Number": "isin_number",
    "CUSIP Number": "cusip_number",
    "Issuer": "issuer",
    "Category": "category",
    "Index": "index_benchmark",
    "Inception": "inception_date",
    "Exchange": "exchange",
    "Region": "region",
    "Country": "country",
    "Leverage": "leverage",
    "Options": "options",
    "Shares": "shares_out",
}
INFO_COLUMNS = [
    "ticker",
    "asset_type",
    "source",
    "name",
    "isin_number",
    "cusip_number",
    "issuer",
    "category",
    "index_benchmark",
    "inception_date",
    "exchange",
    "region",
    "country",
    "leverage",
    "options",
    "shares_out",
    "market_cap_size",
]

FEES_MAPPING = {
    "Exp. Ratio": "expense_ratio",
    "Assets": "assets_aum",
    "Holdings": "holdings_count",
}
FEES_COLUMNS = [
    "ticker",
    "asset_type",
    "source",
    "expense_ratio",
    "initial_charge",
    "exit_charge",
    "assets_aum",
    "top_10_hold_pct",
    "holdings_count",
    "holdings_turnover",
]

RISK_MAPPING = {
    "Sharpe": "sharpe_ratio_5y",
    "Beta (5Y)": "beta_5y",
    "RSI": "rsi_daily",
    "200 MA": "moving_avg_200",
}
RISK_COLUMNS = [
    "ticker",
    "asset_type",
    "source",
    "sharpe_ratio_5y",
    "beta_5y",
    "rsi_daily",
    "moving_avg_200",
]

POLICY_MAPPING = {
    "Div. Yield": "div_yield",
    "Div. Growth": "div_growth_1y",
    "Div. Growth 3Y": "div_growth_3y",
    "Div. Growth 5Y": "div_growth_5y",
    "Div. Growth 10Y": "div_growth_10y",
    "Years": "div_consecutive_years",
    "Payout Ratio": "payout_ratio",
    "Return YTD": "total_return_ytd",
    "Return 1Y": "total_return_1y",
    "PE Ratio": "pe_ratio",
}
POLICY_COLUMNS = [
    "ticker",
    "asset_type",
    "source",
    "div_yield",
    "div_growth_1y",
    "div_growth_3y",
    "div_growth_5y",
    "div_growth_10y",
    "div_consecutive_years",
    "payout_ratio",
    "total_return_ytd",
    "total_return_1y",
    "pe_ratio",
]

FULL_MAPPING = {**INFO_MAPPING, **FEES_MAPPING, **RISK_MAPPING, **POLICY_MAPPING}


def prepare_dataframe(df_source: pd.DataFrame, target_columns, extra_defaults=None) -> pd.DataFrame:
    if extra_defaults is None:
        extra_defaults = {}
    out = pd.DataFrame()
    for col in target_columns:
        if col in df_source.columns:
            out[col] = df_source[col]
        elif col in extra_defaults:
            out[col] = extra_defaults[col]
        else:
            out[col] = None
    return out


async def perform_login(page, email: str, password: str) -> bool:
    if not email or not password:
        logger.error("Missing SA_EMAIL or SA_PASSWORD in environment.")
        return False

    try:
        logger.info("Navigating to Stock Analysis login page...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(1)

        if await page.locator('a[href="/pro/account/"]').count() > 0:
            logger.info("Already logged in.")
            return True

        await page.wait_for_selector('input[type="email"]', state="visible", timeout=30000)
        await page.fill('input[type="email"]', email)
        await asyncio.sleep(0.3)
        await page.fill('input[name="password"]', password)
        await asyncio.sleep(0.3)
        await page.click('button:has-text("Log in")')

        try:
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            logger.info("Login successful.")
            return True
        except Exception:
            if await page.locator("text=Log out").count() > 0:
                return True
            logger.error("Login verification timed out.")
            return False
    except Exception as exc:
        logger.error("Login error: %s", exc)
        return False


async def switch_to_all_indicators(page) -> bool:
    logger.info("Switching to All Indicators tab...")
    try:
        tab_button = page.locator("button").filter(has_text="All Indicators").first
        if await tab_button.is_visible():
            await tab_button.click()
            await asyncio.sleep(3)
            await page.wait_for_load_state("networkidle")
            logger.info("Switched to All Indicators.")
            return True
    except Exception as exc:
        logger.error("Error switching tab: %s", exc)
        return False

    logger.error("All Indicators tab not found.")
    return False


async def download_data(page, temp_dir: Path):
    try:
        logger.info("Downloading screener CSV...")
        download_button = page.locator('button:has-text("Export"), button:has-text("Download")').first
        if not await download_button.is_visible():
            logger.error("Download/Export button not found.")
            return None, False

        await download_button.click()
        await asyncio.sleep(1)

        csv_option = page.locator('li:has-text("CSV"), button:has-text("CSV")').first
        async with page.expect_download(timeout=60000) as download_info:
            if await csv_option.is_visible():
                await csv_option.click()

        download = await download_info.value
        temp_path = temp_dir / f"raw_sa_all_{int(time.time())}.csv"
        await download.save_as(temp_path)
        logger.info("Downloaded: %s", temp_path.name)
        return temp_path, True
    except Exception as exc:
        logger.error("Download error: %s", exc)
        return None, False


def process_csv_and_split(csv_path: Path, output_dir: Path) -> bool:
    try:
        logger.info("Processing and splitting CSV...")
        df_raw = pd.read_csv(csv_path)
        df_raw.rename(columns=FULL_MAPPING, inplace=True)
        df_raw["asset_type"] = "ETF"
        df_raw["source"] = "Stock Analysis"

        df_info = prepare_dataframe(df_raw, INFO_COLUMNS)
        df_info.to_csv(output_dir / "sa_fund_info.csv", index=False, encoding="utf-8-sig")

        df_fees = prepare_dataframe(
            df_raw,
            FEES_COLUMNS,
            {
                "initial_charge": None,
                "exit_charge": None,
                "top_10_hold_pct": None,
                "holdings_turnover": None,
            },
        )
        df_fees.to_csv(output_dir / "sa_fund_fees.csv", index=False, encoding="utf-8-sig")

        df_risk = prepare_dataframe(df_raw, RISK_COLUMNS)
        df_risk.to_csv(output_dir / "sa_fund_risk.csv", index=False, encoding="utf-8-sig")

        df_policy = prepare_dataframe(df_raw, POLICY_COLUMNS)
        df_policy.to_csv(output_dir / "sa_fund_policy.csv", index=False, encoding="utf-8-sig")

        logger.info("Generated info/fees/risk/policy files in %s", output_dir)
        return True
    except Exception as exc:
        logger.error("CSV processing error: %s", exc)
        return False


async def run_sa_full_scraper(headless: bool = True) -> None:
    logger.info("Starting Stock Analysis static detail scraper")
    start_time = time.time()

    today_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = VAL_SA_STATIC / today_str
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = output_dir / "temp_downloads"
    temp_dir.mkdir(exist_ok=True)

    success = False
    email = os.getenv("SA_EMAIL")
    password = os.getenv("SA_PASSWORD")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless, args=["--start-maximized"])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=get_random_user_agent())
        page = await context.new_page()

        try:
            if not await perform_login(page, email=email, password=password):
                await browser.close()
                log_execution_summary(logger, start_time=start_time, total_items=0, status="Failed")
                return

            logger.info("Navigating to screener page...")
            await page.goto(SCREENER_URL, wait_until="domcontentloaded", timeout=60000)
            try:
                await page.wait_for_selector("table tbody tr", timeout=30000)
            except Exception:
                pass

            await mimic_reading(page, min_sec=2, max_sec=3)

            if not await switch_to_all_indicators(page):
                await browser.close()
                log_execution_summary(logger, start_time=start_time, total_items=0, status="Failed")
                return

            csv_path, downloaded = await download_data(page, temp_dir)
            await browser.close()
            if downloaded and csv_path:
                success = process_csv_and_split(csv_path, output_dir)
                try:
                    os.remove(csv_path)
                except Exception:
                    pass
        except Exception as exc:
            logger.error("Critical error: %s", exc)
            await browser.close()

    try:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
    except Exception:
        pass

    log_execution_summary(
        logger,
        start_time=start_time,
        total_items=1 if success else 0,
        status="Completed" if success else "Failed",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Analysis static detail downloader and splitter")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    args = parser.parse_args()
    asyncio.run(run_sa_full_scraper(headless=args.headless))


if __name__ == "__main__":
    main()
