import argparse
import asyncio
import csv
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.async_api import async_playwright

from src.utils.browser_utils import get_random_user_agent, mimic_reading
from src.utils.logger import setup_logger
from src.utils.path_manager import DATA_PERFORMANCE_DIR

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


logger = setup_logger("02_perf_sa_daily_nav")

LOGIN_URL = "https://stockanalysis.com/login"
SCREENER_URL = "https://stockanalysis.com/etf/screener/"


def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _pick_price_column(headers: List[str]) -> Optional[str]:
    candidates = ["Price", "Last", "Close", "Market Price", "MarketPrice"]
    lower_map = {h.lower(): h for h in headers}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    # fallback: any column containing "price"
    for h in headers:
        if "price" in h.lower():
            return h
    return None


def _to_float(text: str) -> Optional[float]:
    if text is None:
        return None
    cleaned = str(text).strip()
    if not cleaned or cleaned in {"--", "N/A", "NA"}:
        return None
    cleaned = cleaned.replace("$", "").replace(",", "")
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)
    try:
        return float(cleaned)
    except ValueError:
        return None


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

    logger.warning("All Indicators tab not found; continue with current screener view.")
    return True


async def download_screener_csv(page, temp_dir: Path) -> Tuple[Optional[Path], bool]:
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
        temp_path = temp_dir / f"raw_sa_nav_{int(time.time())}.csv"
        await download.save_as(temp_path)
        logger.info("Downloaded: %s", temp_path.name)
        return temp_path, True
    except Exception as exc:
        logger.error("Download error: %s", exc)
        return None, False


def transform_nav_csv(raw_csv: Path, output_csv: Path, error_csv: Path, sample: int = 0) -> Tuple[int, int]:
    with raw_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        price_col = _pick_price_column(headers)
        if not price_col:
            raise ValueError(f"Price column not found in screener CSV headers: {headers}")

        rows = list(reader)
        if sample > 0:
            rows = rows[:sample]

    out_rows: List[Dict[str, str]] = []
    err_rows: List[Dict[str, str]] = []
    today = today_yyyymmdd()

    for row in rows:
        ticker = (row.get("Symbol") or "").strip().upper()
        if not ticker:
            continue
        price_val = _to_float(row.get(price_col, ""))
        if price_val is None:
            err_rows.append(
                {
                    "ticker": ticker,
                    "reason": f"missing_or_invalid_price:{price_col}",
                    "scrape_date": today,
                }
            )
            continue

        out_rows.append(
            {
                "ticker": ticker,
                "asset_type": "ETF",
                "source": "Stock Analysis",
                "nav_price": f"{price_val:.8f}",
                "currency": "USD",
                "as_of_date": today,
                "scrape_date": today,
            }
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ticker", "asset_type", "source", "nav_price", "currency", "as_of_date", "scrape_date"],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    with error_csv.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "reason", "scrape_date"])
        writer.writeheader()
        writer.writerows(err_rows)

    return len(out_rows), len(err_rows)


async def run_scraper(headless: bool = True, sample: int = 0) -> None:
    out_dir = DATA_PERFORMANCE_DIR / "stock_analysis" / today_yyyymmdd()
    out_dir.mkdir(parents=True, exist_ok=True)
    output_csv = out_dir / "sa_nav_etf.csv"
    error_csv = out_dir / "sa_nav_errors_etf.csv"
    temp_dir = out_dir / "temp_downloads"
    temp_dir.mkdir(exist_ok=True)

    email = os.getenv("SA_EMAIL")
    password = os.getenv("SA_PASSWORD")

    success_rows = 0
    error_rows = 0

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless, args=["--start-maximized"])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=get_random_user_agent())
        page = await context.new_page()

        try:
            if not await perform_login(page, email=email, password=password):
                await browser.close()
                return

            logger.info("Navigating to screener page...")
            await page.goto(SCREENER_URL, wait_until="domcontentloaded", timeout=60000)
            await mimic_reading(page, min_sec=2, max_sec=3)

            if not await switch_to_all_indicators(page):
                await browser.close()
                return

            raw_csv, downloaded = await download_screener_csv(page, temp_dir)
            await browser.close()
            if downloaded and raw_csv:
                success_rows, error_rows = transform_nav_csv(raw_csv, output_csv, error_csv, sample=sample)
        finally:
            try:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
            except Exception:
                pass

    logger.info("Completed SA daily NAV: success=%s error=%s", success_rows, error_rows)
    logger.info("Output: %s", output_csv)
    logger.info("Errors: %s", error_csv)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Analysis daily NAV scraper")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()
    asyncio.run(run_scraper(headless=args.headless, sample=args.sample))


if __name__ == "__main__":
    main()
