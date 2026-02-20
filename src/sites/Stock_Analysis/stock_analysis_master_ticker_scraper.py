import argparse
import asyncio
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.browser_utils import get_random_user_agent, mimic_reading
from src.utils.logger import log_execution_summary, setup_logger
from src.utils.path_manager import VAL_SA_MASTER

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


logger = setup_logger("01_master_sa_scraper")

LOGIN_URL = "https://stockanalysis.com/login"
SCREENER_URL = "https://stockanalysis.com/etf/screener/"

MASTER_COLUMNS = ["ticker", "name", "ticker_type", "source", "date_scraper", "url"]


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


async def download_screener_csv(page, temp_dir: Path):
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
        temp_path = temp_dir / f"raw_sa_master_{int(time.time())}.csv"
        await download.save_as(temp_path)
        logger.info("Downloaded: %s", temp_path.name)
        return temp_path, True
    except Exception as exc:
        logger.error("Download error: %s", exc)
        return None, False


def convert_to_master(raw_csv: Path, output_csv: Path, sample: int = 0) -> int:
    df = pd.read_csv(raw_csv)
    if "Symbol" not in df.columns:
        raise ValueError("Raw screener CSV missing required column: Symbol")

    name_col = "Fund Name" if "Fund Name" in df.columns else None
    if not name_col:
        for candidate in ["Name", "Fund", "Fund name"]:
            if candidate in df.columns:
                name_col = candidate
                break

    date_scraper = datetime.now().strftime("%Y-%m-%d")
    out = pd.DataFrame()
    out["ticker"] = df["Symbol"].astype(str).str.strip().str.upper()
    out["name"] = df[name_col].astype(str).str.strip() if name_col else ""
    out["ticker_type"] = "ETF"
    out["source"] = "Stock Analysis"
    out["date_scraper"] = date_scraper
    out["url"] = out["ticker"].apply(lambda t: f"https://stockanalysis.com/etf/{str(t).lower()}/")

    out = out[out["ticker"] != ""].drop_duplicates(subset=["ticker"], keep="first")
    if sample > 0:
        out = out.head(sample)

    out = out[MASTER_COLUMNS]
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return len(out)


async def run_scraper(headless: bool = True, sample: int = 0) -> None:
    logger.info("Starting Stock Analysis master list scraper")
    start_time = time.time()

    today = datetime.now().strftime("%Y-%m-%d")
    out_dir = VAL_SA_MASTER / today
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "sa_etf_master.csv"

    temp_dir = out_dir / "temp_downloads"
    temp_dir.mkdir(exist_ok=True)

    email = os.getenv("SA_EMAIL")
    password = os.getenv("SA_PASSWORD")

    success = False
    total_items = 0

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

            raw_csv, downloaded = await download_screener_csv(page, temp_dir)
            await browser.close()
            if downloaded and raw_csv:
                total_items = convert_to_master(raw_csv=raw_csv, output_csv=out_csv, sample=sample)
                success = True
                logger.info("Saved %s rows -> %s", f"{total_items:,}", out_csv)
        except Exception as exc:
            logger.error("Critical error: %s", exc)
            await browser.close()
        finally:
            try:
                if temp_dir.exists():
                    for p in temp_dir.iterdir():
                        p.unlink(missing_ok=True)
                    temp_dir.rmdir()
            except Exception:
                pass

    log_execution_summary(
        logger,
        start_time=start_time,
        total_items=total_items,
        status="Completed" if success else "Failed",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Analysis master list scraper")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()
    asyncio.run(run_scraper(headless=args.headless, sample=args.sample))


if __name__ == "__main__":
    main()
