import argparse
import asyncio
import configparser
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Set

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_SA_DIR, VAL_SA_HOLDINGS


logger = setup_logger("04_holdings_sa_master")

BASE_URL = "https://stockanalysis.com/etf/"
LOGIN_URL_DEFAULT = "https://stockanalysis.com/login"
MAX_CONCURRENT_TICKERS = 4


def get_config(filename: str = "config/database.ini", section: str = "stock_analysis"):
    parser = configparser.ConfigParser()
    config_path = Path(filename)
    if not config_path.exists():
        config_path = Path.cwd() / filename
    if not config_path.exists():
        return {}

    parser.read(config_path)
    if parser.has_section(section):
        return dict(parser.items(section))
    return {}


def resolve_input_csv(explicit_path: str = "") -> Path:
    if explicit_path:
        return Path(explicit_path)

    master_base = VAL_SA_DIR / "01_List_Master"
    if not master_base.exists():
        return master_base / datetime.now().strftime("%Y-%m-%d") / "sa_etf_master.csv"

    date_dirs = sorted([d for d in master_base.iterdir() if d.is_dir()])
    if not date_dirs:
        return master_base / datetime.now().strftime("%Y-%m-%d") / "sa_etf_master.csv"

    for date_dir in reversed(date_dirs):
        candidate = date_dir / "sa_etf_master.csv"
        if candidate.exists():
            return candidate

    return date_dirs[-1] / "sa_etf_master.csv"


def get_processed_tickers(target_dir: Path) -> Set[str]:
    if not target_dir.exists():
        return set()

    processed_tickers = set()
    for file_path in target_dir.glob("*_holdings.csv"):
        ticker = file_path.name.split("_holdings.csv")[0]
        if file_path.stat().st_size > 0:
            processed_tickers.add(ticker)
    return processed_tickers


async def login_to_sa(page, login_url: str, email: str, password: str) -> bool:
    logger.info("Attempting login...")
    try:
        await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)

        if "login" in page.url:
            email_selectors = ['input[type="email"]', 'input[name="email"]']
            password_selectors = ['input[type="password"]', 'input[name="password"]']

            email_ok = False
            for sel in email_selectors:
                locator = page.locator(sel).first
                if await locator.count() > 0:
                    await locator.fill(email)
                    email_ok = True
                    break
            if not email_ok:
                logger.error("Login failed: email input not found.")
                return False

            password_ok = False
            for sel in password_selectors:
                locator = page.locator(sel).first
                if await locator.count() > 0:
                    await locator.fill(password)
                    password_ok = True
                    break
            if not password_ok:
                logger.error("Login failed: password input not found.")
                return False

            submit = page.locator('button:has-text("Log in"), button:has-text("Login"), button[type="submit"]').first
            if await submit.count() > 0 and await submit.is_visible():
                await submit.click()
            else:
                await page.keyboard.press("Enter")
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)

            if "login" not in page.url:
                logger.info("Login successful.")
                return True
            logger.error("Login failed (still on login page).")
            return False

        logger.info("Session already authenticated.")
        return True
    except Exception as exc:
        logger.error("Critical login error: %s", exc)
        return False


async def download_holdings(page, ticker: str, target_dir: Path) -> bool:
    url = f"{BASE_URL}{ticker.lower()}/holdings/"
    save_path = target_dir / f"{ticker}_holdings.csv"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        download_btn = page.locator('button:has-text("Download")')

        if await download_btn.count() > 0 and await download_btn.first.is_visible():
            await download_btn.first.click()
            csv_option = page.locator(
                'button:has-text("Download to CSV"), div[role="menu"] button:has-text("Download to CSV")'
            )

            try:
                await csv_option.first.wait_for(state="visible", timeout=3000)
            except Exception:
                await download_btn.first.click()
                await asyncio.sleep(0.5)

            async with page.expect_download(timeout=15000) as download_info:
                await csv_option.first.click(force=True)

            download = await download_info.value
            await download.save_as(save_path)
            return save_path.exists() and save_path.stat().st_size > 0
    except Exception:
        return False

    return False


def generate_report(output_dir: Path, start_time: float, total: int, success: int, skipped: int) -> Path:
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = elapsed % 60

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"Report_Holdings_{timestamp}.txt"

    with report_path.open("w", encoding="utf-8") as file:
        file.write("============================================================\n")
        file.write("SCRAPING REPORT: ETF HOLDINGS (DATE FOLDER)\n")
        file.write("============================================================\n")
        file.write(f"Execution Date : {datetime.now().strftime('%d %B %Y, %H:%M:%S')}\n")
        file.write(f"Data Location  : {output_dir}\n")
        file.write("-" * 60 + "\n")
        file.write(f"Total Tickers         : {total:,}\n")
        file.write(f"Downloaded            : {success:,}\n")
        file.write(f"No Data / Skipped     : {skipped:,}\n")
        file.write(f"Time Taken            : {minutes}m {seconds:.2f}s\n")
        file.write("============================================================\n")

    return report_path


async def worker(ticker: str, context, today_dir: Path, all_tickers: List[str], counters):
    page = await context.new_page()
    try:
        async with counters["lock"]:
            counters["total_count"] += 1
            current_index = counters["total_count"]

        logger.info("[%s/%s] Holdings: %s", current_index, len(all_tickers), ticker)
        is_saved = await download_holdings(page, ticker, today_dir)

        async with counters["lock"]:
            if is_saved:
                counters["success_count"] += 1
            else:
                counters["skipped_count"] += 1
    except Exception:
        async with counters["lock"]:
            counters["skipped_count"] += 1
    finally:
        await page.close()


async def run_scraper(input_csv_path: str = "", sample: int = 0):
    start_time = time.time()
    today_dir = VAL_SA_HOLDINGS / datetime.now().strftime("%Y-%m-%d")
    today_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Target folder: %s", today_dir)

    input_csv = resolve_input_csv(input_csv_path)
    if not input_csv.exists():
        logger.error("Input master CSV not found: %s", input_csv)
        return

    try:
        df = pd.read_csv(input_csv)
        all_tickers = df["ticker"].astype(str).tolist()
        if sample > 0:
            all_tickers = all_tickers[:sample]
        processed_tickers = get_processed_tickers(today_dir)
        tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
    except Exception as exc:
        logger.error("Error reading input CSV: %s", exc)
        return

    logger.info("Loaded=%s | Already processed=%s | Remaining=%s", len(all_tickers), len(processed_tickers), len(tickers_to_process))
    if not tickers_to_process:
        logger.info("All tickers already processed.")
        return

    config = get_config()
    login_url = config.get("login_url", LOGIN_URL_DEFAULT)
    email = os.getenv("SA_EMAIL") or config.get("email", "")
    password = os.getenv("SA_PASSWORD") or config.get("password", "")
    if not email or not password:
        logger.error("Missing credentials. Set SA_EMAIL/SA_PASSWORD in environment or config/database.ini.")
        return

    counters = {
        "total_count": len(processed_tickers),
        "success_count": 0,
        "skipped_count": 0,
        "lock": asyncio.Lock(),
    }
    initial_processed = len(processed_tickers)

    async with async_playwright() as playwright:
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir="./tmp/sa_session",
            headless=True,
            accept_downloads=True,
            viewport={"width": 1280, "height": 800},
        )

        page = await context.new_page()
        if not await login_to_sa(page, login_url=login_url, email=email, password=password):
            await context.close()
            logger.error("CRITICAL: Initial login failed.")
            return
        await page.close()

        logger.info("Starting acquisition with %s workers", MAX_CONCURRENT_TICKERS)
        tasks = [worker(ticker, context, today_dir, all_tickers, counters) for ticker in tickers_to_process]

        for i in range(0, len(tasks), MAX_CONCURRENT_TICKERS):
            await asyncio.gather(*tasks[i : i + MAX_CONCURRENT_TICKERS])

        await context.close()

    final_success = initial_processed + counters["success_count"]
    final_skipped = counters["skipped_count"]
    report_path = generate_report(VAL_SA_HOLDINGS, start_time, len(all_tickers), final_success, final_skipped)
    logger.info("Finished. Report: %s", report_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Analysis holdings downloader")
    parser.add_argument("--input-csv", default="", help="Optional explicit master CSV path")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()
    asyncio.run(run_scraper(input_csv_path=args.input_csv, sample=args.sample))


if __name__ == "__main__":
    main()
