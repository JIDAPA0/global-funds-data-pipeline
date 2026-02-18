import asyncio
import csv
import logging
import random
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.utils.browser_utils import get_context_options, get_launch_args
from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_YF_DIR


logger = setup_logger("01_master_yf_scraper", log_level=logging.INFO)


@dataclass(frozen=True)
class YahooFinanceMasterConfig:
    long_timeout_ms: int = 30000
    items_per_page: int = 100
    max_pages_to_check: int = 100
    concurrent_limit: int = 4
    output_dir: Path = VAL_YF_DIR / "master_tickers"


CSV_HEADERS = ["ticker", "name", "ticker_type", "source", "date_scraper", "url"]

ETF_MARKETS_URLS = {
    "Most_Active": "https://finance.yahoo.com/markets/etfs/most-active/",
    "Top_Gainers": "https://finance.yahoo.com/markets/etfs/gainers/",
    "Top_Losers": "https://finance.yahoo.com/markets/etfs/losers/",
    "Top_Performing": "https://finance.yahoo.com/markets/etfs/top-performing/",
    "Trending": "https://finance.yahoo.com/markets/etfs/trending/",
    "Best_Historical_Performance": "https://finance.yahoo.com/markets/etfs/best-historical-performance/",
}

MUTUAL_FUND_MARKETS_URLS = {
    "Top_Mutual_Funds": "https://finance.yahoo.com/markets/mutualfunds/most-active/",
    "Top_Gainers": "https://finance.yahoo.com/markets/mutualfunds/gainers/",
    "Top_Losers": "https://finance.yahoo.com/markets/mutualfunds/losers/",
    "Top_Performing": "https://finance.yahoo.com/markets/mutualfunds/top-performing/",
    "Trending": "https://finance.yahoo.com/markets/mutualfunds/trending/",
    "Best_Historical_Performance": "https://finance.yahoo.com/markets/mutualfunds/best-historical-performance/",
    "High_Yield": "https://finance.yahoo.com/markets/mutualfunds/high-yield/",
}


def get_random_user_agent() -> str:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    return random.choice(user_agents)


async def human_sleep(min_sec: float = 0.5, max_sec: float = 1.5) -> None:
    await asyncio.sleep(random.uniform(min_sec, max_sec))


async def dismiss_popups(page) -> None:
    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
    except Exception:
        pass

    selectors = [
        'button[name="agree"]',
        'button[value="agree"]',
        'button[aria-label="Close"]',
        "button.close",
        "div.ox-close",
        'button:has-text("Maybe later")',
        'button:has-text("No thanks")',
    ]

    for selector in selectors:
        try:
            if await page.locator(selector).count() > 0:
                await page.locator(selector).first.click(force=True)
                await asyncio.sleep(0.5)
        except Exception:
            continue


async def get_website_total_count(page) -> int:
    try:
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")
        matches = soup.find_all(string=re.compile(r"of\s+[\d,]+\s+results"))
        for text in matches:
            numbers = re.findall(r"of\s+([\d,]+)\s+results", text)
            if numbers:
                return int(numbers[0].replace(",", ""))
        return 0
    except Exception:
        return 0


def extract_full_table_data(soup: BeautifulSoup, ticker_type_label: str) -> List[Dict]:
    extracted: List[Dict] = []

    rows = soup.select("table tbody tr")
    if not rows:
        rows = soup.select('div[data-testid="list-item"]')
    if not rows:
        rows = soup.select("tr.simpTblRow")

    current_date = datetime.now().strftime("%Y-%m-%d")

    for row in rows:
        ticker_text = ""
        name_text = ""
        url = ""

        link = row.find("a", href=lambda x: x and "/quote/" in x)
        if link:
            href = link.get("href", "")
            url = f"https://finance.yahoo.com{href}" if href.startswith("/") else href

            candidate_ticker = link.get_text(strip=True).split(" ")[0]
            if not candidate_ticker and "/quote/" in href:
                parts = href.split("/quote/")
                if len(parts) > 1:
                    candidate_ticker = parts[1].split("?")[0].split("/")[0]

            ticker_text = candidate_ticker

            if link.get("title"):
                name_text = link.get("title").strip()
            elif link.get("aria-label"):
                name_text = link.get("aria-label").strip()
            else:
                name_span = row.find("span", title=True)
                if name_span:
                    name_text = name_span.get("title").strip()
                else:
                    columns = row.find_all(["td", "div"], recursive=False)
                    if len(columns) > 1:
                        name_text = columns[1].get_text(strip=True)

        if ticker_text and not ticker_text.isdigit() and len(ticker_text) < 15 and re.match(r"^[A-Z0-9.\-]+$", ticker_text, re.IGNORECASE):
            if not name_text:
                name_text = "N/A"

            extracted.append(
                {
                    "ticker": ticker_text,
                    "name": name_text,
                    "ticker_type": ticker_type_label,
                    "source": "Yahoo Finance",
                    "date_scraper": current_date,
                    "url": url,
                }
            )

    return extracted


async def scrape_single_category(
    semaphore: asyncio.Semaphore,
    browser,
    asset_key: str,
    category_name: str,
    url_template: str,
    cfg: YahooFinanceMasterConfig,
) -> Tuple[str, str, List[Dict], int]:
    async with semaphore:
        await human_sleep(0.5, 1.5)

        items: List[Dict] = []
        seen_tickers = set()
        website_total = 0

        context_options = get_context_options()
        context_options["user_agent"] = get_random_user_agent()
        context_options["viewport"] = {"width": 1280, "height": 800}

        context = await browser.new_context(**context_options)
        await context.route(
            "**/*",
            lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_(),
        )

        page = await context.new_page()
        start_index = 0
        max_limit = cfg.max_pages_to_check * cfg.items_per_page

        try:
            while start_index < max_limit:
                separator = "&" if "?" in url_template else "?"
                url = f"{url_template}{separator}count={cfg.items_per_page}&start={start_index}"

                retry = 0
                success = False
                new_items: List[Dict] = []

                while retry < 2 and not success:
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=cfg.long_timeout_ms)
                        if start_index == 0:
                            await dismiss_popups(page)

                        try:
                            await page.wait_for_selector('table tbody tr, div[data-testid="list-item"]', timeout=5000)
                        except Exception:
                            pass

                        if start_index == 0:
                            website_total = await get_website_total_count(page)
                            if website_total > 0:
                                logger.info("[%s] web total: %s", category_name, f"{website_total:,}")

                        content = await page.content()
                        new_items = extract_full_table_data(BeautifulSoup(content, "lxml"), asset_key)
                        success = True
                    except Exception:
                        retry += 1
                        await asyncio.sleep(1)

                if not new_items:
                    break

                added = 0
                for item in new_items:
                    ticker = item["ticker"]
                    if ticker not in seen_tickers:
                        seen_tickers.add(ticker)
                        items.append(item)
                        added += 1

                if added == 0:
                    break

                start_index += cfg.items_per_page
                if len(new_items) < 10:
                    break

                await human_sleep(0.5, 1.0)
        except Exception as exc:
            logger.error("error in [%s]: %s", category_name, exc)
        finally:
            await context.close()

        return asset_key, category_name, items, website_total


async def run(cfg: YahooFinanceMasterConfig) -> None:
    logger.info("Starting Yahoo Finance master ticker scrape")

    try:
        from tqdm.asyncio import tqdm
    except ImportError:
        class tqdm:  # type: ignore
            @staticmethod
            def as_completed(tasks, **kwargs):
                return asyncio.as_completed(tasks)

    semaphore = asyncio.Semaphore(cfg.concurrent_limit)

    async with async_playwright() as playwright:
        launch_options = get_launch_args(headless=True)
        browser = await playwright.chromium.launch(**launch_options)

        tasks = []
        for category, url in ETF_MARKETS_URLS.items():
            tasks.append(scrape_single_category(semaphore, browser, "ETF", category, url, cfg))
        for category, url in MUTUAL_FUND_MARKETS_URLS.items():
            tasks.append(scrape_single_category(semaphore, browser, "Fund", category, url, cfg))

        all_raw_items: List[Dict] = []
        audit_report = []

        for future in tqdm.as_completed(tasks, desc="Scraping", total=len(tasks)):
            asset_key, category_name, items, web_total = await future
            all_raw_items.extend(items)

            status = "OK"
            if web_total > 0 and len(items) < (web_total * 0.8) and len(items) < 2000:
                status = "Partial"

            audit_report.append(
                {
                    "Cat": f"{category_name} ({asset_key})",
                    "Web": web_total,
                    "Got": len(items),
                    "Status": status,
                }
            )

        await browser.close()

    today_str = datetime.now().strftime("%Y-%m-%d")
    unique_dict = {item["ticker"]: item for item in all_raw_items}
    unique_list = sorted(unique_dict.values(), key=lambda x: x["ticker"])

    save_path = cfg.output_dir / today_str / "yf_ticker.csv"
    save_path.parent.mkdir(parents=True, exist_ok=True)

    with save_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(unique_list)

    logger.info("Saved %s tickers -> %s", f"{len(unique_list):,}", save_path)

    print("\n" + "=" * 60)
    print(f"{'Category':<35} | {'Web':>8} | {'Got':>8} | {'Status'}")
    print("-" * 60)
    for row in sorted(audit_report, key=lambda x: x["Cat"]):
        print(f"{row['Cat']:<35} | {row['Web']:>8,d} | {row['Got']:>8,d} | {row['Status']}")
    print("=" * 60)


def main() -> None:
    asyncio.run(run(YahooFinanceMasterConfig()))


if __name__ == "__main__":
    main()
