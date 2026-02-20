import argparse
import asyncio
import math
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_YF_DIR, VAL_YF_HOLDINGS


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


logger = setup_logger("04_holdings_yf_master")
CONCURRENCY = 6
BATCH_SIZE = 40

BASE_OUTPUT_DIR = VAL_YF_HOLDINGS
DIR_HOLDINGS = BASE_OUTPUT_DIR / "Holdings"
DIR_SECTORS = BASE_OUTPUT_DIR / "Sectors"
DIR_ALLOCATION = BASE_OUTPUT_DIR / "Allocation"
MISSING_REPORT_FILE = BASE_OUTPUT_DIR / "yf_holdings_missing_report.csv"
PROCESSED_REPORT_FILE = BASE_OUTPUT_DIR / "yf_holdings_processed_report.csv"

for directory in [DIR_HOLDINGS, DIR_SECTORS, DIR_ALLOCATION]:
    directory.mkdir(parents=True, exist_ok=True)


async def _route_minimal_assets(route):
    if route.request.resource_type in {"image", "font", "media"}:
        await route.abort()
    else:
        await route.continue_()


def _load_tickers_from_db() -> Optional[List[Dict[str, str]]]:
    try:
        from src.utils.db_connector import get_active_tickers
    except Exception:
        return None

    try:
        rows = get_active_tickers("Yahoo Finance")
        output = []
        for row in rows:
            ticker = str(row.get("ticker", "")).strip()
            if not ticker:
                continue
            output.append(
                {
                    "ticker": ticker,
                    "asset_type": str(row.get("asset_type", "Fund") or "Fund"),
                }
            )
        return output
    except Exception:
        return None


def _load_tickers_from_master() -> List[Dict[str, str]]:
    master_base = VAL_YF_DIR / "master_tickers"
    if not master_base.exists():
        return []

    date_dirs = sorted([d for d in master_base.iterdir() if d.is_dir()])
    if not date_dirs:
        return []

    master_file = date_dirs[-1] / "yf_ticker.csv"
    if not master_file.exists():
        return []

    try:
        df = pd.read_csv(master_file, encoding="utf-8-sig")
        if "ticker" not in df.columns:
            return []
        if "ticker_type" not in df.columns:
            df["ticker_type"] = "Fund"

        output: List[Dict[str, str]] = []
        for _, row in df.iterrows():
            ticker = str(row.get("ticker", "")).strip()
            if not ticker:
                continue
            asset_type = str(row.get("ticker_type", "Fund") or "Fund")
            output.append({"ticker": ticker, "asset_type": asset_type})
        return output
    except Exception:
        return []


def get_ticker_universe() -> List[Dict[str, str]]:
    from_db = _load_tickers_from_db()
    if from_db is not None:
        return from_db
    return _load_tickers_from_master()


class YFHoldingsScraper:
    def __init__(
        self,
        sample: int = 0,
        concurrency: int = CONCURRENCY,
        batch_size: int = BATCH_SIZE,
        min_wait_sec: float = 0.5,
        max_wait_sec: float = 1.2,
        pause_every_batches: int = 8,
        pause_min_sec: float = 8.0,
        pause_max_sec: float = 15.0,
        per_ticker_timeout_sec: int = 90,
    ):
        self.start_time = time.time()
        self.concurrency = max(1, int(concurrency))
        self.batch_size = max(1, int(batch_size))
        self.min_wait_sec = max(0.1, float(min_wait_sec))
        self.max_wait_sec = max(self.min_wait_sec, float(max_wait_sec))
        self.pause_every_batches = max(0, int(pause_every_batches))
        self.pause_min_sec = max(0.0, float(pause_min_sec))
        self.pause_max_sec = max(self.pause_min_sec, float(pause_max_sec))
        self.per_ticker_timeout_sec = max(30, int(per_ticker_timeout_sec))

        logger.info("Fetching active Yahoo Finance tickers...")
        self.tickers = get_ticker_universe()
        if sample > 0:
            self.tickers = self.tickers[:sample]

        logger.info("Total tickers to process: %s", len(self.tickers))

        self.total_processed = 0
        self.total_success = 0

        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        ]

        if not MISSING_REPORT_FILE.exists():
            pd.DataFrame(columns=["ticker", "asset_type", "reason", "timestamp"]).to_csv(
                MISSING_REPORT_FILE,
                index=False,
                encoding="utf-8-sig",
            )
        if not PROCESSED_REPORT_FILE.exists():
            pd.DataFrame(columns=["ticker", "asset_type", "status", "updated_at"]).to_csv(
                PROCESSED_REPORT_FILE,
                index=False,
                encoding="utf-8-sig",
            )

    def _load_processed_keys(self) -> set[str]:
        processed: set[str] = set()
        try:
            if not PROCESSED_REPORT_FILE.exists() or PROCESSED_REPORT_FILE.stat().st_size == 0:
                pass
            else:
                df = pd.read_csv(PROCESSED_REPORT_FILE, encoding="utf-8-sig")
                # Terminal states for current-day resumability.
                terminal = {"SUCCESS", "NO_DATA", "INVALID_TICKER", "SKIPPED"}
                df = df[df["status"].astype(str).str.upper().isin(terminal)]
                processed |= {
                f"{str(r['ticker']).strip().upper()}|{str(r['asset_type']).strip().upper()}"
                for _, r in df.iterrows()
                if str(r.get("ticker", "")).strip()
                }
        except Exception:
            pass

        # Backward compatible resume: infer from existing output files.
        for directory, suffix in [
            (DIR_HOLDINGS, "_holdings.csv"),
            (DIR_SECTORS, "_sectors.csv"),
            (DIR_ALLOCATION, "_allocation.csv"),
        ]:
            if not directory.exists():
                continue
            for p in directory.glob(f"*{suffix}"):
                stem = p.name[: -len(suffix)]
                parts = stem.rsplit("_", 1)
                if len(parts) == 2:
                    ticker_part, asset_part = parts
                    ticker = ticker_part.replace("_", "/").upper()
                    asset = asset_part.upper()
                    processed.add(f"{ticker}|{asset}")
        return processed

    def _append_processed(self, rows: List[Dict[str, str]]) -> None:
        if not rows:
            return
        pd.DataFrame(rows).to_csv(PROCESSED_REPORT_FILE, mode="a", header=False, index=False, encoding="utf-8-sig")

    def get_random_ua(self) -> str:
        return random.choice(self.user_agents)

    async def log_missing(self, ticker: str, asset_type: str, reason: str) -> None:
        try:
            pd.DataFrame(
                [
                    {
                        "ticker": ticker,
                        "asset_type": asset_type,
                        "reason": reason,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                ]
            ).to_csv(MISSING_REPORT_FILE, mode="a", header=False, index=False, encoding="utf-8-sig")
        except Exception:
            pass

    async def dismiss_popups(self, page) -> None:
        try:
            await page.keyboard.press("Escape")
            selectors = [
                'button[name="reject"]',
                'button[name="agree"]',
                'button[value="agree"]',
                'button[aria-label="Close"]',
                "button.close",
                "div.ox-close",
                "#consent-page button.reject",
                'button:has-text("Maybe later")',
                'button:has-text("Not now")',
            ]
            for selector in selectors:
                if await page.locator(selector).count() > 0:
                    try:
                        await page.locator(selector).first.click(force=True, timeout=500)
                    except Exception:
                        pass
        except Exception:
            pass

    async def search_fallback(self, page, ticker: str) -> Optional[str]:
        try:
            search_box = page.locator("#ybar-sbq")
            if await search_box.count() > 0:
                await search_box.fill(ticker)
                await page.keyboard.press("Enter")
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass

                if "/quote/" in page.url and "lookup" not in page.url:
                    match = re.search(r"/quote/([^/?]+)", page.url)
                    if match:
                        return match.group(1)
                    return ticker
        except Exception:
            pass
        return None

    async def process_ticker(self, context, item: Dict[str, str]) -> str:
        ticker = item["ticker"]
        raw_asset_type = item.get("asset_type", "Fund") or "Fund"
        asset_type = str(raw_asset_type).upper().replace("/", "").replace(" ", "")

        safe_ticker = ticker.replace("/", "_").replace(":", "_")

        file_holdings = DIR_HOLDINGS / f"{safe_ticker}_{asset_type}_holdings.csv"
        file_sectors = DIR_SECTORS / f"{safe_ticker}_{asset_type}_sectors.csv"
        file_allocation = DIR_ALLOCATION / f"{safe_ticker}_{asset_type}_allocation.csv"

        if file_holdings.exists() or file_sectors.exists() or file_allocation.exists():
            return "SKIPPED"

        page = await context.new_page()
        target_ticker = ticker
        url = f"https://finance.yahoo.com/quote/{target_ticker}/holdings/"

        data_found = False

        try:
            await asyncio.sleep(random.uniform(self.min_wait_sec, self.max_wait_sec))
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")

            if "lookup" in page.url:
                new_ticker = await self.search_fallback(page, ticker)
                if new_ticker:
                    target_ticker = new_ticker
                    await page.goto(f"https://finance.yahoo.com/quote/{target_ticker}/holdings/", timeout=60000)
                else:
                    await page.close()
                    await self.log_missing(ticker, asset_type, "INVALID_TICKER (Search Failed)")
                    return "INVALID_TICKER"

            if "lookup" in page.url:
                await page.close()
                await self.log_missing(ticker, asset_type, "INVALID_TICKER (Still Lookup)")
                return "INVALID_TICKER"

            await asyncio.sleep(random.uniform(self.min_wait_sec, self.max_wait_sec))
            await self.dismiss_popups(page)

            holdings_data = []
            section = page.locator('section[data-testid="top-holdings"]')
            if await section.count() > 0:
                rows = section.locator('div[class*="content"]')
                count = await rows.count()
                for i in range(count):
                    text = await rows.nth(i).inner_text()
                    parts = text.split("\n")
                    if len(parts) >= 3:
                        holdings_data.append({"symbol": parts[1], "name": parts[0], "value": parts[-1]})
                    elif len(parts) == 2:
                        holdings_data.append({"symbol": "-", "name": parts[0], "value": parts[1]})

            if not holdings_data:
                tables = page.locator("table")
                table_count = await tables.count()
                for i in range(table_count):
                    rows = tables.nth(i).locator("tbody tr")
                    if await rows.count() == 0:
                        continue
                    first_row = await rows.nth(0).inner_text()
                    if "Symbol" in first_row or "% Assets" in first_row:
                        for r in range(await rows.count()):
                            cols = rows.nth(r).locator("td")
                            if await cols.count() >= 3:
                                symbol = await cols.nth(0).inner_text()
                                name = await cols.nth(1).inner_text()
                                value = await cols.nth(2).inner_text()
                                holdings_data.append({"symbol": symbol, "name": name, "value": value})
                        if holdings_data:
                            break

            if holdings_data:
                df_holdings = pd.DataFrame(holdings_data)
                df_holdings["ticker"] = ticker
                df_holdings["yahoo_ticker"] = target_ticker
                df_holdings["asset_type"] = asset_type
                df_holdings["updated_at"] = datetime.now().strftime("%Y-%m-%d")
                df_holdings.to_csv(file_holdings, index=False, encoding="utf-8-sig")
                data_found = True

            sector_data = []
            section_sector = page.locator('section[data-testid*="sector-weightings"]')
            if await section_sector.count() > 0:
                rows = section_sector.locator('div[class*="content"]')
                count = await rows.count()
                for i in range(count):
                    text = await rows.nth(i).inner_text()
                    parts = text.split("\n")
                    if len(parts) >= 2:
                        sector_data.append({"sector": parts[0], "value": parts[-1]})

            if sector_data:
                df_sector = pd.DataFrame(sector_data)
                df_sector["ticker"] = ticker
                df_sector["asset_type"] = asset_type
                df_sector["updated_at"] = datetime.now().strftime("%Y-%m-%d")
                df_sector.to_csv(file_sectors, index=False, encoding="utf-8-sig")
                data_found = True

            allocation_data = []
            tables = page.locator("table")
            table_count = await tables.count()
            for i in range(table_count):
                rows = tables.nth(i).locator("tbody tr")
                if await rows.count() == 0:
                    continue
                first_cell = await rows.nth(0).locator("td").first.inner_text()
                if any(k in first_cell for k in ["Cash", "Stocks", "Bonds"]):
                    for r in range(await rows.count()):
                        cols = rows.nth(r).locator("td")
                        if await cols.count() >= 2:
                            category = await cols.nth(0).inner_text()
                            value = await cols.nth(1).inner_text()
                            allocation_data.append({"category": category, "value": value})
                    if allocation_data:
                        break

            if allocation_data:
                df_allocation = pd.DataFrame(allocation_data)
                df_allocation["ticker"] = ticker
                df_allocation["asset_type"] = asset_type
                df_allocation["updated_at"] = datetime.now().strftime("%Y-%m-%d")
                df_allocation.to_csv(file_allocation, index=False, encoding="utf-8-sig")
                data_found = True

            if not data_found:
                await self.log_missing(ticker, asset_type, "NO_HOLDINGS_DATA (Page loaded but empty)")

        except Exception as exc:
            await self.log_missing(ticker, asset_type, f"ERROR: {str(exc)[:50]}")
        finally:
            await page.close()

        return "SUCCESS" if data_found else "NO_DATA"

    async def run(self) -> None:
        if not self.tickers:
            return

        logger.info("Starting Yahoo holdings scraper")
        processed_keys = self._load_processed_keys()
        pending: List[Dict[str, str]] = []
        for item in self.tickers:
            ticker = str(item.get("ticker", "")).strip().upper()
            asset_type = str(item.get("asset_type", "Fund") or "Fund").strip().upper().replace("/", "").replace(" ", "")
            if not ticker:
                continue
            if f"{ticker}|{asset_type}" in processed_keys:
                continue
            pending.append(item)
        logger.info("Resume checkpoint: processed=%s | remaining=%s", len(processed_keys), len(pending))
        self.tickers = pending

        total = len(self.tickers)
        if total == 0:
            logger.info("All tickers already processed (checkpoint).")
            return
        batches = math.ceil(total / self.batch_size)

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(viewport={"width": 1280, "height": 800}, user_agent=self.get_random_ua())
            await context.route("**/*", _route_minimal_assets)

            for i in range(batches):
                batch_start = time.time()
                batch = self.tickers[i * self.batch_size : (i + 1) * self.batch_size]

                sem = asyncio.Semaphore(self.concurrency)

                async def run_one(item):
                    async with sem:
                        try:
                            return await asyncio.wait_for(
                                self.process_ticker(context, item),
                                timeout=self.per_ticker_timeout_sec,
                            )
                        except asyncio.TimeoutError:
                            ticker = str(item.get("ticker", "")).strip()
                            asset_type = str(item.get("asset_type", "Fund") or "Fund")
                            await self.log_missing(ticker, asset_type, f"TIMEOUT>{self.per_ticker_timeout_sec}s")
                            return "TIMEOUT"

                results = await asyncio.gather(*[run_one(item) for item in batch])
                processed_rows = []
                for item, status in zip(batch, results):
                    processed_rows.append(
                        {
                            "ticker": str(item.get("ticker", "")).strip(),
                            "asset_type": str(item.get("asset_type", "Fund") or "Fund").strip().upper().replace("/", "").replace(" ", ""),
                            "status": status,
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
                self._append_processed(processed_rows)

                success_count = results.count("SUCCESS")
                skipped_count = results.count("SKIPPED")
                self.total_success += success_count
                self.total_processed += len(batch)

                duration = time.time() - batch_start
                logger.info(
                    "Batch %s/%s | Saved=%s | Skips=%s | Progress=%s/%s | Time=%.2fs | Concurrency=%s",
                    i + 1,
                    batches,
                    success_count,
                    skipped_count,
                    self.total_processed,
                    total,
                    duration,
                    self.concurrency,
                )

                # Periodically recycle context and cool down to reduce block risk.
                if (i + 1) % 10 == 0:
                    await context.close()
                    context = await browser.new_context(viewport={"width": 1280, "height": 800}, user_agent=self.get_random_ua())
                    await context.route("**/*", _route_minimal_assets)
                if self.pause_every_batches > 0 and (i + 1) % self.pause_every_batches == 0:
                    await asyncio.sleep(random.uniform(self.pause_min_sec, self.pause_max_sec))

            await browser.close()

        logger.info("Finished. Total saved tickers: %s", self.total_success)
        logger.info("Missing report: %s", MISSING_REPORT_FILE)


def main() -> None:
    parser = argparse.ArgumentParser(description="Yahoo Finance holdings scraper")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--min-wait-sec", type=float, default=0.5)
    parser.add_argument("--max-wait-sec", type=float, default=1.2)
    parser.add_argument("--pause-every-batches", type=int, default=8)
    parser.add_argument("--pause-min-sec", type=float, default=8.0)
    parser.add_argument("--pause-max-sec", type=float, default=15.0)
    parser.add_argument("--per-ticker-timeout-sec", type=int, default=90)
    args = parser.parse_args()

    asyncio.run(
        YFHoldingsScraper(
            sample=args.sample,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
            min_wait_sec=args.min_wait_sec,
            max_wait_sec=args.max_wait_sec,
            pause_every_batches=args.pause_every_batches,
            pause_min_sec=args.pause_min_sec,
            pause_max_sec=args.pause_max_sec,
            per_ticker_timeout_sec=args.per_ticker_timeout_sec,
        ).run()
    )


if __name__ == "__main__":
    main()
