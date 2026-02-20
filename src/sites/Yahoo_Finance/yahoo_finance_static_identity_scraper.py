import argparse
import asyncio
import csv
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_YF_DIR, VAL_YF_STATIC


logger = setup_logger("03_static_yf_identity")

CSV_COLUMNS = [
    "ticker",
    "name",
    "exchange",
    "issuer",
    "category",
    "inception_date",
    "source",
    "updated_at",
]


def _load_tickers_from_db() -> Optional[List[Dict[str, str]]]:
    try:
        from src.utils.db_connector import get_active_tickers
    except Exception:
        return None

    try:
        rows = get_active_tickers("Yahoo Finance")
        return [
            {
                "ticker": str(row.get("ticker", "")).strip(),
                "name": str(row.get("name", "")).strip() or "N/A",
            }
            for row in rows
            if str(row.get("ticker", "")).strip()
        ]
    except Exception:
        return None


def _load_tickers_from_master() -> List[Dict[str, str]]:
    master_base = VAL_YF_DIR / "master_tickers"
    if not master_base.exists():
        return []

    date_dirs = sorted([d for d in master_base.iterdir() if d.is_dir()])
    if not date_dirs:
        return []

    master_csv = date_dirs[-1] / "yf_ticker.csv"
    if not master_csv.exists():
        return []

    try:
        df = pd.read_csv(master_csv, encoding="utf-8-sig")
        if "ticker" not in df.columns:
            return []
        if "name" not in df.columns:
            df["name"] = "N/A"

        return [
            {"ticker": str(r["ticker"]).strip(), "name": str(r["name"]).strip() or "N/A"}
            for _, r in df.iterrows()
            if str(r.get("ticker", "")).strip()
        ]
    except Exception:
        return []


def get_ticker_universe() -> List[Dict[str, str]]:
    db_rows = _load_tickers_from_db()
    if db_rows is not None:
        return db_rows
    return _load_tickers_from_master()


def resolve_output_path() -> Path:
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_dir = VAL_YF_STATIC / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / "yahoo_finance_identity.csv"


def get_processed_tickers(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path, usecols=["ticker"], encoding="utf-8-sig")
        return set(df["ticker"].astype(str).str.strip().tolist())
    except Exception:
        return set()


class YahooFinanceIdentityScraper:
    def __init__(self, sample: int = 0):
        self.output_file = resolve_output_path()
        self.tickers_data = get_ticker_universe()
        if sample > 0:
            self.tickers_data = self.tickers_data[:sample]

        if not self.output_file.exists():
            with self.output_file.open("w", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()

    async def scrape_ticker(self, context, ticker_info: Dict[str, str]) -> Optional[Dict[str, str]]:
        ticker = ticker_info["ticker"]
        db_name = ticker_info.get("name", "N/A")
        page = await context.new_page()
        url = f"https://finance.yahoo.com/quote/{ticker}/profile/"

        data = {col: "" for col in CSV_COLUMNS}
        data.update(
            {
                "ticker": ticker,
                "name": db_name,
                "source": "Yahoo Finance",
                "updated_at": datetime.now().strftime("%Y-%m-%d"),
            }
        )

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3.5)

            # Exchange often appears in header area on profile pages.
            exchange_loc = page.locator('span[class*="exchange"]')
            if await exchange_loc.count() > 0:
                raw_exchange = await exchange_loc.first.inner_text()
                if raw_exchange:
                    data["exchange"] = raw_exchange.split(" - ")[0].strip()

            rows = await page.locator("table tr").all()
            for row in rows:
                text = await row.inner_text()
                if "\t" not in text:
                    continue
                parts = text.split("\t")
                if len(parts) < 2:
                    continue

                label = parts[0].strip()
                value = parts[1].strip()
                if value == "--":
                    continue

                if label == "Category":
                    data["category"] = value
                elif "Fund Family" in label or "Issuer" in label:
                    data["issuer"] = value
                elif "Inception Date" in label:
                    data["inception_date"] = value

            logger.info("%s extracted", ticker)
            return data
        except Exception as exc:
            logger.error("%s error: %s", ticker, str(exc))
            return None
        finally:
            await page.close()

    async def run(self) -> None:
        processed = get_processed_tickers(self.output_file)
        queue = [row for row in self.tickers_data if row["ticker"] not in processed]

        logger.info("Ticker universe=%s | Already processed=%s | Remaining=%s", len(self.tickers_data), len(processed), len(queue))
        if not queue:
            logger.info("All tasks completed for today")
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            )

            for i, item in enumerate(queue, 1):
                result = await self.scrape_ticker(context, item)
                if result:
                    pd.DataFrame([result])[CSV_COLUMNS].to_csv(
                        self.output_file,
                        mode="a",
                        header=False,
                        index=False,
                        encoding="utf-8-sig",
                    )

                if i % 10 == 0:
                    await asyncio.sleep(random.uniform(2, 5))

            await browser.close()

        logger.info("Output: %s", self.output_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="Yahoo Finance static identity scraper")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()

    scraper = YahooFinanceIdentityScraper(sample=args.sample)
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()
