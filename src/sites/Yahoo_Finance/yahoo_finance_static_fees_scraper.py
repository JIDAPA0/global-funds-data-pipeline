import argparse
import asyncio
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_YF_DIR, VAL_YF_STATIC


logger = setup_logger("03_static_yf_fees")

COLS = [
    "ticker",
    "expense_ratio",
    "initial_charge",
    "exit_charge",
    "assets_aum",
    "top_10_hold_pct",
    "holdings_count",
    "holdings_turnover",
]


def _load_tickers_from_db() -> Optional[List[Dict[str, str]]]:
    try:
        from src.utils.db_connector import get_active_tickers
    except Exception:
        return None

    try:
        rows = get_active_tickers("Yahoo Finance")
        return [{"ticker": str(r.get("ticker", "")).strip()} for r in rows if str(r.get("ticker", "")).strip()]
    except Exception:
        return None


def _load_tickers_from_master() -> List[Dict[str, str]]:
    base = VAL_YF_DIR / "master_tickers"
    if not base.exists():
        return []
    date_dirs = sorted([d for d in base.iterdir() if d.is_dir()])
    if not date_dirs:
        return []
    master = date_dirs[-1] / "yf_ticker.csv"
    if not master.exists():
        return []
    try:
        df = pd.read_csv(master, encoding="utf-8-sig")
        return [{"ticker": str(t).strip()} for t in df["ticker"].dropna().tolist() if str(t).strip()]
    except Exception:
        return []


def get_ticker_universe() -> List[Dict[str, str]]:
    from_db = _load_tickers_from_db()
    if from_db is not None:
        return from_db
    return _load_tickers_from_master()


def resolve_output_path() -> Path:
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_dir = VAL_YF_STATIC / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / "yahoo_finance_fees.csv"


class YFFeesScraper:
    def __init__(self, sample: int = 0):
        self.output_file = resolve_output_path()
        self.tickers = get_ticker_universe()
        if sample > 0:
            self.tickers = self.tickers[:sample]

        if not self.output_file.exists():
            pd.DataFrame(columns=COLS).to_csv(self.output_file, index=False, encoding="utf-8-sig")

    async def scrape_data(self, page, ticker: str) -> Optional[Dict[str, str]]:
        data = {c: "" for c in COLS}
        data["ticker"] = ticker

        try:
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/profile", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

            rows = await page.locator("table tr").all()
            for row in rows:
                text = await row.inner_text()
                if "\t" not in text:
                    continue
                parts = text.split("\t")
                if len(parts) < 2:
                    continue
                label, value = parts[0].strip(), parts[1].strip()
                if value == "--":
                    continue

                if "Net Assets" in label:
                    data["assets_aum"] = value
                elif "Expense Ratio" in label:
                    data["expense_ratio"] = value
                elif "Front-End" in label or "Front End" in label:
                    data["initial_charge"] = value
                elif "Deferred" in label:
                    data["exit_charge"] = value
                elif "Turnover" in label:
                    data["holdings_turnover"] = value

            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/holdings", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

            try:
                header = await page.locator('section[data-testid="top-holdings"] h3').first.inner_text()
                if "(" in header and "%" in header:
                    data["top_10_hold_pct"] = header.split("(")[1].split("%")[0].strip() + "%"
            except Exception:
                pass

            holdings_rows = await page.locator("table tr").all()
            for row in holdings_rows:
                text = await row.inner_text()
                if "\t" not in text:
                    continue
                parts = text.split("\t")
                if len(parts) >= 2 and "Total Holdings" in parts[0]:
                    data["holdings_count"] = parts[1].strip()

            logger.info("%s extracted", ticker)
            return data
        except Exception as exc:
            logger.error("%s error: %s", ticker, str(exc))
            return None

    async def run(self) -> None:
        processed = set(pd.read_csv(self.output_file, usecols=["ticker"], encoding="utf-8-sig")["ticker"].astype(str)) if self.output_file.exists() else set()
        queue = [t for t in self.tickers if t["ticker"] not in processed]

        logger.info("Universe=%s | Processed=%s | Remaining=%s", len(self.tickers), len(processed), len(queue))
        if not queue:
            logger.info("No new tickers to process")
            return

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            for i, item in enumerate(queue, 1):
                result = await self.scrape_data(page, item["ticker"])
                if result:
                    pd.DataFrame([result])[COLS].to_csv(self.output_file, mode="a", header=False, index=False, encoding="utf-8-sig")

                if i % 10 == 0:
                    await asyncio.sleep(random.uniform(2, 4))

            await browser.close()

        logger.info("Output: %s", self.output_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="Yahoo Finance static fees scraper")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()
    asyncio.run(YFFeesScraper(sample=args.sample).run())


if __name__ == "__main__":
    main()
