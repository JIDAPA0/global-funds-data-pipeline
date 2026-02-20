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


logger = setup_logger("03_static_yf_policy")


async def _route_minimal_assets(route):
    if route.request.resource_type in {"image", "font", "media"}:
        await route.abort()
    else:
        await route.continue_()

COLS = ["ticker", "div_yield", "pe_ratio", "total_return_ytd", "total_return_1y", "updated_at"]


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
    return out_dir / "yahoo_finance_policy.csv"


class YFPolicyScraper:
    def __init__(self, sample: int = 0):
        self.output_file = resolve_output_path()
        self.tickers = get_ticker_universe()
        if sample > 0:
            self.tickers = self.tickers[:sample]

        if not self.output_file.exists():
            pd.DataFrame(columns=COLS).to_csv(self.output_file, index=False, encoding="utf-8-sig")

    async def scrape_policy(self, page, ticker: str) -> Optional[Dict[str, str]]:
        data = {c: "" for c in COLS}
        data.update({"ticker": ticker, "updated_at": datetime.now().strftime("%Y-%m-%d")})

        try:
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

            summary_items = await page.locator('div[data-testid="quote-statistics"] li, table tr').all()
            for item in summary_items:
                text = await item.inner_text()
                if not text:
                    continue

                parts = text.replace("\n", "\t").split("\t")
                if len(parts) < 2:
                    continue

                label = parts[0].strip()
                value = parts[-1].strip()
                if value == "--":
                    continue

                if "Yield" in label:
                    data["div_yield"] = value
                elif "PE Ratio" in label:
                    data["pe_ratio"] = value
                elif "YTD Return" in label:
                    data["total_return_ytd"] = value

            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/performance", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

            one_year_row = page.locator('tr:has-text("1-Year"), tr:has-text("1Y")')
            if await one_year_row.count() > 0:
                cells = one_year_row.first.locator("td")
                if await cells.count() >= 2:
                    data["total_return_1y"] = (await cells.nth(1).inner_text()).strip()

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
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()
            await context.route("**/*", _route_minimal_assets)

            for i, item in enumerate(queue, 1):
                try:
                    result = await asyncio.wait_for(self.scrape_policy(page, item["ticker"]), timeout=70)
                except asyncio.TimeoutError:
                    logger.warning("%s timeout > 70s", item["ticker"])
                    result = None
                row = result if result else {c: "" for c in COLS}
                row["ticker"] = item["ticker"]
                row["updated_at"] = datetime.now().strftime("%Y-%m-%d")
                pd.DataFrame([row])[COLS].to_csv(self.output_file, mode="a", header=False, index=False, encoding="utf-8-sig")
                if i % 25 == 0:
                    await page.close()
                    page = await context.new_page()
                await asyncio.sleep(random.uniform(1, 3) if i % 10 else random.uniform(2, 4))
                if i % 100 == 0:
                    logger.info("Progress: %s/%s", i, len(queue))

            await browser.close()

        logger.info("Output: %s", self.output_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="Yahoo Finance static policy scraper")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()
    asyncio.run(YFPolicyScraper(sample=args.sample).run())


if __name__ == "__main__":
    main()
