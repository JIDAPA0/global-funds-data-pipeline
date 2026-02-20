import argparse
import asyncio
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_YF_DIR, VAL_YF_STATIC


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def _route_minimal_assets(route):
    if route.request.resource_type in {"image", "font", "media"}:
        await route.abort()
    else:
        await route.continue_()


logger = setup_logger("03_static_yf_risk")

METRICS = [
    "alpha",
    "beta",
    "mean_annual_return",
    "r_squared",
    "standard_deviation",
    "sharpe_ratio",
    "treynor_ratio",
]

COLS = ["ticker", "morningstar_rating"]
for metric in METRICS:
    for horizon in ["3y", "5y", "10y"]:
        COLS.append(f"{metric}_{horizon}")


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
    return out_dir / "yahoo_finance_risk.csv"


class YFRiskScraper:
    def __init__(self, sample: int = 0):
        self.output_file = resolve_output_path()
        self.tickers = get_ticker_universe()
        if sample > 0:
            self.tickers = self.tickers[:sample]

        if not self.output_file.exists():
            pd.DataFrame(columns=COLS).to_csv(self.output_file, index=False, encoding="utf-8-sig")

    async def scrape_risk(self, page, ticker: str) -> Optional[Dict[str, str]]:
        data = {c: "" for c in COLS}
        data["ticker"] = ticker

        try:
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/risk", wait_until="domcontentloaded", timeout=60000)

            target = 'section[data-testid="risk-statistics-table"]'
            try:
                await page.wait_for_selector(target, timeout=20000)
            except Exception:
                return None

            await asyncio.sleep(2)

            rows = page.locator(f"{target} tbody tr")
            count = await rows.count()
            for i in range(count):
                cells = rows.nth(i).locator("td")
                cell_count = await cells.count()
                if cell_count < 2:
                    continue

                label = (await cells.nth(0).inner_text()).lower().strip()

                for metric in METRICS:
                    metric_label = metric.replace("_", " ")
                    if metric_label in label or (metric == "beta" and label == "beta"):
                        data[f"{metric}_3y"] = (await cells.nth(1).inner_text()).strip() if cell_count > 1 else ""
                        data[f"{metric}_5y"] = (await cells.nth(3).inner_text()).strip() if cell_count > 3 else ""
                        data[f"{metric}_10y"] = (await cells.nth(5).inner_text()).strip() if cell_count > 5 else ""

            try:
                rating_row = page.locator('section[data-testid="risk-overview"] tr:has-text("Morningstar Risk Rating")')
                if await rating_row.count() > 0:
                    raw_rating = (await rating_row.locator("td").last.inner_text()).strip()
                    if "★" in raw_rating:
                        data["morningstar_rating"] = str(raw_rating.count("★"))
                    elif raw_rating.isdigit():
                        data["morningstar_rating"] = raw_rating
            except Exception:
                pass

            logger.info("%s extracted (rating=%s)", ticker, data.get("morningstar_rating", ""))
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
                    result = await asyncio.wait_for(self.scrape_risk(page, item["ticker"]), timeout=80)
                except asyncio.TimeoutError:
                    logger.warning("%s timeout > 80s", item["ticker"])
                    result = None
                row = result if result else {c: "" for c in COLS}
                row["ticker"] = item["ticker"]
                pd.DataFrame([row])[COLS].to_csv(self.output_file, mode="a", header=False, index=False, encoding="utf-8-sig")

                if i % 25 == 0:
                    await page.close()
                    page = await context.new_page()
                if i % 10 == 0:
                    await asyncio.sleep(random.uniform(3, 6))
                else:
                    await asyncio.sleep(random.uniform(1, 2))
                if i % 100 == 0:
                    logger.info("Progress: %s/%s", i, len(queue))

            await browser.close()

        logger.info("Output: %s", self.output_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="Yahoo Finance static risk scraper")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()
    asyncio.run(YFRiskScraper(sample=args.sample).run())


if __name__ == "__main__":
    main()
