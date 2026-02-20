import argparse
import asyncio
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
from playwright.async_api import async_playwright

from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_SA_DIR, VAL_SA_HOLDINGS


logger = setup_logger("04_holdings_sa_sector_country")

BASE_URL = "https://stockanalysis.com/etf/"
KNOWN_SECTORS = {
    "Technology",
    "Financials",
    "Health Care",
    "Consumer Discretionary",
    "Industrials",
    "Communication Services",
    "Consumer Staples",
    "Energy",
    "Utilities",
    "Real Estate",
    "Materials",
}
DEFAULT_WORKERS = 6
DEFAULT_BATCH_SIZE = 120


async def _route_minimal_assets(route):
    if route.request.resource_type in {"image", "font", "media"}:
        await route.abort()
    else:
        await route.continue_()


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


def get_tickers(input_csv_path: str = "", sample: int = 0, tickers: str = "") -> List[str]:
    if tickers.strip():
        ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
        return ticker_list[:sample] if sample > 0 else ticker_list

    input_csv = resolve_input_csv(input_csv_path)
    if not input_csv.exists():
        return []

    try:
        df = pd.read_csv(input_csv, encoding="utf-8-sig")
        ticker_list = df["ticker"].astype(str).str.strip().str.upper().tolist()
        return ticker_list[:sample] if sample > 0 else ticker_list
    except Exception:
        return []


def classify_row(name: str, value: float) -> str:
    if name in KNOWN_SECTORS:
        return "Sector"

    if name in {"Stocks", "Other", "Cash", "Bond", "Bonds"}:
        return "Skip"

    if value < 100 and not any(token in name for token in ["Inc", "Corp", "Ltd", "Class", "Group"]):
        return "Country"

    return "Skip"


async def get_etf_data(ticker: str, context) -> tuple[List[Dict], str]:
    url = f"{BASE_URL}{ticker.lower()}/holdings/"
    logger.info("[%s] Fetching...", ticker)

    page = await context.new_page()
    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(random.randint(700, 1500))
        html_content = await page.content()

        pattern = re.compile(r"name\s*:\s*['\"]([^'\"]+)['\"]\s*,\s*y\s*:\s*([\d\.]+)")
        matches = pattern.findall(html_content)

        results = []
        seen = set()
        for name, value in matches:
            try:
                value_float = float(value)
            except Exception:
                continue

            key = f"{name}-{value_float}"
            if key in seen:
                continue
            seen.add(key)

            row_type = classify_row(name, value_float)
            if row_type == "Skip":
                continue

            results.append(
                {
                    "ticker": ticker,
                    "category_name": name,
                    "percentage": value_float,
                    "type": row_type,
                    "source": "Stock Analysis",
                    "date_scraper": datetime.now().strftime("%Y-%m-%d"),
                    "url": url,
                }
            )

        logger.info("[%s] Done (%s rows)", ticker, len(results))
        return results, ("SUCCESS" if results else "NO_DATA")
    except Exception as exc:
        logger.error("[%s] Error: %s", ticker, exc)
        return [], "ERROR"
    finally:
        await page.close()


async def run_scraper(
    input_csv_path: str = "",
    sample: int = 0,
    tickers: str = "",
    workers: int = DEFAULT_WORKERS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    ticker_list = get_tickers(input_csv_path=input_csv_path, sample=sample, tickers=tickers)
    workers = max(1, workers)
    batch_size = max(1, batch_size)
    if not ticker_list:
        logger.error("No tickers available for processing.")
        return

    today_dir = VAL_SA_HOLDINGS / datetime.now().strftime("%Y-%m-%d")
    today_dir.mkdir(parents=True, exist_ok=True)
    sector_output = today_dir / "sa_sector_allocation.csv"
    country_output = today_dir / "sa_country_allocation.csv"
    processed_output = today_dir / "sa_sector_country_processed.csv"

    def _read_processed_tickers(path: Path) -> Set[str]:
        if not path.exists() or path.stat().st_size == 0:
            return set()
        try:
            hdr = pd.read_csv(path, nrows=0)
            col = "ticker" if "ticker" in hdr.columns else ("Ticker" if "Ticker" in hdr.columns else hdr.columns[0])
            df = pd.read_csv(path, usecols=[col], encoding="utf-8-sig")
            return set(df[col].astype(str).str.strip().str.upper())
        except Exception:
            return set()

    if not processed_output.exists() or processed_output.stat().st_size == 0:
        pd.DataFrame(columns=["ticker", "status", "updated_at"]).to_csv(processed_output, index=False, encoding="utf-8-sig")

    processed = set()
    try:
        if processed_output.exists() and processed_output.stat().st_size > 0:
            p_df = pd.read_csv(processed_output, encoding="utf-8-sig")
            if not p_df.empty:
                p_df = p_df[p_df["status"].astype(str).str.upper().isin({"SUCCESS", "NO_DATA"})]
                processed |= set(p_df["ticker"].astype(str).str.strip().str.upper())
    except Exception:
        pass
    # Backward-compatible resume from existing outputs.
    processed |= _read_processed_tickers(sector_output) | _read_processed_tickers(country_output)
    pending = [t for t in ticker_list if t not in processed]
    logger.info("Universe=%s | Processed=%s | Remaining=%s", len(ticker_list), len(processed), len(pending))
    if not pending:
        logger.info("All tickers already processed.")
        return

    sector_fields = ["ticker", "category_name", "percentage", "type", "source", "date_scraper", "url"]
    country_fields = sector_fields
    if not sector_output.exists() or sector_output.stat().st_size == 0:
        pd.DataFrame(columns=sector_fields).to_csv(sector_output, index=False, encoding="utf-8-sig")
    if not country_output.exists() or country_output.stat().st_size == 0:
        pd.DataFrame(columns=country_fields).to_csv(country_output, index=False, encoding="utf-8-sig")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
        )
        await context.route("**/*", _route_minimal_assets)

        done = 0
        sem = asyncio.Semaphore(workers)

        for i in range(0, len(pending), batch_size):
            batch = pending[i : i + batch_size]
            batch_start = datetime.now()

            async def run_one(ticker: str) -> tuple[str, List[Dict], str]:
                async with sem:
                    await asyncio.sleep(random.uniform(0.15, 0.8))
                    rows, status = await get_etf_data(ticker, context)
                    return ticker, rows, status

            results_nested = await asyncio.gather(*[run_one(t) for t in batch])
            flat = [r for _, rows, _ in results_nested for r in rows]
            if flat:
                df = pd.DataFrame(flat)
                df_sector = df[df["type"] == "Sector"][sector_fields].copy()
                df_country = df[df["type"] == "Country"][country_fields].copy()
                if not df_sector.empty:
                    df_sector.to_csv(sector_output, mode="a", header=False, index=False, encoding="utf-8-sig")
                if not df_country.empty:
                    df_country.to_csv(country_output, mode="a", header=False, index=False, encoding="utf-8-sig")
            processed_rows = [
                {"ticker": t, "status": status, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                for t, _, status in results_nested
            ]
            pd.DataFrame(processed_rows).to_csv(processed_output, mode="a", header=False, index=False, encoding="utf-8-sig")

            done += len(batch)
            elapsed = (datetime.now() - batch_start).total_seconds()
            logger.info("Batch done: %s/%s tickers | rows=%s | workers=%s | %.1fs", done, len(pending), len(flat), workers, elapsed)

            # Cooldown and periodic context refresh to reduce block chance.
            if ((i // batch_size) + 1) % 5 == 0:
                await asyncio.sleep(random.uniform(8, 16))
                await context.close()
                context = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    )
                )
                await context.route("**/*", _route_minimal_assets)

        await browser.close()

    logger.info("Saved sectors: %s", sector_output)
    logger.info("Saved countries: %s", country_output)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Analysis sector/country allocation scraper")
    parser.add_argument("--input-csv", default="", help="Optional explicit master CSV path")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers (overrides input CSV)")
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent workers (recommend 4-8)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Tickers per batch before cooldown")
    args = parser.parse_args()

    asyncio.run(
        run_scraper(
            input_csv_path=args.input_csv,
            sample=args.sample,
            tickers=args.tickers,
            workers=args.workers,
            batch_size=args.batch_size,
        )
    )


if __name__ == "__main__":
    main()
