import argparse
import asyncio
import csv
import os
import random
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd

from src.utils.browser_utils import get_random_headers
from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_FT_DIR


try:
    from zoneinfo import ZoneInfo

    TZ_BANGKOK = ZoneInfo("Asia/Bangkok")
except Exception:
    TZ_BANGKOK = None


def now_bangkok() -> datetime:
    return datetime.now(TZ_BANGKOK) if TZ_BANGKOK else datetime.now()


def today_yyyymmdd() -> str:
    return now_bangkok().strftime("%Y-%m-%d")


logger = setup_logger("04_ft_avg_fund_return")


@dataclass
class FinancialTimesAvgReturnConfig:
    master_base_dir: Path = VAL_FT_DIR / "master_tickers"
    master_filename: str = "financial_times_master_tickers.csv"

    out_base_dir: Path = VAL_FT_DIR / "05_Avg_Fund_Return"
    out_filename: str = "financial_times_avg_fund_return.csv"

    concurrency: int = 40
    save_interval: int = 100
    request_timeout_sec: int = 15
    max_retries: int = 2
    sample: int = 0


def normalize_text(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def get_ft_ticker(row: Dict[str, str]) -> str:
    for key in ("ft_ticker", "\ufeffft_ticker"):
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def resolve_paths(cfg: FinancialTimesAvgReturnConfig) -> Tuple[Path, Path]:
    if cfg.master_base_dir.exists():
        date_dirs = sorted(
            [
                directory
                for directory in cfg.master_base_dir.iterdir()
                if directory.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", directory.name)
            ]
        )
        master_path = None
        for directory in reversed(date_dirs):
            candidate = directory / cfg.master_filename
            if candidate.exists():
                master_path = candidate
                break
        if not master_path:
            master_path = cfg.master_base_dir / today_yyyymmdd() / cfg.master_filename
    else:
        master_path = cfg.master_base_dir / today_yyyymmdd() / cfg.master_filename

    out_dir = cfg.out_base_dir / today_yyyymmdd()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / cfg.out_filename
    return master_path, out_path


def clean_pct_value(value) -> Optional[float]:
    if pd.isna(value):
        return None
    text = normalize_text(value)
    if text in {"", "--", "-", "nan", "N/A"}:
        return None
    text = text.replace("%", "").replace("+", "").replace(",", "").strip()
    try:
        return float(text)
    except Exception:
        return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        cols = []
        for col in out.columns:
            parts = [normalize_text(part) for part in col if normalize_text(part)]
            cols.append(" ".join(parts).strip().lower())
        out.columns = cols
    else:
        out.columns = [normalize_text(col).lower() for col in out.columns]
    return out


def find_year_column(columns: List[str], years: int) -> Optional[str]:
    pattern = re.compile(rf"(^|\b){years}\s*year(s)?(\b|$)")
    for col in columns:
        normalized = normalize_text(col).lower()
        if pattern.search(normalized):
            if years == 1 and "10 year" in normalized:
                continue
            return col
    return None


def extract_avg_returns_from_html(html: str) -> Dict[str, str]:
    output = {
        "fund_name_perf": "",
        "avg_fund_return_1y_raw": "",
        "avg_fund_return_3y_raw": "",
        "avg_fund_return_1y": "",
        "avg_fund_return_3y": "",
    }
    if not html:
        return output

    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return output

    target_df = None
    col_1y = None
    col_3y = None

    for table in tables:
        normalized_df = normalize_columns(table)
        columns = list(normalized_df.columns)
        year_1_col = find_year_column(columns, 1)
        year_3_col = find_year_column(columns, 3)
        if year_1_col and year_3_col and len(normalized_df) > 0:
            target_df = normalized_df
            col_1y = year_1_col
            col_3y = year_3_col
            break

    if target_df is None or col_1y is None or col_3y is None:
        return output

    first_col = target_df.columns[0]
    picked = None
    for _, row in target_df.iterrows():
        first_cell = normalize_text(row.get(first_col, "")).lower()
        if "average annual" in first_cell or "annualised" in first_cell or "return" in first_cell:
            picked = row
            break
    if picked is None:
        for _, row in target_df.iterrows():
            if clean_pct_value(row.get(col_1y)) is not None or clean_pct_value(row.get(col_3y)) is not None:
                picked = row
                break
    if picked is None:
        picked = target_df.iloc[0]

    raw_1y = normalize_text(picked.get(col_1y, ""))
    raw_3y = normalize_text(picked.get(col_3y, ""))
    val_1y = clean_pct_value(raw_1y)
    val_3y = clean_pct_value(raw_3y)

    output["fund_name_perf"] = normalize_text(picked.get(first_col, ""))
    output["avg_fund_return_1y_raw"] = raw_1y
    output["avg_fund_return_3y_raw"] = raw_3y
    output["avg_fund_return_1y"] = "" if val_1y is None else f"{val_1y:.6f}"
    output["avg_fund_return_3y"] = "" if val_3y is None else f"{val_3y:.6f}"
    return output


async def fetch_html(session: aiohttp.ClientSession, url: str, cfg: FinancialTimesAvgReturnConfig) -> Optional[str]:
    for attempt in range(cfg.max_retries + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=cfg.request_timeout_sec)) as response:
                if response.status == 200:
                    return await response.text()
                if response.status == 404:
                    return None
        except Exception:
            pass
        if attempt < cfg.max_retries:
            await asyncio.sleep(random.uniform(0.2, 0.7) * (attempt + 1))
    return None


def build_performance_url(ft_ticker: str, ticker_type: str) -> str:
    base = "etfs" if str(ticker_type).strip().lower() == "etf" else "funds"
    return f"https://markets.ft.com/data/{base}/tearsheet/performance?s={ft_ticker}"


async def process_one(
    session: aiohttp.ClientSession,
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cfg: FinancialTimesAvgReturnConfig,
) -> Dict[str, str]:
    ft_ticker = get_ft_ticker(row)
    ticker_type = (row.get("ticker_type") or "Fund").strip()
    url = build_performance_url(ft_ticker, ticker_type)

    async with semaphore:
        html = await fetch_html(session, url, cfg)

    perf = extract_avg_returns_from_html(html or "")
    output = row.copy()
    output.update(perf)
    output["ft_ticker"] = ft_ticker
    output["source"] = "Financial Times"
    output["date_scraper"] = today_yyyymmdd()
    output["url"] = url
    return output


async def run_scraper(cfg: FinancialTimesAvgReturnConfig) -> None:
    master_path, out_path = resolve_paths(cfg)
    logger.info("Input: %s", master_path)
    logger.info("Output: %s", out_path)

    if not master_path.exists():
        logger.error("Master file not found: %s", master_path)
        return

    with master_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        all_rows = list(csv.DictReader(csv_file))
    if cfg.sample > 0:
        all_rows = all_rows[: cfg.sample]

    completed = set()
    if out_path.exists():
        try:
            shutil.copy(out_path, out_path.with_suffix(".csv.bak"))
        except Exception:
            pass
        try:
            with out_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
                for row in csv.DictReader(csv_file):
                    ft_ticker = get_ft_ticker(row)
                    if ft_ticker:
                        completed.add(ft_ticker)
        except Exception:
            pass

    logger.info("Resuming: found %s completed rows", f"{len(completed):,}")
    todo_rows = [row for row in all_rows if get_ft_ticker(row) not in completed]
    total_todo = len(todo_rows)
    logger.info("Workload: %s items", f"{total_todo:,}")
    if total_todo == 0:
        logger.info("All done")
        return

    fieldnames = [
        "ft_ticker",
        "ticker",
        "name",
        "ticker_type",
        "fund_name_perf",
        "avg_fund_return_1y_raw",
        "avg_fund_return_3y_raw",
        "avg_fund_return_1y",
        "avg_fund_return_3y",
        "source",
        "date_scraper",
        "url",
    ]

    if not out_path.exists() or os.stat(out_path).st_size == 0:
        with out_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

    out_handle = out_path.open("a", newline="", encoding="utf-8-sig")
    writer = csv.DictWriter(out_handle, fieldnames=fieldnames)

    semaphore = asyncio.Semaphore(cfg.concurrency)
    connector = aiohttp.TCPConnector(limit=cfg.concurrency + 30, ttl_dns_cache=300)

    start = time.time()
    processed = 0

    async with aiohttp.ClientSession(connector=connector, headers=get_random_headers()) as session:
        for start_idx in range(0, total_todo, cfg.save_interval):
            batch_rows = todo_rows[start_idx : start_idx + cfg.save_interval]
            tasks = [process_one(session, row, semaphore, cfg) for row in batch_rows]
            results = await asyncio.gather(*tasks)

            clean_results = [{k: row.get(k, "") for k in fieldnames} for row in results]
            writer.writerows(clean_results)
            out_handle.flush()
            os.fsync(out_handle.fileno())

            processed += len(batch_rows)
            elapsed = time.time() - start
            speed = processed / elapsed if elapsed > 0 else 0.0
            eta_min = ((total_todo - processed) / speed / 60) if speed > 0 else 0.0
            logger.info("Progress: %s/%s | Speed: %.1f/s | ETA: %.1f min", processed, total_todo, speed, eta_min)

    out_handle.close()
    logger.info("DONE")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape FT average fund return (1Y/3Y) from performance page.")
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--save-interval", type=int, default=100)
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()

    cfg = FinancialTimesAvgReturnConfig()
    cfg.concurrency = args.concurrency
    cfg.save_interval = args.save_interval
    cfg.sample = args.sample

    asyncio.run(run_scraper(cfg))


if __name__ == "__main__":
    main()

