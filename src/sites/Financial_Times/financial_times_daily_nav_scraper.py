import argparse
import asyncio
import csv
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from bs4 import BeautifulSoup

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


logger = setup_logger("02_perf_ft_daily_nav")


@dataclass
class FinancialTimesDailyNavConfig:
    master_base_dir: Path = VAL_FT_DIR / "master_tickers"
    master_filename: str = "financial_times_master_tickers.csv"
    output_base_dir: Path = VAL_FT_DIR / "Daily_NAV"

    concurrency: int = 120
    save_interval: int = 200
    request_timeout_sec: int = 10
    max_retries: int = 2
    sample: int = 0


QUOTE_RE = re.compile(
    r"\b(?:NAV|Price)\s*\(([A-Z]{3})\)\s*([0-9][0-9,]*(?:\.[0-9]+)?)\b",
    re.IGNORECASE,
)


def resolve_paths(cfg: FinancialTimesDailyNavConfig) -> Tuple[Path, Path]:
    today_master = cfg.master_base_dir / today_yyyymmdd() / cfg.master_filename

    if today_master.exists():
        master_path = today_master
    elif cfg.master_base_dir.exists():
        date_dirs = sorted(
            [
                directory
                for directory in cfg.master_base_dir.iterdir()
                if directory.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", directory.name)
            ]
        )
        master_path = (date_dirs[-1] / cfg.master_filename) if date_dirs else today_master
    else:
        master_path = today_master

    output_dir = cfg.output_base_dir / today_yyyymmdd()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "financial_times_daily_nav.csv"

    return master_path, output_path


def load_master_data(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Master file not found: {path}")

    with path.open("r", encoding="utf-8-sig") as csv_file:
        return list(csv.DictReader(csv_file))


def load_existing_good_data(path: Path) -> Tuple[List[Dict[str, str]], Set[str]]:
    good_rows: List[Dict[str, str]] = []
    good_ids: Set[str] = set()

    if not path.exists():
        return good_rows, good_ids

    backup_path = path.with_suffix(".csv.bak")
    try:
        shutil.copy(path, backup_path)
        logger.info("Backed up existing file to %s", backup_path.name)
    except Exception as exc:
        logger.warning("Backup failed: %s", exc)

    try:
        with path.open("r", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                ft_ticker = row.get("ft_ticker", "").strip()
                price = row.get("nav_price", "").strip()
                currency = row.get("currency", "").strip() or row.get("nav_currency", "").strip()

                # A row is treated as complete when ticker + currency exist.
                if ft_ticker and currency:
                    if not price:
                        logger.debug("Keeping row without price but with currency: %s", ft_ticker)
                    good_rows.append(row)
                    good_ids.add(ft_ticker)
    except Exception as exc:
        logger.warning("Error reading existing file (%s). Starting fresh.", exc)

    return good_rows, good_ids


def parse_summary(html: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    if not html:
        return None, None, None

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    all_text = soup.get_text(" ", strip=True)

    match = QUOTE_RE.search(all_text)
    price: Optional[float] = None
    currency: Optional[str] = None
    if match:
        currency = (match.group(1) or "").upper()
        try:
            price = float(match.group(2).replace(",", ""))
        except Exception:
            price = None

    as_of = today_yyyymmdd()
    disclaimer = soup.select_one(".mod-disclaimer")
    date_text = disclaimer.get_text(" ", strip=True) if disclaimer else all_text

    date_match = re.search(r"(\w{3})\s+(\d{1,2})[\s,]+(\d{4})", date_text)
    if not date_match:
        date_match = re.search(r"(\d{1,2})\s+(\w{3})[\s,]+(\d{4})", date_text)

    if date_match:
        date_string = f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}"
        for date_fmt in ("%b %d %Y", "%d %b %Y"):
            try:
                as_of = datetime.strptime(date_string, date_fmt).strftime("%Y-%m-%d")
                break
            except Exception:
                continue

    return price, currency, as_of


async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    cfg: FinancialTimesDailyNavConfig,
) -> Optional[str]:
    for _ in range(cfg.max_retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=cfg.request_timeout_sec)) as response:
                if response.status == 200:
                    return await response.text()
                if response.status == 404:
                    return None
        except Exception:
            continue
    return None


def build_summary_url(ft_ticker: str, ticker_type: str) -> str:
    base_path = "etfs" if str(ticker_type).strip().lower() == "etf" else "funds"
    return f"https://markets.ft.com/data/{base_path}/tearsheet/summary?s={ft_ticker}"


async def process_one(
    session: aiohttp.ClientSession,
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cfg: FinancialTimesDailyNavConfig,
) -> Dict[str, str]:
    ft_ticker = row.get("ft_ticker", "").strip()
    ticker_type = row.get("ticker_type", "Fund").strip()
    url = row.get("url", "") or build_summary_url(ft_ticker, ticker_type)

    async with semaphore:
        html = await fetch_html(session, url, cfg)

    nav_price, parsed_currency, nav_as_of = parse_summary(html)

    input_currency = row.get("currency", "").strip()
    final_currency = input_currency if input_currency else (parsed_currency or "")

    row_output = row.copy()
    row_output.update(
        {
            "nav_price": nav_price,
            "nav_currency": final_currency,
            "currency": final_currency,
            "nav_as_of": nav_as_of,
            "date_scraper": today_yyyymmdd(),
            "source": "Financial Times",
            "url": url,
        }
    )

    return row_output


async def run(cfg: FinancialTimesDailyNavConfig) -> None:
    master_path, output_path = resolve_paths(cfg)
    logger.info("Master input: %s", master_path)
    logger.info("Output file: %s", output_path)

    all_master_rows = load_master_data(master_path)
    if cfg.sample:
        all_master_rows = all_master_rows[: cfg.sample]

    good_rows, good_ids = load_existing_good_data(output_path)
    logger.info("Found %s complete records; preserving them", f"{len(good_rows):,}")

    todo_rows = [row for row in all_master_rows if row.get("ft_ticker", "").strip() not in good_ids]
    total_todo = len(todo_rows)
    logger.info("Repair/new workload: %s items", f"{total_todo:,}")

    fieldnames = [
        "ft_ticker",
        "ticker",
        "name",
        "ticker_type",
        "nav_price",
        "nav_currency",
        "nav_as_of",
        "currency",
        "source",
        "date_scraper",
        "url",
    ]

    with output_path.open("w", newline="", encoding="utf-8-sig") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(good_rows)

    if total_todo == 0:
        logger.info("All data already complete")
        return

    start_time = time.time()
    processed_count = 0
    semaphore = asyncio.Semaphore(cfg.concurrency)
    connector = aiohttp.TCPConnector(limit=cfg.concurrency + 50, ttl_dns_cache=300)

    with output_path.open("a", newline="", encoding="utf-8-sig") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)

        async with aiohttp.ClientSession(connector=connector, headers=get_random_headers()) as session:
            for index in range(0, total_todo, cfg.save_interval):
                batch_rows = todo_rows[index : index + cfg.save_interval]
                tasks = [process_one(session, row, semaphore, cfg) for row in batch_rows]
                results = await asyncio.gather(*tasks)

                writer.writerows(results)
                output_file.flush()
                os.fsync(output_file.fileno())

                processed_count += len(results)
                elapsed = time.time() - start_time
                speed = processed_count / elapsed if elapsed > 0 else 0.0
                remaining = (total_todo - processed_count) / speed if speed > 0 else 0.0
                logger.info(
                    "Speed=%.1f/s Progress=%s/%s ETA=%.1f min",
                    speed,
                    processed_count,
                    total_todo,
                    remaining / 60,
                )

    logger.info("Daily NAV scrape completed")
    logger.info("Output: %s", output_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Financial Times daily NAV smart scrape")
    parser.add_argument("--concurrency", type=int, default=120)
    parser.add_argument("--save-interval", type=int, default=200)
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--request-timeout", type=int, default=10)
    parser.add_argument("--max-retries", type=int, default=2)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cfg = FinancialTimesDailyNavConfig(
        concurrency=args.concurrency,
        save_interval=args.save_interval,
        sample=args.sample,
        request_timeout_sec=args.request_timeout,
        max_retries=args.max_retries,
    )
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
