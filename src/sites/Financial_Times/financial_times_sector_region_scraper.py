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
from pathlib import Path
from typing import Dict, List, Tuple

import aiohttp
from bs4 import BeautifulSoup

from src.utils.browser_utils import get_random_headers
from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_FT_DIR, VAL_FT_HOLDINGS


try:
    from zoneinfo import ZoneInfo

    TZ_BANGKOK = ZoneInfo("Asia/Bangkok")
except Exception:
    TZ_BANGKOK = None


def now_bangkok() -> datetime:
    return datetime.now(TZ_BANGKOK) if TZ_BANGKOK else datetime.now()


def today_yyyymmdd() -> str:
    return now_bangkok().strftime("%Y-%m-%d")


logger = setup_logger("04_holdings_ft_sector_region")


@dataclass
class FinancialTimesSectorRegionConfig:
    master_base_dir: Path = VAL_FT_DIR / "master_tickers"
    master_filename: str = "financial_times_master_tickers.csv"

    out_base_dir: Path = VAL_FT_HOLDINGS / "Sector_Region"
    sector_filename: str = "financial_times_sector_allocation.csv"
    region_filename: str = "financial_times_region_allocation.csv"
    processed_filename: str = "financial_times_sector_region_processed.csv"
    split_output_by_ticker: bool = True

    concurrency: int = 40
    save_interval: int = 100
    request_timeout_sec: int = 12
    max_retries: int = 2
    sample: int = 0


def normalize_text(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def sanitize_filename_token(value: str) -> str:
    cleaned = normalize_text(value)
    if not cleaned:
        return "unknown"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned)


def ticker_sector_path(out_dir: Path, ticker: str) -> Path:
    return out_dir / f"{sanitize_filename_token(ticker)}_sector_allocation.csv"


def ticker_region_path(out_dir: Path, ticker: str) -> Path:
    return out_dir / f"{sanitize_filename_token(ticker)}_region_allocation.csv"


def write_csv_with_header(path: Path, fieldnames: List[str], rows: List[Dict]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def clean_percent(value: str) -> float:
    text = normalize_text(value).replace("%", "").replace("+", "").replace(",", "").strip()
    try:
        return float(text)
    except Exception:
        return 0.0


def get_ft_ticker(row: Dict[str, str]) -> str:
    for key in ("ft_ticker", "\ufeffft_ticker"):
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def resolve_paths(cfg: FinancialTimesSectorRegionConfig) -> Tuple[Path, Path, Path, Path]:
    if cfg.master_base_dir.exists():
        date_dirs = sorted(
            d
            for d in cfg.master_base_dir.iterdir()
            if d.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", d.name)
        )
        master_path = None
        for date_dir in reversed(date_dirs):
            candidate = date_dir / cfg.master_filename
            if candidate.exists():
                master_path = candidate
                break
        if not master_path:
            master_path = cfg.master_base_dir / today_yyyymmdd() / cfg.master_filename
    else:
        master_path = cfg.master_base_dir / today_yyyymmdd() / cfg.master_filename

    out_dir = cfg.out_base_dir / today_yyyymmdd()
    out_dir.mkdir(parents=True, exist_ok=True)

    sector_path = out_dir / cfg.sector_filename
    region_path = out_dir / cfg.region_filename
    processed_path = out_dir / cfg.processed_filename
    return master_path, sector_path, region_path, processed_path


def build_url_attempts(ft_ticker: str, ticker_type: str) -> List[Tuple[str, str]]:
    ticker = normalize_text(ft_ticker)
    if not ticker:
        return []

    attempts: List[Tuple[str, str]] = []
    is_etf = normalize_text(ticker_type).lower() == "etf"
    primary = "etfs" if is_etf else "funds"
    secondary = "funds" if is_etf else "etfs"

    attempts.append((f"https://markets.ft.com/data/{primary}/tearsheet/holdings?s={ticker}", primary))
    attempts.append((f"https://markets.ft.com/data/{secondary}/tearsheet/holdings?s={ticker}", secondary))

    if ":" in ticker:
        clean_ticker = ticker.split(":")[0]
        attempts.append((f"https://markets.ft.com/data/{primary}/tearsheet/holdings?s={clean_ticker}", f"{primary}_clean"))
        attempts.append((f"https://markets.ft.com/data/{secondary}/tearsheet/holdings?s={clean_ticker}", f"{secondary}_clean"))

    return attempts


def is_percent_like(text: str) -> bool:
    value = normalize_text(text)
    if not value:
        return False
    if "%" in value:
        return True
    return bool(re.match(r"^[+-]?\d+(\.\d+)?$", value))


def parse_weight_table(
    table,
    allocation_type: str,
    ft_ticker: str,
    row: Dict[str, str],
    url: str,
    url_type: str,
) -> List[Dict]:
    output: List[Dict] = []

    for table_row in table.find_all("tr"):
        columns = table_row.find_all(["td", "th"])
        if len(columns) < 2:
            continue

        name = normalize_text(columns[0].get_text())
        raw_value = normalize_text(columns[1].get_text())
        if not name or not is_percent_like(raw_value):
            continue

        lower_name = name.lower()
        skip_words = [
            "sector",
            "region",
            "market",
            "total",
            "net assets",
            "category",
            "fund quartile",
            "type",
            "% net assets",
            "% short",
            "% long",
        ]
        if any(word in lower_name for word in skip_words):
            continue

        output.append(
            {
                "ft_ticker": ft_ticker,
                "ticker": row.get("ticker", ""),
                "name": row.get("name", ""),
                "ticker_type": row.get("ticker_type", ""),
                "category_name": name,
                "weight_pct": f"{clean_percent(raw_value):.2f}",
                "allocation_type": allocation_type,
                "url_type_used": url_type,
                "source": "Financial Times",
                "date_scraper": today_yyyymmdd(),
                "url": url,
            }
        )

    return output


def extract_allocations_from_html(
    html: str,
    ft_ticker: str,
    row: Dict[str, str],
    url: str,
    url_type: str,
) -> List[Dict]:
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    output: List[Dict] = []
    sector_tables = soup.select("#sectors-panel table, .mod-weightings__sectors__table table")
    region_tables = soup.select("#regions-panel table, .mod-weightings__regions__table table")

    for table in sector_tables:
        output.extend(parse_weight_table(table, "Sector Allocation", ft_ticker, row, url, url_type))
    for table in region_tables:
        output.extend(parse_weight_table(table, "Region Allocation", ft_ticker, row, url, url_type))

    return output


async def fetch_html(session: aiohttp.ClientSession, url: str, cfg: FinancialTimesSectorRegionConfig) -> str:
    for attempt in range(cfg.max_retries + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=cfg.request_timeout_sec)) as response:
                if response.status == 200:
                    return await response.text()
                if response.status == 404:
                    return ""
        except Exception:
            pass

        if attempt < cfg.max_retries:
            await asyncio.sleep(random.uniform(0.2, 0.7) * (attempt + 1))

    return ""


async def process_one(
    session: aiohttp.ClientSession,
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cfg: FinancialTimesSectorRegionConfig,
) -> Tuple[List[Dict], str]:
    ft_ticker = get_ft_ticker(row)
    ticker_type = row.get("ticker_type", "")
    attempts = build_url_attempts(ft_ticker, ticker_type)

    for url, url_type in attempts:
        async with semaphore:
            html = await fetch_html(session, url, cfg)
        if not html:
            continue

        data = extract_allocations_from_html(html, ft_ticker, row, url, url_type)
        if data:
            return data, "ok"

    return [], "no_data"


async def run(cfg: FinancialTimesSectorRegionConfig) -> None:
    master_path, sector_path, region_path, processed_path = resolve_paths(cfg)
    output_dir = sector_path.parent

    logger.info("Input: %s", master_path)
    if cfg.split_output_by_ticker:
        logger.info("Output: %s/<ticker>_sector_allocation.csv", output_dir)
        logger.info("Output: %s/<ticker>_region_allocation.csv", output_dir)
    else:
        logger.info("Sector output: %s", sector_path)
        logger.info("Region output: %s", region_path)

    if not master_path.exists():
        logger.error("Master file not found: %s", master_path)
        return

    with master_path.open("r", encoding="utf-8-sig") as master_file:
        all_rows = list(csv.DictReader(master_file))

    if cfg.sample > 0:
        all_rows = all_rows[: cfg.sample]

    completed = set()
    if processed_path.exists():
        try:
            shutil.copy(processed_path, processed_path.with_suffix(".csv.bak"))
        except Exception:
            pass

        try:
            with processed_path.open("r", encoding="utf-8-sig") as processed_file:
                for row in csv.DictReader(processed_file):
                    ft_ticker = get_ft_ticker(row)
                    if ft_ticker:
                        completed.add(ft_ticker)
        except Exception:
            pass

    todo_rows = [row for row in all_rows if get_ft_ticker(row) not in completed]
    logger.info("Resuming: found %s processed tickers", f"{len(completed):,}")
    logger.info("Workload: %s tickers", f"{len(todo_rows):,}")
    if not todo_rows:
        logger.info("All done")
        return

    fields = [
        "ft_ticker",
        "ticker",
        "name",
        "ticker_type",
        "category_name",
        "weight_pct",
        "allocation_type",
        "url_type_used",
        "source",
        "date_scraper",
        "url",
    ]
    processed_fields = ["ft_ticker", "ticker", "name", "ticker_type", "status", "date_scraper"]

    targets = [(processed_path, processed_fields)]
    if not cfg.split_output_by_ticker:
        targets = [(sector_path, fields), (region_path, fields), (processed_path, processed_fields)]

    for path, fieldnames in targets:
        if not path.exists() or os.stat(path).st_size == 0:
            with path.open("w", newline="", encoding="utf-8-sig") as output_file:
                csv.DictWriter(output_file, fieldnames=fieldnames).writeheader()

    sector_file = None
    region_file = None
    processed_file = processed_path.open("a", newline="", encoding="utf-8-sig")
    sector_writer = None
    region_writer = None

    if not cfg.split_output_by_ticker:
        sector_file = sector_path.open("a", newline="", encoding="utf-8-sig")
        region_file = region_path.open("a", newline="", encoding="utf-8-sig")
        sector_writer = csv.DictWriter(sector_file, fieldnames=fields)
        region_writer = csv.DictWriter(region_file, fieldnames=fields)

    processed_writer = csv.DictWriter(processed_file, fieldnames=processed_fields)

    semaphore = asyncio.Semaphore(cfg.concurrency)
    connector = aiohttp.TCPConnector(limit=cfg.concurrency + 30, ttl_dns_cache=300)

    start_time = time.time()
    processed_count = 0
    sector_rows_count = 0
    region_rows_count = 0

    async with aiohttp.ClientSession(connector=connector, headers=get_random_headers()) as session:
        for index in range(0, len(todo_rows), cfg.save_interval):
            batch = todo_rows[index : index + cfg.save_interval]
            tasks = [process_one(session, row, semaphore, cfg) for row in batch]
            results = await asyncio.gather(*tasks)

            sector_rows: List[Dict] = []
            region_rows: List[Dict] = []
            processed_rows: List[Dict] = []

            for row, (items, status) in zip(batch, results):
                ft_ticker = get_ft_ticker(row)
                for item in items:
                    if item.get("allocation_type") == "Sector Allocation":
                        sector_rows.append(item)
                    elif item.get("allocation_type") == "Region Allocation":
                        region_rows.append(item)

                processed_rows.append(
                    {
                        "ft_ticker": ft_ticker,
                        "ticker": row.get("ticker", ""),
                        "name": row.get("name", ""),
                        "ticker_type": row.get("ticker_type", ""),
                        "status": status,
                        "date_scraper": today_yyyymmdd(),
                    }
                )

            if cfg.split_output_by_ticker:
                for row, (items, _) in zip(batch, results):
                    ft_ticker = get_ft_ticker(row)
                    sector_only = [item for item in items if item.get("allocation_type") == "Sector Allocation"]
                    region_only = [item for item in items if item.get("allocation_type") == "Region Allocation"]
                    write_csv_with_header(ticker_sector_path(output_dir, ft_ticker), fields, sector_only)
                    write_csv_with_header(ticker_region_path(output_dir, ft_ticker), fields, region_only)
            else:
                if sector_rows:
                    sector_writer.writerows(sector_rows)
                if region_rows:
                    region_writer.writerows(region_rows)

            processed_writer.writerows(processed_rows)

            if sector_file and region_file:
                sector_file.flush()
                region_file.flush()
            processed_file.flush()

            if sector_file and region_file:
                os.fsync(sector_file.fileno())
                os.fsync(region_file.fileno())
            os.fsync(processed_file.fileno())

            processed_count += len(batch)
            sector_rows_count += len(sector_rows)
            region_rows_count += len(region_rows)
            elapsed = time.time() - start_time
            speed = processed_count / elapsed if elapsed > 0 else 0.0
            eta_minutes = (len(todo_rows) - processed_count) / speed / 60 if speed > 0 else 0.0

            logger.info(
                "Tickers=%s/%s SectorRows=%s RegionRows=%s Speed=%.1f/s ETA=%.1f min",
                processed_count,
                len(todo_rows),
                sector_rows_count,
                region_rows_count,
                speed,
                eta_minutes,
            )

    if sector_file:
        sector_file.close()
    if region_file:
        region_file.close()
    processed_file.close()

    logger.info("Sector/Region scrape completed")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Financial Times sector/region allocation scraper")
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--save-interval", type=int, default=100)
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    parser.add_argument("--no-split-by-ticker", action="store_true", help="Write combined sector/region files")
    parser.add_argument("--master-filename", default="financial_times_master_tickers.csv")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cfg = FinancialTimesSectorRegionConfig(
        concurrency=args.concurrency,
        save_interval=args.save_interval,
        sample=args.sample,
        split_output_by_ticker=not args.no_split_by_ticker,
        master_filename=args.master_filename,
    )
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
