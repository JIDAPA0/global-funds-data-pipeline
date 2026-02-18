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


logger = setup_logger("04_holdings_ft_acquisition")


@dataclass
class FinancialTimesHoldingsConfig:
    master_base_dir: Path = VAL_FT_DIR / "master_tickers"
    master_filename: str = "financial_times_master_tickers.csv"

    output_base_dir: Path = VAL_FT_HOLDINGS
    output_filename: str = "financial_times_holdings.csv"

    concurrency: int = 50
    save_interval: int = 100
    request_timeout_sec: int = 15
    max_retries: int = 2
    split_output_by_ticker: bool = False
    sample: int = 0


def resolve_paths(cfg: FinancialTimesHoldingsConfig) -> Tuple[Path, Path]:
    if cfg.master_base_dir.exists():
        date_dirs = sorted(
            [
                directory
                for directory in cfg.master_base_dir.iterdir()
                if directory.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", directory.name)
            ]
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

    output_dir = cfg.output_base_dir / today_yyyymmdd()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / cfg.output_filename

    return master_path, output_path


def normalize_text(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def sanitize_filename_token(value: str) -> str:
    cleaned = normalize_text(value)
    if not cleaned:
        return "unknown"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned)


def ticker_holding_file_path(output_dir: Path, ticker: str) -> Path:
    return output_dir / f"{sanitize_filename_token(ticker)}_holding.csv"


def write_single_ticker_csv(path: Path, fieldnames: List[str], rows: List[Dict]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def determine_holding_type_from_url(url: str) -> str:
    if not url:
        return "Unknown"

    lowered = url.lower()
    if "/equities/" in lowered:
        return "Equity"
    if "/funds/" in lowered:
        return "Fund"
    if "/etfs/" in lowered:
        return "ETF"
    if "/indices/" in lowered:
        return "Index"
    if "/currencies/" in lowered:
        return "Currency"
    return "Other"


def extract_holding_symbol(holding_ticker: str, holding_type: str) -> str:
    if normalize_text(holding_type).lower() != "equity":
        return ""
    ticker = normalize_text(holding_ticker)
    if not ticker:
        return ""
    return ticker.split(":")[0].strip()


def parse_holdings(html: str) -> List[Dict]:
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    target_table = None
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        has_security_col = any(k in h for h in headers for k in ["security", "company", "constituent"])
        has_weight_col = any(k in h for h in headers for k in ["%", "assets", "weight", "value"])
        if has_security_col and has_weight_col:
            target_table = table
            break

    if not target_table:
        return []

    holdings: List[Dict] = []
    for row in target_table.find_all("tr"):
        columns = row.find_all("td")
        if len(columns) < 3:
            continue

        name_col = columns[0]
        holding_name = normalize_text(name_col.get_text())
        holding_ticker = ""
        holding_url = ""

        link = name_col.find("a")
        if link and "href" in link.attrs:
            raw_href = link["href"]
            holding_url = f"https://markets.ft.com{raw_href}" if raw_href.startswith("/") else raw_href
            if "s=" in raw_href:
                holding_ticker = raw_href.split("s=")[-1].strip()

        holding_type = determine_holding_type_from_url(holding_url)

        portfolio_weight_pct = 0.0
        for col in columns[2:]:
            text = normalize_text(col.get_text())
            if "%" in text or re.search(r"\d+\.\d+", text):
                try:
                    numeric = re.sub(r"[^\d\.]", "", text)
                    if numeric:
                        portfolio_weight_pct = float(numeric)
                        break
                except Exception:
                    continue

        holdings.append(
            {
                "holding_name": holding_name,
                "holding_ticker": holding_ticker,
                "holding_type": holding_type,
                "holding_symbol": extract_holding_symbol(holding_ticker, holding_type),
                "holding_url": holding_url,
                "portfolio_weight_pct": portfolio_weight_pct,
            }
        )

    return holdings


async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    cfg: FinancialTimesHoldingsConfig,
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


def build_holdings_url(ft_ticker: str, ticker_type: str) -> str:
    base = "etfs" if str(ticker_type).strip().lower() == "etf" else "funds"
    return f"https://markets.ft.com/data/{base}/tearsheet/holdings?s={ft_ticker}"


async def process_one_fund(
    session: aiohttp.ClientSession,
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cfg: FinancialTimesHoldingsConfig,
) -> List[Dict]:
    ft_ticker = row.get("ft_ticker", "").strip()
    ticker_type = row.get("ticker_type", "Fund").strip()
    holdings_url = build_holdings_url(ft_ticker, ticker_type)

    async with semaphore:
        html = await fetch_html(session, holdings_url, cfg)

    holdings = parse_holdings(html)
    if not holdings:
        return []

    top_10_holdings_weight = sum(item["portfolio_weight_pct"] for item in holdings)
    other_holding_weight = max(0.0, 100.0 - top_10_holdings_weight)

    results: List[Dict] = []
    for item in holdings:
        results.append(
            {
                "ticker": row.get("ticker", ""),
                "name": row.get("name", ""),
                "ticker_type": row.get("ticker_type", ""),
                "allocation_type": "top_10_holdings",
                "holding_name": item["holding_name"],
                "holding_ticker": item["holding_ticker"],
                "holding_type": item["holding_type"],
                "holding_symbol": item["holding_symbol"],
                "holding_url": item["holding_url"],
                "portfolio_weight_pct": item["portfolio_weight_pct"],
                "top_10_holdings_weight_pct": f"{top_10_holdings_weight:.2f}",
                "other_holding_weight_pct": f"{other_holding_weight:.2f}",
                "source": "Financial Times",
                "date_scraper": today_yyyymmdd(),
                "url": holdings_url,
            }
        )

    return results


async def run(cfg: FinancialTimesHoldingsConfig) -> None:
    master_path, output_path = resolve_paths(cfg)
    output_dir = output_path.parent

    logger.info("Input: %s", master_path)
    if cfg.split_output_by_ticker:
        logger.info("Output: %s/<ticker>_holding.csv", output_dir)
    else:
        logger.info("Output: %s", output_path)

    if not master_path.exists():
        logger.error("Master file not found: %s", master_path)
        return

    with master_path.open("r", encoding="utf-8-sig") as master_file:
        all_rows = list(csv.DictReader(master_file))

    if cfg.sample:
        all_rows = all_rows[: cfg.sample]

    completed_tickers: Set[str] = set()
    if cfg.split_output_by_ticker:
        existing_tokens = {
            p.name[: -len("_holding.csv")]
            for p in output_dir.glob("*_holding.csv")
            if p.is_file()
        }
        for row in all_rows:
            ticker = (row.get("ticker") or "").strip()
            if ticker and sanitize_filename_token(ticker) in existing_tokens:
                completed_tickers.add(ticker)
    elif output_path.exists():
        try:
            shutil.copy(output_path, output_path.with_suffix(".csv.bak"))
        except Exception:
            pass

        try:
            with output_path.open("r", encoding="utf-8-sig") as output_file:
                for row in csv.DictReader(output_file):
                    ticker = (row.get("ticker") or "").strip()
                    if ticker:
                        completed_tickers.add(ticker)
        except Exception:
            pass

    logger.info("Resuming: found %s funds with holdings", f"{len(completed_tickers):,}")

    todo_rows = [row for row in all_rows if row.get("ticker", "").strip() not in completed_tickers]
    total_todo = len(todo_rows)
    logger.info("Workload: %s funds", f"{total_todo:,}")

    if total_todo == 0:
        logger.info("All done")
        return

    fieldnames = [
        "ticker",
        "name",
        "ticker_type",
        "allocation_type",
        "holding_name",
        "holding_ticker",
        "holding_type",
        "holding_symbol",
        "holding_url",
        "portfolio_weight_pct",
        "top_10_holdings_weight_pct",
        "other_holding_weight_pct",
        "source",
        "date_scraper",
        "url",
    ]

    file_handle = None
    writer = None

    if not cfg.split_output_by_ticker:
        if not output_path.exists() or os.stat(output_path).st_size == 0:
            with output_path.open("w", newline="", encoding="utf-8-sig") as output_file:
                csv.DictWriter(output_file, fieldnames=fieldnames).writeheader()

        file_handle = output_path.open("a", newline="", encoding="utf-8-sig")
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames)

    connector = aiohttp.TCPConnector(limit=cfg.concurrency + 50, ttl_dns_cache=300)
    semaphore = asyncio.Semaphore(cfg.concurrency)

    start_time = time.time()
    processed_count = 0
    rows_written = 0

    async with aiohttp.ClientSession(connector=connector, headers=get_random_headers()) as session:
        for index in range(0, total_todo, cfg.save_interval):
            batch_rows = todo_rows[index : index + cfg.save_interval]
            tasks = [process_one_fund(session, row, semaphore, cfg) for row in batch_rows]
            results_list = await asyncio.gather(*tasks)

            if cfg.split_output_by_ticker:
                flat_results: List[Dict] = []
                for row, result_rows in zip(batch_rows, results_list):
                    ticker = (row.get("ticker") or "").strip() or "unknown"
                    single_path = ticker_holding_file_path(output_dir, ticker)
                    write_single_ticker_csv(single_path, fieldnames, result_rows)
                    flat_results.extend(result_rows)
            else:
                flat_results = []
                for result_rows in results_list:
                    flat_results.extend(result_rows)
                if flat_results:
                    writer.writerows(flat_results)
                file_handle.flush()
                os.fsync(file_handle.fileno())

            processed_count += len(batch_rows)
            rows_written += len(flat_results)
            elapsed = time.time() - start_time
            speed = processed_count / elapsed if elapsed > 0 else 0.0
            eta_seconds = (total_todo - processed_count) / speed if speed > 0 else 0.0

            logger.info(
                "Funds=%s/%s Rows=%s Speed=%.1f funds/s ETA=%.1f min",
                processed_count,
                total_todo,
                rows_written,
                speed,
                eta_seconds / 60,
            )

    if file_handle:
        file_handle.close()

    logger.info("Holdings scrape completed")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Financial Times holdings acquisition scraper")
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--save-interval", type=int, default=100)
    parser.add_argument("--split-by-ticker", action="store_true", help="Write one file per ticker: <ticker>_holding.csv")
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--master-filename", default="financial_times_master_tickers.csv")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cfg = FinancialTimesHoldingsConfig(
        concurrency=args.concurrency,
        save_interval=args.save_interval,
        split_output_by_ticker=args.split_by_ticker,
        sample=args.sample,
        master_filename=args.master_filename,
    )
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
