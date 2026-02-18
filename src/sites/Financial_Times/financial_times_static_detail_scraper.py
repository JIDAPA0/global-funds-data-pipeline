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
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from src.utils.browser_utils import get_random_headers
from src.utils.logger import setup_logger
from src.utils.path_manager import VAL_FT_DIR, VAL_FT_STATIC


try:
    from zoneinfo import ZoneInfo

    TZ_BANGKOK = ZoneInfo("Asia/Bangkok")
except Exception:
    TZ_BANGKOK = None


def now_bangkok() -> datetime:
    return datetime.now(TZ_BANGKOK) if TZ_BANGKOK else datetime.now()


def today_yyyymmdd() -> str:
    return now_bangkok().strftime("%Y-%m-%d")


logger = setup_logger("03_static_ft_master_detail")


@dataclass
class FinancialTimesStaticDetailConfig:
    master_base_dir: Path = VAL_FT_DIR / "master_tickers"
    master_filename: str = "financial_times_master_tickers.csv"

    output_base_dir: Path = VAL_FT_STATIC
    output_filename: str = "financial_times_static_detail.csv"

    concurrency: int = 60
    save_interval: int = 100
    request_timeout_sec: int = 12
    max_retries: int = 1
    fsync_every_batches: int = 10
    use_pandas_fallback: bool = False
    max_items_per_run: int = 100
    sample: int = 0


def resolve_paths(cfg: FinancialTimesStaticDetailConfig) -> Tuple[Path, Path]:
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


def get_ft_ticker(row: Dict[str, str]) -> str:
    for key in ("ft_ticker", "\ufeffft_ticker"):
        parsed = (row.get(key) or "").strip()
        if parsed:
            return parsed
    return ""


def parse_aum_string(raw: str) -> Dict[str, str]:
    output = {"value": "", "unit": "", "currency": "", "as_of": "", "full_value": ""}
    if not raw or raw == "--":
        return output

    cleaned = normalize_text(raw)
    date_match = re.search(r"\((?:as of)?\s*(.*?)\)", cleaned, re.IGNORECASE) or re.search(
        r"As of\s+(.*)",
        cleaned,
        re.IGNORECASE,
    )
    if date_match:
        date_text = date_match.group(1).strip()
        for fmt in ("%b %d %Y", "%d %b %Y"):
            try:
                output["as_of"] = datetime.strptime(date_text, fmt).strftime("%Y-%m-%d")
                break
            except Exception:
                continue
        if not output["as_of"]:
            output["as_of"] = date_text

    main_part = re.sub(r"\(.*?\)|As of.*", "", cleaned, flags=re.IGNORECASE).strip()
    value_match = re.search(r"^([\d,\.]+)\s*([kKmMbBtT]?n?)\s*([A-Za-z]{3})?", main_part)
    if not value_match:
        return output

    value_raw = value_match.group(1).replace(",", "")
    unit_raw = value_match.group(2).lower()
    currency_raw = (value_match.group(3) or "").upper()

    output["value"] = value_raw
    output["unit"] = unit_raw
    output["currency"] = currency_raw

    try:
        value_float = float(value_raw)
        multiplier = 1.0
        if "k" in unit_raw:
            multiplier = 1_000.0
        elif "m" in unit_raw:
            multiplier = 1_000_000.0
        elif "b" in unit_raw:
            multiplier = 1_000_000_000.0
        elif "t" in unit_raw:
            multiplier = 1_000_000_000_000.0
        output["full_value"] = f"{value_float * multiplier:.2f}"
    except Exception:
        pass

    return output


def extract_key_value_from_table_bs4(table) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for row in table.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        key = normalize_text(cells[0].get_text(" ", strip=True))
        value = normalize_text(cells[1].get_text(" ", strip=True))
        if key and key.lower() != "nan" and value and value.lower() != "nan":
            output[key.lower()] = value
    return output


def extract_tables_with_pandas(html: str) -> Dict[str, str]:
    combined: Dict[str, str] = {}
    try:
        dataframes = pd.read_html(StringIO(html))
        for dataframe in dataframes:
            if dataframe.shape[1] < 2:
                continue
            for _, row in dataframe.iterrows():
                key = normalize_text(row[0])
                value = normalize_text(row[1])
                if key and key.lower() != "nan" and value and value.lower() != "nan":
                    combined[key.lower()] = value
    except Exception:
        pass
    return combined


def extract_tables_with_bs4(html: str) -> Dict[str, str]:
    combined: Dict[str, str] = {}
    try:
        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        for table in tables[:2]:
            parsed = extract_key_value_from_table_bs4(table)
            if parsed:
                combined.update(parsed)
    except Exception:
        pass
    return combined


def first_non_empty(mapping: Dict[str, str], keys: List[str]) -> str:
    for key in keys:
        value = normalize_text(mapping.get(key.lower(), ""))
        if value:
            return value
    return ""


def parse_ft_date(raw: str) -> str:
    cleaned = normalize_text(raw)
    if not cleaned:
        return ""
    for fmt in ("%d %b %Y", "%b %d %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return cleaned


def find_aum_in_text(text: str) -> str:
    patterns = [
        r"(?:total\s+net\s+assets|net\s+assets|fund\s+size|share\s+class\s+size)\s*[:\-]?\s*([\d\.,]+\s*[kmbt]n?\s*[A-Z]{3}(?:\s+As of\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{4})?)",
        r"([\d\.,]+\s*[kmbt]n?\s*[A-Z]{3}\s+As of\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return normalize_text(match.group(1))
    return ""


def parse_expense_pct(raw: str) -> str:
    cleaned = normalize_text(raw)
    if not cleaned:
        return ""
    match = re.search(r"([\d\.]+)\s*%", cleaned)
    return match.group(1) if match else ""


def find_expense_in_text(text: str) -> str:
    match = re.search(
        r"(?:ongoing\s+charge|net\s+expense\s+ratio|max\s+annual\s+charge|expense\s+ratio)\s*[:\-]?\s*([\d\.]+\s*%)",
        text,
        flags=re.IGNORECASE,
    )
    return normalize_text(match.group(1)) if match else ""


def parse_details(html: str, cfg: FinancialTimesStaticDetailConfig) -> Dict[str, str]:
    if not html:
        return {}

    key_values = extract_tables_with_bs4(html)
    page_text = normalize_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))

    if cfg.use_pandas_fallback and not key_values:
        fallback_values = extract_tables_with_pandas(html)
        if fallback_values:
            key_values.update(fallback_values)

    isin = first_non_empty(key_values, ["isin", "isin code"])
    domicile = first_non_empty(key_values, ["domicile", "fund domicile"])
    inception = parse_ft_date(first_non_empty(key_values, ["launch date", "inception date", "fund launch date"]))
    category = first_non_empty(key_values, ["morningstar category", "category"])
    income = first_non_empty(key_values, ["income treatment", "income"])

    expense_raw = first_non_empty(
        key_values,
        [
            "ongoing charge",
            "net expense ratio",
            "max annual charge",
            "expense ratio",
            "ter",
            "total expense ratio",
        ],
    )
    if not expense_raw:
        expense_raw = find_expense_in_text(page_text)
    expense_pct = parse_expense_pct(expense_raw)

    aum_raw = first_non_empty(
        key_values,
        [
            "fund size",
            "total net assets",
            "net assets",
            "share class size",
            "total assets",
            "aum",
            "assets under management",
        ],
    )
    if not aum_raw:
        aum_raw = find_aum_in_text(page_text)
    aum_data = parse_aum_string(aum_raw)

    return {
        "morningstar_category": category,
        "inception_date": inception,
        "domicile": domicile,
        "scraped_isin": isin,
        "assets_aum_raw": aum_raw,
        "assets_aum_value": aum_data["value"],
        "assets_aum_unit": aum_data["unit"],
        "assets_aum_currency": aum_data["currency"],
        "assets_aum_full_value": aum_data["full_value"],
        "assets_aum_as_of": aum_data["as_of"],
        "expense_ratio_raw": expense_raw,
        "expense_pct": expense_pct,
        "income_treatment": income,
    }


async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    cfg: FinancialTimesStaticDetailConfig,
) -> Optional[str]:
    for attempt in range(cfg.max_retries + 1):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=cfg.request_timeout_sec),
                headers=get_random_headers(),
            ) as response:
                if response.status == 200:
                    return await response.text()
                if response.status == 404:
                    return None
        except Exception:
            pass

        if attempt < cfg.max_retries:
            await asyncio.sleep(random.uniform(0.2, 0.8) * (attempt + 1))

    return None


def build_summary_url(ft_ticker: str, ticker_type: str) -> str:
    base = "etfs" if str(ticker_type).strip().lower() == "etf" else "funds"
    return f"https://markets.ft.com/data/{base}/tearsheet/summary?s={ft_ticker}"


async def process_one(
    session: aiohttp.ClientSession,
    row: Dict[str, str],
    semaphore: asyncio.Semaphore,
    cfg: FinancialTimesStaticDetailConfig,
) -> Dict[str, str]:
    ft_ticker = get_ft_ticker(row)
    ticker_type = row.get("ticker_type", "Fund").strip()
    url = row.get("url", "") or build_summary_url(ft_ticker, ticker_type)

    async with semaphore:
        html = await fetch_html(session, url, cfg)

    details = parse_details(html, cfg)

    input_isin = row.get("isin_number", "").strip() or row.get("isin", "").strip()
    scraped_isin = details.pop("scraped_isin", "")
    final_isin = input_isin if input_isin else scraped_isin

    row_output = row.copy()
    row_output.update(details)
    row_output["isin_number"] = final_isin
    row_output["source"] = "Financial Times"
    row_output["date_scraper"] = today_yyyymmdd()
    row_output["url"] = url
    return row_output


async def run(cfg: FinancialTimesStaticDetailConfig) -> None:
    master_path, output_path = resolve_paths(cfg)
    logger.info("Input: %s", master_path)
    logger.info("Output: %s", output_path)

    if not master_path.exists():
        logger.error("Master file not found at %s", master_path)
        return

    with master_path.open("r", encoding="utf-8-sig") as master_file:
        all_rows = list(csv.DictReader(master_file))

    if cfg.sample:
        all_rows = all_rows[: cfg.sample]

    completed_ids: Set[str] = set()
    if output_path.exists():
        try:
            shutil.copy(output_path, output_path.with_suffix(".csv.bak"))
        except Exception:
            pass

        try:
            with output_path.open("r", encoding="utf-8-sig") as output_file:
                for row in csv.DictReader(output_file):
                    ft_ticker = get_ft_ticker(row)
                    if ft_ticker:
                        completed_ids.add(ft_ticker)
        except Exception:
            pass

    logger.info("Resuming: found %s completed records", f"{len(completed_ids):,}")

    todo_rows = [row for row in all_rows if get_ft_ticker(row) not in completed_ids]
    if cfg.max_items_per_run and cfg.max_items_per_run > 0:
        todo_rows = todo_rows[: cfg.max_items_per_run]

    total_todo = len(todo_rows)
    logger.info("Workload: %s items", f"{total_todo:,}")
    if cfg.max_items_per_run and cfg.max_items_per_run > 0:
        logger.info("Step mode: processing max %s items", cfg.max_items_per_run)

    if total_todo == 0:
        logger.info("All done")
        return

    fieldnames = [
        "ft_ticker",
        "ticker",
        "name",
        "ticker_type",
        "morningstar_category",
        "inception_date",
        "domicile",
        "isin_number",
        "assets_aum_raw",
        "assets_aum_full_value",
        "assets_aum_value",
        "assets_aum_unit",
        "assets_aum_currency",
        "assets_aum_as_of",
        "expense_ratio_raw",
        "expense_pct",
        "income_treatment",
        "source",
        "date_scraper",
        "url",
    ]

    if not output_path.exists() or os.stat(output_path).st_size == 0:
        with output_path.open("w", newline="", encoding="utf-8-sig") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()

    connector = aiohttp.TCPConnector(limit=cfg.concurrency + 50, ttl_dns_cache=300)
    semaphore = asyncio.Semaphore(cfg.concurrency)

    start_time = time.time()
    processed_count = 0
    batch_count = 0

    with output_path.open("a", newline="", encoding="utf-8-sig") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)

        async with aiohttp.ClientSession(connector=connector, headers=get_random_headers()) as session:
            for index in range(0, total_todo, cfg.save_interval):
                batch_rows = todo_rows[index : index + cfg.save_interval]
                tasks = [process_one(session, row, semaphore, cfg) for row in batch_rows]
                results = await asyncio.gather(*tasks)

                cleaned_results = []
                for result in results:
                    cleaned_results.append({key: result.get(key, "") for key in fieldnames})

                writer.writerows(cleaned_results)

                batch_count += 1
                if batch_count % cfg.fsync_every_batches == 0:
                    output_file.flush()
                    os.fsync(output_file.fileno())

                processed_count += len(results)
                elapsed = time.time() - start_time
                speed = processed_count / elapsed if elapsed > 0 else 0.0
                eta_seconds = (total_todo - processed_count) / speed if speed > 0 else 0.0
                logger.info(
                    "Speed=%.1f/s Progress=%s/%s ETA=%.1f min",
                    speed,
                    processed_count,
                    total_todo,
                    eta_seconds / 60,
                )

        output_file.flush()
        os.fsync(output_file.fileno())

    logger.info("Static detail scrape completed")
    logger.info("Output: %s", output_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Financial Times master detail static scraper")
    parser.add_argument("--concurrency", type=int, default=60)
    parser.add_argument("--save-interval", type=int, default=100)
    parser.add_argument("--max-items-per-run", type=int, default=100, help="0 = process all pending items")
    parser.add_argument("--full-run", action="store_true", help="Process all pending items in one run")
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--master-filename", default="financial_times_master_tickers.csv")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    cfg = FinancialTimesStaticDetailConfig(
        concurrency=args.concurrency,
        save_interval=args.save_interval,
        sample=args.sample,
        master_filename=args.master_filename,
        max_items_per_run=(0 if args.full_run else args.max_items_per_run),
    )
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
