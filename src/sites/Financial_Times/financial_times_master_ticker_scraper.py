import argparse
import asyncio
import csv
import itertools
import random
import re
import string
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import aiohttp
import requests
from bs4 import BeautifulSoup
from src.utils.browser_utils import get_random_headers
from src.utils.logger import setup_logger


logger = setup_logger("FinancialTimesMasterTickerScraper")


# =========================================================
# Config
# =========================================================
@dataclass
class FinancialTimesScraperConfig:
    # Funds listing (AJAX)
    funds_api_url: str = "https://markets.ft.com/data/funds/ajax/update-screener-results"
    funds_main_url: str = "https://markets.ft.com/data/funds/uk/results"
    funds_params: str = "r:f"
    funds_items_per_page: int = 10

    # ETFs search
    etf_search_url: str = "https://markets.ft.com/data/search"
    etf_asset_class: str = "ETF"
    etf_query_mode: str = "light"  # light: a-z + 0-9

    # Network
    max_retries: int = 3
    backoff_factor: float = 1.5
    timeout_sec: int = 15
    list_concurrency: int = 50
    jitter_sleep: Tuple[float, float] = (0.01, 0.1)

    # Batching
    funds_page_batch: int = 200
    etf_query_batch: int = 25
    funds_fallback_max_page: int = 2000

    # SSL
    verify_ssl: bool = True

    # Sample mode
    sample_funds: int = 0
    sample_etfs: int = 0


# =========================================================
# Output schema
# =========================================================
CSV_COLUMNS = [
    "ft_ticker",
    "ticker",
    "name",
    "ticker_type",
    "source",
    "date_scraper",
    "url",
]


# =========================================================
# Headers + Helpers
# =========================================================
ACRONYMS_KEEP_UPPER = {
    "ETF",
    "ETN",
    "USA",
    "US",
    "UK",
    "UAE",
    "USD",
    "GBP",
    "EUR",
    "JPY",
    "CNY",
    "CNH",
    "HKD",
    "SGD",
    "THB",
    "LSE",
    "NYSE",
    "NASDAQ",
    "MSCI",
    "FTSE",
    "S&P",
    "SP",
    "UCITS",
    "OEIC",
    "SICAV",
    "JPM",
    "GS",
    "BNP",
    "HSBC",
}


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def make_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def clean_name(name: str) -> str:
    normalized = normalize_whitespace(name)
    if not normalized:
        return ""

    normalized = re.sub(r"^\([^\)]+\)\s*", "", normalized)
    normalized = re.sub(r"\s*\([^\)]+\)$", "", normalized)
    normalized = re.sub(r"^[\-–—]\s*", "", normalized).strip()

    words = normalized.split(" ")
    output = []

    for word in words:
        if not word:
            continue

        cleaned = re.sub(r"[^\w&/.-]", "", word)

        if cleaned.upper() in ACRONYMS_KEEP_UPPER:
            output.append(cleaned.upper())
            continue

        if (len(cleaned) >= 2 and any(c.isupper() for c in cleaned[1:])) or (
            cleaned.isupper() and len(cleaned) >= 2
        ):
            output.append(cleaned)
            continue

        if re.fullmatch(r"[ivxlcdm]+", cleaned.lower()):
            output.append(cleaned.upper())
            continue

        output.append(cleaned[:1].upper() + cleaned[1:].lower() if cleaned else word)

    return " ".join(output).strip()


# =========================================================
# HTTP with retries
# =========================================================
async def request_with_retries(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    cfg: FinancialTimesScraperConfig,
    headers=None,
    params=None,
    data=None,
    expect_json: bool = False,
):
    for attempt in range(cfg.max_retries):
        try:
            await asyncio.sleep(random.uniform(*cfg.jitter_sleep))
            async with session.request(
                method,
                url,
                headers=headers,
                params=params,
                data=data,
                timeout=aiohttp.ClientTimeout(total=cfg.timeout_sec),
            ) as response:
                if response.status == 200:
                    return await (response.json() if expect_json else response.text())

                if response.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(cfg.backoff_factor ** (attempt + 1))
                    continue

                return None
        except Exception:
            if attempt == cfg.max_retries - 1:
                return None
            await asyncio.sleep(0.5)

    return None


# =========================================================
# Funds List Logic
# =========================================================
def extract_ft_symbol_from_href(href: str) -> str:
    try:
        parsed = urlparse("https://markets.ft.com" + href)
        query_values = parse_qs(parsed.query)
        symbol = query_values.get("s", [None])[0]

        if symbol:
            return unquote(symbol).strip()

        if "s=" in href:
            return normalize_whitespace(href.split("s=")[-1])

        return ""
    except Exception:
        return ""


def parse_fund_rows(html_fragment: str, date_str: str) -> List[Dict[str, str]]:
    soup = make_soup(html_fragment)
    output = []

    for row in soup.find_all("tr"):
        columns = row.find_all("td")
        if not columns:
            continue

        link = columns[0].find("a")
        if not link or not link.get("href"):
            continue

        raw_name = link.get_text(strip=True)
        name = clean_name(raw_name)
        href = link["href"]
        url = "https://markets.ft.com" + href
        ft_ticker = extract_ft_symbol_from_href(href)

        if not ft_ticker or not name:
            continue

        ticker = ft_ticker.split(":")[0].strip()

        output.append(
            {
                "ft_ticker": ft_ticker,
                "ticker": ticker,
                "name": name,
                "ticker_type": "Fund",
                "source": "Financial Times",
                "date_scraper": date_str,
                "url": url,
            }
        )

    return output


def get_fund_total_count(main_url: str, verify_ssl: bool) -> int:
    try:
        response = requests.get(main_url, headers=get_random_headers(), timeout=15, verify=verify_ssl)
        soup = make_soup(response.text)

        result_info = soup.select_one(".mod-ui-table__results-info")
        if not result_info:
            result_text = soup.find(string=re.compile(r"Displaying", re.IGNORECASE))
            if result_text:
                result_info = result_text.parent

        if result_info:
            text = result_info.get_text().strip()
            numbers = re.findall(r"[\d,]+", text)
            if numbers:
                return int(numbers[-1].replace(",", ""))
    except Exception as exc:
        logger.error("Failed to get fund total count: %s", exc)

    return 0


async def fetch_fund_page(
    session: aiohttp.ClientSession,
    page: int,
    cfg: FinancialTimesScraperConfig,
    semaphore: asyncio.Semaphore,
    date_str: str,
) -> List[Dict[str, str]]:
    payload = {
        "page": str(page),
        "itemsPerPage": str(cfg.funds_items_per_page),
        "params": cfg.funds_params,
    }
    headers = get_random_headers()
    headers.update(
        {
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
    )

    async with semaphore:
        payload_json = await request_with_retries(
            session,
            "POST",
            cfg.funds_api_url,
            headers=headers,
            data=payload,
            expect_json=True,
            cfg=cfg,
        )

    html = (payload_json or {}).get("html", "") if isinstance(payload_json, dict) else ""
    return parse_fund_rows(html, date_str) if html else []


async def scrape_funds_full(
    session: aiohttp.ClientSession,
    cfg: FinancialTimesScraperConfig,
    date_str: str,
) -> List[Dict[str, str]]:
    logger.info("Funds: starting listing")
    total = get_fund_total_count(cfg.funds_main_url, verify_ssl=cfg.verify_ssl)
    collected: Dict[str, Dict[str, str]] = {}
    semaphore = asyncio.Semaphore(cfg.list_concurrency)

    if total > 0:
        total_pages = (total + cfg.funds_items_per_page - 1) // cfg.funds_items_per_page
        logger.info("Funds total=%s pages=%s", f"{total:,}", f"{total_pages:,}")

        page = 1
        while page <= total_pages:
            end = min(page + cfg.funds_page_batch - 1, total_pages)
            tasks = [fetch_fund_page(session, p, cfg, semaphore, date_str) for p in range(page, end + 1)]
            results = await asyncio.gather(*tasks)

            for result in results:
                for row in result:
                    collected[row["ft_ticker"]] = row

            if page % (cfg.funds_page_batch * 5) == 1:
                logger.info(
                    "Funds progress=%s-%s/%s unique=%s",
                    page,
                    end,
                    total_pages,
                    f"{len(collected):,}",
                )

            page = end + 1
            if cfg.sample_funds and len(collected) >= cfg.sample_funds:
                break
    else:
        logger.warning("Funds total count not found; switching to fallback mode")
        page = 1

        while True:
            tasks = [fetch_fund_page(session, page + offset, cfg, semaphore, date_str) for offset in range(50)]
            results = await asyncio.gather(*tasks)

            found_in_batch = 0
            for result in results:
                if not result:
                    continue
                found_in_batch += len(result)
                for row in result:
                    collected[row["ft_ticker"]] = row

            logger.info(
                "Funds fallback page=%s-%s found=%s total_unique=%s",
                page,
                page + 49,
                found_in_batch,
                f"{len(collected):,}",
            )

            if found_in_batch == 0:
                logger.info("Funds end of list detected")
                break

            page += 50
            if cfg.sample_funds and len(collected) >= cfg.sample_funds:
                break
            if page > cfg.funds_fallback_max_page:
                logger.warning("Funds reached safety cap (%s pages)", f"{cfg.funds_fallback_max_page:,}")
                break

    rows = sorted(collected.values(), key=lambda x: x["ft_ticker"])
    if cfg.sample_funds:
        rows = rows[: cfg.sample_funds]
    logger.info("Funds completed unique_rows=%s", f"{len(rows):,}")
    return rows


# =========================================================
# ETF Logic
# =========================================================
def parse_etf_rows(html: str, date_str: str) -> List[Dict[str, str]]:
    soup = make_soup(html)
    output = []

    for row in soup.select('table[class*="mod-ui-table"] tbody tr'):
        columns = row.find_all("td")
        if len(columns) < 2:
            continue

        raw_name = columns[0].get_text(" ", strip=True)
        ft_ticker = normalize_whitespace(columns[1].get_text(" ", strip=True))
        if not ft_ticker:
            continue

        ticker = ft_ticker.split(":")[0].strip()
        url = f"https://markets.ft.com/data/etfs/tearsheet/summary?s={ft_ticker}"

        output.append(
            {
                "ft_ticker": ft_ticker,
                "ticker": ticker,
                "name": clean_name(raw_name),
                "ticker_type": "ETF",
                "source": "Financial Times",
                "date_scraper": date_str,
                "url": url,
            }
        )

    return output


async def fetch_etf_query(
    session: aiohttp.ClientSession,
    query: str,
    cfg: FinancialTimesScraperConfig,
    semaphore: asyncio.Semaphore,
    date_str: str,
) -> List[Dict[str, str]]:
    params = {"query": query, "assetClass": cfg.etf_asset_class}

    async with semaphore:
        html = await request_with_retries(
            session,
            "GET",
            cfg.etf_search_url,
            headers=get_random_headers(),
            params=params,
            cfg=cfg,
        )

    return parse_etf_rows(html, date_str) if html else []


async def scrape_etfs_full(
    session: aiohttp.ClientSession,
    cfg: FinancialTimesScraperConfig,
    date_str: str,
) -> List[Dict[str, str]]:
    logger.info("ETFs: starting listing")
    semaphore = asyncio.Semaphore(cfg.list_concurrency)
    collected: Dict[str, Dict[str, str]] = {}

    base_chars = list(string.ascii_lowercase) + [str(i) for i in range(10)]
    queries = list(base_chars)
    if cfg.etf_query_mode == "full":
        queries += ["".join(pair) for pair in itertools.product(base_chars, repeat=2)]

    for index in range(0, len(queries), cfg.etf_query_batch):
        batch = queries[index : index + cfg.etf_query_batch]
        tasks = [fetch_etf_query(session, q, cfg, semaphore, date_str) for q in batch]
        results = await asyncio.gather(*tasks)

        for result in results:
            for row in result:
                collected[row["ft_ticker"]] = row

        if index % 100 == 0:
            logger.info("ETF progress=%s/%s unique=%s", index, len(queries), f"{len(collected):,}")

        if cfg.sample_etfs and len(collected) >= cfg.sample_etfs:
            break

    rows = sorted(collected.values(), key=lambda x: x["ft_ticker"])
    if cfg.sample_etfs:
        rows = rows[: cfg.sample_etfs]
    logger.info("ETFs completed unique_rows=%s", f"{len(rows):,}")
    return rows


# =========================================================
# Main Runner
# =========================================================
async def run_scraper(cfg: FinancialTimesScraperConfig) -> None:
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path("validation_output") / "Financial_Times" / "master_tickers" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)

    output_csv_path = output_dir / "financial_times_master_tickers.csv"

    connector = aiohttp.TCPConnector(
        limit=cfg.list_concurrency + 50,
        ssl=None if cfg.verify_ssl else False,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        funds_task = scrape_funds_full(session, cfg, date_str)
        etfs_task = scrape_etfs_full(session, cfg, date_str)
        funds, etfs = await asyncio.gather(funds_task, etfs_task)

    all_rows = funds + etfs

    with output_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info("Completed Financial Times master ticker scrape")
    logger.info("Funds=%s ETFs=%s Total=%s", f"{len(funds):,}", f"{len(etfs):,}", f"{len(all_rows):,}")
    logger.info("Output=%s", output_csv_path)


# =========================================================
# CLI
# =========================================================
def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape Financial Times fund and ETF master tickers")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Concurrent listing requests (default: 50)",
    )
    parser.add_argument(
        "--etf-mode",
        choices=["light", "full"],
        default="light",
        help="ETF query mode: light (a-z + 0-9) or full (a-z0-9 + 2-char alnum prefixes)",
    )
    parser.add_argument(
        "--funds-params",
        type=str,
        default="r:f",
        help="FT funds screener params (default: r:f for broader/global coverage)",
    )
    parser.add_argument(
        "--sample-funds",
        type=int,
        default=0,
        help="Limit number of fund rows collected (0 = all)",
    )
    parser.add_argument(
        "--sample-etfs",
        type=int,
        default=0,
        help="Limit number of ETF rows collected (0 = all)",
    )
    parser.add_argument(
        "--funds-fallback-max-page",
        type=int,
        default=2000,
        help="Safety cap for fallback funds paging (default: 2000)",
    )
    return parser


def main() -> None:
    args = build_cli_parser().parse_args()

    cfg = FinancialTimesScraperConfig()
    cfg.list_concurrency = args.concurrency
    cfg.etf_query_mode = args.etf_mode
    cfg.funds_params = args.funds_params
    cfg.funds_fallback_max_page = args.funds_fallback_max_page
    cfg.sample_funds = args.sample_funds
    cfg.sample_etfs = args.sample_etfs

    asyncio.run(run_scraper(cfg))


if __name__ == "__main__":
    main()
