import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from src.utils.logger import setup_logger
from src.utils.path_manager import DATA_PERFORMANCE_DIR, VAL_YF_DIR


LOGGER_MAP = {
    "ETF": "02_perf_yf_etf_nav",
    "FUND": "02_perf_yf_fund_nav",
}

SOURCE_NAME = "Yahoo Finance"


@dataclass
class YahooFinanceNavConfig:
    asset_type: str
    batch_size: int = 40
    normal_delay_sec: int = 2
    cool_down_delay_sec: int = 120
    sample: int = 0

    @property
    def current_date(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    @property
    def output_dir(self) -> Path:
        return DATA_PERFORMANCE_DIR / "yahoo_finance" / self.current_date

    @property
    def output_file(self) -> Path:
        return self.output_dir / f"yf_nav_{self.asset_type.lower()}.csv"

    @property
    def error_file(self) -> Path:
        return self.output_dir / f"yf_errors_{self.asset_type.lower()}.csv"


def get_custom_session() -> requests.Session:
    session = requests.Session()
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    ]
    session.headers.update(
        {
            "User-Agent": random.choice(user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
    )
    return session


def get_processed_tickers(output_file: Path) -> Set[str]:
    if not output_file.exists():
        return set()
    try:
        df = pd.read_csv(output_file, usecols=["ticker"])
        return set(df["ticker"].astype(str).str.strip().tolist())
    except Exception:
        return set()


def _fetch_tickers_from_db(source_name: str, asset_type: str) -> Optional[List[str]]:
    try:
        from src.utils.db_connector import get_active_tickers
    except Exception:
        return None

    rows = get_active_tickers(source_name)
    tickers = [
        row["ticker"]
        for row in rows
        if str(row.get("asset_type", "")).upper() == asset_type.upper() and row.get("ticker")
    ]
    return tickers


def _fetch_tickers_from_latest_master(asset_type: str) -> List[str]:
    master_base = VAL_YF_DIR / "master_tickers"
    if not master_base.exists():
        return []

    date_dirs = sorted([d for d in master_base.iterdir() if d.is_dir()])
    if not date_dirs:
        return []

    latest_master = date_dirs[-1] / "yf_ticker.csv"
    if not latest_master.exists():
        return []

    try:
        df = pd.read_csv(latest_master, encoding="utf-8-sig")
        filtered = df[df["ticker_type"].astype(str).str.upper() == asset_type.upper()]
        return filtered["ticker"].astype(str).str.strip().dropna().unique().tolist()
    except Exception:
        return []


def get_target_tickers(asset_type: str) -> List[str]:
    db_tickers = _fetch_tickers_from_db(SOURCE_NAME, asset_type)
    if db_tickers is not None:
        return db_tickers
    return _fetch_tickers_from_latest_master(asset_type)


def fetch_via_web_scraping(ticker: str, cfg: YahooFinanceNavConfig) -> Optional[Dict]:
    url = f"https://finance.yahoo.com/quote/{ticker}"
    try:
        response = get_custom_session().get(url, timeout=10)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "lxml")
        price_tag = soup.find("fin-streamer", {"data-field": "regularMarketPrice"})
        if not price_tag:
            price_tag = soup.find("fin-streamer", {"data-field": "regularMarketOpen"})

        if price_tag and price_tag.text:
            raw_price = price_tag.text.replace(",", "").strip()
            nav_price = float(raw_price)
            return {
                "ticker": ticker,
                "asset_type": cfg.asset_type,
                "source": SOURCE_NAME,
                "nav_price": nav_price,
                "currency": "USD",
                "as_of_date": cfg.current_date,
                "scrape_date": cfg.current_date,
            }
    except Exception:
        return None

    return None


def fetch_single_ticker_retry(ticker: str, cfg: YahooFinanceNavConfig) -> Optional[Dict]:
    try:
        time.sleep(random.uniform(0.5, 1.0))
        ticker_data = yf.Ticker(ticker, session=get_custom_session())
        history = ticker_data.history(period="5d")

        if not history.empty:
            last_valid = history["Close"].dropna().tail(1)
            if not last_valid.empty:
                return {
                    "ticker": ticker,
                    "asset_type": cfg.asset_type,
                    "source": SOURCE_NAME,
                    "nav_price": float(last_valid.iloc[0]),
                    "currency": ticker_data.fast_info.get("currency", "USD"),
                    "as_of_date": last_valid.index[0].strftime("%Y-%m-%d"),
                    "scrape_date": cfg.current_date,
                }
    except Exception:
        pass

    return fetch_via_web_scraping(ticker, cfg)


def fetch_batch_data(tickers: List[str], cfg: YahooFinanceNavConfig):
    results = []
    failed_candidates = []
    need_cool_down = False

    try:
        tickers_str = " ".join(tickers)
        data = yf.download(tickers_str, period="1mo", group_by="ticker", threads=True, progress=False)

        for ticker in tickers:
            try:
                frame = data if len(tickers) == 1 else (data[ticker] if ticker in data else pd.DataFrame())
                valid = False
                if not frame.empty and "Close" in frame.columns:
                    last_valid = frame["Close"].dropna().tail(1)
                    if not last_valid.empty:
                        results.append(
                            {
                                "ticker": ticker,
                                "asset_type": cfg.asset_type,
                                "source": SOURCE_NAME,
                                "nav_price": float(last_valid.iloc[0]),
                                "currency": "USD",
                                "as_of_date": last_valid.index[0].strftime("%Y-%m-%d"),
                                "scrape_date": cfg.current_date,
                            }
                        )
                        valid = True
                if not valid:
                    failed_candidates.append(ticker)
            except Exception:
                failed_candidates.append(ticker)
    except Exception:
        failed_candidates = tickers

    real_fails = []
    if failed_candidates:
        if len(failed_candidates) > (len(tickers) * 0.5):
            need_cool_down = True

        for ticker in failed_candidates:
            result = fetch_single_ticker_retry(ticker, cfg)
            if result:
                results.append(result)
            else:
                real_fails.append(ticker)

    return results, real_fails, need_cool_down


def _try_insert_dataframe(df: pd.DataFrame) -> None:
    try:
        from src.utils.db_connector import insert_dataframe
    except Exception:
        return

    try:
        insert_dataframe(df, "stg_daily_nav")
    except Exception:
        pass


def run_nav_scraper(cfg: YahooFinanceNavConfig) -> None:
    logger = setup_logger(LOGGER_MAP.get(cfg.asset_type.upper(), "02_perf_yf_nav"))
    start_time = time.time()
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching %s tickers...", cfg.asset_type)
    target_tickers = get_target_tickers(cfg.asset_type)
    if cfg.sample > 0:
        target_tickers = target_tickers[: cfg.sample]

    if not target_tickers:
        logger.warning("No %s tickers found.", cfg.asset_type)
        return

    processed_tickers = get_processed_tickers(cfg.output_file)
    todos = [t for t in target_tickers if t not in processed_tickers]

    logger.info("Summary total=%s skipped=%s remaining=%s", len(target_tickers), len(processed_tickers), len(todos))

    if not todos:
        logger.info("All tasks completed for today.")
        return

    success_count = 0
    fail_count = 0

    for i in range(0, len(todos), cfg.batch_size):
        batch = todos[i : i + cfg.batch_size]
        results, fails, need_cool_down = fetch_batch_data(batch, cfg)

        if results:
            result_df = pd.DataFrame(results)
            _try_insert_dataframe(result_df)
            result_df.to_csv(
                cfg.output_file,
                mode="a",
                header=not cfg.output_file.exists(),
                index=False,
            )
            success_count += len(results)

        if fails:
            error_df = pd.DataFrame(
                {
                    "ticker": fails,
                    "reason": "Failed L3 Scraping",
                    "scraped_at": datetime.now(),
                }
            )
            error_df.to_csv(
                cfg.error_file,
                mode="a",
                header=not cfg.error_file.exists(),
                index=False,
            )
            fail_count += len(fails)

        current_batch = i // cfg.batch_size + 1
        total_batches = (len(todos) // cfg.batch_size) + 1
        logger.info(
            "Batch %s/%s | OK=%s | FAIL=%s | TotalSuccess=%s",
            current_batch,
            total_batches,
            len(results),
            len(fails),
            success_count,
        )

        if need_cool_down:
            logger.warning("Cool Down Mode: sleeping %ss", cfg.cool_down_delay_sec)
            time.sleep(cfg.cool_down_delay_sec)
        else:
            time.sleep(cfg.normal_delay_sec)

    total_duration = time.time() - start_time
    logger.info("=" * 50)
    logger.info("Finished. Total Time: %.2f min", total_duration / 60)
    logger.info("New Success: %s", success_count)
    logger.info("New Failed: %s", fail_count)
    logger.info("Output: %s", cfg.output_file)
    logger.info("=" * 50)
