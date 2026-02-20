import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("02_nav_loader")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PLACEHOLDER_NULLS = {"", "--", "N/A", "NA", "NONE", "NULL", "NAN"}


def _norm_text(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    if text.upper() in PLACEHOLDER_NULLS:
        return None
    return text


def _norm_date(value: Optional[str], fallback: Optional[str] = None) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        raw = (fallback or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _to_float(value: Optional[str]) -> Optional[float]:
    text = (value or "").strip()
    if not text or text.upper() in PLACEHOLDER_NULLS:
        return None
    cleaned = text.replace(",", "").replace("$", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _latest_file(base_dir: Path, filename: str) -> Optional[Path]:
    if not base_dir.exists():
        return None
    dirs = sorted([d for d in base_dir.iterdir() if d.is_dir()])
    for d in reversed(dirs):
        candidate = d / filename
        if candidate.exists():
            return candidate
    return None


def _load_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def clean_ft_nav(path: Path) -> Tuple[List[Tuple], Dict[str, int]]:
    stats = {"input": 0, "invalid": 0, "deduped": 0, "ready": 0}
    dedupe: Dict[Tuple[str, str], Tuple] = {}

    for row in _load_csv(path):
        stats["input"] += 1
        ft_ticker = _norm_text(row.get("ft_ticker"))
        ticker = _norm_text(row.get("ticker"))
        nav_as_of = _norm_date(row.get("nav_as_of"), fallback=row.get("date_scraper"))
        nav_price = _to_float(row.get("nav_price"))

        if not ft_ticker or not ticker or not nav_as_of or nav_price is None:
            stats["invalid"] += 1
            continue

        clean_row = (
            ft_ticker,
            ticker.upper(),
            _norm_text(row.get("name")) or ticker.upper(),
            _norm_text(row.get("ticker_type")) or "Unknown",
            nav_price,
            _norm_text(row.get("nav_currency")) or _norm_text(row.get("currency")),
            nav_as_of,
            _norm_text(row.get("source")) or "Financial Times",
            _norm_date(row.get("date_scraper"), fallback=nav_as_of) or nav_as_of,
            _norm_text(row.get("url")),
        )
        dedupe[(ft_ticker, nav_as_of)] = clean_row

    stats["deduped"] = stats["input"] - stats["invalid"] - len(dedupe)
    stats["ready"] = len(dedupe)
    return list(dedupe.values()), stats


def clean_common_nav(path: Path, default_source: str) -> Tuple[List[Tuple], Dict[str, int]]:
    stats = {"input": 0, "invalid": 0, "deduped": 0, "ready": 0}
    dedupe: Dict[Tuple[str, str], Tuple] = {}

    for row in _load_csv(path):
        stats["input"] += 1
        ticker = _norm_text(row.get("ticker"))
        as_of_date = _norm_date(row.get("as_of_date"), fallback=row.get("scrape_date"))
        nav_price = _to_float(row.get("nav_price"))
        if not ticker or not as_of_date or nav_price is None:
            stats["invalid"] += 1
            continue

        clean_row = (
            ticker.upper(),
            _norm_text(row.get("asset_type")) or "Unknown",
            _norm_text(row.get("source")) or default_source,
            nav_price,
            _norm_text(row.get("currency")) or "USD",
            as_of_date,
            _norm_date(row.get("scrape_date"), fallback=as_of_date) or as_of_date,
        )
        dedupe[(ticker.upper(), as_of_date)] = clean_row

    stats["deduped"] = stats["input"] - stats["invalid"] - len(dedupe)
    stats["ready"] = len(dedupe)
    return list(dedupe.values()), stats


def ensure_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_ft_daily_nav (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          ft_ticker VARCHAR(64) NOT NULL,
          ticker VARCHAR(32) NOT NULL,
          name VARCHAR(512) NOT NULL,
          ticker_type VARCHAR(32) NOT NULL,
          nav_price DECIMAL(20,8) NULL,
          nav_currency VARCHAR(16) NULL,
          nav_as_of DATE NULL,
          source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
          date_scraper DATE NOT NULL,
          url VARCHAR(1024) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_ft_nav_ft_ticker_asof (ft_ticker, nav_as_of),
          KEY idx_ft_nav_ticker (ticker),
          KEY idx_ft_nav_date_scraper (date_scraper)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_yf_daily_nav (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          ticker VARCHAR(32) NOT NULL,
          asset_type VARCHAR(32) NOT NULL,
          source VARCHAR(64) NOT NULL DEFAULT 'Yahoo Finance',
          nav_price DECIMAL(20,8) NULL,
          currency VARCHAR(16) NULL,
          as_of_date DATE NULL,
          scrape_date DATE NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_yf_nav_ticker_asof (ticker, as_of_date),
          KEY idx_yf_nav_asset_type (asset_type),
          KEY idx_yf_nav_scrape_date (scrape_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_sa_daily_nav (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          ticker VARCHAR(32) NOT NULL,
          asset_type VARCHAR(32) NOT NULL,
          source VARCHAR(64) NOT NULL DEFAULT 'Stock Analysis',
          nav_price DECIMAL(20,8) NULL,
          currency VARCHAR(16) NULL,
          as_of_date DATE NULL,
          scrape_date DATE NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_sa_nav_ticker_asof (ticker, as_of_date),
          KEY idx_sa_nav_asset_type (asset_type),
          KEY idx_sa_nav_scrape_date (scrape_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def upsert_daily_nav(ft_rows: List[Tuple], yf_rows: List[Tuple], sa_rows: List[Tuple]) -> None:
    db = get_db_config()
    import pymysql

    conn = pymysql.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        database=db.database,
        charset="utf8mb4",
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
            if ft_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_ft_daily_nav
                    (ft_ticker, ticker, name, ticker_type, nav_price, nav_currency, nav_as_of, source, date_scraper, url)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      ticker=VALUES(ticker),
                      name=VALUES(name),
                      ticker_type=VALUES(ticker_type),
                      nav_price=VALUES(nav_price),
                      nav_currency=VALUES(nav_currency),
                      source=VALUES(source),
                      date_scraper=VALUES(date_scraper),
                      url=VALUES(url),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    ft_rows,
                )
            if yf_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_daily_nav
                    (ticker, asset_type, source, nav_price, currency, as_of_date, scrape_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      asset_type=VALUES(asset_type),
                      source=VALUES(source),
                      nav_price=VALUES(nav_price),
                      currency=VALUES(currency),
                      scrape_date=VALUES(scrape_date),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    yf_rows,
                )
            if sa_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_daily_nav
                    (ticker, asset_type, source, nav_price, currency, as_of_date, scrape_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      asset_type=VALUES(asset_type),
                      source=VALUES(source),
                      nav_price=VALUES(nav_price),
                      currency=VALUES(currency),
                      scrape_date=VALUES(scrape_date),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    sa_rows,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and load FT/YF/SA daily NAV into DB staging.")
    parser.add_argument("--dry-run", action="store_true", help="Run cleaning only, skip DB load")
    args = parser.parse_args()

    ft_file = _latest_file(PROJECT_ROOT / "validation_output" / "Financial_Times" / "Daily_NAV", "financial_times_daily_nav.csv")
    yf_etf = _latest_file(PROJECT_ROOT / "data" / "02_performance" / "yahoo_finance", "yf_nav_etf.csv")
    yf_fund = _latest_file(PROJECT_ROOT / "data" / "02_performance" / "yahoo_finance", "yf_nav_fund.csv")
    sa_file = _latest_file(PROJECT_ROOT / "data" / "02_performance" / "stock_analysis", "sa_nav_etf.csv")

    if not ft_file or not yf_etf or not yf_fund or not sa_file:
        raise FileNotFoundError(f"Missing NAV files FT={ft_file} YF_ETF={yf_etf} YF_FUND={yf_fund} SA={sa_file}")

    logger.info("FT NAV file: %s", ft_file)
    logger.info("YF ETF NAV file: %s", yf_etf)
    logger.info("YF FUND NAV file: %s", yf_fund)
    logger.info("SA NAV file: %s", sa_file)

    ft_rows, ft_stats = clean_ft_nav(ft_file)
    yf_etf_rows, yf_etf_stats = clean_common_nav(yf_etf, default_source="Yahoo Finance")
    yf_fund_rows, yf_fund_stats = clean_common_nav(yf_fund, default_source="Yahoo Finance")
    sa_rows, sa_stats = clean_common_nav(sa_file, default_source="Stock Analysis")

    # Merge YF ETF + FUND to table constraint key(ticker,as_of_date)
    yf_dedupe: Dict[Tuple[str, str], Tuple] = {}
    for row in yf_etf_rows + yf_fund_rows:
        key = (row[0], row[5])  # ticker, as_of_date
        yf_dedupe[key] = row
    yf_rows = list(yf_dedupe.values())

    yf_stats = {
        "input": yf_etf_stats["input"] + yf_fund_stats["input"],
        "invalid": yf_etf_stats["invalid"] + yf_fund_stats["invalid"],
        "deduped": (yf_etf_stats["ready"] + yf_fund_stats["ready"]) - len(yf_rows),
        "ready": len(yf_rows),
    }

    logger.info("FT stats: %s", ft_stats)
    logger.info("YF stats: %s", yf_stats)
    logger.info("SA stats: %s", sa_stats)

    if args.dry_run:
        logger.info("Dry-run complete. No DB writes.")
        return

    upsert_daily_nav(ft_rows=ft_rows, yf_rows=yf_rows, sa_rows=sa_rows)
    logger.info("DB load completed: FT=%s YF=%s SA=%s", len(ft_rows), len(yf_rows), len(sa_rows))


if __name__ == "__main__":
    main()
