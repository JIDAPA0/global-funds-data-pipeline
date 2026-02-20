import argparse
import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.utils.logger import setup_logger


logger = setup_logger("01_master_loader")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

PLACEHOLDER_NULLS = {"", "--", "N/A", "NA", "NONE", "NULL", "NAN"}


@dataclass
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


def _load_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    data: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def get_db_config() -> DbConfig:
    env_file = _load_env_file(PROJECT_ROOT / ".env")
    host = os.getenv("MYSQL_HOST") or env_file.get("MYSQL_HOST") or "localhost"
    port_raw = os.getenv("MYSQL_PORT") or env_file.get("MYSQL_PORT") or "3308"
    database = os.getenv("MYSQL_DATABASE") or env_file.get("MYSQL_DATABASE") or "funds_db"
    user = os.getenv("MYSQL_USER") or env_file.get("MYSQL_USER") or "funds_user"
    password = os.getenv("MYSQL_PASSWORD") or env_file.get("MYSQL_PASSWORD") or "funds_pass"
    return DbConfig(host=host, port=int(port_raw), database=database, user=user, password=password)


def latest_file(base_dir: Path, filename: str) -> Optional[Path]:
    if not base_dir.exists():
        return None
    dirs = sorted([d for d in base_dir.iterdir() if d.is_dir()])
    for directory in reversed(dirs):
        candidate = directory / filename
        if candidate.exists():
            return candidate
    return None


def _norm(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    if text.upper() in PLACEHOLDER_NULLS:
        return None
    return text


def _date_or_today(value: Optional[str]) -> str:
    raw = (value or "").strip()
    if not raw:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return datetime.now().strftime("%Y-%m-%d")


def clean_ft_rows(path: Path) -> Tuple[List[Tuple], Dict[str, int]]:
    stats = {"input": 0, "invalid": 0, "deduped": 0, "ready": 0}
    by_key: Dict[str, Tuple] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            stats["input"] += 1
            ft_ticker = _norm(row.get("ft_ticker"))
            ticker = _norm(row.get("ticker"))
            if not ft_ticker or not ticker:
                stats["invalid"] += 1
                continue
            cleaned_name = _norm(row.get("name")) or ticker.upper()
            out = (
                ft_ticker,
                ticker.upper(),
                cleaned_name,
                _norm(row.get("ticker_type")) or "Unknown",
                _norm(row.get("source")) or "Financial Times",
                _date_or_today(row.get("date_scraper")),
                _norm(row.get("url")),
            )
            by_key[ft_ticker] = out
    stats["deduped"] = stats["input"] - stats["invalid"] - len(by_key)
    stats["ready"] = len(by_key)
    return list(by_key.values()), stats


def clean_common_master_rows(path: Path, source_name: str) -> Tuple[List[Tuple], Dict[str, int]]:
    stats = {"input": 0, "invalid": 0, "deduped": 0, "ready": 0}
    by_key: Dict[str, Tuple] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            stats["input"] += 1
            ticker = _norm(row.get("ticker"))
            if not ticker:
                stats["invalid"] += 1
                continue
            cleaned_ticker = ticker.upper()
            cleaned_name = _norm(row.get("name")) or cleaned_ticker
            out = (
                cleaned_ticker,
                cleaned_name,
                _norm(row.get("ticker_type")) or "Unknown",
                _norm(row.get("source")) or source_name,
                _date_or_today(row.get("date_scraper")),
                _norm(row.get("url")),
            )
            by_key[cleaned_ticker] = out
    stats["deduped"] = stats["input"] - stats["invalid"] - len(by_key)
    stats["ready"] = len(by_key)
    return list(by_key.values()), stats


def upsert_master_rows(db: DbConfig, ft_rows: List[Tuple], yf_rows: List[Tuple], sa_rows: List[Tuple]) -> None:
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError("Missing dependency 'pymysql'. Install with: pip install pymysql") from exc

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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_ft_master_ticker (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ft_ticker VARCHAR(64) NOT NULL,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NOT NULL,
                  ticker_type VARCHAR(32) NOT NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
                  date_scraper DATE NOT NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_ft_master_ft_ticker (ft_ticker),
                  KEY idx_ft_master_ticker (ticker),
                  KEY idx_ft_master_type (ticker_type),
                  KEY idx_ft_master_date_scraper (date_scraper)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_master_ticker (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NOT NULL,
                  ticker_type VARCHAR(32) NOT NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Yahoo Finance',
                  date_scraper DATE NOT NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_yf_master_ticker (ticker),
                  KEY idx_yf_master_type (ticker_type),
                  KEY idx_yf_master_date_scraper (date_scraper)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_sa_master_ticker (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NOT NULL,
                  ticker_type VARCHAR(32) NOT NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Stock Analysis',
                  date_scraper DATE NOT NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_sa_master_ticker (ticker),
                  KEY idx_sa_master_type (ticker_type),
                  KEY idx_sa_master_date_scraper (date_scraper)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )

            if ft_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_ft_master_ticker
                    (ft_ticker, ticker, name, ticker_type, source, date_scraper, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      ticker=VALUES(ticker),
                      name=VALUES(name),
                      ticker_type=VALUES(ticker_type),
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
                    INSERT INTO stg_yf_master_ticker
                    (ticker, name, ticker_type, source, date_scraper, url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      name=VALUES(name),
                      ticker_type=VALUES(ticker_type),
                      source=VALUES(source),
                      date_scraper=VALUES(date_scraper),
                      url=VALUES(url),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    yf_rows,
                )
            if sa_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_master_ticker
                    (ticker, name, ticker_type, source, date_scraper, url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      name=VALUES(name),
                      ticker_type=VALUES(ticker_type),
                      source=VALUES(source),
                      date_scraper=VALUES(date_scraper),
                      url=VALUES(url),
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
    parser = argparse.ArgumentParser(description="Clean, dedupe, and load FT/YF/SA master lists into staging tables.")
    parser.add_argument("--dry-run", action="store_true", help="Run cleaning/deduping only, skip DB load")
    args = parser.parse_args()

    ft_file = latest_file(PROJECT_ROOT / "validation_output" / "Financial_Times" / "master_tickers", "financial_times_master_tickers.csv")
    yf_file = latest_file(PROJECT_ROOT / "validation_output" / "Yahoo_Finance" / "master_tickers", "yf_ticker.csv")
    sa_file = latest_file(PROJECT_ROOT / "validation_output" / "Stock_Analysis" / "01_List_Master", "sa_etf_master.csv")

    if not ft_file or not yf_file or not sa_file:
        raise FileNotFoundError(
            f"Missing master files: FT={ft_file}, YF={yf_file}, SA={sa_file}"
        )

    logger.info("STEP 1/3 CLEAN: reading latest files")
    logger.info("FT file: %s", ft_file)
    logger.info("YF file: %s", yf_file)
    logger.info("SA file: %s", sa_file)

    ft_rows, ft_stats = clean_ft_rows(ft_file)
    yf_rows, yf_stats = clean_common_master_rows(yf_file, "Yahoo Finance")
    sa_rows, sa_stats = clean_common_master_rows(sa_file, "Stock Analysis")

    logger.info("STEP 2/3 DEDUPE + VALIDATE")
    logger.info("FT stats: %s", ft_stats)
    logger.info("YF stats: %s", yf_stats)
    logger.info("SA stats: %s", sa_stats)

    if args.dry_run:
        logger.info("STEP 3/3 LOAD: skipped (--dry-run)")
        return

    db = get_db_config()
    logger.info("STEP 3/3 LOAD: writing to DB %s:%s/%s", db.host, db.port, db.database)
    upsert_master_rows(db, ft_rows=ft_rows, yf_rows=yf_rows, sa_rows=sa_rows)
    logger.info("DB load completed: FT=%s YF=%s SA=%s", len(ft_rows), len(yf_rows), len(sa_rows))


if __name__ == "__main__":
    main()
