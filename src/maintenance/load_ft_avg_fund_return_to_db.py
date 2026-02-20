import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("04_ft_avg_return_loader")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PLACEHOLDER_NULLS = {"", "--", "N/A", "NA", "NONE", "NULL", "NAN", "-"}


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
    cleaned = text.replace(",", "").replace("%", "")
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


def load_rows() -> Tuple[List[Tuple], Dict[str, int]]:
    path = _latest_file(
        PROJECT_ROOT / "validation_output" / "Financial_Times" / "05_Avg_Fund_Return",
        "financial_times_avg_fund_return.csv",
    )
    if not path:
        return [], {"input": 0, "invalid": 0, "ready": 0}

    stats = {"input": 0, "invalid": 0, "ready": 0}
    dedup: Dict[str, Tuple] = {}
    for row in _load_csv(path):
        stats["input"] += 1
        ft_ticker = _norm_text(row.get("ft_ticker"))
        ticker = _norm_text(row.get("ticker"))
        date_scraper = _norm_date(row.get("date_scraper"), fallback=datetime.now().strftime("%Y-%m-%d"))
        if not ft_ticker or not ticker or not date_scraper:
            stats["invalid"] += 1
            continue
        dedup[ft_ticker] = (
            ft_ticker,
            ticker.upper(),
            _norm_text(row.get("name")) or ticker.upper(),
            _norm_text(row.get("ticker_type")) or "Unknown",
            _norm_text(row.get("fund_name_perf")),
            _norm_text(row.get("avg_fund_return_1y_raw")),
            _norm_text(row.get("avg_fund_return_3y_raw")),
            _to_float(row.get("avg_fund_return_1y")),
            _to_float(row.get("avg_fund_return_3y")),
            _norm_text(row.get("source")) or "Financial Times",
            date_scraper,
            _norm_text(row.get("url")),
        )

    rows = list(dedup.values())
    stats["ready"] = len(rows)
    return rows, stats


def write_rows(rows: List[Tuple]) -> None:
    import pymysql

    cfg = get_db_config()
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        autocommit=False,
    )
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_ft_avg_fund_return (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ft_ticker VARCHAR(64) NOT NULL,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NOT NULL,
                  ticker_type VARCHAR(32) NOT NULL,
                  fund_name_perf VARCHAR(512) NULL,
                  avg_fund_return_1y_raw VARCHAR(64) NULL,
                  avg_fund_return_3y_raw VARCHAR(64) NULL,
                  avg_fund_return_1y DECIMAL(12,6) NULL,
                  avg_fund_return_3y DECIMAL(12,6) NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
                  date_scraper DATE NOT NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_ft_avg_return_ft_ticker (ft_ticker),
                  KEY idx_ft_avg_return_ticker (ticker),
                  KEY idx_ft_avg_return_date (date_scraper)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )

            cur.execute(
                "DELETE FROM stg_ft_avg_fund_return WHERE date_scraper=%s AND source='Financial Times'",
                (today,),
            )

            if rows:
                cur.executemany(
                    """
                    INSERT INTO stg_ft_avg_fund_return
                    (ft_ticker, ticker, name, ticker_type, fund_name_perf, avg_fund_return_1y_raw, avg_fund_return_3y_raw,
                     avg_fund_return_1y, avg_fund_return_3y, source, date_scraper, url)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      ticker=VALUES(ticker),
                      name=VALUES(name),
                      ticker_type=VALUES(ticker_type),
                      fund_name_perf=VALUES(fund_name_perf),
                      avg_fund_return_1y_raw=VALUES(avg_fund_return_1y_raw),
                      avg_fund_return_3y_raw=VALUES(avg_fund_return_3y_raw),
                      avg_fund_return_1y=VALUES(avg_fund_return_1y),
                      avg_fund_return_3y=VALUES(avg_fund_return_3y),
                      source=VALUES(source),
                      date_scraper=VALUES(date_scraper),
                      url=VALUES(url),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    rows,
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load FT average fund return CSV into stg_ft_avg_fund_return.")
    parser.parse_args()

    rows, stats = load_rows()
    logger.info("FT avg return stats: %s", stats)
    write_rows(rows)
    logger.info("DB load completed: FT avg return rows=%s", f"{len(rows):,}")


if __name__ == "__main__":
    main()

