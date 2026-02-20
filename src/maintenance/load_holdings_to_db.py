import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("04_holdings_loader")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PLACEHOLDER_NULLS = {"", "--", "N/A", "NA", "NONE", "NULL", "NAN"}


def _norm_text(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    if text.upper() in PLACEHOLDER_NULLS:
        return None
    return text


def _clip(value: Optional[str], max_len: int) -> Optional[str]:
    text = _norm_text(value)
    if text is None:
        return None
    return text[:max_len]


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
    cleaned = text.replace(",", "").replace("%", "").replace("$", "")
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


def load_ft_holdings() -> Tuple[List[Tuple], Dict[str, int]]:
    path = _latest_file(PROJECT_ROOT / "validation_output" / "Financial_Times" / "04_Holdings", "financial_times_holdings.csv")
    if not path:
        return [], {"input": 0, "invalid": 0, "ready": 0}

    stats = {"input": 0, "invalid": 0, "ready": 0}
    out = []
    for row in _load_csv(path):
        stats["input"] += 1
        ticker = _norm_text(row.get("ticker"))
        date_scraper = _norm_date(row.get("date_scraper"))
        if not ticker or not date_scraper:
            stats["invalid"] += 1
            continue
        out.append(
            (
                ticker.upper(),
                _norm_text(row.get("name")) or ticker.upper(),
                _norm_text(row.get("ticker_type")) or "Unknown",
                _norm_text(row.get("allocation_type")) or "top_10_holdings",
                _norm_text(row.get("holding_name")) or "Unknown",
                _norm_text(row.get("holding_ticker")),
                _norm_text(row.get("holding_type")),
                _norm_text(row.get("holding_symbol")),
                _norm_text(row.get("holding_url")),
                _to_float(row.get("portfolio_weight_pct")),
                _to_float(row.get("top_10_holdings_weight_pct")),
                _to_float(row.get("other_holding_weight_pct")),
                _norm_text(row.get("source")) or "Financial Times",
                date_scraper,
                _norm_text(row.get("url")),
            )
        )
    stats["ready"] = len(out)
    return out, stats


def load_ft_sector_region() -> Tuple[List[Tuple], Dict[str, int]]:
    base = PROJECT_ROOT / "validation_output" / "Financial_Times" / "04_Holdings" / "Sector_Region"
    if not base.exists():
        return [], {"input": 0, "invalid": 0, "ready": 0}
    dirs = sorted([d for d in base.iterdir() if d.is_dir()])
    if not dirs:
        return [], {"input": 0, "invalid": 0, "ready": 0}
    latest = dirs[-1]

    files = list(latest.glob("*_sector_allocation.csv")) + list(latest.glob("*_region_allocation.csv"))
    stats = {"input": 0, "invalid": 0, "ready": 0}
    out = []
    for file in files:
        for row in _load_csv(file):
            stats["input"] += 1
            ft_ticker = _norm_text(row.get("ft_ticker"))
            date_scraper = _norm_date(row.get("date_scraper"))
            if not ft_ticker or not date_scraper:
                stats["invalid"] += 1
                continue
            out.append(
                (
                    ft_ticker,
                    _norm_text(row.get("ticker")) or ft_ticker.split(":")[0],
                    _norm_text(row.get("name")) or ft_ticker,
                    _norm_text(row.get("ticker_type")) or "Unknown",
                    _norm_text(row.get("category_name")) or "Unknown",
                    _to_float(row.get("weight_pct")),
                    _norm_text(row.get("allocation_type")) or "Unknown",
                    _norm_text(row.get("url_type_used")),
                    _norm_text(row.get("source")) or "Financial Times",
                    date_scraper,
                    _norm_text(row.get("url")),
                )
            )
    stats["ready"] = len(out)
    return out, stats


def load_yf_holdings() -> Tuple[List[Tuple], List[Tuple], List[Tuple], Dict[str, int]]:
    base = PROJECT_ROOT / "validation_output" / "Yahoo_Finance" / "04_Holdings"
    hold_files = list((base / "Holdings").glob("*.csv"))
    sector_files = list((base / "Sectors").glob("*.csv"))
    alloc_files = list((base / "Allocation").glob("*.csv"))

    h_stats = {"input": 0, "invalid": 0, "ready": 0}
    s_stats = {"input": 0, "invalid": 0, "ready": 0}
    a_stats = {"input": 0, "invalid": 0, "ready": 0}
    h_rows, s_rows, a_rows = [], [], []

    for file in hold_files:
        for row in _load_csv(file):
            h_stats["input"] += 1
            ticker = _norm_text(row.get("ticker"))
            if not ticker:
                h_stats["invalid"] += 1
                continue
            h_rows.append(
                (
                    ticker.upper()[:32],
                    _clip(row.get("yahoo_ticker"), 32),
                    _clip(row.get("asset_type"), 32),
                    _clip(row.get("symbol"), 64),
                    _clip(row.get("name"), 512),
                    _clip(row.get("value"), 128),
                    _norm_date(row.get("updated_at"), fallback=datetime.now().strftime("%Y-%m-%d")),
                )
            )
    for file in sector_files:
        for row in _load_csv(file):
            s_stats["input"] += 1
            ticker = _norm_text(row.get("ticker"))
            if not ticker:
                s_stats["invalid"] += 1
                continue
            s_rows.append(
                (
                    ticker.upper()[:32],
                    _clip(row.get("asset_type"), 32),
                    _clip(row.get("sector"), 255) or "Unknown",
                    _clip(row.get("value"), 128),
                    _norm_date(row.get("updated_at"), fallback=datetime.now().strftime("%Y-%m-%d")),
                )
            )
    for file in alloc_files:
        for row in _load_csv(file):
            a_stats["input"] += 1
            ticker = _norm_text(row.get("ticker"))
            if not ticker:
                a_stats["invalid"] += 1
                continue
            a_rows.append(
                (
                    ticker.upper()[:32],
                    _clip(row.get("asset_type"), 32),
                    _clip(row.get("category"), 255) or "Unknown",
                    _clip(row.get("value"), 128),
                    _norm_date(row.get("updated_at"), fallback=datetime.now().strftime("%Y-%m-%d")),
                )
            )

    h_stats["ready"] = len(h_rows)
    s_stats["ready"] = len(s_rows)
    a_stats["ready"] = len(a_rows)
    return h_rows, s_rows, a_rows, {"holdings": h_stats, "sectors": s_stats, "allocation": a_stats}


def load_sa_holdings_and_sector_country() -> Tuple[List[Tuple], List[Tuple], Dict[str, int]]:
    base = PROJECT_ROOT / "validation_output" / "Stock_Analysis" / "04_Holdings"
    if not base.exists():
        return [], [], {"holdings_input": 0, "holdings_ready": 0, "sector_country_input": 0, "sector_country_ready": 0}
    dirs = sorted([d for d in base.iterdir() if d.is_dir()])
    if not dirs:
        return [], [], {"holdings_input": 0, "holdings_ready": 0, "sector_country_input": 0, "sector_country_ready": 0}
    latest = dirs[-1]

    holding_rows = []
    sector_country_rows = []

    holding_files = list(latest.glob("*_holdings.csv"))
    for file in holding_files:
        ticker = file.name.split("_holdings.csv")[0].upper()
        holding_rows.append((ticker, file.name))

    for filename, row_type in [("sa_sector_allocation.csv", "Sector"), ("sa_country_allocation.csv", "Country")]:
        file = latest / filename
        if not file.exists():
            continue
        for row in _load_csv(file):
            ticker = _norm_text(row.get("ticker"))
            if not ticker:
                continue
            sector_country_rows.append(
                (
                    ticker.upper(),
                    _norm_text(row.get("category_name")) or "Unknown",
                    _to_float(row.get("percentage")),
                    _norm_text(row.get("type")) or row_type,
                    _norm_text(row.get("source")) or "Stock Analysis",
                    _norm_date(row.get("date_scraper"), fallback=datetime.now().strftime("%Y-%m-%d")),
                    _norm_text(row.get("url")),
                )
            )

    stats = {
        "holdings_input": len(holding_files),
        "holdings_ready": len(holding_rows),
        "sector_country_input": len(sector_country_rows),
        "sector_country_ready": len(sector_country_rows),
    }
    return holding_rows, sector_country_rows, stats


def write_holdings(
    ft_holdings: List[Tuple],
    ft_sector_region: List[Tuple],
    yf_holdings: List[Tuple],
    yf_sectors: List[Tuple],
    yf_alloc: List[Tuple],
    sa_holdings: List[Tuple],
    sa_sector_country: List[Tuple],
) -> None:
    import pymysql

    db = get_db_config()
    conn = pymysql.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        database=db.database,
        charset="utf8mb4",
        autocommit=False,
    )
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_ft_holdings (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NOT NULL,
                  ticker_type VARCHAR(32) NOT NULL,
                  allocation_type VARCHAR(64) NOT NULL,
                  holding_name VARCHAR(512) NOT NULL,
                  holding_ticker VARCHAR(64) NULL,
                  holding_type VARCHAR(32) NULL,
                  holding_symbol VARCHAR(32) NULL,
                  holding_url VARCHAR(1024) NULL,
                  portfolio_weight_pct DECIMAL(10,4) NULL,
                  top_10_holdings_weight_pct DECIMAL(10,4) NULL,
                  other_holding_weight_pct DECIMAL(10,4) NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
                  date_scraper DATE NOT NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_ft_sector_region (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ft_ticker VARCHAR(64) NOT NULL,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NOT NULL,
                  ticker_type VARCHAR(32) NOT NULL,
                  category_name VARCHAR(255) NOT NULL,
                  weight_pct DECIMAL(10,4) NULL,
                  allocation_type VARCHAR(64) NOT NULL,
                  url_type_used VARCHAR(64) NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
                  date_scraper DATE NOT NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_holdings (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  yahoo_ticker VARCHAR(32) NULL,
                  asset_type VARCHAR(32) NULL,
                  symbol VARCHAR(64) NULL,
                  name VARCHAR(512) NULL,
                  value VARCHAR(128) NULL,
                  updated_at DATE NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_sectors (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  asset_type VARCHAR(32) NULL,
                  sector VARCHAR(255) NOT NULL,
                  value VARCHAR(128) NULL,
                  updated_at DATE NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_allocation (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  asset_type VARCHAR(32) NULL,
                  category VARCHAR(255) NOT NULL,
                  value VARCHAR(128) NULL,
                  updated_at DATE NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_sa_holdings (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  file_name VARCHAR(255) NOT NULL,
                  downloaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_sa_sector_country (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  category_name VARCHAR(255) NOT NULL,
                  percentage DECIMAL(10,4) NULL,
                  type VARCHAR(32) NOT NULL,
                  source VARCHAR(64) NULL,
                  date_scraper DATE NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            # delete today's snapshot for non-unique holdings tables to keep idempotency
            cur.execute("DELETE FROM stg_ft_holdings WHERE date_scraper=%s AND source='Financial Times'", (today,))
            cur.execute("DELETE FROM stg_ft_sector_region WHERE date_scraper=%s AND source='Financial Times'", (today,))
            cur.execute("DELETE FROM stg_yf_holdings WHERE updated_at=%s", (today,))
            cur.execute("DELETE FROM stg_yf_sectors WHERE updated_at=%s", (today,))
            cur.execute("DELETE FROM stg_yf_allocation WHERE updated_at=%s", (today,))
            cur.execute("DELETE FROM stg_sa_sector_country WHERE date_scraper=%s AND source='Stock Analysis'", (today,))

            if sa_holdings:
                file_names = [r[1] for r in sa_holdings]
                placeholders = ",".join(["%s"] * len(file_names))
                cur.execute(f"DELETE FROM stg_sa_holdings WHERE file_name IN ({placeholders})", file_names)

            if ft_holdings:
                cur.executemany(
                    """
                    INSERT INTO stg_ft_holdings
                    (ticker,name,ticker_type,allocation_type,holding_name,holding_ticker,holding_type,holding_symbol,holding_url,
                     portfolio_weight_pct,top_10_holdings_weight_pct,other_holding_weight_pct,source,date_scraper,url)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    ft_holdings,
                )
            if ft_sector_region:
                cur.executemany(
                    """
                    INSERT INTO stg_ft_sector_region
                    (ft_ticker,ticker,name,ticker_type,category_name,weight_pct,allocation_type,url_type_used,source,date_scraper,url)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    ft_sector_region,
                )
            if yf_holdings:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_holdings
                    (ticker,yahoo_ticker,asset_type,symbol,name,value,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    yf_holdings,
                )
            if yf_sectors:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_sectors
                    (ticker,asset_type,sector,value,updated_at)
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    yf_sectors,
                )
            if yf_alloc:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_allocation
                    (ticker,asset_type,category,value,updated_at)
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    yf_alloc,
                )
            if sa_holdings:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_holdings (ticker,file_name)
                    VALUES (%s,%s)
                    """,
                    sa_holdings,
                )
            if sa_sector_country:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_sector_country
                    (ticker,category_name,percentage,type,source,date_scraper,url)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    sa_sector_country,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and load FT/YF/SA holdings outputs into DB staging.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ft_holdings, ft_holdings_stats = load_ft_holdings()
    ft_sr, ft_sr_stats = load_ft_sector_region()
    yf_h, yf_s, yf_a, yf_stats = load_yf_holdings()
    sa_h, sa_sc, sa_stats = load_sa_holdings_and_sector_country()

    logger.info("FT holdings stats: %s", ft_holdings_stats)
    logger.info("FT sector_region stats: %s", ft_sr_stats)
    logger.info("YF holdings stats: %s", yf_stats)
    logger.info("SA holdings/sector_country stats: %s", sa_stats)

    if args.dry_run:
        logger.info("Dry-run complete. No DB writes.")
        return

    write_holdings(
        ft_holdings=ft_holdings,
        ft_sector_region=ft_sr,
        yf_holdings=yf_h,
        yf_sectors=yf_s,
        yf_alloc=yf_a,
        sa_holdings=sa_h,
        sa_sector_country=sa_sc,
    )
    logger.info(
        "DB load completed: FT(holdings/sr)=%s/%s YF(h/s/a)=%s/%s/%s SA(holdings/sc)=%s/%s",
        len(ft_holdings),
        len(ft_sr),
        len(yf_h),
        len(yf_s),
        len(yf_a),
        len(sa_h),
        len(sa_sc),
    )


if __name__ == "__main__":
    main()
