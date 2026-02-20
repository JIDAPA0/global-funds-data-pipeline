import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pymysql

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("01_merge_isin_priority")
NULL_MARKERS = {"", "--", "N/A", "NA", "NONE", "NULL", "NAN"}


@dataclass
class Candidate:
    isin_number: str
    ticker: str
    name: str
    ticker_type: str
    source: str
    source_priority: int
    canonical_url: Optional[str]
    ft_ticker: Optional[str]
    mapped_from: Optional[str] = None


def _norm_text(v: Optional[str]) -> Optional[str]:
    s = (v or "").strip()
    if not s or s.upper() in NULL_MARKERS:
        return None
    return s


def _norm_isin(v: Optional[str]) -> Optional[str]:
    s = _norm_text(v)
    if not s:
        return None
    s = s.replace(" ", "").upper()
    if len(s) != 12:
        return None
    return s


def ensure_target_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_security_master_isin (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          isin_number VARCHAR(16) NOT NULL,
          ticker VARCHAR(32) NOT NULL,
          name VARCHAR(512) NOT NULL,
          ticker_type VARCHAR(32) NULL,
          source VARCHAR(32) NOT NULL,
          source_priority INT NOT NULL,
          canonical_url VARCHAR(1024) NULL,
          ft_ticker VARCHAR(64) NULL,
          mapped_from VARCHAR(64) NULL,
          merged_at DATETIME NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_isin (isin_number),
          KEY idx_ticker (ticker),
          KEY idx_source (source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def load_ft_candidates(cur) -> List[Candidate]:
    cur.execute(
        """
        SELECT
          isin_number, ticker, name, ticker_type, url, ft_ticker
        FROM stg_ft_static_detail
        WHERE isin_number IS NOT NULL AND isin_number <> ''
        ORDER BY updated_at DESC, date_scraper DESC
        """
    )
    seen = set()
    out: List[Candidate] = []
    for isin, ticker, name, ticker_type, url, ft_ticker in cur.fetchall():
        isin_n = _norm_isin(isin)
        if not isin_n or isin_n in seen:
            continue
        seen.add(isin_n)
        out.append(
            Candidate(
                isin_number=isin_n,
                ticker=_norm_text(ticker) or "",
                name=_norm_text(name) or (_norm_text(ticker) or isin_n),
                ticker_type=_norm_text(ticker_type) or "Unknown",
                source="Financial Times",
                source_priority=1,
                canonical_url=_norm_text(url),
                ft_ticker=_norm_text(ft_ticker),
            )
        )
    return out


def load_sa_candidates(cur) -> List[Candidate]:
    cur.execute(
        """
        SELECT
          isin_number, ticker, name, asset_type
        FROM stg_sa_static_info
        WHERE isin_number IS NOT NULL AND isin_number <> ''
        ORDER BY updated_at DESC
        """
    )
    seen = set()
    out: List[Candidate] = []
    for isin, ticker, name, asset_type in cur.fetchall():
        isin_n = _norm_isin(isin)
        if not isin_n or isin_n in seen:
            continue
        seen.add(isin_n)
        out.append(
            Candidate(
                isin_number=isin_n,
                ticker=_norm_text(ticker) or "",
                name=_norm_text(name) or (_norm_text(ticker) or isin_n),
                ticker_type=_norm_text(asset_type) or "Unknown",
                source="Stock Analysis",
                source_priority=3,
                canonical_url=None,
                ft_ticker=None,
            )
        )
    return out


def build_ticker_to_isin_map(ft_rows: List[Candidate], sa_rows: List[Candidate]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for row in sa_rows:
        if row.ticker:
            mapping[row.ticker.upper()] = row.isin_number
    # FT wins mapping precedence
    for row in ft_rows:
        if row.ticker:
            mapping[row.ticker.upper()] = row.isin_number
    return mapping


def load_yf_candidates(cur, ticker_to_isin: Dict[str, str]) -> List[Candidate]:
    # YF has no direct ISIN column, map by ticker from FT/SA ISIN universe.
    cur.execute(
        """
        SELECT ticker, name
        FROM stg_yf_static_identity
        ORDER BY updated_ts DESC
        """
    )
    seen = set()
    out: List[Candidate] = []
    for ticker, name in cur.fetchall():
        t = (_norm_text(ticker) or "").upper()
        if not t:
            continue
        isin_n = ticker_to_isin.get(t)
        if not isin_n:
            continue
        key = (isin_n, t)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            Candidate(
                isin_number=isin_n,
                ticker=t,
                name=_norm_text(name) or t,
                ticker_type="Unknown",
                source="Yahoo Finance",
                source_priority=2,
                canonical_url=f"https://finance.yahoo.com/quote/{t}",
                ft_ticker=None,
                mapped_from="ticker",
            )
        )
    return out


def merge_by_priority(candidates: List[Candidate]) -> Tuple[List[Candidate], Dict[str, int]]:
    stats = {"FT": 0, "YF": 0, "SA": 0, "merged": 0}
    best: Dict[str, Candidate] = {}
    for c in candidates:
        if c.source == "Financial Times":
            stats["FT"] += 1
        elif c.source == "Yahoo Finance":
            stats["YF"] += 1
        elif c.source == "Stock Analysis":
            stats["SA"] += 1
        prev = best.get(c.isin_number)
        if prev is None or c.source_priority < prev.source_priority:
            best[c.isin_number] = c
    merged = list(best.values())
    stats["merged"] = len(merged)
    return merged, stats


def upsert_merged(cur, rows: List[Candidate]) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = [
        (
            r.isin_number,
            r.ticker,
            r.name,
            r.ticker_type,
            r.source,
            r.source_priority,
            r.canonical_url,
            r.ft_ticker,
            r.mapped_from,
            now,
        )
        for r in rows
    ]
    cur.executemany(
        """
        INSERT INTO stg_security_master_isin
        (isin_number, ticker, name, ticker_type, source, source_priority, canonical_url, ft_ticker, mapped_from, merged_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          ticker = VALUES(ticker),
          name = VALUES(name),
          ticker_type = VALUES(ticker_type),
          source = VALUES(source),
          source_priority = VALUES(source_priority),
          canonical_url = VALUES(canonical_url),
          ft_ticker = VALUES(ft_ticker),
          mapped_from = VALUES(mapped_from),
          merged_at = VALUES(merged_at),
          updated_at = CURRENT_TIMESTAMP
        """,
        payload,
    )


def run() -> None:
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
    try:
        with conn.cursor() as cur:
            ensure_target_table(cur)
            ft_rows = load_ft_candidates(cur)
            sa_rows = load_sa_candidates(cur)
            ticker_to_isin = build_ticker_to_isin_map(ft_rows, sa_rows)
            yf_rows = load_yf_candidates(cur, ticker_to_isin=ticker_to_isin)

            merged, stats = merge_by_priority(ft_rows + yf_rows + sa_rows)
            upsert_merged(cur, merged)
        conn.commit()
        logger.info(
            "ISIN merge complete | FT=%s YF(mapped)=%s SA=%s => merged=%s",
            stats["FT"],
            stats["YF"],
            stats["SA"],
            stats["merged"],
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    _ = argparse.ArgumentParser(description="Merge ISIN universe by source priority FT > YF > SA").parse_args()
    run()


if __name__ == "__main__":
    main()

