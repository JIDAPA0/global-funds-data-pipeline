import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger
from src.utils.status_manager import STATUS_ACTIVE, STATUS_INACTIVE, STATUS_NEW


logger = setup_logger("01_master_status_merge")

SOURCE_PRIORITY = ["Financial Times", "Stock Analysis", "Yahoo Finance"]
PLACEHOLDER_NULLS = {"", "--", "N/A", "NA", "NONE", "NULL", "NAN"}


@dataclass
class SecuritySnapshot:
    ticker: str
    name: Optional[str]
    ticker_type: Optional[str]
    preferred_source: str
    source_coverage: str
    canonical_url: Optional[str]
    last_seen: str


def _norm_text(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    if text.upper() in PLACEHOLDER_NULLS:
        return None
    return text


def _source_rank(source_name: str) -> int:
    try:
        return SOURCE_PRIORITY.index(source_name)
    except ValueError:
        return 999


def _pick_field(candidates: List[Tuple[str, Optional[str]]]) -> Optional[str]:
    # pick first non-empty by source priority
    ordered = sorted(candidates, key=lambda x: _source_rank(x[0]))
    for _, value in ordered:
        val = _norm_text(value)
        if val:
            return val
    return None


def ensure_table_exists(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_security_master (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          ticker VARCHAR(32) NOT NULL,
          name VARCHAR(512) NULL,
          ticker_type VARCHAR(32) NULL,
          preferred_source VARCHAR(64) NOT NULL,
          source_coverage VARCHAR(255) NOT NULL,
          canonical_url VARCHAR(1024) NULL,
          status VARCHAR(16) NOT NULL DEFAULT 'new',
          first_seen DATE NOT NULL,
          last_seen DATE NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_security_master_ticker (ticker),
          KEY idx_security_master_status (status),
          KEY idx_security_master_last_seen (last_seen)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def load_latest_source_rows(cur) -> List[Tuple[str, str, Optional[str], Optional[str], Optional[str]]]:
    # (source, ticker, name, ticker_type, url)
    queries = [
        (
            "Financial Times",
            """
            SELECT %s AS source, ticker, name, ticker_type, url
            FROM stg_ft_master_ticker
            WHERE date_scraper = (SELECT MAX(date_scraper) FROM stg_ft_master_ticker)
            """,
        ),
        (
            "Yahoo Finance",
            """
            SELECT %s AS source, ticker, name, ticker_type, url
            FROM stg_yf_master_ticker
            WHERE date_scraper = (SELECT MAX(date_scraper) FROM stg_yf_master_ticker)
            """,
        ),
        (
            "Stock Analysis",
            """
            SELECT %s AS source, ticker, name, ticker_type, url
            FROM stg_sa_master_ticker
            WHERE date_scraper = (SELECT MAX(date_scraper) FROM stg_sa_master_ticker)
            """,
        ),
    ]

    rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[str]]] = []
    for source_name, sql in queries:
        try:
            cur.execute(sql, (source_name,))
            rows.extend(cur.fetchall())
        except Exception as exc:
            logger.warning("Skipping source %s due to query error: %s", source_name, exc)
    return rows


def build_snapshot(rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[str]]], as_of: str) -> List[SecuritySnapshot]:
    by_ticker: Dict[str, Dict[str, object]] = defaultdict(lambda: {"sources": set(), "names": [], "types": [], "urls": []})

    for source, ticker, name, ticker_type, url in rows:
        t = (ticker or "").strip().upper()
        if not t:
            continue
        by_ticker[t]["sources"].add(source)
        by_ticker[t]["names"].append((source, name))
        by_ticker[t]["types"].append((source, ticker_type))
        by_ticker[t]["urls"].append((source, url))

    out: List[SecuritySnapshot] = []
    for ticker, data in by_ticker.items():
        sources: Set[str] = data["sources"]  # type: ignore[assignment]
        preferred = sorted(list(sources), key=_source_rank)[0]
        coverage = ",".join(sorted(list(sources), key=_source_rank))
        name = _pick_field(data["names"])  # type: ignore[arg-type]
        ticker_type = _pick_field(data["types"])  # type: ignore[arg-type]
        url = _pick_field(data["urls"])  # type: ignore[arg-type]

        out.append(
            SecuritySnapshot(
                ticker=ticker,
                name=name,
                ticker_type=ticker_type,
                preferred_source=preferred,
                source_coverage=coverage,
                canonical_url=url,
                last_seen=as_of,
            )
        )

    return out


def merge_security_master(inactive_days: int = 7) -> None:
    db = get_db_config()
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
    today = datetime.now().strftime("%Y-%m-%d")
    cutoff = (datetime.now() - timedelta(days=inactive_days)).strftime("%Y-%m-%d")

    try:
        with conn.cursor() as cur:
            ensure_table_exists(cur)
            source_rows = load_latest_source_rows(cur)
            snapshots = build_snapshot(source_rows, as_of=today)
            logger.info("Prepared consolidated snapshot: %s tickers", f"{len(snapshots):,}")

            if snapshots:
                cur.executemany(
                    """
                    INSERT INTO stg_security_master
                    (ticker, name, ticker_type, preferred_source, source_coverage, canonical_url, status, first_seen, last_seen)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      name=VALUES(name),
                      ticker_type=VALUES(ticker_type),
                      preferred_source=VALUES(preferred_source),
                      source_coverage=VALUES(source_coverage),
                      canonical_url=VALUES(canonical_url),
                      last_seen=VALUES(last_seen),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    [
                        (
                            s.ticker,
                            s.name,
                            s.ticker_type,
                            s.preferred_source,
                            s.source_coverage,
                            s.canonical_url,
                            STATUS_NEW,
                            today,
                            s.last_seen,
                        )
                        for s in snapshots
                    ],
                )

            # Promote new -> active when basic fields are valid
            cur.execute(
                """
                UPDATE stg_security_master
                SET status=%s, updated_at=CURRENT_TIMESTAMP
                WHERE status=%s
                  AND ticker IS NOT NULL AND ticker <> ''
                  AND name IS NOT NULL AND name <> ''
                """,
                (STATUS_ACTIVE, STATUS_NEW),
            )
            promoted_count = cur.rowcount

            # Mark stale active rows inactive
            cur.execute(
                """
                UPDATE stg_security_master
                SET status=%s, updated_at=CURRENT_TIMESTAMP
                WHERE status=%s AND last_seen < %s
                """,
                (STATUS_INACTIVE, STATUS_ACTIVE, cutoff),
            )
            inactivated_count = cur.rowcount

            # Reactivate inactive rows seen again today
            cur.execute(
                """
                UPDATE stg_security_master
                SET status=%s, updated_at=CURRENT_TIMESTAMP
                WHERE status=%s AND last_seen >= %s
                """,
                (STATUS_ACTIVE, STATUS_INACTIVE, today),
            )
            reactivated_count = cur.rowcount

            cur.execute(
                """
                SELECT status, COUNT(*) FROM stg_security_master GROUP BY status ORDER BY status
                """
            )
            summary = cur.fetchall()

        conn.commit()
        logger.info("Status transitions: promoted=%s inactive=%s reactivated=%s", promoted_count, inactivated_count, reactivated_count)
        logger.info("Final status summary: %s", summary)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge source master tables into stg_security_master with status lifecycle.")
    parser.add_argument("--inactive-days", type=int, default=7, help="Days before active rows become inactive")
    args = parser.parse_args()
    merge_security_master(inactive_days=args.inactive_days)


if __name__ == "__main__":
    main()
