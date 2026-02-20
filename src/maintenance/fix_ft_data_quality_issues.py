import argparse
from dataclasses import dataclass

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("99_ft_dq_fix")


@dataclass
class FixStats:
    orphan_nav_rows: int = 0
    duplicate_sector_region_rows: int = 0
    deleted_orphan_nav_rows: int = 0
    deleted_duplicate_sector_region_rows: int = 0


def run_fix(dry_run: bool = False) -> FixStats:
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

    stats = FixStats()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM stg_ft_daily_nav t
                LEFT JOIN stg_ft_master_ticker m ON m.ft_ticker=t.ft_ticker
                WHERE m.ft_ticker IS NULL
                """
            )
            stats.orphan_nav_rows = int(cur.fetchone()[0] or 0)

            cur.execute(
                """
                SELECT COALESCE(SUM(cnt - 1), 0)
                FROM (
                  SELECT COUNT(*) AS cnt
                  FROM stg_ft_sector_region
                  GROUP BY ft_ticker, date_scraper, allocation_type, category_name
                  HAVING COUNT(*) > 1
                ) x
                """
            )
            stats.duplicate_sector_region_rows = int(cur.fetchone()[0] or 0)

            if dry_run:
                conn.rollback()
                return stats

            cur.execute(
                """
                DELETE t
                FROM stg_ft_daily_nav t
                LEFT JOIN stg_ft_master_ticker m ON m.ft_ticker=t.ft_ticker
                WHERE m.ft_ticker IS NULL
                """
            )
            stats.deleted_orphan_nav_rows = cur.rowcount

            cur.execute(
                """
                DELETE sr
                FROM stg_ft_sector_region sr
                JOIN (
                  SELECT id
                  FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                             PARTITION BY ft_ticker, date_scraper, allocation_type, category_name
                             ORDER BY id
                           ) AS rn
                    FROM stg_ft_sector_region
                  ) z
                  WHERE z.rn > 1
                ) d ON d.id = sr.id
                """
            )
            stats.deleted_duplicate_sector_region_rows = cur.rowcount

        conn.commit()
        return stats
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix FT DQ issues (orphan NAV refs + duplicate sector/region rows).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    stats = run_fix(dry_run=args.dry_run)
    logger.info(
        "FT DQ check: orphan_nav=%s duplicate_sector_region=%s",
        stats.orphan_nav_rows,
        stats.duplicate_sector_region_rows,
    )
    if args.dry_run:
        logger.info("Dry-run complete. No DB changes.")
        return

    logger.info(
        "FT DQ fix applied: deleted_orphan_nav=%s deleted_duplicate_sector_region=%s",
        stats.deleted_orphan_nav_rows,
        stats.deleted_duplicate_sector_region_rows,
    )


if __name__ == "__main__":
    main()
