import argparse

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("05_publish_ready_isin_serving")


def publish(serving_db: str = "funds_serving", materialize: bool = False) -> None:
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
    try:
        with conn.cursor() as cur:
            base_db = cfg.database
            target_db = serving_db
            try:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{serving_db}`")
            except Exception:
                target_db = base_db
                logger.warning(
                    "No privilege to create schema '%s'. Fallback to base schema '%s'.",
                    serving_db,
                    base_db,
                )

            cur.execute(f"DROP VIEW IF EXISTS `{target_db}`.`vw_fund_master_isin_ready`")
            cur.execute(
                f"""
                CREATE VIEW `{target_db}`.`vw_fund_master_isin_ready` AS
                WITH latest_nav AS (
                    SELECT *
                    FROM (
                        SELECT
                            UPPER(v.ticker) AS ticker_key,
                            v.ticker,
                            v.nav_source,
                            v.as_of_date,
                            v.nav_price,
                            v.nav_currency,
                            ROW_NUMBER() OVER (
                                PARTITION BY UPPER(v.ticker)
                                ORDER BY
                                    v.as_of_date DESC,
                                    CASE v.nav_source
                                        WHEN 'Financial Times' THEN 1
                                        WHEN 'Yahoo Finance' THEN 2
                                        WHEN 'Stock Analysis' THEN 3
                                        ELSE 9
                                    END
                            ) AS rn
                        FROM (
                            SELECT
                                ticker,
                                'Financial Times' AS nav_source,
                                nav_as_of AS as_of_date,
                                nav_price,
                                UPPER(nav_currency) AS nav_currency
                            FROM `{base_db}`.`stg_ft_daily_nav`
                            UNION ALL
                            SELECT
                                ticker,
                                'Yahoo Finance' AS nav_source,
                                as_of_date,
                                nav_price,
                                UPPER(currency) AS nav_currency
                            FROM `{base_db}`.`stg_yf_daily_nav`
                            UNION ALL
                            SELECT
                                ticker,
                                'Stock Analysis' AS nav_source,
                                as_of_date,
                                nav_price,
                                UPPER(currency) AS nav_currency
                            FROM `{base_db}`.`stg_sa_daily_nav`
                        ) v
                    ) x
                    WHERE x.rn = 1
                )
                SELECT
                    s.isin_number,
                    s.ticker,
                    s.name,
                    s.ticker_type,
                    s.source,
                    s.source_priority,
                    s.canonical_url,
                    ln.nav_source,
                    ln.as_of_date AS nav_as_of_date,
                    ln.nav_price,
                    ln.nav_currency,
                    s.updated_at
                FROM `{base_db}`.`stg_security_master_isin` s
                LEFT JOIN latest_nav ln
                  ON ln.ticker_key = UPPER(s.ticker)
                WHERE ln.ticker_key IS NOT NULL
                  AND (
                        (s.source = 'Financial Times' AND EXISTS (
                            SELECT 1
                            FROM `{base_db}`.`stg_ft_static_detail` f
                            WHERE f.isin_number = s.isin_number
                        ))
                     OR (s.source = 'Yahoo Finance' AND EXISTS (
                            SELECT 1
                            FROM `{base_db}`.`stg_yf_static_identity` y
                            WHERE UPPER(y.ticker) = UPPER(s.ticker)
                        ))
                     OR (s.source = 'Stock Analysis' AND EXISTS (
                            SELECT 1
                            FROM `{base_db}`.`stg_sa_static_info` a
                            WHERE a.isin_number = s.isin_number
                        ))
                  )
                """
            )

            if materialize:
                cur.execute(f"DROP TABLE IF EXISTS `{target_db}`.`fund_master_isin_ready`")
                cur.execute(
                    f"""
                    CREATE TABLE `{target_db}`.`fund_master_isin_ready` AS
                    SELECT *
                    FROM `{target_db}`.`vw_fund_master_isin_ready`
                    """
                )
                cur.execute(
                    f"ALTER TABLE `{target_db}`.`fund_master_isin_ready` ADD INDEX idx_isin (isin_number)"
                )
                cur.execute(
                    f"ALTER TABLE `{target_db}`.`fund_master_isin_ready` ADD INDEX idx_ticker (ticker)"
                )
                cur.execute(
                    f"ALTER TABLE `{target_db}`.`fund_master_isin_ready` ADD INDEX idx_source (source)"
                )

        conn.commit()
        logger.info(
            "Published serving objects in schema '%s': vw_fund_master_isin_ready%s",
            target_db,
            ", fund_master_isin_ready" if materialize else "",
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish unique ISIN ready dataset into a serving schema."
    )
    parser.add_argument("--serving-db", default="funds_serving")
    parser.add_argument(
        "--materialize",
        action="store_true",
        help="Also create materialized table fund_master_isin_ready.",
    )
    args = parser.parse_args()
    publish(serving_db=args.serving_db, materialize=args.materialize)


if __name__ == "__main__":
    main()
