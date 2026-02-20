import argparse

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("05_nav_mart")


def build_views() -> None:
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
    try:
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS vw_nav_unified")
            cur.execute("DROP VIEW IF EXISTS vw_nav_usd")

            cur.execute(
                """
                CREATE VIEW vw_nav_unified AS
                SELECT
                    'Financial Times' AS source_name,
                    ft.id AS source_row_id,
                    ft.ft_ticker AS instrument_ref,
                    ft.ticker,
                    ft.name,
                    ft.ticker_type AS asset_type,
                    ft.nav_price,
                    UPPER(ft.nav_currency) AS nav_currency,
                    ft.nav_as_of AS as_of_date,
                    ft.date_scraper,
                    ft.url
                FROM stg_ft_daily_nav ft
                UNION ALL
                SELECT
                    'Yahoo Finance' AS source_name,
                    yf.id AS source_row_id,
                    NULL AS instrument_ref,
                    yf.ticker,
                    m.name,
                    yf.asset_type,
                    yf.nav_price,
                    UPPER(yf.currency) AS nav_currency,
                    yf.as_of_date,
                    yf.scrape_date AS date_scraper,
                    m.url
                FROM stg_yf_daily_nav yf
                LEFT JOIN stg_yf_master_ticker m ON m.ticker = yf.ticker
                UNION ALL
                SELECT
                    'Stock Analysis' AS source_name,
                    sa.id AS source_row_id,
                    NULL AS instrument_ref,
                    sa.ticker,
                    m.name,
                    sa.asset_type,
                    sa.nav_price,
                    UPPER(sa.currency) AS nav_currency,
                    sa.as_of_date,
                    sa.scrape_date AS date_scraper,
                    m.url
                FROM stg_sa_daily_nav sa
                LEFT JOIN stg_sa_master_ticker m ON m.ticker = sa.ticker
                """
            )

            # Conversion uses nearest previous available fx date (<= as_of_date)
            cur.execute(
                """
                CREATE VIEW vw_nav_usd AS
                SELECT
                    n.source_name,
                    n.source_row_id,
                    n.instrument_ref,
                    n.ticker,
                    n.name,
                    n.asset_type,
                    n.nav_price,
                    n.nav_currency,
                    n.as_of_date,
                    n.date_scraper,
                    n.url,
                    n.fx_from_currency,
                    n.unit_factor,
                    fx.rate_date AS fx_rate_date,
                    fx.fx_rate AS fx_rate_to_usd,
                    CASE
                        WHEN n.fx_from_currency = 'USD' THEN (n.nav_price * n.unit_factor)
                        WHEN fx.fx_rate IS NULL THEN NULL
                        ELSE (n.nav_price * n.unit_factor * fx.fx_rate)
                    END AS nav_price_usd,
                    CASE
                        WHEN n.fx_from_currency = 'USD' THEN 0
                        WHEN fx.rate_date IS NULL THEN NULL
                        ELSE DATEDIFF(n.as_of_date, fx.rate_date)
                    END AS fx_staleness_days
                FROM (
                    SELECT
                        u.*,
                        CASE UPPER(u.nav_currency)
                            WHEN 'GBX' THEN 'GBP'
                            WHEN 'ZAX' THEN 'ZAR'
                            WHEN 'CNH' THEN 'CNY'
                            WHEN 'ILA' THEN 'ILS'
                            ELSE UPPER(u.nav_currency)
                        END AS fx_from_currency,
                        CASE UPPER(u.nav_currency)
                            WHEN 'GBX' THEN 0.01
                            WHEN 'ZAX' THEN 0.01
                            ELSE 1
                        END AS unit_factor
                    FROM vw_nav_unified u
                ) n
                LEFT JOIN daily_fx_rates fx
                  ON fx.from_currency = n.fx_from_currency
                 AND fx.to_currency = 'USD'
                 AND fx.rate_date = (
                    SELECT MAX(f2.rate_date)
                    FROM daily_fx_rates f2
                    WHERE f2.from_currency = n.fx_from_currency
                      AND f2.to_currency = 'USD'
                      AND f2.rate_date <= n.as_of_date
                 )
                """
            )

        conn.commit()
        logger.info("Created views: vw_nav_unified, vw_nav_usd")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create NAV data mart views (unified + USD converted).")
    parser.parse_args()
    build_views()


if __name__ == "__main__":
    main()
