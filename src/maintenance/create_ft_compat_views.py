import argparse

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("05_create_ft_compat_views")


def create_views() -> None:
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
            # 1) ft_static_detail
            cur.execute("DROP VIEW IF EXISTS ft_static_detail")
            cur.execute(
                """
                CREATE VIEW ft_static_detail AS
                SELECT
                    id,
                    ft_ticker,
                    ticker,
                    name,
                    ticker_type,
                    morningstar_category,
                    inception_date,
                    domicile,
                    isin_number,
                    assets_aum_raw,
                    assets_aum_full_value,
                    assets_aum_value,
                    assets_aum_unit,
                    assets_aum_currency,
                    assets_aum_as_of,
                    expense_ratio_raw,
                    expense_pct,
                    income_treatment,
                    source,
                    date_scraper,
                    url,
                    created_at,
                    updated_at
                FROM stg_ft_static_detail
                """
            )

            # 2) ft_holdings
            cur.execute("DROP VIEW IF EXISTS ft_holdings")
            cur.execute(
                """
                CREATE VIEW ft_holdings AS
                SELECT
                    id,
                    ticker,
                    name,
                    ticker_type,
                    allocation_type,
                    holding_name,
                    holding_ticker,
                    holding_type,
                    holding_symbol,
                    holding_url,
                    portfolio_weight_pct,
                    top_10_holdings_weight_pct,
                    other_holding_weight_pct,
                    source,
                    date_scraper,
                    url,
                    created_at,
                    updated_at
                FROM stg_ft_holdings
                """
            )

            # 3) ft_sector_allocation
            cur.execute("DROP VIEW IF EXISTS ft_sector_allocation")
            cur.execute(
                """
                CREATE VIEW ft_sector_allocation AS
                SELECT
                    id,
                    ft_ticker,
                    ticker,
                    name,
                    ticker_type,
                    category_name AS sector_name,
                    weight_pct AS sector_weight_pct,
                    allocation_type,
                    url_type_used,
                    source,
                    date_scraper,
                    url,
                    created_at,
                    updated_at
                FROM stg_ft_sector_region
                WHERE UPPER(allocation_type) LIKE 'SECTOR%'
                """
            )

            # 4) ft_region_allocation
            cur.execute("DROP VIEW IF EXISTS ft_region_allocation")
            cur.execute(
                """
                CREATE VIEW ft_region_allocation AS
                SELECT
                    id,
                    ft_ticker,
                    ticker,
                    name,
                    ticker_type,
                    category_name AS region_name,
                    weight_pct AS region_weight_pct,
                    allocation_type,
                    url_type_used,
                    source,
                    date_scraper,
                    url,
                    created_at,
                    updated_at
                FROM stg_ft_sector_region
                WHERE UPPER(allocation_type) LIKE 'REGION%'
                """
            )

            # 5) ft_avg_fund_return
            cur.execute("DROP VIEW IF EXISTS ft_avg_fund_return")
            cur.execute("SHOW TABLES LIKE 'stg_ft_avg_fund_return'")
            has_return_table = bool(cur.fetchone())
            if has_return_table:
                cur.execute(
                    """
                    CREATE VIEW ft_avg_fund_return AS
                    SELECT
                        r.ft_ticker,
                        r.ticker,
                        r.name,
                        r.ticker_type,
                        r.date_scraper AS as_of_date,
                        r.avg_fund_return_1y AS avg_fund_return_1y,
                        r.avg_fund_return_3y AS avg_fund_return_3y,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_return_1m_pct,
                        r.avg_fund_return_3y AS avg_return_3m_pct,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_return_6m_pct,
                        r.avg_fund_return_1y AS avg_return_1y_pct
                    FROM stg_ft_avg_fund_return r
                    """
                )
            else:
                cur.execute(
                    """
                    CREATE VIEW ft_avg_fund_return AS
                    SELECT
                        NULL AS ft_ticker,
                        x.ticker,
                        x.name,
                        x.ticker_type,
                        x.latest_nav_as_of AS as_of_date,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_fund_return_1y,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_fund_return_3y,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_return_1m_pct,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_return_3m_pct,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_return_6m_pct,
                        CAST(NULL AS DECIMAL(12,6)) AS avg_return_1y_pct
                    FROM (
                        SELECT
                            n.ticker,
                            MAX(n.name) AS name,
                            MAX(n.ticker_type) AS ticker_type,
                            MAX(n.nav_as_of) AS latest_nav_as_of
                        FROM stg_ft_daily_nav n
                        GROUP BY n.ticker
                    ) x
                    """
                )

        conn.commit()
        logger.info(
            "Created compatibility views: ft_static_detail, ft_holdings, "
            "ft_sector_allocation, ft_region_allocation, ft_avg_fund_return"
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create FT compatibility views from stg_ft_* tables.")
    parser.parse_args()
    create_views()


if __name__ == "__main__":
    main()
