import argparse

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("05_create_canonical_views_3src")


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
            cur.execute("DROP VIEW IF EXISTS vw_canonical_fund_static")
            cur.execute(
                """
                CREATE VIEW vw_canonical_fund_static AS
                SELECT
                    'Financial Times' AS source,
                    fs.ft_ticker,
                    fs.ticker,
                    fs.name,
                    fs.ticker_type,
                    fs.isin_number,
                    fs.date_scraper,
                    fs.created_at,
                    CAST(fs.assets_aum_full_value AS DECIMAL(20,2)) AS assets_aum_full_value
                FROM ft_static_detail fs
                UNION ALL
                SELECT
                    'Yahoo Finance' AS source,
                    NULL AS ft_ticker,
                    yi.ticker,
                    yi.name,
                    COALESCE(ym.ticker_type, 'Unknown') AS ticker_type,
                    smi.isin_number,
                    COALESCE(yi.updated_at, DATE(yi.updated_ts), DATE(yi.created_at)) AS date_scraper,
                    yi.created_at,
                    NULL AS assets_aum_full_value
                FROM stg_yf_static_identity yi
                LEFT JOIN stg_yf_master_ticker ym ON UPPER(ym.ticker) = UPPER(yi.ticker)
                LEFT JOIN stg_security_master_isin smi
                  ON UPPER(smi.ticker) = UPPER(yi.ticker)
                 AND smi.source = 'Yahoo Finance'
                UNION ALL
                SELECT
                    'Stock Analysis' AS source,
                    NULL AS ft_ticker,
                    si.ticker,
                    si.name,
                    COALESCE(si.asset_type, 'Unknown') AS ticker_type,
                    si.isin_number,
                    DATE(si.updated_at) AS date_scraper,
                    si.created_at,
                    NULL AS assets_aum_full_value
                FROM stg_sa_static_info si
                """
            )

            cur.execute("DROP VIEW IF EXISTS vw_canonical_holdings_top")
            cur.execute(
                """
                CREATE VIEW vw_canonical_holdings_top AS
                SELECT
                    'Financial Times' AS source,
                    fh.ticker,
                    fh.holding_name,
                    fh.holding_ticker,
                    fh.holding_type,
                    fh.portfolio_weight_pct,
                    fh.allocation_type,
                    fh.date_scraper
                FROM ft_holdings fh
                WHERE LOWER(fh.allocation_type) = 'top_10_holdings'
                UNION ALL
                SELECT
                    'Yahoo Finance' AS source,
                    yh.ticker,
                    yh.name AS holding_name,
                    yh.symbol AS holding_ticker,
                    NULL AS holding_type,
                    CAST(NULLIF(REGEXP_REPLACE(yh.value, '[^0-9.-]', ''), '') AS DECIMAL(12,4)) AS portfolio_weight_pct,
                    'top_10_holdings' AS allocation_type,
                    yh.updated_at AS date_scraper
                FROM stg_yf_holdings yh
                UNION ALL
                SELECT
                    'Stock Analysis' AS source,
                    sh.ticker,
                    NULL AS holding_name,
                    NULL AS holding_ticker,
                    NULL AS holding_type,
                    NULL AS portfolio_weight_pct,
                    'top_10_holdings' AS allocation_type,
                    DATE(sh.downloaded_at) AS date_scraper
                FROM stg_sa_holdings sh
                """
            )

            cur.execute("DROP VIEW IF EXISTS vw_canonical_sector_allocation")
            cur.execute(
                """
                CREATE VIEW vw_canonical_sector_allocation AS
                SELECT
                    'Financial Times' AS source,
                    fsa.ticker,
                    fsa.sector_name,
                    fsa.sector_weight_pct,
                    fsa.date_scraper
                FROM ft_sector_allocation fsa
                UNION ALL
                SELECT
                    'Yahoo Finance' AS source,
                    ys.ticker,
                    ys.sector AS sector_name,
                    CAST(NULLIF(REGEXP_REPLACE(ys.value, '[^0-9.-]', ''), '') AS DECIMAL(12,4)) AS sector_weight_pct,
                    ys.updated_at AS date_scraper
                FROM stg_yf_sectors ys
                UNION ALL
                SELECT
                    'Stock Analysis' AS source,
                    sc.ticker,
                    sc.category_name AS sector_name,
                    sc.percentage AS sector_weight_pct,
                    sc.date_scraper
                FROM stg_sa_sector_country sc
                WHERE LOWER(sc.type) = 'sector'
                """
            )

            cur.execute("DROP VIEW IF EXISTS vw_canonical_region_allocation")
            cur.execute(
                """
                CREATE VIEW vw_canonical_region_allocation AS
                SELECT
                    'Financial Times' AS source,
                    fra.ticker,
                    fra.region_name,
                    fra.region_weight_pct,
                    fra.date_scraper
                FROM ft_region_allocation fra
                UNION ALL
                SELECT
                    'Yahoo Finance' AS source,
                    ya.ticker,
                    ya.category AS region_name,
                    CAST(NULLIF(REGEXP_REPLACE(ya.value, '[^0-9.-]', ''), '') AS DECIMAL(12,4)) AS region_weight_pct,
                    ya.updated_at AS date_scraper
                FROM stg_yf_allocation ya
                UNION ALL
                SELECT
                    'Stock Analysis' AS source,
                    sc.ticker,
                    sc.category_name AS region_name,
                    sc.percentage AS region_weight_pct,
                    sc.date_scraper
                FROM stg_sa_sector_country sc
                WHERE LOWER(sc.type) IN ('country', 'region')
                """
            )

            cur.execute("DROP VIEW IF EXISTS vw_canonical_fund_return")
            cur.execute(
                """
                CREATE VIEW vw_canonical_fund_return AS
                SELECT
                    'Financial Times' AS source,
                    fr.ticker,
                    fr.ticker AS fund_key,
                    fr.avg_return_1y_pct AS avg_fund_return_1y,
                    fr.avg_fund_return_3y AS avg_fund_return_3y,
                    fr.as_of_date
                FROM ft_avg_fund_return fr
                UNION ALL
                SELECT
                    'Yahoo Finance' AS source,
                    yp.ticker,
                    yp.ticker AS fund_key,
                    CAST(NULLIF(REGEXP_REPLACE(yp.total_return_1y, '[^0-9.-]', ''), '') AS DECIMAL(12,6)) AS avg_fund_return_1y,
                    NULL AS avg_fund_return_3y,
                    yp.updated_at AS as_of_date
                FROM stg_yf_static_policy yp
                UNION ALL
                SELECT
                    'Stock Analysis' AS source,
                    sp.ticker,
                    sp.ticker AS fund_key,
                    CAST(NULLIF(REGEXP_REPLACE(sp.total_return_1y, '[^0-9.-]', ''), '') AS DECIMAL(12,6)) AS avg_fund_return_1y,
                    CAST(NULLIF(REGEXP_REPLACE(sp.div_growth_3y, '[^0-9.-]', ''), '') AS DECIMAL(12,6)) AS avg_fund_return_3y,
                    DATE(sp.updated_at) AS as_of_date
                FROM stg_sa_static_policy sp
                """
            )

        conn.commit()
        logger.info(
            "Created views: vw_canonical_fund_static, vw_canonical_holdings_top, "
            "vw_canonical_sector_allocation, vw_canonical_region_allocation, vw_canonical_fund_return"
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create canonical cross-source views for full fund flow.")
    parser.parse_args()
    create_views()


if __name__ == "__main__":
    main()
