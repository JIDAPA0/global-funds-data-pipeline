import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("99_quality_report")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REPORT_ROOT = PROJECT_ROOT / "validation_output" / "System" / "quality_reports"


@dataclass
class TableCheck:
    source: str
    table: str
    null_condition: str
    dup_key_expr: str
    missing_ref_join: Optional[str] = None
    missing_ref_condition: Optional[str] = None


CHECKS: List[TableCheck] = [
    # Financial Times
    TableCheck("Financial_Times", "stg_ft_master_ticker", "ft_ticker IS NULL OR ft_ticker='' OR ticker IS NULL OR ticker=''", "ft_ticker"),
    TableCheck(
        "Financial_Times",
        "stg_ft_daily_nav",
        "ft_ticker IS NULL OR ft_ticker='' OR ticker IS NULL OR ticker='' OR nav_as_of IS NULL",
        "CONCAT_WS('|', ft_ticker, nav_as_of)",
        "LEFT JOIN stg_ft_master_ticker m ON m.ft_ticker=t.ft_ticker",
        "m.ft_ticker IS NULL",
    ),
    TableCheck(
        "Financial_Times",
        "stg_ft_static_detail",
        "ft_ticker IS NULL OR ft_ticker='' OR ticker IS NULL OR ticker='' OR date_scraper IS NULL",
        "CONCAT_WS('|', ft_ticker, date_scraper)",
        "LEFT JOIN stg_ft_master_ticker m ON m.ft_ticker=t.ft_ticker",
        "m.ft_ticker IS NULL",
    ),
    TableCheck(
        "Financial_Times",
        "stg_ft_holdings",
        "ticker IS NULL OR ticker='' OR holding_name IS NULL OR holding_name='' OR date_scraper IS NULL",
        "CONCAT_WS('|', ticker, date_scraper, allocation_type, holding_name)",
        "LEFT JOIN stg_ft_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Financial_Times",
        "stg_ft_sector_region",
        "ft_ticker IS NULL OR ft_ticker='' OR category_name IS NULL OR category_name='' OR date_scraper IS NULL",
        "CONCAT_WS('|', ft_ticker, date_scraper, allocation_type, category_name)",
        "LEFT JOIN stg_ft_master_ticker m ON m.ft_ticker=t.ft_ticker",
        "m.ft_ticker IS NULL",
    ),
    # Yahoo Finance
    TableCheck("Yahoo_Finance", "stg_yf_master_ticker", "ticker IS NULL OR ticker=''", "ticker"),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_daily_nav",
        "ticker IS NULL OR ticker='' OR as_of_date IS NULL",
        "CONCAT_WS('|', ticker, as_of_date)",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_static_identity",
        "ticker IS NULL OR ticker='' OR updated_at IS NULL",
        "CONCAT_WS('|', ticker, updated_at)",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_static_fees",
        "ticker IS NULL OR ticker=''",
        "ticker",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_static_risk",
        "ticker IS NULL OR ticker=''",
        "ticker",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_static_policy",
        "ticker IS NULL OR ticker='' OR updated_at IS NULL",
        "CONCAT_WS('|', ticker, updated_at)",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_holdings",
        "ticker IS NULL OR ticker='' OR updated_at IS NULL",
        "CONCAT_WS('|', ticker, updated_at, COALESCE(symbol,''), COALESCE(name,''))",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_sectors",
        "ticker IS NULL OR ticker='' OR sector IS NULL OR sector='' OR updated_at IS NULL",
        "CONCAT_WS('|', ticker, updated_at, sector)",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Yahoo_Finance",
        "stg_yf_allocation",
        "ticker IS NULL OR ticker='' OR category IS NULL OR category='' OR updated_at IS NULL",
        "CONCAT_WS('|', ticker, updated_at, category)",
        "LEFT JOIN stg_yf_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    # Stock Analysis
    TableCheck("Stock_Analysis", "stg_sa_master_ticker", "ticker IS NULL OR ticker=''", "ticker"),
    TableCheck(
        "Stock_Analysis",
        "stg_sa_daily_nav",
        "ticker IS NULL OR ticker='' OR as_of_date IS NULL",
        "CONCAT_WS('|', ticker, as_of_date)",
        "LEFT JOIN stg_sa_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Stock_Analysis",
        "stg_sa_static_info",
        "ticker IS NULL OR ticker=''",
        "ticker",
        "LEFT JOIN stg_sa_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Stock_Analysis",
        "stg_sa_static_fees",
        "ticker IS NULL OR ticker=''",
        "ticker",
        "LEFT JOIN stg_sa_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Stock_Analysis",
        "stg_sa_static_risk",
        "ticker IS NULL OR ticker=''",
        "ticker",
        "LEFT JOIN stg_sa_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Stock_Analysis",
        "stg_sa_static_policy",
        "ticker IS NULL OR ticker=''",
        "ticker",
        "LEFT JOIN stg_sa_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Stock_Analysis",
        "stg_sa_holdings",
        "ticker IS NULL OR ticker='' OR file_name IS NULL OR file_name=''",
        "CONCAT_WS('|', ticker, file_name)",
        "LEFT JOIN stg_sa_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
    TableCheck(
        "Stock_Analysis",
        "stg_sa_sector_country",
        "ticker IS NULL OR ticker='' OR category_name IS NULL OR category_name=''",
        "CONCAT_WS('|', ticker, date_scraper, type, category_name)",
        "LEFT JOIN stg_sa_master_ticker m ON m.ticker=t.ticker",
        "m.ticker IS NULL",
    ),
]


def fetch_scalar(cur, query: str) -> int:
    cur.execute(query)
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def run_checks() -> List[Dict[str, int]]:
    import pymysql

    db = get_db_config()
    conn = pymysql.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        database=db.database,
        charset="utf8mb4",
        autocommit=True,
    )

    rows: List[Dict[str, int]] = []
    try:
        with conn.cursor() as cur:
            for check in CHECKS:
                total = fetch_scalar(cur, f"SELECT COUNT(*) FROM {check.table}")
                null_count = fetch_scalar(cur, f"SELECT COUNT(*) FROM {check.table} t WHERE {check.null_condition}")
                dup_count = fetch_scalar(
                    cur,
                    f"SELECT COALESCE(SUM(x.cnt - 1), 0) FROM ("
                    f"SELECT {check.dup_key_expr} AS k, COUNT(*) AS cnt FROM {check.table} t "
                    f"GROUP BY {check.dup_key_expr} HAVING COUNT(*) > 1"
                    f") x",
                )

                missing_count = 0
                if check.missing_ref_join and check.missing_ref_condition:
                    missing_count = fetch_scalar(
                        cur,
                        f"SELECT COUNT(*) FROM {check.table} t {check.missing_ref_join} WHERE {check.missing_ref_condition}",
                    )

                rows.append(
                    {
                        "source": check.source,
                        "table": check.table,
                        "total_rows": total,
                        "null_critical": null_count,
                        "dup_rows": dup_count,
                        "missing_master_ref": missing_count,
                    }
                )
    finally:
        conn.close()

    return rows


def write_reports(results: List[Dict[str, int]]) -> Dict[str, Path]:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    detail_csv = REPORT_ROOT / f"data_quality_detail_{run_ts}.csv"
    summary_csv = REPORT_ROOT / f"data_quality_summary_{run_ts}.csv"
    summary_md = REPORT_ROOT / f"data_quality_summary_{run_ts}.md"

    fields = ["source", "table", "total_rows", "null_critical", "dup_rows", "missing_master_ref"]
    with detail_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)

    aggregate: Dict[str, Dict[str, int]] = {}
    for row in results:
        src = row["source"]
        if src not in aggregate:
            aggregate[src] = {
                "tables": 0,
                "total_rows": 0,
                "null_critical": 0,
                "dup_rows": 0,
                "missing_master_ref": 0,
            }
        aggregate[src]["tables"] += 1
        aggregate[src]["total_rows"] += int(row["total_rows"])
        aggregate[src]["null_critical"] += int(row["null_critical"])
        aggregate[src]["dup_rows"] += int(row["dup_rows"])
        aggregate[src]["missing_master_ref"] += int(row["missing_master_ref"])

    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["source", "tables", "total_rows", "null_critical", "dup_rows", "missing_master_ref"],
        )
        w.writeheader()
        for src in sorted(aggregate):
            w.writerow({"source": src, **aggregate[src]})

    with summary_md.open("w", encoding="utf-8") as f:
        f.write("# Data Quality Summary\n\n")
        f.write("| Source | Tables | Total Rows | Null Critical | Duplicate Rows | Missing Master Ref |\n")
        f.write("|---|---:|---:|---:|---:|---:|\n")
        for src in sorted(aggregate):
            item = aggregate[src]
            f.write(
                f"| {src} | {item['tables']} | {item['total_rows']} | {item['null_critical']} | "
                f"{item['dup_rows']} | {item['missing_master_ref']} |\n"
            )

    return {"detail_csv": detail_csv, "summary_csv": summary_csv, "summary_md": summary_md}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate data quality report (null/dup/missing) per source.")
    parser.parse_args()

    results = run_checks()
    output = write_reports(results)
    logger.info("Data quality detail report: %s", output["detail_csv"])
    logger.info("Data quality source summary CSV: %s", output["summary_csv"])
    logger.info("Data quality source summary MD: %s", output["summary_md"])


if __name__ == "__main__":
    main()
