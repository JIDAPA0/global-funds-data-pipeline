import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_SQL = PROJECT_ROOT / "db/init/001_schema.sql"
REPORT_DIR = PROJECT_ROOT / "validation_output" / "System" / "schema_checks"


EXPECTED_MAPPINGS = {
    "FT master": {
        "table": "stg_ft_master_ticker",
        "columns": {"ft_ticker", "ticker", "name", "ticker_type", "source", "date_scraper", "url"},
        "sample_glob": "validation_output/Financial_Times/master_tickers/*/financial_times_master_tickers.csv",
    },
    "FT daily nav": {
        "table": "stg_ft_daily_nav",
        "columns": {"ft_ticker", "ticker", "name", "ticker_type", "nav_price", "nav_currency", "nav_as_of", "source", "date_scraper", "url"},
        "sample_glob": "validation_output/Financial_Times/Daily_NAV/*/financial_times_daily_nav.csv",
    },
    "FT static detail": {
        "table": "stg_ft_static_detail",
        "columns": {
            "ft_ticker",
            "ticker",
            "name",
            "ticker_type",
            "morningstar_category",
            "inception_date",
            "domicile",
            "isin_number",
            "assets_aum_raw",
            "assets_aum_full_value",
            "assets_aum_value",
            "assets_aum_unit",
            "assets_aum_currency",
            "assets_aum_as_of",
            "expense_ratio_raw",
            "expense_pct",
            "income_treatment",
            "source",
            "date_scraper",
            "url",
        },
        "sample_glob": "validation_output/Financial_Times/03_Detail_Static/*/financial_times_static_detail.csv",
    },
    "FT holdings": {
        "table": "stg_ft_holdings",
        "columns": {
            "ticker",
            "name",
            "ticker_type",
            "allocation_type",
            "holding_name",
            "holding_ticker",
            "holding_type",
            "holding_symbol",
            "holding_url",
            "portfolio_weight_pct",
            "top_10_holdings_weight_pct",
            "other_holding_weight_pct",
            "source",
            "date_scraper",
            "url",
        },
        "sample_glob": "validation_output/Financial_Times/04_Holdings/*/financial_times_holdings.csv",
    },
    "FT sector_region": {
        "table": "stg_ft_sector_region",
        "columns": {"ft_ticker", "ticker", "name", "ticker_type", "category_name", "weight_pct", "allocation_type", "url_type_used", "source", "date_scraper", "url"},
        "sample_glob": "validation_output/Financial_Times/04_Holdings/Sector_Region/*/*.csv",
    },
    "YF master": {
        "table": "stg_yf_master_ticker",
        "columns": {"ticker", "name", "ticker_type", "source", "date_scraper", "url"},
        "sample_glob": "validation_output/Yahoo_Finance/master_tickers/*/yf_ticker.csv",
    },
    "SA master": {
        "table": "stg_sa_master_ticker",
        "columns": {"ticker", "name", "ticker_type", "source", "date_scraper", "url"},
        "sample_glob": "validation_output/Stock_Analysis/01_List_Master/*/sa_etf_master.csv",
    },
    "YF nav": {
        "table": "stg_yf_daily_nav",
        "columns": {"ticker", "asset_type", "source", "nav_price", "currency", "as_of_date", "scrape_date"},
        "sample_glob": "data/02_performance/yahoo_finance/*/yf_nav_*.csv",
    },
    "YF static identity": {
        "table": "stg_yf_static_identity",
        "columns": {"ticker", "name", "exchange", "issuer", "category", "inception_date", "source", "updated_at"},
        "sample_glob": "validation_output/Yahoo_Finance/03_Detail_Static/*/yahoo_finance_identity.csv",
    },
    "YF static fees": {
        "table": "stg_yf_static_fees",
        "columns": {"ticker", "expense_ratio", "initial_charge", "exit_charge", "assets_aum", "top_10_hold_pct", "holdings_count", "holdings_turnover"},
        "sample_glob": "validation_output/Yahoo_Finance/03_Detail_Static/*/yahoo_finance_fees.csv",
    },
    "YF static risk": {
        "table": "stg_yf_static_risk",
        "columns": {
            "ticker",
            "morningstar_rating",
            "alpha_3y",
            "alpha_5y",
            "alpha_10y",
            "beta_3y",
            "beta_5y",
            "beta_10y",
            "mean_annual_return_3y",
            "mean_annual_return_5y",
            "mean_annual_return_10y",
            "r_squared_3y",
            "r_squared_5y",
            "r_squared_10y",
            "standard_deviation_3y",
            "standard_deviation_5y",
            "standard_deviation_10y",
            "sharpe_ratio_3y",
            "sharpe_ratio_5y",
            "sharpe_ratio_10y",
            "treynor_ratio_3y",
            "treynor_ratio_5y",
            "treynor_ratio_10y",
        },
        "sample_glob": "validation_output/Yahoo_Finance/03_Detail_Static/*/yahoo_finance_risk.csv",
    },
    "YF static policy": {
        "table": "stg_yf_static_policy",
        "columns": {"ticker", "div_yield", "pe_ratio", "total_return_ytd", "total_return_1y", "updated_at"},
        "sample_glob": "validation_output/Yahoo_Finance/03_Detail_Static/*/yahoo_finance_policy.csv",
    },
    "YF holdings": {
        "table": "stg_yf_holdings",
        "columns": {"ticker", "yahoo_ticker", "asset_type", "symbol", "name", "value", "updated_at"},
        "sample_glob": "validation_output/Yahoo_Finance/04_Holdings/Holdings/*.csv",
    },
    "YF sectors": {
        "table": "stg_yf_sectors",
        "columns": {"ticker", "asset_type", "sector", "value", "updated_at"},
        "sample_glob": "validation_output/Yahoo_Finance/04_Holdings/Sectors/*.csv",
    },
    "YF allocation": {
        "table": "stg_yf_allocation",
        "columns": {"ticker", "asset_type", "category", "value", "updated_at"},
        "sample_glob": "validation_output/Yahoo_Finance/04_Holdings/Allocation/*.csv",
    },
    "SA static info": {
        "table": "stg_sa_static_info",
        "columns": {
            "ticker",
            "asset_type",
            "source",
            "name",
            "isin_number",
            "cusip_number",
            "issuer",
            "category",
            "index_benchmark",
            "inception_date",
            "exchange",
            "region",
            "country",
            "leverage",
            "options",
            "shares_out",
            "market_cap_size",
        },
        "sample_glob": "validation_output/Stock_Analysis/03_Detail_Static/*/sa_fund_info.csv",
    },
    "SA static fees": {
        "table": "stg_sa_static_fees",
        "columns": {"ticker", "asset_type", "source", "expense_ratio", "initial_charge", "exit_charge", "assets_aum", "top_10_hold_pct", "holdings_count", "holdings_turnover"},
        "sample_glob": "validation_output/Stock_Analysis/03_Detail_Static/*/sa_fund_fees.csv",
    },
    "SA static risk": {
        "table": "stg_sa_static_risk",
        "columns": {"ticker", "asset_type", "source", "sharpe_ratio_5y", "beta_5y", "rsi_daily", "moving_avg_200"},
        "sample_glob": "validation_output/Stock_Analysis/03_Detail_Static/*/sa_fund_risk.csv",
    },
    "SA static policy": {
        "table": "stg_sa_static_policy",
        "columns": {"ticker", "asset_type", "source", "div_yield", "div_growth_1y", "div_growth_3y", "div_growth_5y", "div_growth_10y", "div_consecutive_years", "payout_ratio", "total_return_ytd", "total_return_1y", "pe_ratio"},
        "sample_glob": "validation_output/Stock_Analysis/03_Detail_Static/*/sa_fund_policy.csv",
    },
    "SA sector_country": {
        "table": "stg_sa_sector_country",
        "columns": {"ticker", "category_name", "percentage", "type", "source", "date_scraper", "url"},
        "sample_glob": "validation_output/Stock_Analysis/04_Holdings/*/sa_*_allocation.csv",
    },
}


def parse_sql_table_columns(sql_text: str) -> Dict[str, Set[str]]:
    tables: Dict[str, Set[str]] = {}
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z0-9_]+)\s*\((.*?)\)\s*ENGINE=",
        re.IGNORECASE | re.DOTALL,
    )
    for table_name, body in pattern.findall(sql_text):
        cols = set()
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            if line.upper().startswith(("PRIMARY KEY", "UNIQUE KEY", "KEY ", "CONSTRAINT")):
                continue
            match = re.match(r"([a-zA-Z0-9_]+)\s+", line)
            if match:
                cols.add(match.group(1))
        tables[table_name] = cols
    return tables


def check_mapping(sql_tables: Dict[str, Set[str]]) -> List[Tuple[str, str, str, List[str]]]:
    findings = []
    for output_name, cfg in EXPECTED_MAPPINGS.items():
        table = cfg["table"]
        expected_cols = set(cfg["columns"])
        table_cols = sql_tables.get(table, set())
        if not table_cols:
            findings.append((output_name, table, "MISSING_TABLE", sorted(expected_cols)))
            continue

        missing_cols = sorted(expected_cols - table_cols)
        status = "OK" if not missing_cols else "MISSING_COLUMNS"
        findings.append((output_name, table, status, missing_cols))
    return findings


def write_report(findings: List[Tuple[str, str, str, List[str]]]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"schema_mapping_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Schema Mapping Report\n\n")
        f.write("| Output | Target Table | Status | Missing Columns |\n")
        f.write("|---|---|---|---|\n")
        for output_name, table, status, missing in findings:
            missing_text = ", ".join(missing) if missing else "-"
            f.write(f"| {output_name} | `{table}` | {status} | {missing_text} |\n")

    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Check output schema-to-DB-table mapping coverage.")
    parser.parse_args()

    if not SCHEMA_SQL.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_SQL}")

    sql_text = SCHEMA_SQL.read_text(encoding="utf-8")
    tables = parse_sql_table_columns(sql_text)
    findings = check_mapping(tables)
    report_path = write_report(findings)

    print(f"Schema mapping report written to: {report_path}")
    for output_name, table, status, missing in findings:
        if status != "OK":
            print(f"[{status}] {output_name} -> {table}: {missing}")


if __name__ == "__main__":
    main()
