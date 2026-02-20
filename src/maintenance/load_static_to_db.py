import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("03_static_loader")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PLACEHOLDER_NULLS = {"", "--", "N/A", "NA", "NONE", "NULL", "NAN"}


def _norm_text(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    if text.upper() in PLACEHOLDER_NULLS:
        return None
    return text


def _norm_date(value: Optional[str]) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d %b %Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _to_float(value: Optional[str]) -> Optional[float]:
    text = (value or "").strip()
    if not text or text.upper() in PLACEHOLDER_NULLS:
        return None
    cleaned = text.replace(",", "").replace("$", "")
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


def clean_ft_static(path: Path) -> Tuple[List[Tuple], Dict[str, int]]:
    stats = {"input": 0, "invalid": 0, "deduped": 0, "ready": 0}
    dedupe: Dict[Tuple[str, str], Tuple] = {}
    for row in _load_csv(path):
        stats["input"] += 1
        ft_ticker = _norm_text(row.get("ft_ticker"))
        date_scraper = _norm_date(row.get("date_scraper")) or datetime.now().strftime("%Y-%m-%d")
        if not ft_ticker:
            stats["invalid"] += 1
            continue
        payload = (
            ft_ticker,
            _norm_text(row.get("ticker")) or ft_ticker.split(":")[0],
            _norm_text(row.get("name")) or ft_ticker,
            _norm_text(row.get("ticker_type")) or "Unknown",
            _norm_text(row.get("morningstar_category")),
            _norm_date(row.get("inception_date")),
            _norm_text(row.get("domicile")),
            _norm_text(row.get("isin_number")),
            _norm_text(row.get("assets_aum_raw")),
            _to_float(row.get("assets_aum_full_value")),
            _to_float(row.get("assets_aum_value")),
            _norm_text(row.get("assets_aum_unit")),
            _norm_text(row.get("assets_aum_currency")),
            _norm_date(row.get("assets_aum_as_of")),
            _norm_text(row.get("expense_ratio_raw")),
            _to_float(row.get("expense_pct")),
            _norm_text(row.get("income_treatment")),
            _norm_text(row.get("source")) or "Financial Times",
            date_scraper,
            _norm_text(row.get("url")),
        )
        dedupe[(ft_ticker, date_scraper)] = payload
    stats["deduped"] = stats["input"] - stats["invalid"] - len(dedupe)
    stats["ready"] = len(dedupe)
    return list(dedupe.values()), stats


def clean_simple_by_ticker(path: Path, columns: List[str], defaults: Dict[str, str]) -> Tuple[List[Tuple], Dict[str, int]]:
    stats = {"input": 0, "invalid": 0, "deduped": 0, "ready": 0}
    dedupe: Dict[str, Tuple] = {}
    for row in _load_csv(path):
        stats["input"] += 1
        ticker = _norm_text(row.get("ticker"))
        if not ticker:
            stats["invalid"] += 1
            continue
        out: List[Optional[str]] = []
        for col in columns:
            val = _norm_text(row.get(col))
            if val is None and col in defaults:
                val = defaults[col]
            out.append(val)
        dedupe[ticker.upper()] = tuple(out)
    stats["deduped"] = stats["input"] - stats["invalid"] - len(dedupe)
    stats["ready"] = len(dedupe)
    return list(dedupe.values()), stats


def upsert_static(
    ft_rows: List[Tuple],
    yf_identity_rows: List[Tuple],
    yf_fees_rows: List[Tuple],
    yf_risk_rows: List[Tuple],
    yf_policy_rows: List[Tuple],
    sa_info_rows: List[Tuple],
    sa_fees_rows: List[Tuple],
    sa_risk_rows: List[Tuple],
    sa_policy_rows: List[Tuple],
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
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_ft_static_detail (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ft_ticker VARCHAR(64) NOT NULL,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NOT NULL,
                  ticker_type VARCHAR(32) NOT NULL,
                  morningstar_category VARCHAR(255) NULL,
                  inception_date DATE NULL,
                  domicile VARCHAR(128) NULL,
                  isin_number VARCHAR(32) NULL,
                  assets_aum_raw VARCHAR(255) NULL,
                  assets_aum_full_value DECIMAL(22,2) NULL,
                  assets_aum_value DECIMAL(22,8) NULL,
                  assets_aum_unit VARCHAR(16) NULL,
                  assets_aum_currency VARCHAR(16) NULL,
                  assets_aum_as_of DATE NULL,
                  expense_ratio_raw VARCHAR(64) NULL,
                  expense_pct DECIMAL(10,4) NULL,
                  income_treatment VARCHAR(128) NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
                  date_scraper DATE NOT NULL,
                  url VARCHAR(1024) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_ft_static_ft_ticker_date (ft_ticker, date_scraper)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_static_identity (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  name VARCHAR(512) NULL,
                  exchange VARCHAR(128) NULL,
                  issuer VARCHAR(255) NULL,
                  category VARCHAR(255) NULL,
                  inception_date VARCHAR(64) NULL,
                  source VARCHAR(64) NOT NULL DEFAULT 'Yahoo Finance',
                  updated_at DATE NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_yf_identity_ticker_date (ticker, updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_static_fees (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  expense_ratio VARCHAR(64) NULL,
                  initial_charge VARCHAR(64) NULL,
                  exit_charge VARCHAR(64) NULL,
                  assets_aum VARCHAR(128) NULL,
                  top_10_hold_pct VARCHAR(64) NULL,
                  holdings_count VARCHAR(64) NULL,
                  holdings_turnover VARCHAR(64) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_yf_fees_ticker (ticker)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_static_risk (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  morningstar_rating VARCHAR(16) NULL,
                  alpha_3y VARCHAR(64) NULL,
                  alpha_5y VARCHAR(64) NULL,
                  alpha_10y VARCHAR(64) NULL,
                  beta_3y VARCHAR(64) NULL,
                  beta_5y VARCHAR(64) NULL,
                  beta_10y VARCHAR(64) NULL,
                  mean_annual_return_3y VARCHAR(64) NULL,
                  mean_annual_return_5y VARCHAR(64) NULL,
                  mean_annual_return_10y VARCHAR(64) NULL,
                  r_squared_3y VARCHAR(64) NULL,
                  r_squared_5y VARCHAR(64) NULL,
                  r_squared_10y VARCHAR(64) NULL,
                  standard_deviation_3y VARCHAR(64) NULL,
                  standard_deviation_5y VARCHAR(64) NULL,
                  standard_deviation_10y VARCHAR(64) NULL,
                  sharpe_ratio_3y VARCHAR(64) NULL,
                  sharpe_ratio_5y VARCHAR(64) NULL,
                  sharpe_ratio_10y VARCHAR(64) NULL,
                  treynor_ratio_3y VARCHAR(64) NULL,
                  treynor_ratio_5y VARCHAR(64) NULL,
                  treynor_ratio_10y VARCHAR(64) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_yf_risk_ticker (ticker)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_yf_static_policy (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  div_yield VARCHAR(64) NULL,
                  pe_ratio VARCHAR(64) NULL,
                  total_return_ytd VARCHAR(64) NULL,
                  total_return_1y VARCHAR(64) NULL,
                  updated_at DATE NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_yf_policy_ticker_date (ticker, updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_sa_static_info (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  asset_type VARCHAR(32) NULL,
                  source VARCHAR(64) NULL,
                  name VARCHAR(512) NULL,
                  isin_number VARCHAR(32) NULL,
                  cusip_number VARCHAR(32) NULL,
                  issuer VARCHAR(255) NULL,
                  category VARCHAR(255) NULL,
                  index_benchmark VARCHAR(255) NULL,
                  inception_date VARCHAR(64) NULL,
                  exchange VARCHAR(64) NULL,
                  region VARCHAR(128) NULL,
                  country VARCHAR(128) NULL,
                  leverage VARCHAR(64) NULL,
                  options VARCHAR(64) NULL,
                  shares_out VARCHAR(64) NULL,
                  market_cap_size VARCHAR(64) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_sa_info_ticker (ticker)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_sa_static_fees (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  asset_type VARCHAR(32) NULL,
                  source VARCHAR(64) NULL,
                  expense_ratio VARCHAR(64) NULL,
                  initial_charge VARCHAR(64) NULL,
                  exit_charge VARCHAR(64) NULL,
                  assets_aum VARCHAR(128) NULL,
                  top_10_hold_pct VARCHAR(64) NULL,
                  holdings_count VARCHAR(64) NULL,
                  holdings_turnover VARCHAR(64) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_sa_fees_ticker (ticker)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_sa_static_risk (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  asset_type VARCHAR(32) NULL,
                  source VARCHAR(64) NULL,
                  sharpe_ratio_5y VARCHAR(64) NULL,
                  beta_5y VARCHAR(64) NULL,
                  rsi_daily VARCHAR(64) NULL,
                  moving_avg_200 VARCHAR(64) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_sa_risk_ticker (ticker)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stg_sa_static_policy (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                  ticker VARCHAR(32) NOT NULL,
                  asset_type VARCHAR(32) NULL,
                  source VARCHAR(64) NULL,
                  div_yield VARCHAR(64) NULL,
                  div_growth_1y VARCHAR(64) NULL,
                  div_growth_3y VARCHAR(64) NULL,
                  div_growth_5y VARCHAR(64) NULL,
                  div_growth_10y VARCHAR(64) NULL,
                  div_consecutive_years VARCHAR(64) NULL,
                  payout_ratio VARCHAR(64) NULL,
                  total_return_ytd VARCHAR(64) NULL,
                  total_return_1y VARCHAR(64) NULL,
                  pe_ratio VARCHAR(64) NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uq_sa_policy_ticker (ticker)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            if ft_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_ft_static_detail
                    (ft_ticker,ticker,name,ticker_type,morningstar_category,inception_date,domicile,isin_number,
                     assets_aum_raw,assets_aum_full_value,assets_aum_value,assets_aum_unit,assets_aum_currency,
                     assets_aum_as_of,expense_ratio_raw,expense_pct,income_treatment,source,date_scraper,url)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      ticker=VALUES(ticker),name=VALUES(name),ticker_type=VALUES(ticker_type),
                      morningstar_category=VALUES(morningstar_category),inception_date=VALUES(inception_date),
                      domicile=VALUES(domicile),isin_number=VALUES(isin_number),assets_aum_raw=VALUES(assets_aum_raw),
                      assets_aum_full_value=VALUES(assets_aum_full_value),assets_aum_value=VALUES(assets_aum_value),
                      assets_aum_unit=VALUES(assets_aum_unit),assets_aum_currency=VALUES(assets_aum_currency),
                      assets_aum_as_of=VALUES(assets_aum_as_of),expense_ratio_raw=VALUES(expense_ratio_raw),
                      expense_pct=VALUES(expense_pct),income_treatment=VALUES(income_treatment),
                      source=VALUES(source),url=VALUES(url),updated_at=CURRENT_TIMESTAMP
                    """,
                    ft_rows,
                )
            if yf_identity_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_static_identity
                    (ticker,name,exchange,issuer,category,inception_date,source,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      name=VALUES(name),exchange=VALUES(exchange),issuer=VALUES(issuer),category=VALUES(category),
                      inception_date=VALUES(inception_date),source=VALUES(source),updated_ts=CURRENT_TIMESTAMP
                    """,
                    yf_identity_rows,
                )
            if yf_fees_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_static_fees
                    (ticker,expense_ratio,initial_charge,exit_charge,assets_aum,top_10_hold_pct,holdings_count,holdings_turnover)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      expense_ratio=VALUES(expense_ratio),initial_charge=VALUES(initial_charge),exit_charge=VALUES(exit_charge),
                      assets_aum=VALUES(assets_aum),top_10_hold_pct=VALUES(top_10_hold_pct),holdings_count=VALUES(holdings_count),
                      holdings_turnover=VALUES(holdings_turnover),updated_at=CURRENT_TIMESTAMP
                    """,
                    yf_fees_rows,
                )
            if yf_risk_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_static_risk
                    (ticker,morningstar_rating,alpha_3y,alpha_5y,alpha_10y,beta_3y,beta_5y,beta_10y,
                     mean_annual_return_3y,mean_annual_return_5y,mean_annual_return_10y,r_squared_3y,r_squared_5y,r_squared_10y,
                     standard_deviation_3y,standard_deviation_5y,standard_deviation_10y,sharpe_ratio_3y,sharpe_ratio_5y,sharpe_ratio_10y,
                     treynor_ratio_3y,treynor_ratio_5y,treynor_ratio_10y)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      morningstar_rating=VALUES(morningstar_rating),alpha_3y=VALUES(alpha_3y),alpha_5y=VALUES(alpha_5y),alpha_10y=VALUES(alpha_10y),
                      beta_3y=VALUES(beta_3y),beta_5y=VALUES(beta_5y),beta_10y=VALUES(beta_10y),
                      mean_annual_return_3y=VALUES(mean_annual_return_3y),mean_annual_return_5y=VALUES(mean_annual_return_5y),
                      mean_annual_return_10y=VALUES(mean_annual_return_10y),r_squared_3y=VALUES(r_squared_3y),r_squared_5y=VALUES(r_squared_5y),
                      r_squared_10y=VALUES(r_squared_10y),standard_deviation_3y=VALUES(standard_deviation_3y),standard_deviation_5y=VALUES(standard_deviation_5y),
                      standard_deviation_10y=VALUES(standard_deviation_10y),sharpe_ratio_3y=VALUES(sharpe_ratio_3y),sharpe_ratio_5y=VALUES(sharpe_ratio_5y),
                      sharpe_ratio_10y=VALUES(sharpe_ratio_10y),treynor_ratio_3y=VALUES(treynor_ratio_3y),treynor_ratio_5y=VALUES(treynor_ratio_5y),
                      treynor_ratio_10y=VALUES(treynor_ratio_10y),updated_at=CURRENT_TIMESTAMP
                    """,
                    yf_risk_rows,
                )
            if yf_policy_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_yf_static_policy
                    (ticker,div_yield,pe_ratio,total_return_ytd,total_return_1y,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      div_yield=VALUES(div_yield),pe_ratio=VALUES(pe_ratio),total_return_ytd=VALUES(total_return_ytd),
                      total_return_1y=VALUES(total_return_1y),updated_ts=CURRENT_TIMESTAMP
                    """,
                    yf_policy_rows,
                )
            if sa_info_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_static_info
                    (ticker,asset_type,source,name,isin_number,cusip_number,issuer,category,index_benchmark,inception_date,
                     exchange,region,country,leverage,options,shares_out,market_cap_size)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      asset_type=VALUES(asset_type),source=VALUES(source),name=VALUES(name),isin_number=VALUES(isin_number),
                      cusip_number=VALUES(cusip_number),issuer=VALUES(issuer),category=VALUES(category),index_benchmark=VALUES(index_benchmark),
                      inception_date=VALUES(inception_date),exchange=VALUES(exchange),region=VALUES(region),country=VALUES(country),
                      leverage=VALUES(leverage),options=VALUES(options),shares_out=VALUES(shares_out),market_cap_size=VALUES(market_cap_size),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    sa_info_rows,
                )
            if sa_fees_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_static_fees
                    (ticker,asset_type,source,expense_ratio,initial_charge,exit_charge,assets_aum,top_10_hold_pct,holdings_count,holdings_turnover)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      asset_type=VALUES(asset_type),source=VALUES(source),expense_ratio=VALUES(expense_ratio),initial_charge=VALUES(initial_charge),
                      exit_charge=VALUES(exit_charge),assets_aum=VALUES(assets_aum),top_10_hold_pct=VALUES(top_10_hold_pct),
                      holdings_count=VALUES(holdings_count),holdings_turnover=VALUES(holdings_turnover),updated_at=CURRENT_TIMESTAMP
                    """,
                    sa_fees_rows,
                )
            if sa_risk_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_static_risk
                    (ticker,asset_type,source,sharpe_ratio_5y,beta_5y,rsi_daily,moving_avg_200)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      asset_type=VALUES(asset_type),source=VALUES(source),sharpe_ratio_5y=VALUES(sharpe_ratio_5y),
                      beta_5y=VALUES(beta_5y),rsi_daily=VALUES(rsi_daily),moving_avg_200=VALUES(moving_avg_200),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    sa_risk_rows,
                )
            if sa_policy_rows:
                cur.executemany(
                    """
                    INSERT INTO stg_sa_static_policy
                    (ticker,asset_type,source,div_yield,div_growth_1y,div_growth_3y,div_growth_5y,div_growth_10y,
                     div_consecutive_years,payout_ratio,total_return_ytd,total_return_1y,pe_ratio)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      asset_type=VALUES(asset_type),source=VALUES(source),div_yield=VALUES(div_yield),div_growth_1y=VALUES(div_growth_1y),
                      div_growth_3y=VALUES(div_growth_3y),div_growth_5y=VALUES(div_growth_5y),div_growth_10y=VALUES(div_growth_10y),
                      div_consecutive_years=VALUES(div_consecutive_years),payout_ratio=VALUES(payout_ratio),
                      total_return_ytd=VALUES(total_return_ytd),total_return_1y=VALUES(total_return_1y),pe_ratio=VALUES(pe_ratio),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    sa_policy_rows,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and load FT/YF/SA static outputs into DB staging.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ft_file = _latest_file(PROJECT_ROOT / "validation_output" / "Financial_Times" / "03_Detail_Static", "financial_times_static_detail.csv")
    yf_base = PROJECT_ROOT / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
    sa_base = PROJECT_ROOT / "validation_output" / "Stock_Analysis" / "03_Detail_Static"

    yf_identity = _latest_file(yf_base, "yahoo_finance_identity.csv")
    yf_fees = _latest_file(yf_base, "yahoo_finance_fees.csv")
    yf_risk = _latest_file(yf_base, "yahoo_finance_risk.csv")
    yf_policy = _latest_file(yf_base, "yahoo_finance_policy.csv")

    sa_info = _latest_file(sa_base, "sa_fund_info.csv")
    sa_fees = _latest_file(sa_base, "sa_fund_fees.csv")
    sa_risk = _latest_file(sa_base, "sa_fund_risk.csv")
    sa_policy = _latest_file(sa_base, "sa_fund_policy.csv")

    required = [ft_file, yf_identity, yf_fees, yf_risk, yf_policy, sa_info, sa_fees, sa_risk, sa_policy]
    if any(p is None for p in required):
        raise FileNotFoundError(f"Missing static files: {required}")

    ft_rows, ft_stats = clean_ft_static(ft_file)  # type: ignore[arg-type]
    yf_identity_rows, yf_identity_stats = clean_simple_by_ticker(
        yf_identity,  # type: ignore[arg-type]
        ["ticker", "name", "exchange", "issuer", "category", "inception_date", "source", "updated_at"],
        {"source": "Yahoo Finance", "updated_at": datetime.now().strftime("%Y-%m-%d")},
    )
    yf_fees_rows, yf_fees_stats = clean_simple_by_ticker(
        yf_fees,  # type: ignore[arg-type]
        ["ticker", "expense_ratio", "initial_charge", "exit_charge", "assets_aum", "top_10_hold_pct", "holdings_count", "holdings_turnover"],
        {},
    )
    yf_risk_rows, yf_risk_stats = clean_simple_by_ticker(
        yf_risk,  # type: ignore[arg-type]
        [
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
        ],
        {},
    )
    yf_policy_rows, yf_policy_stats = clean_simple_by_ticker(
        yf_policy,  # type: ignore[arg-type]
        ["ticker", "div_yield", "pe_ratio", "total_return_ytd", "total_return_1y", "updated_at"],
        {"updated_at": datetime.now().strftime("%Y-%m-%d")},
    )

    sa_info_rows, sa_info_stats = clean_simple_by_ticker(
        sa_info,  # type: ignore[arg-type]
        [
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
        ],
        {"asset_type": "ETF", "source": "Stock Analysis"},
    )
    sa_fees_rows, sa_fees_stats = clean_simple_by_ticker(
        sa_fees,  # type: ignore[arg-type]
        ["ticker", "asset_type", "source", "expense_ratio", "initial_charge", "exit_charge", "assets_aum", "top_10_hold_pct", "holdings_count", "holdings_turnover"],
        {"asset_type": "ETF", "source": "Stock Analysis"},
    )
    sa_risk_rows, sa_risk_stats = clean_simple_by_ticker(
        sa_risk,  # type: ignore[arg-type]
        ["ticker", "asset_type", "source", "sharpe_ratio_5y", "beta_5y", "rsi_daily", "moving_avg_200"],
        {"asset_type": "ETF", "source": "Stock Analysis"},
    )
    sa_policy_rows, sa_policy_stats = clean_simple_by_ticker(
        sa_policy,  # type: ignore[arg-type]
        [
            "ticker",
            "asset_type",
            "source",
            "div_yield",
            "div_growth_1y",
            "div_growth_3y",
            "div_growth_5y",
            "div_growth_10y",
            "div_consecutive_years",
            "payout_ratio",
            "total_return_ytd",
            "total_return_1y",
            "pe_ratio",
        ],
        {"asset_type": "ETF", "source": "Stock Analysis"},
    )

    logger.info("FT static stats: %s", ft_stats)
    logger.info("YF static stats: identity=%s fees=%s risk=%s policy=%s", yf_identity_stats, yf_fees_stats, yf_risk_stats, yf_policy_stats)
    logger.info("SA static stats: info=%s fees=%s risk=%s policy=%s", sa_info_stats, sa_fees_stats, sa_risk_stats, sa_policy_stats)

    if args.dry_run:
        logger.info("Dry-run complete. No DB writes.")
        return

    upsert_static(
        ft_rows=ft_rows,
        yf_identity_rows=yf_identity_rows,
        yf_fees_rows=yf_fees_rows,
        yf_risk_rows=yf_risk_rows,
        yf_policy_rows=yf_policy_rows,
        sa_info_rows=sa_info_rows,
        sa_fees_rows=sa_fees_rows,
        sa_risk_rows=sa_risk_rows,
        sa_policy_rows=sa_policy_rows,
    )
    logger.info(
        "DB load completed: FT=%s YF(identity/fees/risk/policy)=%s/%s/%s/%s SA(info/fees/risk/policy)=%s/%s/%s/%s",
        len(ft_rows),
        len(yf_identity_rows),
        len(yf_fees_rows),
        len(yf_risk_rows),
        len(yf_policy_rows),
        len(sa_info_rows),
        len(sa_fees_rows),
        len(sa_risk_rows),
        len(sa_policy_rows),
    )


if __name__ == "__main__":
    main()
