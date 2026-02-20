import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Iterable, List, Set

import requests

from src.maintenance.load_master_lists_to_db import get_db_config
from src.utils.logger import setup_logger


logger = setup_logger("02_fx_rates")
API_BASE = "https://api.frankfurter.app"


@dataclass
class FxRateRow:
    rate_date: str
    from_currency: str
    to_currency: str
    fx_rate: Decimal
    provider: str


def ensure_fx_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_fx_rates (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          rate_date DATE NOT NULL,
          from_currency VARCHAR(16) NOT NULL,
          to_currency VARCHAR(16) NOT NULL,
          fx_rate DECIMAL(20,10) NOT NULL,
          provider VARCHAR(64) NOT NULL DEFAULT 'frankfurter',
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_fx_rate_date_pair (rate_date, from_currency, to_currency),
          KEY idx_fx_from_to_date (from_currency, to_currency, rate_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def load_required_currencies(cur, target_currency: str) -> Set[str]:
    currencies: Set[str] = {target_currency.upper(), "USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "HKD", "SGD", "THB", "CNY"}
    for table, col in [
        ("stg_ft_daily_nav", "nav_currency"),
        ("stg_yf_daily_nav", "currency"),
        ("stg_sa_daily_nav", "currency"),
    ]:
        try:
            cur.execute(f"SELECT DISTINCT UPPER({col}) FROM {table} WHERE {col} IS NOT NULL AND {col} <> ''")
            for (ccy,) in cur.fetchall():
                if ccy:
                    currencies.add(str(ccy).upper())
        except Exception as exc:
            logger.warning("Skip currency discovery on %s: %s", table, exc)
    # Remove obvious placeholders
    return {c for c in currencies if c and c not in {"N/A", "NA", "NULL", "NONE", "--"}}


def daterange(start_date: date, end_date: date) -> Iterable[date]:
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


def fetch_usd_cross_for_date(day: date, quote_currencies: List[str], timeout_sec: int = 20) -> dict:
    # Fetch base USD -> quotes; later invert to quote -> USD.
    to_list = ",".join(sorted([c for c in quote_currencies if c != "USD"]))
    if not to_list:
        return {"date": day.strftime("%Y-%m-%d"), "rates": {}}
    url = f"{API_BASE}/{day.strftime('%Y-%m-%d')}"
    resp = requests.get(url, params={"from": "USD", "to": to_list}, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.json()


def fetch_usd_cross_for_range(start_day: date, end_day: date, quote_currencies: List[str], timeout_sec: int = 30) -> dict:
    to_list = ",".join(sorted([c for c in quote_currencies if c != "USD"]))
    if not to_list:
        return {"start_date": start_day.strftime("%Y-%m-%d"), "end_date": end_day.strftime("%Y-%m-%d"), "rates": {}}
    url = f"{API_BASE}/{start_day.strftime('%Y-%m-%d')}..{end_day.strftime('%Y-%m-%d')}"
    resp = requests.get(url, params={"from": "USD", "to": to_list}, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.json()


def build_rows(payload: dict, target_currency: str, provider: str) -> List[FxRateRow]:
    out: List[FxRateRow] = []
    rate_date = str(payload.get("date"))
    rates = payload.get("rates", {}) or {}

    target = target_currency.upper()
    # Identity rows
    out.append(FxRateRow(rate_date=rate_date, from_currency=target, to_currency=target, fx_rate=Decimal("1"), provider=provider))

    if target == "USD":
        # Build quote -> USD using inverse of USD->quote.
        out.append(FxRateRow(rate_date=rate_date, from_currency="USD", to_currency="USD", fx_rate=Decimal("1"), provider=provider))
        for quote, usd_to_quote in rates.items():
            q = str(quote).upper()
            if q == "USD":
                continue
            try:
                v = Decimal(str(usd_to_quote))
                if v == 0:
                    continue
                out.append(
                    FxRateRow(
                        rate_date=rate_date,
                        from_currency=q,
                        to_currency="USD",
                        fx_rate=(Decimal("1") / v),
                        provider=provider,
                    )
                )
            except Exception:
                continue
        return out

    # Generic target flow (not used now, but supported):
    # Need USD->target and USD->quote to derive quote->target = (USD->target)/(USD->quote)
    usd_to_target = Decimal(str(rates.get(target))) if rates.get(target) is not None else None
    if usd_to_target is None:
        return out
    out.append(FxRateRow(rate_date=rate_date, from_currency="USD", to_currency=target, fx_rate=usd_to_target, provider=provider))
    for quote, usd_to_quote in rates.items():
        q = str(quote).upper()
        if q == target:
            continue
        try:
            uq = Decimal(str(usd_to_quote))
            if uq == 0:
                continue
            out.append(
                FxRateRow(
                    rate_date=rate_date,
                    from_currency=q,
                    to_currency=target,
                    fx_rate=(usd_to_target / uq),
                    provider=provider,
                )
            )
        except Exception:
            continue
    return out


def build_rows_from_rates_map(rates_map: dict, target_currency: str, provider: str) -> List[FxRateRow]:
    rows: List[FxRateRow] = []
    for rate_date, rates in (rates_map or {}).items():
        rows.extend(build_rows({"date": str(rate_date), "rates": rates or {}}, target_currency=target_currency, provider=provider))
    return rows


def upsert_rows(cur, rows: List[FxRateRow]) -> None:
    if not rows:
        return
    cur.executemany(
        """
        INSERT INTO daily_fx_rates (rate_date, from_currency, to_currency, fx_rate, provider)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          fx_rate = VALUES(fx_rate),
          provider = VALUES(provider),
          updated_at = CURRENT_TIMESTAMP
        """,
        [(r.rate_date, r.from_currency, r.to_currency, str(r.fx_rate), r.provider) for r in rows],
    )


def _parse_iso_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _discover_nav_min_date(cur) -> date:
    mins = []
    for table, col in [
        ("stg_ft_daily_nav", "nav_as_of"),
        ("stg_yf_daily_nav", "as_of_date"),
        ("stg_sa_daily_nav", "as_of_date"),
    ]:
        try:
            cur.execute(f"SELECT MIN({col}) FROM {table} WHERE {col} IS NOT NULL")
            v = cur.fetchone()[0]
            if v:
                mins.append(v)
        except Exception:
            continue
    if not mins:
        return datetime.now().date() - timedelta(days=89)
    return min(mins)


def run(
    days: int,
    target_currency: str,
    provider: str,
    dry_run: bool,
    from_date: str | None = None,
    to_date: str | None = None,
    from_nav_min: bool = False,
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

    end_day = _parse_iso_date(to_date) if to_date else datetime.now().date()
    start_day = end_day - timedelta(days=max(days - 1, 0))

    total_upserts = 0
    total_days = 0
    try:
        with conn.cursor() as cur:
            ensure_fx_table(cur)
            currencies = load_required_currencies(cur, target_currency=target_currency)
            if from_nav_min:
                start_day = _discover_nav_min_date(cur)
            if from_date:
                start_day = _parse_iso_date(from_date)
            if start_day > end_day:
                raise ValueError(f"start_day {start_day} is after end_day {end_day}")
            quotes = sorted(currencies - {"USD"}) if target_currency.upper() == "USD" else sorted(currencies | {target_currency.upper()})
            logger.info("FX backfill range: %s -> %s (%s days)", start_day, end_day, days)
            logger.info("Currencies (%s): %s", len(currencies), ",".join(sorted(currencies)))

            # Use range requests in chunks to avoid one-request-per-day over long backfills.
            chunk_days = 365
            chunk_start = start_day
            while chunk_start <= end_day:
                chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), end_day)
                total_days += (chunk_end - chunk_start).days + 1
                try:
                    payload = fetch_usd_cross_for_range(chunk_start, chunk_end, quote_currencies=quotes)
                    rows = build_rows_from_rates_map(payload.get("rates", {}), target_currency=target_currency, provider=provider)
                    upsert_rows(cur, rows)
                    total_upserts += len(rows)
                    logger.info("FX chunk done: %s -> %s | rows=%s", chunk_start, chunk_end, len(rows))
                except Exception as exc:
                    logger.warning("FX fetch failed on range %s -> %s: %s", chunk_start, chunk_end, exc)
                chunk_start = chunk_end + timedelta(days=1)

        if dry_run:
            conn.rollback()
            logger.info("Dry-run complete. No DB write. simulated_rows=%s days=%s", total_upserts, total_days)
            return
        conn.commit()
        logger.info("FX backfill completed: upsert_rows=%s days=%s", total_upserts, total_days)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and backfill daily FX rates into daily_fx_rates table.")
    parser.add_argument("--days", type=int, default=90, help="Backfill days ending today (default 90)")
    parser.add_argument("--target-currency", default="USD", help="Target currency for normalized conversion (default USD)")
    parser.add_argument("--provider", default="frankfurter", help="Provider label (default frankfurter)")
    parser.add_argument("--from-date", default=None, help="Override start date YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, help="Override end date YYYY-MM-DD")
    parser.add_argument("--from-nav-min", action="store_true", help="Backfill from earliest NAV date in DB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(
        days=args.days,
        target_currency=args.target_currency,
        provider=args.provider,
        dry_run=args.dry_run,
        from_date=args.from_date,
        to_date=args.to_date,
        from_nav_min=args.from_nav_min,
    )


if __name__ == "__main__":
    main()
