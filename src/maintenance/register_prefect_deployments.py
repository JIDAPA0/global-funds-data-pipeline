import argparse
from dataclasses import dataclass
from typing import Dict, Optional

from src.maintenance.prefect_pipeline import run_data_pipeline

try:
    from prefect.deployments import Deployment
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Prefect is required to register deployments. "
        "Install dependencies in a Python 3.12 environment: pip install -r requirements.txt"
    ) from exc

try:
    from prefect.server.schemas.schedules import CronSchedule
except Exception:  # pragma: no cover
    from prefect.client.schemas.schedules import CronSchedule  # type: ignore


@dataclass
class DeploymentConfig:
    name: str
    cron: str
    timezone: str
    tags: list[str]
    description: str
    parameters: Dict[str, object]


def build_daily_config(timezone: str, daily_cron: str) -> DeploymentConfig:
    return DeploymentConfig(
        name="global-funds-daily",
        cron=daily_cron,
        timezone=timezone,
        tags=["prod", "daily", "market-open"],
        description=(
            "Daily pipeline run for FT/YF/SA with DB loaders, FT DQ fix, and data quality report. "
            "Holdings are still controlled by flow logic (market-closed gate)."
        ),
        parameters={
            "run_financial_times": True,
            "run_yahoo_finance": True,
            "run_stock_analysis": True,
            "holdings_weekly_on_market_closed": True,
            "force_run_holdings": False,
            "run_master_db_load": True,
            "run_security_master_merge": True,
            "run_daily_nav_db_load": True,
            "run_static_db_load": True,
            "run_holdings_db_load": True,
            "run_fx_rates_load": True,
            "fx_backfill_days": 1,
            "run_nav_data_mart_refresh": True,
            "run_ft_dq_fix": True,
            "run_data_quality_report": True,
            "parallel_by_source": True,
        },
    )


def build_weekly_config(timezone: str, weekly_cron: str) -> DeploymentConfig:
    return DeploymentConfig(
        name="global-funds-weekly-holdings",
        cron=weekly_cron,
        timezone=timezone,
        tags=["prod", "weekly", "holdings"],
        description=(
            "Weekly holdings refresh (FT/YF/SA holdings-related scrapers only), then DB load + DQ."
        ),
        parameters={
            "run_financial_times": True,
            "run_yahoo_finance": True,
            "run_stock_analysis": True,
            "run_ft_master": False,
            "run_ft_daily_nav": False,
            "run_ft_static_detail": False,
            "run_ft_holdings": True,
            "run_ft_sector_region": True,
            "run_yf_master": False,
            "run_yf_nav_etf": False,
            "run_yf_nav_fund": False,
            "run_yf_static_identity": False,
            "run_yf_static_fees": False,
            "run_yf_static_risk": False,
            "run_yf_static_policy": False,
            "run_yf_holdings": True,
            "run_sa_master": False,
            "run_sa_static_detail": False,
            "run_sa_holdings": True,
            "run_sa_sector_country": True,
            "run_master_db_load": False,
            "run_security_master_merge": False,
            "run_daily_nav_db_load": False,
            "run_static_db_load": False,
            "run_holdings_db_load": True,
            "run_fx_rates_load": True,
            "fx_backfill_days": 1,
            "run_nav_data_mart_refresh": True,
            "run_ft_dq_fix": True,
            "run_data_quality_report": True,
            "holdings_weekly_on_market_closed": False,
            "force_run_holdings": True,
            "parallel_by_source": True,
        },
    )


def apply_deployment(
    cfg: DeploymentConfig,
    work_queue_name: str,
    infra_overrides: Optional[dict] = None,
) -> str:
    deployment = Deployment.build_from_flow(
        flow=run_data_pipeline,
        name=cfg.name,
        version="1",
        description=cfg.description,
        tags=cfg.tags,
        parameters=cfg.parameters,
        schedule=CronSchedule(cron=cfg.cron, timezone=cfg.timezone),
        work_queue_name=work_queue_name,
        infra_overrides=infra_overrides or {},
    )
    return deployment.apply()


def main() -> None:
    parser = argparse.ArgumentParser(description="Register Prefect deployments (daily + weekly) in one run.")
    parser.add_argument("--timezone", default="America/New_York")
    parser.add_argument("--daily-cron", default="30 6 * * 1-5", help="Default: 06:30 Mon-Fri")
    parser.add_argument("--weekly-cron", default="0 9 * * 6", help="Default: 09:00 Saturday")
    parser.add_argument("--work-queue", default="default")
    args = parser.parse_args()

    daily_cfg = build_daily_config(args.timezone, args.daily_cron)
    weekly_cfg = build_weekly_config(args.timezone, args.weekly_cron)

    daily_id = apply_deployment(daily_cfg, work_queue_name=args.work_queue)
    weekly_id = apply_deployment(weekly_cfg, work_queue_name=args.work_queue)

    print("Registered deployments successfully:")
    print(f"- {daily_cfg.name} | id={daily_id} | cron='{daily_cfg.cron}' tz={daily_cfg.timezone}")
    print(f"- {weekly_cfg.name} | id={weekly_id} | cron='{weekly_cfg.cron}' tz={weekly_cfg.timezone}")


if __name__ == "__main__":
    main()
