import argparse

from src.sites.Yahoo_Finance.yahoo_finance_nav_common import YahooFinanceNavConfig, run_nav_scraper


def main() -> None:
    parser = argparse.ArgumentParser(description="Yahoo Finance Fund NAV scraper")
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--normal-delay", type=int, default=2)
    parser.add_argument("--cool-down-delay", type=int, default=120)
    parser.add_argument("--sample", type=int, default=0, help="0 = all")
    args = parser.parse_args()

    cfg = YahooFinanceNavConfig(
        asset_type="FUND",
        batch_size=args.batch_size,
        normal_delay_sec=args.normal_delay,
        cool_down_delay_sec=args.cool_down_delay,
        sample=args.sample,
    )
    run_nav_scraper(cfg)


if __name__ == "__main__":
    main()
