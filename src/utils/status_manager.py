from datetime import datetime, timedelta
from typing import Any, Dict, Optional


STATUS_NEW = "new"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"

INACTIVE_THRESHOLD_DAYS = 7


class StatusManager:
    @staticmethod
    def get_inactive_cutoff_date(reference_date: Optional[datetime] = None) -> str:
        if reference_date is None:
            reference_date = datetime.now()

        cutoff_date = reference_date - timedelta(days=INACTIVE_THRESHOLD_DAYS)
        return cutoff_date.strftime("%Y-%m-%d")

    @staticmethod
    def determine_initial_status(ticker: str, name: str, source: str) -> str:
        if not name or name.strip() == "" or name.lower() == "nan":
            return STATUS_NEW
        return STATUS_NEW

    @staticmethod
    def should_promote_to_active(row_data: Dict[str, Any]) -> bool:
        ticker = row_data.get("ticker")
        name = row_data.get("name")

        has_ticker = ticker and str(ticker).strip() != ""
        has_name = name and str(name).strip() not in ["", "None", "NaN", "N/A"]
        return bool(has_ticker and has_name)

    @staticmethod
    def should_mark_inactive(last_seen_str: str) -> bool:
        if not last_seen_str:
            return True

        try:
            last_seen = datetime.strptime(last_seen_str, "%Y-%m-%d")
            cutoff_date = datetime.now() - timedelta(days=INACTIVE_THRESHOLD_DAYS)
            return last_seen < cutoff_date
        except ValueError:
            return False

    @staticmethod
    def get_sql_update_inactive(table_name: str = "stg_security_master") -> str:
        return f"""
            UPDATE {table_name}
            SET
                status = '{STATUS_INACTIVE}',
                updated_at = NOW()
            WHERE
                status = '{STATUS_ACTIVE}'
                AND last_seen < :cutoff_date
        """

    @staticmethod
    def get_sql_promote_new_to_active(table_name: str = "stg_security_master") -> str:
        return f"""
            UPDATE {table_name}
            SET
                status = '{STATUS_ACTIVE}',
                updated_at = NOW()
            WHERE
                status = '{STATUS_NEW}'
                AND name IS NOT NULL
                AND name != ''
                AND name != 'N/A'
        """
