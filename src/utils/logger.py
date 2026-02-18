import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from src.utils.path_manager import LOG_DIR
except ImportError:
    base_dir = Path(__file__).resolve().parent.parent.parent
    LOG_DIR = base_dir / "logs"


LOG_CATEGORY_MAP = {
    "01_master": "01_master_list_acquisition",
    "02_perf": "02_daily_performance",
    "03_static": "03_master_detail_static",
    "04_holdings": "04_holdings_acquisition",
    "05_sync": "05_db_synchronization",
    "99_sys": "99_system_maintenance",
}


def setup_logger(name: str, log_level: int = logging.INFO) -> logging.Logger:
    category_folder = "general"
    for prefix, folder_name in LOG_CATEGORY_MAP.items():
        if name.startswith(prefix):
            category_folder = folder_name
            break

    target_log_dir = LOG_DIR / category_folder
    target_log_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    log_file_path = target_log_dir / f"{name}_{today}.log"
    error_file_path = target_log_dir / f"{name}_{today}_error.log"

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    error_handler = logging.FileHandler(error_file_path, encoding="utf-8")
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    logger.addHandler(error_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    return logger


def log_execution_summary(
    logger: logging.Logger,
    start_time: Any,
    total_items: int = 0,
    success_count: int = 0,
    error_count: int = 0,
    status: str = "Completed",
    extra_info: Optional[Dict[str, Any]] = None,
) -> None:
    end_time = datetime.now()
    if isinstance(start_time, float):
        duration = timedelta(seconds=int(time.time() - start_time))
    else:
        duration = end_time - start_time

    def safe_int(value: Any) -> int:
        try:
            return int(str(value).replace(",", ""))
        except (ValueError, TypeError):
            return 0

    total_safe = safe_int(total_items)
    success_safe = safe_int(success_count)
    error_safe = safe_int(error_count)

    logger.info("=" * 60)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info("Duration:    %s", duration)
    logger.info("Total Items: %s", total_safe)

    if success_safe > 0 or error_safe > 0:
        logger.info("Success:     %s", success_safe)
        logger.info("Errors:      %s", error_safe)

    logger.info("Status:      %s", status)

    if extra_info:
        for key, value in extra_info.items():
            logger.info("%s: %s", key, value)

    logger.info("=" * 60)
