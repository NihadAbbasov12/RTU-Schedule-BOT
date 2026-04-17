"""Application configuration loading."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


def _parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean environment variable."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int | None = None) -> int | None:
    """Parse an integer environment variable."""
    if value is None or value.strip() == "":
        return default
    return int(value)


def _parse_float(value: str | None, default: float) -> float:
    """Parse a float environment variable."""
    if value is None or value.strip() == "":
        return default
    return float(value)


def _require(name: str, value: str | None) -> str:
    """Ensure a required environment variable is present."""
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


@dataclass(slots=True, frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    telegram_bot_token: str
    telegram_chat_id: int | None = None
    rtu_base_url: str = "https://nodarbibas.rtu.lv"
    rtu_lang: str = "en"
    rtu_semester_id: int = 29
    rtu_program_id: int = 1128
    rtu_course_id: int = 1
    # Legacy values kept for one-time migration from older single-group deployments.
    rtu_group: str = "4"
    rtu_semester_program_id: int | None = None
    enable_scheduler: bool = True
    daily_today_hour: int = 7
    daily_tomorrow_hour: int = 19
    weekend_check_interval_minutes: int = 15
    timezone: str = "Europe/Riga"
    db_path: Path = Path("rtu_schedule.db")
    log_level: str = "INFO"
    request_timeout_seconds: int = 20
    request_connect_timeout_seconds: int = 10
    request_retries: int = 5
    request_backoff_seconds: float = 0.8

    @property
    def zoneinfo(self) -> ZoneInfo:
        """Return the configured timezone as a ZoneInfo instance."""
        return ZoneInfo(self.timezone)

    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from `.env` and the process environment."""
        load_dotenv()

        token = _require("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN"))
        chat_id = _parse_int(os.getenv("TELEGRAM_CHAT_ID"), None)
        semester_program_raw = os.getenv("RTU_SEMESTER_PROGRAM_ID")
        semester_program_id = _parse_int(semester_program_raw, None)

        return cls(
            telegram_bot_token=token,
            telegram_chat_id=chat_id,
            rtu_base_url=os.getenv("RTU_BASE_URL", "https://nodarbibas.rtu.lv").rstrip("/"),
            rtu_lang=os.getenv("RTU_LANG", "en").strip() or "en",
            rtu_semester_id=_parse_int(os.getenv("RTU_SEMESTER_ID"), 29) or 29,
            rtu_program_id=_parse_int(os.getenv("RTU_PROGRAM_ID"), 1128) or 1128,
            rtu_course_id=_parse_int(os.getenv("RTU_COURSE_ID"), 1) or 1,
            rtu_group=os.getenv("RTU_GROUP", "4").strip() or "4",
            rtu_semester_program_id=semester_program_id,
            enable_scheduler=_parse_bool(os.getenv("ENABLE_SCHEDULER"), True),
            daily_today_hour=_parse_int(os.getenv("DAILY_TODAY_HOUR"), 7) or 7,
            daily_tomorrow_hour=_parse_int(os.getenv("DAILY_TOMORROW_HOUR"), 19) or 19,
            weekend_check_interval_minutes=(
                _parse_int(os.getenv("WEEKEND_CHECK_INTERVAL_MINUTES"), 15) or 15
            ),
            timezone=os.getenv("TIMEZONE", "Europe/Riga").strip() or "Europe/Riga",
            db_path=Path(os.getenv("DB_PATH", "rtu_schedule.db")),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip() or "INFO",
            request_timeout_seconds=(
                _parse_int(os.getenv("REQUEST_TIMEOUT_SECONDS"), 20) or 20
            ),
            request_connect_timeout_seconds=(
                _parse_int(os.getenv("REQUEST_CONNECT_TIMEOUT_SECONDS"), 10) or 10
            ),
            request_retries=_parse_int(os.getenv("REQUEST_RETRIES"), 5) or 5,
            request_backoff_seconds=_parse_float(
                os.getenv("REQUEST_BACKOFF_SECONDS"),
                0.8,
            ),
        )
