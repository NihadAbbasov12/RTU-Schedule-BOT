"""APScheduler integration for daily Telegram delivery."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import Settings

LOGGER = logging.getLogger(__name__)


class BotScheduler:
    """Manage periodic Telegram schedule delivery."""

    def __init__(
        self,
        settings: Settings,
        send_today: Callable[[], Awaitable[None]],
        send_tomorrow: Callable[[], Awaitable[None]],
        send_weekend: Callable[[], Awaitable[None]],
    ) -> None:
        self.settings = settings
        self._scheduler = AsyncIOScheduler(timezone=ZoneInfo(settings.timezone))
        self._send_today = send_today
        self._send_tomorrow = send_tomorrow
        self._send_weekend = send_weekend

    def start(self) -> None:
        """Start the daily jobs."""
        self._scheduler.add_job(
            self._wrap_job("send_today_schedule", self._send_today),
            trigger=CronTrigger(hour=self.settings.daily_today_hour, minute=0),
            id="send_today_schedule",
            replace_existing=True,
            misfire_grace_time=900,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._wrap_job("send_tomorrow_schedule", self._send_tomorrow),
            trigger=CronTrigger(hour=self.settings.daily_tomorrow_hour, minute=0),
            id="send_tomorrow_schedule",
            replace_existing=True,
            misfire_grace_time=900,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._wrap_job("send_weekend_notification", self._send_weekend),
            trigger=IntervalTrigger(minutes=self.settings.weekend_check_interval_minutes),
            id="send_weekend_notification",
            replace_existing=True,
            misfire_grace_time=900,
            coalesce=True,
        )
        self._scheduler.start()

    def shutdown(self) -> None:
        """Shut down the scheduler if it is running."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    @staticmethod
    def _wrap_job(
        job_name: str,
        callback: Callable[[], Awaitable[None]],
    ) -> Callable[[], Awaitable[None]]:
        async def runner() -> None:
            try:
                await callback()
            except Exception:
                LOGGER.exception("Scheduled job failed: %s", job_name)

        return runner
