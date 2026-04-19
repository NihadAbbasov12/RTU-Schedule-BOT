"""Application entrypoint."""

from __future__ import annotations

import asyncio
import logging

from bot import ScheduleBotApp
from config import Settings
from rtu_api import RTUScheduleClient
from scheduler import BotScheduler
from storage import SnapshotStorage


def configure_logging(level: str) -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def main() -> None:
    """Start the RTU Telegram bot application."""
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting RTU schedule bot")

    storage = SnapshotStorage(
        settings.db_path,
        legacy_chat_id=settings.telegram_chat_id,
        legacy_semester_id=settings.rtu_semester_id,
        legacy_program_id=settings.rtu_program_id,
        legacy_course_id=settings.rtu_course_id,
        legacy_group=settings.rtu_group,
        legacy_semester_program_id=settings.rtu_semester_program_id,
    )
    api_client = RTUScheduleClient(settings)
    bot_app = ScheduleBotApp(settings=settings, api_client=api_client, storage=storage)
    scheduler: BotScheduler | None = None

    try:
        if settings.enable_scheduler:
            scheduler = BotScheduler(
                settings=settings,
                send_today=bot_app.send_today_scheduled,
                send_tomorrow=bot_app.send_tomorrow_scheduled,
                send_weekend=bot_app.send_weekend_notifications,
                send_reminders=bot_app.send_lesson_reminders,
            )
            scheduler.start()
            logger.info("Scheduler enabled")
        else:
            logger.info("Scheduler disabled")

        await bot_app.start_polling()
    finally:
        if scheduler is not None:
            scheduler.shutdown()
        await bot_app.close()
        api_client.close()
        storage.close()


if __name__ == "__main__":
    asyncio.run(main())
