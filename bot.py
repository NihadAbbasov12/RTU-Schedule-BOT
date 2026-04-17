"""Telegram bot handlers and schedule delivery logic."""

from __future__ import annotations

import asyncio
import logging
from calendar import monthrange
from collections.abc import Awaitable, Callable
from datetime import date, datetime

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand, KeyboardButton, Message, ReplyKeyboardMarkup

from config import Settings
from formatter import (
    format_changes,
    format_daily_schedule,
    format_range_schedule,
    format_status,
    format_subjects,
    split_message,
)
from models import (
    ChatSelection,
    ResolvedSemesterProgram,
    ScheduleDiff,
    ScheduleEvent,
    combine_local_datetime,
    get_academic_week_range,
    get_month_range,
    get_now,
    get_today_range,
    get_tomorrow_range,
    get_week_key,
    get_week_range,
)
from rtu_api import RTUAPIError, RTUPublicationError, RTUScheduleClient
from storage import SnapshotStorage

LOGGER = logging.getLogger(__name__)

BUTTON_TODAY = "Today"
BUTTON_TOMORROW = "Tomorrow"
BUTTON_WEEK = "Week"
BUTTON_SUBJECTS = "Subjects"
BUTTON_REFRESH = "Refresh"
BUTTON_STATUS = "Status"
PROGRAM_TITLE = "Computer Systems"

SUPPORTED_GROUPS = ("1", "2", "3", "4")
GROUP_KEYCAPS = {
    "1": "1\ufe0f\u20e3",
    "2": "2\ufe0f\u20e3",
    "3": "3\ufe0f\u20e3",
    "4": "4\ufe0f\u20e3",
}
GROUP_BUTTON_LABELS = {
    group: f"Group {GROUP_KEYCAPS[group]}"
    for group in SUPPORTED_GROUPS
}
GROUP_BUTTONS = {
    label: group
    for group, label in GROUP_BUTTON_LABELS.items()
}
GROUP_BUTTON_ALIASES = {
    **GROUP_BUTTONS,
    **{f"Group {group}": group for group in SUPPORTED_GROUPS},
}
WEEKEND_MESSAGE = "That was the last lesson for this week. Have a great weekend!"


class ScheduleBotApp:
    """Encapsulate aiogram handlers and RTU schedule orchestration."""

    def __init__(
        self,
        settings: Settings,
        api_client: RTUScheduleClient,
        storage: SnapshotStorage,
    ) -> None:
        self.settings = settings
        self.api_client = api_client
        self.storage = storage
        self.bot = Bot(token=settings.telegram_bot_token)
        self.dispatcher = Dispatcher()
        self.router = Router()
        self.dispatcher.include_router(self.router)
        self._register_handlers()

    async def start_polling(self) -> None:
        """Start Telegram long polling."""
        await self._configure_telegram_commands()
        await self.dispatcher.start_polling(
            self.bot,
            allowed_updates=self.dispatcher.resolve_used_update_types(),
        )

    async def close(self) -> None:
        """Close bot resources."""
        await self.bot.session.close()

    async def send_today_scheduled(self) -> None:
        """Send today's lessons to all chats with an active selection."""
        await self._broadcast_schedule_for_predefined_range(
            label="Today",
            range_factory=get_today_range,
            action="scheduled_today",
        )

    async def send_tomorrow_scheduled(self) -> None:
        """Send tomorrow's lessons to all chats with an active selection."""
        await self._broadcast_schedule_for_predefined_range(
            label="Tomorrow",
            range_factory=get_tomorrow_range,
            action="scheduled_tomorrow",
        )

    async def send_weekend_notifications(self) -> None:
        """Send the weekly weekend message after the last lesson has ended."""
        selections = await asyncio.to_thread(self.storage.list_chat_selections)
        if not selections:
            LOGGER.debug("Weekend check skipped because no chats have selected a group")
            return

        now = get_now(self.settings.zoneinfo)
        week_start, week_end = get_academic_week_range(self.settings.zoneinfo, now)
        week_key = get_week_key(week_start)
        pending: list[ChatSelection] = []

        for selection in selections:
            sent = await asyncio.to_thread(
                self.storage.has_weekend_notification,
                selection.chat_id,
                week_key,
            )
            if not sent:
                pending.append(selection)

        if not pending:
            LOGGER.debug("Weekend check skipped because all chats are already marked for %s", week_key)
            return

        targets_by_group: dict[str, ResolvedSemesterProgram] = {}
        events_by_semester_program: dict[int, list[ScheduleEvent]] = {}

        for selection in pending:
            try:
                target = await self._resolve_group_target(selection.selected_group)
                targets_by_group[selection.selected_group] = target
                if selection.semester_program_id != target.semester_program_id:
                    await asyncio.to_thread(
                        self.storage.save_chat_selection,
                        selection.chat_id,
                        selection.selected_group,
                        target.semester_program_id,
                    )
            except RTUAPIError:
                LOGGER.exception(
                    "Weekend check failed while resolving group=%s chat_id=%s",
                    selection.selected_group,
                    selection.chat_id,
                )

        for target in targets_by_group.values():
            try:
                events_by_semester_program[target.semester_program_id] = await asyncio.to_thread(
                    self.api_client.get_events_for_range,
                    target.semester_program_id,
                    week_start,
                    week_end,
                )
            except RTUAPIError:
                LOGGER.exception(
                    "Weekend check failed while loading events for semester_program_id=%s",
                    target.semester_program_id,
                )

        for selection in pending:
            try:
                target = targets_by_group.get(selection.selected_group)
                if target is None:
                    continue

                events = events_by_semester_program.get(target.semester_program_id, [])
                last_end = self._find_last_event_end(events)
                if last_end is None or now < last_end:
                    continue

                await self._send_text(
                    selection.chat_id,
                    WEEKEND_MESSAGE,
                    reply_markup=self._main_menu(),
                )
                await asyncio.to_thread(
                    self.storage.mark_weekend_notification_sent,
                    selection.chat_id,
                    week_key,
                    selection.selected_group,
                    target.semester_program_id,
                )
            except Exception:
                LOGGER.exception(
                    "Weekend notification failed for chat_id=%s group=%s",
                    selection.chat_id,
                    selection.selected_group,
                )

    def _register_handlers(self) -> None:
        self.router.message.register(self.cmd_start, CommandStart())
        self.router.message.register(self.cmd_today, Command("today"))
        self.router.message.register(self.cmd_tomorrow, Command("tomorrow"))
        self.router.message.register(self.cmd_week, Command("week"))
        self.router.message.register(self.cmd_month, Command("month"))
        self.router.message.register(self.cmd_subjects, Command("subjects"))
        self.router.message.register(self.cmd_status, Command("status"))
        self.router.message.register(self.cmd_refresh, Command("refresh"))
        self.router.message.register(self.btn_group_select, F.text.in_(tuple(GROUP_BUTTON_ALIASES.keys())))
        self.router.message.register(self.btn_today, F.text == BUTTON_TODAY)
        self.router.message.register(self.btn_tomorrow, F.text == BUTTON_TOMORROW)
        self.router.message.register(self.btn_week, F.text == BUTTON_WEEK)
        self.router.message.register(self.btn_subjects, F.text == BUTTON_SUBJECTS)
        self.router.message.register(self.btn_refresh, F.text == BUTTON_REFRESH)
        self.router.message.register(self.btn_status, F.text == BUTTON_STATUS)

    async def cmd_start(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="start",
            source="command",
            callback=lambda: self._show_start(message.chat.id),
            fallback_message="I couldn't open the welcome screen right now. Please try again.",
        )

    async def cmd_today(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="today",
            source="command",
            callback=lambda: self._show_today(message.chat.id),
            fallback_message="I couldn't load today's schedule right now. Please try again.",
        )

    async def cmd_tomorrow(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="tomorrow",
            source="command",
            callback=lambda: self._show_tomorrow(message.chat.id),
            fallback_message="I couldn't load tomorrow's schedule right now. Please try again.",
        )

    async def cmd_week(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="week",
            source="command",
            callback=lambda: self._show_week(message.chat.id),
            fallback_message="I couldn't load the weekly schedule right now. Please try again.",
        )

    async def cmd_month(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="month",
            source="command",
            callback=lambda: self._show_month(message.chat.id),
            fallback_message="I couldn't load the monthly schedule right now. Please try again.",
        )

    async def cmd_subjects(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="subjects",
            source="command",
            callback=lambda: self._show_subjects(message.chat.id),
            fallback_message="I couldn't load the subject list right now. Please try again.",
        )

    async def cmd_status(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="status",
            source="command",
            callback=lambda: self._show_status(message.chat.id),
            fallback_message="I couldn't load the current status right now. Please try again.",
        )

    async def cmd_refresh(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="refresh",
            source="command",
            callback=lambda: self._show_refresh(message.chat.id),
            fallback_message="I couldn't refresh the schedule right now. Please try again.",
        )

    async def btn_group_select(self, message: Message) -> None:
        button_text = (message.text or "").strip()
        group = GROUP_BUTTON_ALIASES.get(button_text)
        if group is None:
            return

        await self._run_action(
            chat_id=message.chat.id,
            action="select_group",
            source="button",
            callback=lambda: self._select_group(message.chat.id, group),
            fallback_message="I couldn't switch the group right now. Please try again.",
        )

    async def btn_today(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="today",
            source="button",
            callback=lambda: self._show_today(message.chat.id),
            fallback_message="I couldn't load today's schedule right now. Please try again.",
        )

    async def btn_tomorrow(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="tomorrow",
            source="button",
            callback=lambda: self._show_tomorrow(message.chat.id),
            fallback_message="I couldn't load tomorrow's schedule right now. Please try again.",
        )

    async def btn_week(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="week",
            source="button",
            callback=lambda: self._show_week(message.chat.id),
            fallback_message="I couldn't load the weekly schedule right now. Please try again.",
        )

    async def btn_subjects(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="subjects",
            source="button",
            callback=lambda: self._show_subjects(message.chat.id),
            fallback_message="I couldn't load the subject list right now. Please try again.",
        )

    async def btn_refresh(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="refresh",
            source="button",
            callback=lambda: self._show_refresh(message.chat.id),
            fallback_message="I couldn't refresh the schedule right now. Please try again.",
        )

    async def btn_status(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="status",
            source="button",
            callback=lambda: self._show_status(message.chat.id),
            fallback_message="I couldn't load the current status right now. Please try again.",
        )

    async def _run_action(
        self,
        chat_id: int,
        action: str,
        source: str,
        callback: Callable[[], Awaitable[None]],
        fallback_message: str,
    ) -> None:
        LOGGER.info(
            "Handler triggered: action=%s source=%s chat_id=%s",
            action,
            source,
            chat_id,
        )
        try:
            await callback()
        except Exception:
            LOGGER.exception(
                "Handler failed: action=%s source=%s chat_id=%s",
                action,
                source,
                chat_id,
            )
            await self._send_text_safe(chat_id, fallback_message)

    async def _show_start(self, chat_id: int) -> None:
        selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        lines = [
            "RTU Schedule Bot",
            f"{PROGRAM_TITLE} | semesterId {self.settings.rtu_semester_id}",
            "",
            "Choose your group below to open the schedule menu.",
            "Use /start again any time to change the group.",
        ]
        if selection is not None:
            lines.extend(
                [
                    "",
                    f"Current group: {self._format_group_name(selection.selected_group)}",
                ]
            )

        await self._send_text(
            chat_id,
            "\n".join(lines),
            reply_markup=self._group_selection_menu(),
        )

    async def _select_group(self, chat_id: int, group: str) -> None:
        try:
            target = await self._resolve_group_target(group)
        except RTUPublicationError as exc:
            await self._send_text(
                chat_id,
                f"{self._format_group_name(group)} is currently unavailable: {exc}",
                reply_markup=self._group_selection_menu(),
            )
            return
        except RTUAPIError as exc:
            await self._send_text(
                chat_id,
                f"I couldn't activate {self._format_group_name(group)} right now: {exc}",
                reply_markup=self._group_selection_menu(),
            )
            return

        await asyncio.to_thread(
            self.storage.save_chat_selection,
            chat_id,
            group,
            target.semester_program_id,
        )

        text = "\n".join(
            [
                f"{self._format_group_name(group)} is now active.",
                f"Program: {PROGRAM_TITLE}",
                f"Resolved semesterProgramId: {target.semester_program_id}",
                "",
                "The main menu is ready below.",
                "Use Today, Tomorrow, Week, Subjects, Refresh, or Status.",
                "Use /start any time to change the group.",
            ]
        )
        await self._send_text(chat_id, text, reply_markup=self._main_menu())

    async def _show_today(self, chat_id: int) -> None:
        await self._send_schedule_for_predefined_range(
            chat_id=chat_id,
            label="Today",
            range_factory=get_today_range,
        )

    async def _show_tomorrow(self, chat_id: int) -> None:
        await self._send_schedule_for_predefined_range(
            chat_id=chat_id,
            label="Tomorrow",
            range_factory=get_tomorrow_range,
        )

    async def _show_week(self, chat_id: int) -> None:
        await self._send_schedule_for_predefined_range(
            chat_id=chat_id,
            label="Week",
            range_factory=get_week_range,
        )

    async def _show_month(self, chat_id: int) -> None:
        context = await self._resolve_chat_target(chat_id)
        if context is None:
            return

        _, target = context
        start_date, end_date = get_month_range(self.settings.zoneinfo)
        await self._send_schedule(
            chat_id=chat_id,
            label="Month",
            semester_program_id=target.semester_program_id,
            start_date=start_date,
            end_date=end_date,
        )

    async def _show_subjects(self, chat_id: int) -> None:
        context = await self._resolve_chat_target(chat_id)
        if context is None:
            return

        selection, target = context
        try:
            subjects = await asyncio.to_thread(
                self.api_client.get_subjects,
                target.semester_program_id,
            )
        except RTUAPIError as exc:
            await self._send_text(chat_id, f"I couldn't load the subject list right now: {exc}")
            return

        await self._send_text(
            chat_id,
            format_subjects(
                subjects,
                heading=f"Subjects | {self._format_group_name(selection.selected_group)}",
            ),
        )

    async def _show_status(self, chat_id: int) -> None:
        selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        if selection is None:
            await self._send_text(
                chat_id,
                "\n".join(
                    [
                        "No group is selected for this chat yet.",
                        "",
                        format_status(
                            semester_id=self.settings.rtu_semester_id,
                            program_id=self.settings.rtu_program_id,
                            course_id=self.settings.rtu_course_id,
                            group=None,
                            semester_program_id=None,
                            scheduler_enabled=self.settings.enable_scheduler,
                            timezone=self.settings.timezone,
                            program_title=PROGRAM_TITLE,
                        ),
                    ]
                ),
                reply_markup=self._group_selection_menu(),
            )
            return

        context = await self._resolve_chat_target(chat_id)
        if context is None:
            return

        current_selection, target = context
        text = format_status(
            semester_id=target.semester_id,
            program_id=target.program_id,
            course_id=target.course_id,
            group=current_selection.selected_group,
            semester_program_id=target.semester_program_id,
            scheduler_enabled=self.settings.enable_scheduler,
            timezone=self.settings.timezone,
            program_title=PROGRAM_TITLE,
        )
        await self._send_text(chat_id, text)

    async def _show_refresh(self, chat_id: int) -> None:
        context = await self._resolve_chat_target(chat_id)
        if context is None:
            return

        _, target = context
        try:
            changes = await asyncio.to_thread(
                self._refresh_current_and_next_months,
                chat_id,
                target.semester_program_id,
            )
        except RTUAPIError as exc:
            await self._send_text(chat_id, f"Refresh failed: {exc}")
            return

        await self._send_text(chat_id, format_changes(changes))

    async def _send_schedule_for_predefined_range(
        self,
        chat_id: int,
        label: str,
        range_factory: Callable[..., tuple[date, date]],
    ) -> None:
        context = await self._resolve_chat_target(chat_id)
        if context is None:
            return

        _, target = context
        start_date, end_date = range_factory(self.settings.zoneinfo)
        LOGGER.debug(
            "Selected schedule range: label=%s start_date=%s end_date=%s chat_id=%s",
            label,
            start_date,
            end_date,
            chat_id,
        )
        await self._send_schedule(
            chat_id=chat_id,
            label=label,
            semester_program_id=target.semester_program_id,
            start_date=start_date,
            end_date=end_date,
        )

    async def _send_schedule(
        self,
        chat_id: int,
        label: str,
        semester_program_id: int,
        start_date: date,
        end_date: date,
    ) -> None:
        LOGGER.debug(
            "Fetching schedule: label=%s semester_program_id=%s start_date=%s end_date=%s chat_id=%s",
            label,
            semester_program_id,
            start_date,
            end_date,
            chat_id,
        )
        try:
            events = await asyncio.to_thread(
                self.api_client.get_events_for_range,
                semester_program_id,
                start_date,
                end_date,
            )
        except RTUPublicationError as exc:
            await self._send_text(chat_id, f"Schedule is not published right now: {exc}")
            return
        except RTUAPIError as exc:
            await self._send_text(chat_id, f"I couldn't load the schedule right now: {exc}")
            return

        LOGGER.debug(
            "Fetched schedule events: label=%s count=%s start_date=%s end_date=%s chat_id=%s",
            label,
            len(events),
            start_date,
            end_date,
            chat_id,
        )

        await self._send_text(chat_id, self._render_schedule_message(label, start_date, end_date, events))

    async def _resolve_chat_target(
        self,
        chat_id: int,
        prompt_if_missing: bool = True,
    ) -> tuple[ChatSelection, ResolvedSemesterProgram] | None:
        selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        if selection is None:
            if prompt_if_missing:
                await self._send_text(
                    chat_id,
                    "Choose your group first to continue.",
                    reply_markup=self._group_selection_menu(),
                )
            return None

        try:
            target = await self._resolve_group_target(selection.selected_group)
        except RTUPublicationError as exc:
            if prompt_if_missing:
                await self._send_text(
                    chat_id,
                    f"{self._format_group_name(selection.selected_group)} is currently unavailable: {exc}",
                    reply_markup=self._group_selection_menu(),
                )
            else:
                LOGGER.warning(
                    "Unable to resolve unpublished semester program for chat_id=%s group=%s: %s",
                    chat_id,
                    selection.selected_group,
                    exc,
                )
            return None
        except RTUAPIError as exc:
            if prompt_if_missing:
                await self._send_text(
                    chat_id,
                    f"I couldn't resolve {self._format_group_name(selection.selected_group)} right now: {exc}",
                    reply_markup=self._group_selection_menu(),
                )
            else:
                LOGGER.warning(
                    "Unable to resolve semester program for chat_id=%s group=%s: %s",
                    chat_id,
                    selection.selected_group,
                    exc,
                )
            return None

        if selection.semester_program_id != target.semester_program_id:
            await asyncio.to_thread(
                self.storage.save_chat_selection,
                chat_id,
                selection.selected_group,
                target.semester_program_id,
            )
            selection = ChatSelection(
                chat_id=selection.chat_id,
                selected_group=selection.selected_group,
                semester_program_id=target.semester_program_id,
            )

        return selection, target

    async def _resolve_group_target(self, group: str) -> ResolvedSemesterProgram:
        return await asyncio.to_thread(self.api_client.resolve_semester_program, group)

    async def _broadcast_schedule_for_predefined_range(
        self,
        label: str,
        range_factory: Callable[..., tuple[date, date]],
        action: str,
    ) -> None:
        selections = await asyncio.to_thread(self.storage.list_chat_selections)
        if not selections:
            LOGGER.info("Scheduled action skipped because no chats have selected a group: %s", action)
            return

        start_date, end_date = range_factory(self.settings.zoneinfo)
        targets_by_group: dict[str, ResolvedSemesterProgram] = {}
        errors_by_group: dict[str, str] = {}
        text_by_semester_program: dict[int, str] = {}
        errors_by_semester_program: dict[int, str] = {}

        for selection in selections:
            if selection.selected_group in targets_by_group or selection.selected_group in errors_by_group:
                continue
            try:
                targets_by_group[selection.selected_group] = await self._resolve_group_target(
                    selection.selected_group
                )
            except RTUAPIError as exc:
                LOGGER.exception(
                    "Scheduled action failed while resolving group=%s action=%s",
                    selection.selected_group,
                    action,
                )
                errors_by_group[selection.selected_group] = (
                    f"I couldn't load the scheduled {label.lower()} update right now: {exc}"
                )

        for target in targets_by_group.values():
            if target.semester_program_id in text_by_semester_program:
                continue
            try:
                events = await asyncio.to_thread(
                    self.api_client.get_events_for_range,
                    target.semester_program_id,
                    start_date,
                    end_date,
                )
                text_by_semester_program[target.semester_program_id] = self._render_schedule_message(
                    label,
                    start_date,
                    end_date,
                    events,
                )
            except RTUAPIError as exc:
                LOGGER.exception(
                    "Scheduled action failed while loading events for semester_program_id=%s action=%s",
                    target.semester_program_id,
                    action,
                )
                errors_by_semester_program[target.semester_program_id] = (
                    f"I couldn't load the scheduled {label.lower()} update right now: {exc}"
                )

        for selection in selections:
            try:
                target = targets_by_group.get(selection.selected_group)
                if target is None:
                    await self._send_text_safe(
                        selection.chat_id,
                        errors_by_group.get(
                            selection.selected_group,
                            f"I couldn't load the scheduled {label.lower()} update right now.",
                        ),
                        reply_markup=self._main_menu(),
                    )
                    continue

                if selection.semester_program_id != target.semester_program_id:
                    await asyncio.to_thread(
                        self.storage.save_chat_selection,
                        selection.chat_id,
                        selection.selected_group,
                        target.semester_program_id,
                    )

                text = text_by_semester_program.get(target.semester_program_id)
                if text is None:
                    await self._send_text_safe(
                        selection.chat_id,
                        errors_by_semester_program.get(
                            target.semester_program_id,
                            f"I couldn't load the scheduled {label.lower()} update right now.",
                        ),
                        reply_markup=self._main_menu(),
                    )
                    continue

                await self._send_text_safe(
                    selection.chat_id,
                    text,
                    reply_markup=self._main_menu(),
                )
            except Exception:
                LOGGER.exception(
                    "Scheduled action delivery failed: action=%s chat_id=%s",
                    action,
                    selection.chat_id,
                )

    def _refresh_current_and_next_months(
        self,
        chat_id: int,
        semester_program_id: int,
    ) -> list[ScheduleDiff]:
        current_start, current_end = get_month_range(self.settings.zoneinfo)
        next_month_year, next_month = self._next_month(current_start.year, current_start.month)
        _, next_month_days = monthrange(next_month_year, next_month)
        next_start = date(next_month_year, next_month, 1)
        next_end = date(next_month_year, next_month, next_month_days)

        LOGGER.debug(
            "Refreshing schedule snapshots: chat_id=%s semester_program_id=%s current=%s..%s next=%s..%s",
            chat_id,
            semester_program_id,
            current_start,
            current_end,
            next_start,
            next_end,
        )

        current_events = self.api_client.get_events_for_range(
            semester_program_id,
            current_start,
            current_end,
        )
        next_events = self.api_client.get_events_for_range(
            semester_program_id,
            next_start,
            next_end,
        )

        LOGGER.debug(
            "Fetched refresh event counts: chat_id=%s semester_program_id=%s current=%s next=%s",
            chat_id,
            semester_program_id,
            len(current_events),
            len(next_events),
        )

        changes = self.storage.sync_month(
            chat_id,
            semester_program_id,
            current_start.year,
            current_start.month,
            current_events,
        )
        changes.extend(
            self.storage.sync_month(
                chat_id,
                semester_program_id,
                next_month_year,
                next_month,
                next_events,
            )
        )
        return changes

    @staticmethod
    def _next_month(year: int, month: int) -> tuple[int, int]:
        if month == 12:
            return year + 1, 1
        return year, month + 1

    @staticmethod
    def _render_schedule_message(
        label: str,
        start_date: date,
        end_date: date,
        events: list[ScheduleEvent],
    ) -> str:
        if start_date == end_date:
            return format_daily_schedule(label=label, target_date=start_date, events=events)
        return format_range_schedule(
            label=label,
            start_date=start_date,
            end_date=end_date,
            events=events,
        )

    def _find_last_event_end(self, events: list[ScheduleEvent]) -> datetime | None:
        latest: datetime | None = None
        for event in events:
            candidate = combine_local_datetime(
                event.event_date,
                event.end_time or event.start_time,
                self.settings.zoneinfo,
            )
            if candidate is None:
                continue
            if latest is None or candidate > latest:
                latest = candidate
        return latest

    async def _configure_telegram_commands(self) -> None:
        await self.bot.set_my_commands(
            [
                BotCommand(command="start", description="Show welcome text and change group"),
                BotCommand(command="today", description="Show today's lessons"),
                BotCommand(command="tomorrow", description="Show tomorrow's lessons"),
                BotCommand(command="week", description="Show the next 7 days"),
                BotCommand(command="month", description="Show the current month"),
                BotCommand(command="subjects", description="Show current semester subjects"),
                BotCommand(command="refresh", description="Refresh snapshots and detect changes"),
                BotCommand(command="status", description="Show the current configuration"),
            ]
        )

    @staticmethod
    def _group_selection_menu() -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(text=GROUP_BUTTON_LABELS["1"]),
                    KeyboardButton(text=GROUP_BUTTON_LABELS["2"]),
                ],
                [
                    KeyboardButton(text=GROUP_BUTTON_LABELS["3"]),
                    KeyboardButton(text=GROUP_BUTTON_LABELS["4"]),
                ],
            ],
            resize_keyboard=True,
            is_persistent=True,
            input_field_placeholder="Choose your group",
        )

    @staticmethod
    def _main_menu() -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(text=BUTTON_TODAY),
                    KeyboardButton(text=BUTTON_TOMORROW),
                    KeyboardButton(text=BUTTON_WEEK),
                ],
                [
                    KeyboardButton(text=BUTTON_SUBJECTS),
                    KeyboardButton(text=BUTTON_REFRESH),
                    KeyboardButton(text=BUTTON_STATUS),
                ],
            ],
            resize_keyboard=True,
            is_persistent=True,
            input_field_placeholder="Choose a schedule action",
        )

    async def _send_text(
        self,
        chat_id: int,
        text: str,
        reply_markup: ReplyKeyboardMarkup | None = None,
    ) -> None:
        chunks = split_message(text)
        for index, chunk in enumerate(chunks):
            markup = reply_markup if index == 0 else None
            try:
                await self.bot.send_message(chat_id, chunk, reply_markup=markup)
            except TelegramBadRequest:
                await self.bot.send_message(chat_id, chunk, parse_mode=None, reply_markup=markup)

    async def _send_text_safe(
        self,
        chat_id: int,
        text: str,
        reply_markup: ReplyKeyboardMarkup | None = None,
    ) -> None:
        try:
            await self._send_text(chat_id, text, reply_markup=reply_markup)
        except Exception:
            LOGGER.exception("Failed to send Telegram message to chat_id=%s", chat_id)

    @staticmethod
    def _format_group_name(group: str) -> str:
        return f"Group {group}"
