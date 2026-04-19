"""Telegram bot handlers and schedule delivery logic."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from calendar import monthrange
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from config import Settings
from formatter import (
    format_admin_stats,
    format_changes,
    format_daily_schedule,
    format_reminder,
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
    StudyDepartment,
    StudyProgramFamily,
    combine_local_datetime,
    get_academic_week_range,
    get_month_range,
    get_now,
    get_today_range,
    get_tomorrow_range,
    get_week_key,
    get_week_range,
)
from rtu_api import RTUAPIError, RTUPublicationError, RTUResolutionError, RTUScheduleClient
from storage import SnapshotStorage

LOGGER = logging.getLogger(__name__)

BUTTON_TODAY = "Today"
BUTTON_TOMORROW = "Tomorrow"
BUTTON_WEEK = "Week"
BUTTON_SUBJECTS = "Subjects"
BUTTON_REFRESH = "Refresh"
BUTTON_STATUS = "Status"
BUTTON_CHANGE_SELECTION = "Change selection"
BUTTON_STATS = "Stats"

CALLBACK_PREFIX = "cfg"
WEEKEND_MESSAGE = "That was the last lesson for this week. Have a great weekend!"
SELECTION_TITLE = "RTU Schedule Setup"
SCHEDULE_REQUEST_ACTIONS = {"today", "tomorrow", "week", "month", "subjects", "refresh", "status"}

PROGRAM_PAGE_SIZE = 8
SMALL_PAGE_SIZE = 8


@dataclass(slots=True)
class SelectionDraft:
    """In-memory selection flow state for one Telegram chat."""

    semester_id: int | None = None
    semester_title: str | None = None
    department_id: int | None = None
    department_title: str | None = None
    program_family: str | None = None
    program_id: int | None = None
    program_title: str | None = None
    program_code: str | None = None
    course_id: int | None = None

    def clear_from_program(self) -> None:
        self.program_family = None
        self.program_id = None
        self.program_title = None
        self.program_code = None
        self.course_id = None


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
        self._selection_drafts: dict[int, SelectionDraft] = {}
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
            LOGGER.debug("Weekend check skipped because no chats have a saved study selection")
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

        targets_by_key: dict[tuple[int | None, int | None, int | None, str], ResolvedSemesterProgram] = {}
        events_by_semester_program: dict[int, list[ScheduleEvent]] = {}

        for selection in pending:
            if not selection.is_complete():
                LOGGER.warning(
                    "Weekend check skipped incomplete selection for chat_id=%s",
                    selection.chat_id,
                )
                continue

            selection_key = selection.selection_key()
            if selection_key in targets_by_key:
                continue

            try:
                _, target = await self._resolve_selection_context(selection)
                targets_by_key[selection_key] = target
            except RTUAPIError:
                LOGGER.exception(
                    "Weekend check failed while resolving chat_id=%s selection=%s",
                    selection.chat_id,
                    selection_key,
                )

        for target in targets_by_key.values():
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
            if not selection.is_complete():
                continue

            try:
                target = targets_by_key.get(selection.selection_key())
                if target is None:
                    continue

                events = events_by_semester_program.get(target.semester_program_id)
                if events is None:
                    events = await asyncio.to_thread(
                        self.api_client.get_events_for_range,
                        target.semester_program_id,
                        week_start,
                        week_end,
                    )
                    events_by_semester_program[target.semester_program_id] = events

                last_end = self._find_last_event_end(events)
                if last_end is None or now < last_end:
                    continue

                await self._send_text(
                    selection.chat_id,
                    WEEKEND_MESSAGE,
                    reply_markup=self._main_menu(selection.chat_id),
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
                    "Weekend notification failed for chat_id=%s selection=%s",
                    selection.chat_id,
                    selection.selection_key(),
                )

    async def send_lesson_reminders(self) -> None:
        """Send lesson reminders shortly before each lesson starts."""
        if not self.settings.reminder_enabled:
            LOGGER.debug("Reminder scan skipped because reminders are disabled")
            return

        selections = await asyncio.to_thread(self.storage.list_chat_selections)
        if not selections:
            LOGGER.debug("Reminder scan skipped because no chats have a saved study selection")
            return

        now = get_now(self.settings.zoneinfo)
        start_date = now.date()
        end_date = start_date + timedelta(days=1)
        lower_bound_minutes, upper_bound_minutes = self._reminder_window_bounds()
        LOGGER.info(
            "Reminder scan started: chats=%s window=%s..%s minutes",
            len(selections),
            lower_bound_minutes,
            upper_bound_minutes,
        )

        targets_by_key: dict[tuple[int | None, int | None, int | None, str], ResolvedSemesterProgram] = {}
        events_by_semester_program: dict[int, list[ScheduleEvent]] = {}
        reminders_sent = 0
        skipped_duplicates = 0

        for selection in selections:
            if not selection.is_complete():
                LOGGER.warning(
                    "Reminder scan skipped incomplete selection for chat_id=%s",
                    selection.chat_id,
                )
                continue

            selection_key = selection.selection_key()
            try:
                if selection_key not in targets_by_key:
                    _, target = await self._resolve_selection_context(selection)
                    targets_by_key[selection_key] = target

                target = targets_by_key[selection_key]
                if target.semester_program_id not in events_by_semester_program:
                    events_by_semester_program[target.semester_program_id] = await asyncio.to_thread(
                        self.api_client.get_events_for_range,
                        target.semester_program_id,
                        start_date,
                        end_date,
                    )

                due_events = self._due_reminder_events(
                    events_by_semester_program[target.semester_program_id],
                    now,
                    lower_bound_minutes,
                    upper_bound_minutes,
                )
                for event in due_events:
                    if event.start_time is None:
                        continue

                    reminder_key = self._build_reminder_key(
                        selection.chat_id,
                        target.semester_program_id,
                        event,
                    )
                    acquired = await asyncio.to_thread(
                        self.storage.try_acquire_reminder_delivery,
                        selection.chat_id,
                        reminder_key,
                        event.event_date,
                        event.start_time.strftime("%H:%M:%S"),
                        target.semester_program_id,
                    )
                    if not acquired:
                        skipped_duplicates += 1
                        continue

                    try:
                        await self._send_text(
                            selection.chat_id,
                            format_reminder(event, self.settings.reminder_minutes_before),
                            reply_markup=self._main_menu(selection.chat_id),
                        )
                        reminders_sent += 1
                    except Exception:
                        await asyncio.to_thread(
                            self.storage.delete_reminder_delivery,
                            selection.chat_id,
                            reminder_key,
                        )
                        raise
            except Exception:
                LOGGER.exception(
                    "Reminder delivery failed for chat_id=%s selection=%s",
                    selection.chat_id,
                    selection.selection_key(),
                )

        LOGGER.info(
            "Reminder scan finished: chats_scanned=%s reminders_sent=%s duplicates_skipped=%s",
            len(selections),
            reminders_sent,
            skipped_duplicates,
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
        self.router.message.register(self.cmd_stats, Command("stats"))
        self.router.message.register(self.btn_today, F.text == BUTTON_TODAY)
        self.router.message.register(self.btn_tomorrow, F.text == BUTTON_TOMORROW)
        self.router.message.register(self.btn_week, F.text == BUTTON_WEEK)
        self.router.message.register(self.btn_subjects, F.text == BUTTON_SUBJECTS)
        self.router.message.register(self.btn_refresh, F.text == BUTTON_REFRESH)
        self.router.message.register(self.btn_status, F.text == BUTTON_STATUS)
        self.router.message.register(self.btn_change_selection, F.text == BUTTON_CHANGE_SELECTION)
        self.router.message.register(self.btn_stats, F.text == BUTTON_STATS)
        self.router.callback_query.register(
            self.handle_configuration_callback,
            F.data.startswith(f"{CALLBACK_PREFIX}:"),
        )

    async def cmd_start(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="start",
            source="command",
            callback=lambda: self._show_start(message.chat.id),
            fallback_message="I couldn't open the study selection flow right now. Please try again.",
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

    async def cmd_stats(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="stats",
            source="command",
            callback=lambda: self._show_stats(message.chat.id),
            fallback_message="I couldn't load the bot statistics right now. Please try again.",
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

    async def btn_change_selection(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="change_selection",
            source="button",
            callback=lambda: self._show_start(message.chat.id),
            fallback_message="I couldn't restart the selection flow right now. Please try again.",
        )

    async def btn_stats(self, message: Message) -> None:
        await self._run_action(
            chat_id=message.chat.id,
            action="stats",
            source="button",
            callback=lambda: self._show_stats(message.chat.id),
            fallback_message="I couldn't load the bot statistics right now. Please try again.",
        )

    async def handle_configuration_callback(self, callback: CallbackQuery) -> None:
        data = callback.data or ""
        parts = data.split(":")
        chat_id = callback.message.chat.id if callback.message else callback.from_user.id
        message = callback.message if isinstance(callback.message, Message) else None

        if len(parts) < 3:
            await callback.answer()
            return

        try:
            await asyncio.to_thread(self.storage.touch_chat_activity, chat_id, False)
            action = parts[1]
            mode = parts[2]

            if action == "nav":
                if mode == "back":
                    await callback.answer()
                    await self._navigate_back(chat_id, message)
                    return
                if mode == "cancel":
                    await callback.answer()
                    await self._cancel_configuration(chat_id, message)
                    return
                await callback.answer()
                return

            if action in {"period", "dept"}:
                await callback.answer("Setup refreshed.")
                await self._start_selection_prompt(
                    chat_id,
                    message=message,
                    intro_text=(
                        "The setup flow was refreshed.\n\n"
                        f"Study period: {self.settings.rtu_semester_title}\n"
                        f"Department: {self.settings.rtu_department_title}\n\n"
                        "Choose your program family."
                    ),
                )
                return

            if chat_id not in self._selection_drafts:
                await callback.answer("Setup expired. Starting over.")
                await self._start_selection_prompt(
                    chat_id,
                    message=message,
                    intro_text=(
                        "The previous setup expired.\n\n"
                        f"Study period: {self.settings.rtu_semester_title}\n"
                        f"Department: {self.settings.rtu_department_title}\n\n"
                        "Choose your program family again."
                    ),
                )
                return

            if mode == "page":
                page = int(parts[3])
                await callback.answer()
                await self._show_selection_page(chat_id, action, message=message, page=page)
                return

            if mode == "select":
                identifier = int(parts[3])
                await callback.answer()
                if action == "prog":
                    await self._select_program(chat_id, identifier, message)
                elif action == "course":
                    await self._select_course(chat_id, identifier, message)
                elif action == "group":
                    await self._select_group(chat_id, identifier, message)
                else:
                    await self._start_selection_prompt(
                        chat_id,
                        message=message,
                        intro_text=(
                            "The setup flow was refreshed.\n\n"
                            f"Study period: {self.settings.rtu_semester_title}\n"
                            f"Department: {self.settings.rtu_department_title}\n\n"
                            "Choose your program family."
                        ),
                    )
                return

            await callback.answer()
        except Exception:
            LOGGER.exception("Selection callback failed for chat_id=%s data=%s", chat_id, data)
            await callback.answer("Something went wrong. Please try again.", show_alert=True)

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
            await asyncio.to_thread(
                self.storage.touch_chat_activity,
                chat_id,
                action in SCHEDULE_REQUEST_ACTIONS,
            )
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
        intro_lines = [
            "RTU Schedule Bot",
            "",
            "This bot is locked to the current Foreign Students setup:",
            f"Study period: {self.settings.rtu_semester_title}",
            f"Department: {self.settings.rtu_department_title}",
            "",
            "Choose your program family, then your course and group.",
        ]
        if selection is not None:
            intro_lines.extend(["", "Current selection:"])
            intro_lines.extend(self._selection_summary_lines(selection))

        await self._start_selection_prompt(chat_id, intro_text="\n".join(intro_lines))

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

        selection, target = context
        start_date, end_date = get_month_range(self.settings.zoneinfo)
        await self._send_schedule(
            chat_id=chat_id,
            label="Month",
            semester_program_id=target.semester_program_id,
            start_date=start_date,
            end_date=end_date,
            context_line=self._selection_context_line(selection, target),
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
                heading=self._subjects_heading(selection, target),
            ),
            reply_markup=self._main_menu(chat_id),
        )

    async def _show_status(self, chat_id: int) -> None:
        selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        heading_lines: list[str] = []
        current_selection = selection
        target: ResolvedSemesterProgram | None = None

        if selection is None:
            heading_lines.extend(
                [
                    "No study selection is saved for this chat yet.",
                    "Use Change selection to choose your program family, course, and group.",
                    "",
                ]
            )
        elif not selection.is_complete():
            heading_lines.extend(
                [
                    "Your saved selection is incomplete.",
                    "Use Change selection to finish the setup again.",
                    "",
                ]
            )
        else:
            context = await self._resolve_chat_target(chat_id)
            if context is None:
                return
            current_selection, target = context

        text = format_status(
            semester_id=self.settings.rtu_semester_id,
            semester_title=self.settings.rtu_semester_title,
            department_title=self.settings.rtu_department_title,
            program_family=current_selection.program_family if current_selection is not None else None,
            program_id=current_selection.program_id if current_selection is not None else None,
            program_title=(
                current_selection.program_title or (target.program_title if target is not None else None)
            )
            if current_selection is not None
            else None,
            program_code=(
                current_selection.program_code or (target.program_code if target is not None else None)
            )
            if current_selection is not None
            else None,
            course_id=current_selection.course_id if current_selection is not None else None,
            group=current_selection.selected_group if current_selection is not None else None,
            semester_program_id=target.semester_program_id if target is not None else None,
            scheduler_enabled=self.settings.enable_scheduler,
            timezone=self.settings.timezone,
        )
        status_text = text if not heading_lines else "\n".join(heading_lines + [text])
        await self._send_text(chat_id, status_text, reply_markup=self._main_menu(chat_id))

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

        await self._send_text(chat_id, format_changes(changes), reply_markup=self._main_menu(chat_id))

    async def _show_stats(self, chat_id: int) -> None:
        if not self.settings.is_admin_chat(chat_id):
            LOGGER.info("Stats access denied for chat_id=%s", chat_id)
            await self._send_text(chat_id, "Access denied.")
            return

        LOGGER.info("Stats requested by admin chat_id=%s", chat_id)
        stats = await asyncio.to_thread(self.storage.get_bot_usage_stats)
        await self._send_text(
            chat_id,
            format_admin_stats(
                stats=stats,
                scheduler_enabled=self.settings.enable_scheduler,
                reminder_enabled=self.settings.reminder_enabled,
                timezone=self.settings.timezone,
            ),
            reply_markup=self._main_menu(chat_id),
        )

    async def _send_schedule_for_predefined_range(
        self,
        chat_id: int,
        label: str,
        range_factory: Callable[..., tuple[date, date]],
    ) -> None:
        context = await self._resolve_chat_target(chat_id)
        if context is None:
            return

        selection, target = context
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
            context_line=self._selection_context_line(selection, target),
        )

    async def _send_schedule(
        self,
        chat_id: int,
        label: str,
        semester_program_id: int,
        start_date: date,
        end_date: date,
        context_line: str | None = None,
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

        await self._send_text(
            chat_id,
            self._render_schedule_message(
                label,
                start_date,
                end_date,
                events,
                context_line=context_line,
            ),
            reply_markup=self._main_menu(chat_id),
        )

    async def _resolve_chat_target(
        self,
        chat_id: int,
        prompt_if_missing: bool = True,
    ) -> tuple[ChatSelection, ResolvedSemesterProgram] | None:
        selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        if selection is None:
            if prompt_if_missing:
                await self._prompt_for_selection(
                    chat_id,
                    "Choose your program family, course, and group first to continue.",
                )
            return None

        if selection.semester_id != self.settings.rtu_semester_id:
            if prompt_if_missing:
                await self._prompt_for_selection(
                    chat_id,
                    (
                        "This bot now uses only the current Foreign Students setup.\n\n"
                        f"Study period: {self.settings.rtu_semester_title}\n"
                        f"Department: {self.settings.rtu_department_title}\n\n"
                        "Choose your program family again."
                    ),
                )
            return None

        if selection.program_id is not None:
            try:
                saved_program = await asyncio.to_thread(
                    self.api_client.get_study_program,
                    selection.semester_id,
                    selection.program_id,
                )
            except RTUAPIError as exc:
                if prompt_if_missing:
                    await self._send_text(
                        chat_id,
                        f"I couldn't validate your saved selection right now: {exc}",
                        reply_markup=self._main_menu(chat_id),
                    )
                else:
                    LOGGER.warning(
                        "Unable to validate selection scope for chat_id=%s program_id=%s: %s",
                        chat_id,
                        selection.program_id,
                        exc,
                    )
                return None

            if (
                saved_program is None
                or saved_program.department_code != self.settings.rtu_department_code
            ):
                if prompt_if_missing:
                    await self._prompt_for_selection(
                        chat_id,
                        (
                            "Your saved selection is outside the locked department.\n\n"
                            f"Study period: {self.settings.rtu_semester_title}\n"
                            f"Department: {self.settings.rtu_department_title}\n\n"
                            "Choose your program family again."
                        ),
                    )
                return None

        if not selection.is_complete():
            if prompt_if_missing:
                await self._prompt_for_selection(
                    chat_id,
                    "Your saved selection is incomplete. Choose your program family, course, and group again.",
                )
            return None

        try:
            return await self._resolve_selection_context(selection)
        except RTUPublicationError as exc:
            if prompt_if_missing:
                await self._send_text(
                    chat_id,
                    f"Your saved selection is currently unavailable: {exc}\nUse Change selection to choose another one.",
                    reply_markup=self._main_menu(chat_id),
                )
            else:
                LOGGER.warning(
                    "Selection is unpublished for chat_id=%s selection=%s: %s",
                    chat_id,
                    selection.selection_key(),
                    exc,
                )
            return None
        except RTUResolutionError as exc:
            if prompt_if_missing:
                await self._prompt_for_selection(
                    chat_id,
                    f"I couldn't resolve your saved selection ({exc}). Please choose your program family again.",
                )
            else:
                LOGGER.warning(
                    "Unable to resolve selection for chat_id=%s selection=%s: %s",
                    chat_id,
                    selection.selection_key(),
                    exc,
                )
            return None
        except RTUAPIError as exc:
            if prompt_if_missing:
                await self._send_text(
                    chat_id,
                    f"I couldn't resolve your saved selection right now: {exc}",
                    reply_markup=self._main_menu(chat_id),
                )
            else:
                LOGGER.warning(
                    "Unable to resolve selection for chat_id=%s selection=%s: %s",
                    chat_id,
                    selection.selection_key(),
                    exc,
                )
            return None

    async def _resolve_selection_context(
        self,
        selection: ChatSelection,
    ) -> tuple[ChatSelection, ResolvedSemesterProgram]:
        target = await asyncio.to_thread(self.api_client.resolve_chat_selection, selection)
        updated_selection = await self._enrich_selection(selection, target)
        if updated_selection != selection:
            await asyncio.to_thread(self.storage.save_chat_selection, updated_selection)
        return updated_selection, target

    async def _enrich_selection(
        self,
        selection: ChatSelection,
        target: ResolvedSemesterProgram,
    ) -> ChatSelection:
        semester_title = selection.semester_title or self.settings.rtu_semester_title
        department_title = selection.department_title or self.settings.rtu_department_title
        program_family = selection.program_family
        program_title = selection.program_title or target.program_title
        program_code = selection.program_code or target.program_code

        if selection.semester_id is not None and selection.program_id is not None:
            try:
                program = await asyncio.to_thread(
                    self.api_client.get_study_program,
                    selection.semester_id,
                    selection.program_id,
                )
                if program is not None:
                    department_title = self.settings.rtu_department_title
                    program_title = program_title or program.title
                    program_code = program_code or program.code
            except RTUAPIError:
                LOGGER.debug(
                    "Unable to enrich program metadata for chat_id=%s program_id=%s",
                    selection.chat_id,
                    selection.program_id,
                )

            if not program_family:
                try:
                    family = await asyncio.to_thread(
                        self.api_client.get_program_family_by_representative_id,
                        selection.semester_id,
                        self.settings.rtu_department_code,
                        selection.program_id,
                    )
                    if family is not None:
                        program_family = family.display_name
                except RTUAPIError:
                    LOGGER.debug(
                        "Unable to enrich program family for chat_id=%s program_id=%s",
                        selection.chat_id,
                        selection.program_id,
                    )

        if not program_family:
            program_family = program_title

        return ChatSelection(
            chat_id=selection.chat_id,
            semester_id=selection.semester_id or target.semester_id,
            semester_title=semester_title,
            program_family=program_family,
            program_id=selection.program_id or target.program_id,
            program_title=program_title,
            program_code=program_code,
            course_id=selection.course_id or target.course_id,
            selected_group=target.group,
            semester_program_id=target.semester_program_id,
            department_title=department_title,
        )

    async def _broadcast_schedule_for_predefined_range(
        self,
        label: str,
        range_factory: Callable[..., tuple[date, date]],
        action: str,
    ) -> None:
        selections = await asyncio.to_thread(self.storage.list_chat_selections)
        if not selections:
            LOGGER.info("Scheduled action skipped because no chats have a saved selection: %s", action)
            return

        start_date, end_date = range_factory(self.settings.zoneinfo)
        targets_by_key: dict[tuple[int | None, int | None, int | None, str], ResolvedSemesterProgram] = {}
        selection_by_key: dict[tuple[int | None, int | None, int | None, str], ChatSelection] = {}
        errors_by_key: dict[tuple[int | None, int | None, int | None, str], str] = {}
        text_by_semester_program: dict[int, str] = {}
        errors_by_semester_program: dict[int, str] = {}

        for selection in selections:
            if not selection.is_complete():
                LOGGER.warning(
                    "Scheduled action skipped incomplete selection for chat_id=%s",
                    selection.chat_id,
                )
                continue

            selection_key = selection.selection_key()
            if selection_key in targets_by_key or selection_key in errors_by_key:
                continue

            try:
                updated_selection, target = await self._resolve_selection_context(selection)
                selection_by_key[selection_key] = updated_selection
                targets_by_key[selection_key] = target
            except RTUPublicationError as exc:
                LOGGER.warning(
                    "Scheduled action skipped unpublished selection=%s action=%s: %s",
                    selection_key,
                    action,
                    exc,
                )
                errors_by_key[selection_key] = (
                    f"The scheduled {label.lower()} update is unavailable because the saved selection is not published right now."
                )
            except RTUResolutionError as exc:
                LOGGER.warning(
                    "Scheduled action failed while resolving selection=%s action=%s: %s",
                    selection_key,
                    action,
                    exc,
                )
                errors_by_key[selection_key] = (
                    f"I couldn't resolve the saved selection for the scheduled {label.lower()} update. Use Change selection to choose it again."
                )
            except RTUAPIError as exc:
                LOGGER.exception(
                    "Scheduled action failed while resolving selection=%s action=%s",
                    selection_key,
                    action,
                )
                errors_by_key[selection_key] = (
                    f"I couldn't load the scheduled {label.lower()} update right now: {exc}"
                )

        for selection_key, target in targets_by_key.items():
            if target.semester_program_id in text_by_semester_program:
                continue

            try:
                events = await asyncio.to_thread(
                    self.api_client.get_events_for_range,
                    target.semester_program_id,
                    start_date,
                    end_date,
                )
                selection = selection_by_key.get(selection_key)
                text_by_semester_program[target.semester_program_id] = self._render_schedule_message(
                    label,
                    start_date,
                    end_date,
                    events,
                    context_line=self._selection_context_line(selection, target),
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
            if not selection.is_complete():
                continue

            selection_key = selection.selection_key()
            try:
                target = targets_by_key.get(selection_key)
                if target is None:
                    await self._send_text_safe(
                        selection.chat_id,
                        errors_by_key.get(
                            selection_key,
                            f"I couldn't load the scheduled {label.lower()} update right now.",
                        ),
                        reply_markup=self._main_menu(selection.chat_id),
                    )
                    continue

                text = text_by_semester_program.get(target.semester_program_id)
                if text is None:
                    await self._send_text_safe(
                        selection.chat_id,
                        errors_by_semester_program.get(
                            target.semester_program_id,
                            f"I couldn't load the scheduled {label.lower()} update right now.",
                        ),
                        reply_markup=self._main_menu(selection.chat_id),
                    )
                    continue

                await self._send_text_safe(
                    selection.chat_id,
                    text,
                    reply_markup=self._main_menu(selection.chat_id),
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
        context_line: str | None = None,
    ) -> str:
        if start_date == end_date:
            return format_daily_schedule(
                label=label,
                target_date=start_date,
                events=events,
                context_line=context_line,
            )
        return format_range_schedule(
            label=label,
            start_date=start_date,
            end_date=end_date,
            events=events,
            context_line=context_line,
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

    def _reminder_window_bounds(self) -> tuple[int, int]:
        tolerance_minutes = max(self.settings.reminder_check_interval_minutes, 5)
        lower_bound = max(0, self.settings.reminder_minutes_before - tolerance_minutes)
        return lower_bound, self.settings.reminder_minutes_before

    def _due_reminder_events(
        self,
        events: list[ScheduleEvent],
        now: datetime,
        lower_bound_minutes: int,
        upper_bound_minutes: int,
    ) -> list[ScheduleEvent]:
        due_events: list[ScheduleEvent] = []
        for event in events:
            lesson_start = combine_local_datetime(
                event.event_date,
                event.start_time,
                self.settings.zoneinfo,
            )
            if lesson_start is None:
                continue

            minutes_until_start = (lesson_start - now).total_seconds() / 60
            if lower_bound_minutes <= minutes_until_start <= upper_bound_minutes:
                due_events.append(event)
        return due_events

    @staticmethod
    def _build_reminder_key(
        chat_id: int,
        semester_program_id: int,
        event: ScheduleEvent,
    ) -> str:
        start_time = event.start_time.isoformat() if event.start_time is not None else "TBA"
        fingerprint = (
            f"{chat_id}|{semester_program_id}|{event.event_date.isoformat()}|{start_time}|"
            f"{event.stable_id()}|{event.title}|{event.lecturer}|{event.room}"
        )
        return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()

    async def _configure_telegram_commands(self) -> None:
        await self.bot.set_my_commands(
            [
                BotCommand(command="start", description="Choose your program family, course, and group"),
                BotCommand(command="today", description="Show today's lessons"),
                BotCommand(command="tomorrow", description="Show tomorrow's lessons"),
                BotCommand(command="week", description="Show the next 7 days"),
                BotCommand(command="month", description="Show the current month"),
                BotCommand(command="subjects", description="Show current semester subjects"),
                BotCommand(command="refresh", description="Refresh snapshots and detect changes"),
                BotCommand(command="status", description="Show the current configuration"),
            ]
        )

    def _main_menu(self, chat_id: int | None = None) -> ReplyKeyboardMarkup:
        keyboard = [
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
        ]
        final_row = [KeyboardButton(text=BUTTON_CHANGE_SELECTION)]
        if chat_id is not None and self.settings.is_admin_chat(chat_id):
            final_row.append(KeyboardButton(text=BUTTON_STATS))
        keyboard.append(final_row)
        return ReplyKeyboardMarkup(
            keyboard=keyboard,
            resize_keyboard=True,
            is_persistent=True,
            input_field_placeholder="Choose a schedule action",
        )

    async def _start_selection_prompt(
        self,
        chat_id: int,
        message: Message | None = None,
        intro_text: str = "Choose your program family to begin.",
    ) -> None:
        period = await asyncio.to_thread(self.api_client.get_locked_study_period)
        department = await asyncio.to_thread(
            self.api_client.get_locked_department,
            period.semester_id,
        )
        department_title = self._format_department_label(department)
        self._selection_drafts[chat_id] = SelectionDraft(
            semester_id=period.semester_id,
            semester_title=period.title,
            department_id=department.department_id,
            department_title=department_title,
        )
        LOGGER.info(
            "Starting locked selection flow: chat_id=%s semester_id=%s semester_title=%s department_id=%s department=%s",
            chat_id,
            period.semester_id,
            period.title,
            department.department_id,
            department_title,
        )

        if message is None:
            await self._send_text(chat_id, intro_text, reply_markup=ReplyKeyboardRemove())
        else:
            await self._upsert_selection_message(
                chat_id=chat_id,
                text=intro_text,
                reply_markup=None,
                message=message,
            )

        await self._show_program_prompt(chat_id)

    async def _prompt_for_selection(self, chat_id: int, reason: str) -> None:
        await self._start_selection_prompt(chat_id, intro_text=reason)

    async def _cancel_configuration(self, chat_id: int, message: Message | None) -> None:
        self._selection_drafts.pop(chat_id, None)
        current_selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        text = "Setup cancelled. Use Change selection when you want to update the study selection."
        reply_markup = self._main_menu(chat_id) if current_selection is not None else ReplyKeyboardRemove()

        if message is not None:
            await self._upsert_selection_message(
                chat_id=chat_id,
                text=text,
                reply_markup=None,
                message=message,
            )

        await self._send_text(chat_id, text, reply_markup=reply_markup)

    async def _navigate_back(self, chat_id: int, message: Message | None) -> None:
        draft = self._selection_drafts.get(chat_id)
        if draft is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your program family to begin.",
            )
            return

        if draft.course_id is not None:
            draft.course_id = None
            await self._show_course_prompt(chat_id, message=message)
            return

        if draft.program_id is not None:
            draft.clear_from_program()
            await self._show_program_prompt(chat_id, message=message)
            return

        await self._show_program_prompt(chat_id, message=message)

    async def _show_selection_page(
        self,
        chat_id: int,
        step: str,
        message: Message | None,
        page: int,
    ) -> None:
        if step == "prog":
            await self._show_program_prompt(chat_id, message=message, page=page)
        elif step == "course":
            await self._show_course_prompt(chat_id, message=message, page=page)
        elif step == "group":
            await self._show_group_prompt(chat_id, message=message, page=page)

    async def _show_program_prompt(
        self,
        chat_id: int,
        message: Message | None = None,
        page: int = 0,
        notice: str | None = None,
    ) -> None:
        draft = self._selection_drafts.get(chat_id)
        if draft is None or draft.semester_id is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your program family to begin.",
            )
            return

        families = await asyncio.to_thread(
            self.api_client.get_program_families,
            draft.semester_id,
            self.settings.rtu_department_code,
        )
        if not families:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="No study programs were returned for the locked department. Please try again later.",
            )
            return

        page_items, page, total_pages = self._paginate(families, page, PROGRAM_PAGE_SIZE)
        text = self._build_setup_text(
            summary_lines=self._draft_summary_lines(draft),
            prompt="Choose your program family.",
            notice=notice,
            page=page,
            total_pages=total_pages,
        )
        markup = self._build_paginated_markup(
            items=page_items,
            label_builder=lambda item: item.display_name,
            callback_builder=lambda item: self._callback(
                "prog",
                "select",
                item.representative_program.program_id,
            ),
            footer_rows=[self._back_cancel_row()],
            page=page,
            total_pages=total_pages,
            page_callback_builder=lambda value: self._callback("prog", "page", value),
        )
        await self._upsert_selection_message(chat_id, text, markup, message)

    async def _show_course_prompt(
        self,
        chat_id: int,
        message: Message | None = None,
        page: int = 0,
        notice: str | None = None,
    ) -> None:
        draft = self._selection_drafts.get(chat_id)
        if draft is None or draft.semester_id is None or draft.program_id is None:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program family first.",
            )
            return

        courses = await asyncio.to_thread(
            self.api_client.get_courses,
            draft.semester_id,
            draft.program_id,
        )
        if not courses:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="No course options were returned for the selected program family.",
            )
            return

        page_items, page, total_pages = self._paginate(courses, page, SMALL_PAGE_SIZE)
        text = self._build_setup_text(
            summary_lines=self._draft_summary_lines(draft),
            prompt="Choose your course.",
            notice=notice,
            page=page,
            total_pages=total_pages,
        )
        markup = self._build_paginated_markup(
            items=page_items,
            label_builder=lambda course: f"Course {course}",
            callback_builder=lambda course: self._callback("course", "select", course),
            footer_rows=[self._back_cancel_row()],
            page=page,
            total_pages=total_pages,
            page_callback_builder=lambda value: self._callback("course", "page", value),
            columns=2,
        )
        await self._upsert_selection_message(chat_id, text, markup, message)

    async def _show_group_prompt(
        self,
        chat_id: int,
        message: Message | None = None,
        page: int = 0,
        notice: str | None = None,
    ) -> None:
        draft = self._selection_drafts.get(chat_id)
        if draft is None or draft.semester_id is None or draft.program_id is None or draft.course_id is None:
            await self._show_course_prompt(
                chat_id,
                message=message,
                notice="Choose the course first.",
            )
            return

        groups = await asyncio.to_thread(
            self.api_client.get_display_groups,
            draft.semester_id,
            draft.program_id,
            draft.course_id,
        )
        if not groups:
            await self._show_course_prompt(
                chat_id,
                message=message,
                notice="No groups were returned for the selected course.",
            )
            return

        page_items, page, total_pages = self._paginate(groups, page, SMALL_PAGE_SIZE)
        text = self._build_setup_text(
            summary_lines=self._draft_summary_lines(draft),
            prompt="Choose your group.",
            notice=notice,
            page=page,
            total_pages=total_pages,
        )
        markup = self._build_paginated_markup(
            items=page_items,
            label_builder=lambda item: self._format_group_name(item.group),
            callback_builder=lambda item: self._callback("group", "select", item.semester_program_id),
            footer_rows=[self._back_cancel_row()],
            page=page,
            total_pages=total_pages,
            page_callback_builder=lambda value: self._callback("group", "page", value),
            columns=2,
        )
        await self._upsert_selection_message(chat_id, text, markup, message)

    async def _select_program(
        self,
        chat_id: int,
        program_id: int,
        message: Message | None,
    ) -> None:
        draft = self._selection_drafts.get(chat_id)
        if draft is None or draft.semester_id is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your program family to begin.",
            )
            return

        family = await asyncio.to_thread(
            self.api_client.get_program_family_by_representative_id,
            draft.semester_id,
            self.settings.rtu_department_code,
            program_id,
        )
        if family is None:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="That program family is no longer available. Please choose another one.",
            )
            return

        representative = family.representative_program
        LOGGER.info(
            "Program family selected: chat_id=%s family=%s representative_program_id=%s code=%s variants=%s",
            chat_id,
            family.display_name,
            representative.program_id,
            representative.code,
            [
                f"{variant.program_id}:{variant.code or 'no-code'}"
                for variant in family.variants
            ],
        )
        draft.program_family = family.display_name
        draft.program_id = representative.program_id
        draft.program_title = representative.title
        draft.program_code = representative.code
        draft.department_title = self.settings.rtu_department_title
        draft.course_id = None

        courses = await asyncio.to_thread(
            self.api_client.get_courses,
            draft.semester_id,
            draft.program_id,
        )
        if not courses:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="RTU returned no course options for this program family.",
            )
            return

        if len(courses) == 1:
            draft.course_id = courses[0]
            groups = await asyncio.to_thread(
                self.api_client.get_display_groups,
                draft.semester_id,
                draft.program_id,
                draft.course_id,
            )
            if not groups:
                await self._show_program_prompt(
                    chat_id,
                    message=message,
                    notice="RTU returned no valid groups for the selected course.",
                )
                return
            if len(groups) == 1:
                await self._complete_selection(
                    chat_id,
                    groups[0].semester_program_id,
                    message=message,
                    notice=(
                        f"Program family selected: {family.display_name}. "
                        f"Course {courses[0]} and {self._format_group_name(groups[0].group)} were selected automatically."
                    ),
                )
                return
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice=(
                    f"Program family selected: {family.display_name}. "
                    f"Course {courses[0]} was selected automatically."
                ),
            )
            return

        await self._show_course_prompt(
            chat_id,
            message=message,
            notice=f"Program family selected: {family.display_name}",
        )

    async def _select_course(
        self,
        chat_id: int,
        course_id: int,
        message: Message | None,
    ) -> None:
        draft = self._selection_drafts.get(chat_id)
        if draft is None or draft.semester_id is None or draft.program_id is None:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program family first.",
            )
            return

        courses = await asyncio.to_thread(
            self.api_client.get_courses,
            draft.semester_id,
            draft.program_id,
        )
        if course_id not in courses:
            await self._show_course_prompt(
                chat_id,
                message=message,
                notice="That course is no longer available. Please choose another one.",
            )
            return

        draft.course_id = course_id
        LOGGER.info(
            "Course selected: chat_id=%s family=%s program_id=%s course_id=%s",
            chat_id,
            draft.program_family,
            draft.program_id,
            course_id,
        )
        groups = await asyncio.to_thread(
            self.api_client.get_display_groups,
            draft.semester_id,
            draft.program_id,
            draft.course_id,
        )
        if not groups:
            await self._show_course_prompt(
                chat_id,
                message=message,
                notice="RTU returned no groups for this course.",
            )
            return

        if len(groups) == 1:
            await self._complete_selection(
                chat_id,
                groups[0].semester_program_id,
                message=message,
                notice=(
                    f"Course {course_id} selected. "
                    f"{self._format_group_name(groups[0].group)} was selected automatically."
                ),
            )
            return

        await self._show_group_prompt(
            chat_id,
            message=message,
            notice=f"Course selected: {course_id}",
        )

    async def _select_group(
        self,
        chat_id: int,
        semester_program_id: int,
        message: Message | None,
    ) -> None:
        await self._complete_selection(chat_id, semester_program_id, message=message)

    async def _complete_selection(
        self,
        chat_id: int,
        semester_program_id: int,
        message: Message | None,
        notice: str | None = None,
    ) -> None:
        draft = self._selection_drafts.get(chat_id)
        if (
            draft is None
            or draft.semester_id is None
            or draft.program_id is None
            or draft.course_id is None
        ):
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program family, course, and group first.",
            )
            return

        groups = await asyncio.to_thread(
            self.api_client.get_display_groups,
            draft.semester_id,
            draft.program_id,
            draft.course_id,
        )
        group = next(
            (item for item in groups if item.semester_program_id == semester_program_id),
            None,
        )
        if group is None:
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice="That group is no longer available. Please choose another one.",
            )
            return

        try:
            target = await asyncio.to_thread(
                self.api_client.resolve_semester_program,
                draft.semester_id,
                draft.program_id,
                draft.course_id,
                group.group,
                semester_program_id,
            )
        except RTUPublicationError as exc:
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice=f"The selected group is not published right now: {exc}",
            )
            return
        except RTUAPIError as exc:
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice=f"I couldn't activate that group right now: {exc}",
            )
            return

        LOGGER.info(
            "Final selection resolved: chat_id=%s family=%s program_id=%s course_id=%s group=%s semester_program_id=%s",
            chat_id,
            draft.program_family,
            draft.program_id,
            draft.course_id,
            target.group,
            target.semester_program_id,
        )
        selection = ChatSelection(
            chat_id=chat_id,
            semester_id=draft.semester_id,
            semester_title=draft.semester_title or self.settings.rtu_semester_title,
            program_family=draft.program_family,
            program_id=draft.program_id,
            program_title=draft.program_title or target.program_title,
            program_code=draft.program_code or target.program_code,
            course_id=draft.course_id,
            selected_group=target.group,
            semester_program_id=target.semester_program_id,
            department_title=draft.department_title or self.settings.rtu_department_title,
        )
        await asyncio.to_thread(self.storage.save_chat_selection, selection)
        self._selection_drafts.pop(chat_id, None)

        summary_lines = ["Selection saved."]
        if notice:
            summary_lines.extend(["", notice])
        summary_lines.extend(["", *self._selection_summary_lines(selection, include_resolved=True)])

        if message is not None:
            await self._upsert_selection_message(
                chat_id=chat_id,
                text="\n".join(summary_lines),
                reply_markup=None,
                message=message,
            )

        await self._send_text(
            chat_id,
            "The main menu is ready below. Use Change selection whenever you want to switch program family, course, or group.",
            reply_markup=self._main_menu(chat_id),
        )

    def _build_setup_text(
        self,
        summary_lines: list[str],
        prompt: str,
        notice: str | None = None,
        page: int = 0,
        total_pages: int = 1,
    ) -> str:
        lines = [SELECTION_TITLE]
        if summary_lines:
            lines.extend(["", *summary_lines])
        lines.extend(["", prompt])
        if total_pages > 1:
            lines.append(f"Page {page + 1} of {total_pages}")
        if notice:
            lines.extend(["", notice])
        return "\n".join(lines)

    def _build_paginated_markup(
        self,
        items: list[object],
        label_builder: Callable[[object], str],
        callback_builder: Callable[[object], str],
        footer_rows: list[list[InlineKeyboardButton]],
        page: int,
        total_pages: int,
        page_callback_builder: Callable[[int], str],
        columns: int = 1,
    ) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        buttons = [
            InlineKeyboardButton(
                text=self._truncate_label(label_builder(item)),
                callback_data=callback_builder(item),
            )
            for item in items
        ]
        rows.extend(self._chunk_buttons(buttons, columns))

        if total_pages > 1:
            nav_buttons: list[InlineKeyboardButton] = []
            if page > 0:
                nav_buttons.append(
                    InlineKeyboardButton(
                        text="Previous",
                        callback_data=page_callback_builder(page - 1),
                    )
                )
            if page < total_pages - 1:
                nav_buttons.append(
                    InlineKeyboardButton(
                        text="Next",
                        callback_data=page_callback_builder(page + 1),
                    )
                )
            if nav_buttons:
                rows.append(nav_buttons)

        rows.extend(footer_rows)
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def _chunk_buttons(
        buttons: list[InlineKeyboardButton],
        size: int,
    ) -> list[list[InlineKeyboardButton]]:
        return [buttons[index:index + size] for index in range(0, len(buttons), size)]

    @staticmethod
    def _paginate(items: list[object], page: int, page_size: int) -> tuple[list[object], int, int]:
        if not items:
            return [], 0, 1
        total_pages = max(1, (len(items) + page_size - 1) // page_size)
        normalized_page = min(max(page, 0), total_pages - 1)
        start = normalized_page * page_size
        end = start + page_size
        return items[start:end], normalized_page, total_pages

    async def _upsert_selection_message(
        self,
        chat_id: int,
        text: str,
        reply_markup: InlineKeyboardMarkup | None,
        message: Message | None,
    ) -> None:
        if message is not None:
            try:
                await message.edit_text(text, reply_markup=reply_markup)
                return
            except TelegramBadRequest:
                LOGGER.debug("Selection prompt edit failed for chat_id=%s; sending a new message", chat_id)

        await self._send_text(chat_id, text, reply_markup=reply_markup)

    @staticmethod
    def _callback(action: str, mode: str, value: int | None = None) -> str:
        if value is None:
            return f"{CALLBACK_PREFIX}:{action}:{mode}"
        return f"{CALLBACK_PREFIX}:{action}:{mode}:{value}"

    @classmethod
    def _back_cancel_row(cls) -> list[InlineKeyboardButton]:
        return [
            InlineKeyboardButton(text="Back", callback_data=cls._callback("nav", "back")),
            InlineKeyboardButton(text="Cancel", callback_data=cls._callback("nav", "cancel")),
        ]

    @classmethod
    def _cancel_row(cls) -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton(text="Cancel", callback_data=cls._callback("nav", "cancel"))]

    def _selection_summary_lines(
        self,
        selection: ChatSelection,
        include_resolved: bool = False,
    ) -> list[str]:
        lines: list[str] = []
        if selection.semester_title:
            lines.append(f"Study period: {selection.semester_title}")
        elif selection.semester_id is not None:
            lines.append(f"Study period ID: {selection.semester_id}")

        if selection.department_title:
            lines.append(f"Department: {selection.department_title}")

        if selection.program_family:
            lines.append(f"Program family: {selection.program_family}")

        if selection.program_title:
            lines.append(
                f"Underlying RTU program: {self._format_program_name(selection.program_title, selection.program_code)}"
            )
        elif selection.program_id is not None:
            lines.append(f"Underlying RTU program ID: {selection.program_id}")

        if selection.course_id is not None:
            lines.append(f"Course: {selection.course_id}")

        if selection.selected_group:
            lines.append(f"Group: {selection.selected_group}")

        if include_resolved and selection.semester_program_id is not None:
            lines.append(f"semesterProgramId: {selection.semester_program_id}")

        return lines

    def _draft_summary_lines(self, draft: SelectionDraft) -> list[str]:
        lines = [
            f"Study period: {draft.semester_title or self.settings.rtu_semester_title}",
            f"Department: {draft.department_title or self.settings.rtu_department_title}",
        ]
        if draft.program_family:
            lines.append(f"Program family: {draft.program_family}")
        if draft.program_title:
            lines.append(
                f"Underlying RTU program: {self._format_program_name(draft.program_title, draft.program_code)}"
            )
        if draft.course_id is not None:
            lines.append(f"Course: {draft.course_id}")
        return lines

    @staticmethod
    def _format_program_name(title: str | None, code: str | None) -> str:
        if not title:
            return "Not selected"
        if code:
            return f"{title} ({code})"
        return title

    @staticmethod
    def _format_department_label(department: StudyDepartment) -> str:
        if department.code:
            return f"{department.title} ({department.code})"
        return department.title

    @staticmethod
    def _truncate_label(text: str, max_length: int = 48) -> str:
        if len(text) <= max_length:
            return text
        return f"{text[: max_length - 3].rstrip()}..."

    @staticmethod
    def _format_group_name(group: str) -> str:
        cleaned = group.strip()
        if not cleaned:
            return "Default group"
        return f"Group {cleaned}"

    def _selection_context_line(
        self,
        selection: ChatSelection | None,
        target: ResolvedSemesterProgram,
    ) -> str | None:
        program_family = None
        program_title = target.program_title
        program_code = target.program_code
        course_id = target.course_id
        group = target.group

        if selection is not None:
            program_family = selection.program_family
            program_title = selection.program_title or program_title
            program_code = selection.program_code or program_code
            course_id = selection.course_id or course_id
            group = selection.selected_group or group

        parts: list[str] = []
        if program_family:
            parts.append(program_family)
        elif program_title:
            parts.append(self._format_program_name(program_title, program_code))
        if course_id is not None:
            parts.append(f"Course {course_id}")
        if group:
            parts.append(self._format_group_name(group))

        return " | ".join(parts) if parts else None

    def _subjects_heading(
        self,
        selection: ChatSelection,
        target: ResolvedSemesterProgram,
    ) -> str:
        context_line = self._selection_context_line(selection, target)
        if not context_line:
            return "Subjects"
        return f"Subjects | {context_line}"

    async def _send_text(
        self,
        chat_id: int,
        text: str,
        reply_markup: object | None = None,
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
        reply_markup: object | None = None,
    ) -> None:
        try:
            await self._send_text(chat_id, text, reply_markup=reply_markup)
        except Exception:
            LOGGER.exception("Failed to send Telegram message to chat_id=%s", chat_id)
