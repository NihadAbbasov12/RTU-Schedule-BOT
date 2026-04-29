"""Telegram bot handlers and schedule delivery logic."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from calendar import monthrange
from collections.abc import Awaitable, Callable
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
    SelectionDraft,
    StudyDepartment,
    StudyProgram,
    clean_group_label,
    combine_local_datetime,
    get_academic_week_range,
    get_month_range,
    get_now,
    get_today_range,
    get_tomorrow_range,
    get_week_key,
    get_week_range,
    normalize_group_code,
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

PROGRAM_PAGE_SIZE = 12
SMALL_PAGE_SIZE = 8
SelectionCacheKey = tuple[int | None, int | None, int | None, str, int | None]


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

        targets_by_key: dict[SelectionCacheKey, ResolvedSemesterProgram] = {}
        selection_by_key: dict[SelectionCacheKey, ChatSelection] = {}
        events_by_semester_program: dict[int, list[ScheduleEvent]] = {}

        for selection in pending:
            if not self._selection_has_resolvable_target(selection):
                LOGGER.warning(
                    "Skipping chat %s during weekend check: selection is incomplete or missing group_code course_id=%s saved_group_code=%s saved_group=%s",
                    selection.chat_id,
                    selection.course_id,
                    selection.resolved_group_code() or None,
                    selection.display_group(),
                )
                continue

            selection_key = selection.selection_key()
            if selection_key in targets_by_key:
                continue

            try:
                updated_selection, target = await self._resolve_selection_context(selection)
                selection_by_key[selection_key] = updated_selection
                targets_by_key[selection_key] = target
            except RTUResolutionError as exc:
                LOGGER.warning(
                    "Skipping chat %s: saved group_code %s could not be resolved for semester_id=%s program_id=%s course_id=%s",
                    selection.chat_id,
                    selection.resolved_group_code() or None,
                    selection.semester_id,
                    selection.program_id,
                    selection.course_id,
                )
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
            if not self._selection_has_resolvable_target(selection):
                continue

            try:
                target = targets_by_key.get(selection.selection_key())
                if target is None:
                    continue

                resolved_selection = selection_by_key.get(selection.selection_key(), selection)
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
                    resolved_selection.resolved_group_code() or target.group_code,
                    resolved_selection.display_group() or target.group,
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

        targets_by_key: dict[SelectionCacheKey, ResolvedSemesterProgram] = {}
        selection_by_key: dict[SelectionCacheKey, ChatSelection] = {}
        events_by_semester_program: dict[int, list[ScheduleEvent]] = {}
        reminders_sent = 0
        skipped_duplicates = 0

        for selection in selections:
            if not self._selection_has_resolvable_target(selection):
                LOGGER.warning(
                    "Skipping chat %s during reminder scan: selection is incomplete or missing group_code course_id=%s saved_group_code=%s saved_group=%s",
                    selection.chat_id,
                    selection.course_id,
                    selection.resolved_group_code() or None,
                    selection.display_group(),
                )
                continue

            selection_key = selection.selection_key()
            try:
                if selection_key not in targets_by_key:
                    updated_selection, target = await self._resolve_selection_context(selection)
                    selection_by_key[selection_key] = updated_selection
                    targets_by_key[selection_key] = target

                target = targets_by_key[selection_key]
                resolved_selection = selection_by_key.get(selection_key, selection)
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
                        resolved_selection,
                        event,
                    )
                    acquired = await asyncio.to_thread(
                        self.storage.try_acquire_reminder_delivery,
                        selection.chat_id,
                        reminder_key,
                        event.event_date,
                        event.start_time.strftime("%H:%M:%S"),
                        resolved_selection.resolved_group_code() or target.group_code,
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
            except RTUResolutionError as exc:
                LOGGER.warning(
                    "Skipping chat %s: saved group_code %s could not be resolved for semester_id=%s program_id=%s course_id=%s",
                    selection.chat_id,
                    selection.resolved_group_code() or None,
                    selection.semester_id,
                    selection.program_id,
                    selection.course_id,
                )
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
            identifier = parts[3] if len(parts) > 3 else None
            LOGGER.info(
                "Configuration callback received: chat_id=%s action=%s mode=%s identifier=%s",
                chat_id,
                action,
                mode,
                identifier,
            )

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
                        "Choose your study program."
                    ),
                )
                return

            draft = await self._get_selection_draft(chat_id)
            if draft is None:
                LOGGER.warning(
                    "Selection draft missing for callback: chat_id=%s action=%s mode=%s identifier=%s",
                    chat_id,
                    action,
                    mode,
                    identifier,
                )
                await callback.answer("Setup expired. Starting over.")
                await self._start_selection_prompt(
                    chat_id,
                    message=message,
                    intro_text=(
                        "The previous setup could not be recovered.\n\n"
                        f"Study period: {self.settings.rtu_semester_title}\n"
                        f"Department: {self.settings.rtu_department_title}\n\n"
                        "Choose your study program again."
                    ),
                )
                return

            if mode in {"page", "select"} and len(parts) < 4:
                LOGGER.warning(
                    "Configuration callback payload is incomplete: chat_id=%s action=%s mode=%s data=%s",
                    chat_id,
                    action,
                    mode,
                    data,
                )
                await callback.answer("That selection is no longer valid. Please try again.", show_alert=True)
                return

            if mode == "page":
                page = int(parts[3])
                await callback.answer()
                await self._show_selection_page(chat_id, action, message=message, page=page)
                return

            if mode == "select":
                identifier = parts[3]
                await callback.answer()
                if action == "progtitle":
                    await self._select_program_title(chat_id, int(identifier), message)
                elif action in {"progcode", "prog"}:
                    await self._select_program(chat_id, int(identifier), message)
                elif action == "course":
                    await self._select_course(chat_id, int(identifier), message)
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
                            "Choose your study program."
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

    async def _get_selection_draft(self, chat_id: int) -> SelectionDraft | None:
        draft = self._selection_drafts.get(chat_id)
        if draft is not None:
            return draft

        draft = await asyncio.to_thread(self.storage.get_selection_draft, chat_id)
        if draft is not None:
            self._selection_drafts[chat_id] = draft
            LOGGER.info("Recovered selection draft from SQLite for chat_id=%s", chat_id)
        return draft

    async def _save_selection_draft(self, chat_id: int, draft: SelectionDraft) -> None:
        self._selection_drafts[chat_id] = draft
        await asyncio.to_thread(self.storage.save_selection_draft, chat_id, draft)
        LOGGER.debug(
            "Saved selection draft: chat_id=%s semester_id=%s program_id=%s course_id=%s",
            chat_id,
            draft.semester_id,
            draft.program_id,
            draft.course_id,
        )

    async def _clear_selection_draft(self, chat_id: int) -> None:
        self._selection_drafts.pop(chat_id, None)
        await asyncio.to_thread(self.storage.delete_selection_draft, chat_id)
        LOGGER.debug("Cleared selection draft for chat_id=%s", chat_id)

    async def _show_start(self, chat_id: int) -> None:
        selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        intro_lines = [
            "RTU Schedule Bot",
            "",
            "This bot is locked to the current Foreign Students setup:",
            f"Study period: {self.settings.rtu_semester_title}",
            f"Department: {self.settings.rtu_department_title}",
            "",
            "Choose your study program, then your course and group code.",
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
                    "Use Change selection to choose your study program, course, and group.",
                    "",
                ]
            )
        elif not self._selection_has_resolvable_target(selection):
            heading_lines.extend(
                [
                    "Your saved selection is incomplete or missing a valid group code.",
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
            group_code=(
                current_selection.resolved_group_code()
                if current_selection is not None
                else (target.group_code if target is not None else None)
            ),
            group_name=(
                current_selection.display_group()
                if current_selection is not None
                else (target.group_name if target is not None else None)
            ),
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

        selection, target = context
        try:
            changes = await asyncio.to_thread(
                self._refresh_current_and_next_months,
                chat_id,
                selection.resolved_group_code() or target.group_code,
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

    @staticmethod
    def _selection_has_resolvable_target(selection: ChatSelection) -> bool:
        return (
            selection.semester_id is not None
            and selection.program_id is not None
            and selection.course_id is not None
            and bool(selection.resolved_group_code() or selection.semester_program_id is not None)
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
                    "Choose your study program, course, and group first to continue.",
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
                        "Choose your study program again."
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
                            "Choose your study program again."
                        ),
                    )
                return None

        if not self._selection_has_resolvable_target(selection):
            if prompt_if_missing:
                await self._prompt_for_selection(
                    chat_id,
                    "Your saved selection is missing a valid group code. Choose your study program, course, and group again.",
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
                    f"I couldn't resolve your saved selection ({exc}). Please choose your study program again.",
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
            LOGGER.info(
                "Persisting refreshed selection metadata: chat_id=%s old_group_code=%s new_group_code=%s semester_program_id=%s",
                selection.chat_id,
                selection.resolved_group_code() or None,
                updated_selection.resolved_group_code() or None,
                updated_selection.semester_program_id,
            )
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
                        "Unable to enrich program label for chat_id=%s program_id=%s",
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
            program_id=target.program_id,
            program_title=target.program_title or program_title,
            program_code=target.program_code or program_code,
            course_id=selection.course_id or target.course_id,
            selected_group=target.group,
            semester_program_id=target.semester_program_id,
            department_title=department_title,
            group_code=target.group_code,
            group_name=target.group_name,
            group_id=target.group_id,
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
        targets_by_key: dict[SelectionCacheKey, ResolvedSemesterProgram] = {}
        selection_by_key: dict[SelectionCacheKey, ChatSelection] = {}
        errors_by_key: dict[SelectionCacheKey, str] = {}
        text_by_semester_program: dict[int, str] = {}
        errors_by_semester_program: dict[int, str] = {}

        for selection in selections:
            if not self._selection_has_resolvable_target(selection):
                LOGGER.warning(
                    "Skipping chat %s for scheduled action %s: selection is incomplete or missing group_code course_id=%s saved_group_code=%s saved_group=%s",
                    selection.chat_id,
                    action,
                    selection.course_id,
                    selection.resolved_group_code() or None,
                    selection.display_group(),
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
                    "Skipping chat %s: saved group_code %s could not be resolved for semester_id=%s program_id=%s course_id=%s action=%s: %s",
                    selection.chat_id,
                    selection.resolved_group_code() or None,
                    selection.semester_id,
                    selection.program_id,
                    selection.course_id,
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
            if not self._selection_has_resolvable_target(selection):
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
        group_code: str,
        semester_program_id: int,
    ) -> list[ScheduleDiff]:
        current_start, current_end = get_month_range(self.settings.zoneinfo)
        next_month_year, next_month = self._next_month(current_start.year, current_start.month)
        _, next_month_days = monthrange(next_month_year, next_month)
        next_start = date(next_month_year, next_month, 1)
        next_end = date(next_month_year, next_month, next_month_days)

        LOGGER.debug(
            "Refreshing schedule snapshots: chat_id=%s group_code=%s semester_program_id=%s current=%s..%s next=%s..%s",
            chat_id,
            group_code,
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
            group_code,
            semester_program_id,
            current_start.year,
            current_start.month,
            current_events,
        )
        changes.extend(
            self.storage.sync_month(
                chat_id,
                group_code,
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
        selection: ChatSelection,
        event: ScheduleEvent,
    ) -> str:
        start_time = event.start_time.isoformat() if event.start_time is not None else "TBA"
        group_code = selection.resolved_group_code() or "UNKNOWN"
        fingerprint = (
            f"{chat_id}|{selection.semester_id}|{selection.program_id}|{selection.course_id}|"
            f"{group_code}|{event.event_date.isoformat()}|{start_time}|"
            f"{event.stable_id()}|{event.title}|{event.lecturer}|{event.room}"
        )
        return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()

    async def _configure_telegram_commands(self) -> None:
        await self.bot.set_my_commands(
            [
                BotCommand(command="start", description="Choose your study program, course, and group"),
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
        intro_text: str = "Choose your study program to begin.",
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
        await self._save_selection_draft(chat_id, self._selection_drafts[chat_id])
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
        await self._clear_selection_draft(chat_id)
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
        draft = await self._get_selection_draft(chat_id)
        if draft is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your study program to begin.",
            )
            return

        if draft.course_id is not None:
            draft.clear_from_course()
            await self._save_selection_draft(chat_id, draft)
            await self._show_course_prompt(chat_id, message=message)
            return

        if draft.program_id is not None:
            if not draft.selected_program_title:
                draft.selected_program_title = draft.program_title or draft.program_family
            draft.clear_exact_program()
            await self._save_selection_draft(chat_id, draft)
            await self._show_program_code_prompt(chat_id, message=message)
            return

        if draft.selected_program_title:
            draft.clear_from_program()
            await self._save_selection_draft(chat_id, draft)
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
        if step in {"progtitle", "prog"}:
            await self._show_program_prompt(chat_id, message=message, page=page)
        elif step == "progcode":
            await self._show_program_code_prompt(chat_id, message=message, page=page)
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
        draft = await self._get_selection_draft(chat_id)
        if draft is None or draft.semester_id is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your study program to begin.",
            )
            return

        titles = await asyncio.to_thread(
            self.api_client.get_department_program_titles,
            draft.semester_id,
            self.settings.rtu_department_code,
        )
        if not titles:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="No study program titles were returned for the locked department. Please try again later.",
            )
            return

        indexed_titles = list(enumerate(titles))
        page_items, page, total_pages = self._paginate(indexed_titles, page, PROGRAM_PAGE_SIZE)
        page_labels = [item[1] for item in page_items]
        LOGGER.info(
            "Showing program title prompt: chat_id=%s semester_id=%s department=%s page=%s/%s page_labels=%s all_titles=%s",
            chat_id,
            draft.semester_id,
            self.settings.rtu_department_code,
            page + 1,
            total_pages,
            page_labels,
            titles,
        )
        text = self._build_setup_text(
            summary_lines=self._draft_summary_lines(draft),
            prompt="Choose your program.",
            notice=notice,
            page=page,
            total_pages=total_pages,
        )
        markup = self._build_paginated_markup(
            items=page_items,
            label_builder=lambda item: item[1],
            callback_builder=lambda item: self._callback("progtitle", "select", item[0]),
            footer_rows=[self._back_cancel_row()],
            page=page,
            total_pages=total_pages,
            page_callback_builder=lambda value: self._callback("progtitle", "page", value),
        )
        await self._upsert_selection_message(chat_id, text, markup, message)

    async def _show_program_code_prompt(
        self,
        chat_id: int,
        message: Message | None = None,
        page: int = 0,
        notice: str | None = None,
    ) -> None:
        draft = await self._get_selection_draft(chat_id)
        if draft is None or draft.semester_id is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your study program to begin.",
            )
            return

        selected_title = draft.selected_title()
        if not selected_title:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program first.",
            )
            return

        variants = await asyncio.to_thread(
            self.api_client.get_department_program_variants_by_title,
            draft.semester_id,
            self.settings.rtu_department_code,
            selected_title,
        )
        if not variants:
            draft.clear_from_program()
            await self._save_selection_draft(chat_id, draft)
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="No RTU program codes were returned for that program. Please choose another program.",
            )
            return

        page_items, page, total_pages = self._paginate(variants, page, SMALL_PAGE_SIZE)
        code_labels = [self._format_program_code_option_label(item) for item in variants]
        LOGGER.info(
            "Showing program code prompt: chat_id=%s semester_id=%s department=%s title=%r page=%s/%s variants=%s",
            chat_id,
            draft.semester_id,
            self.settings.rtu_department_code,
            selected_title,
            page + 1,
            total_pages,
            [(program.code, program.program_id) for program in variants],
        )
        prompt = "Choose the RTU program code.\n\n" + selected_title + ":\n" + "\n".join(code_labels)
        text = self._build_setup_text(
            summary_lines=self._draft_summary_lines(draft),
            prompt=prompt,
            notice=notice,
            page=page,
            total_pages=total_pages,
        )
        markup = self._build_paginated_markup(
            items=page_items,
            label_builder=self._format_program_code_option_label,
            callback_builder=lambda item: self._callback("progcode", "select", item.program_id),
            footer_rows=[self._back_cancel_row()],
            page=page,
            total_pages=total_pages,
            page_callback_builder=lambda value: self._callback("progcode", "page", value),
            columns=3,
        )
        await self._upsert_selection_message(chat_id, text, markup, message)

    async def _show_course_prompt(
        self,
        chat_id: int,
        message: Message | None = None,
        page: int = 0,
        notice: str | None = None,
    ) -> None:
        draft = await self._get_selection_draft(chat_id)
        if draft is None or draft.semester_id is None:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program first.",
            )
            return

        if draft.program_id is None:
            if draft.selected_program_title:
                await self._show_program_code_prompt(
                    chat_id,
                    message=message,
                    notice="Choose the RTU program code first.",
                )
                return
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program first.",
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
                notice="No course options were returned for the selected study program.",
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
        draft = await self._get_selection_draft(chat_id)
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
            draft.program_family,
            include_family_variants=False,
        )
        self._log_group_options("Showing group prompt", chat_id, draft, groups)
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
            label_builder=self._format_group_option_label,
            callback_builder=lambda item: self._callback("group", "select", item.group_code),
            footer_rows=[self._back_cancel_row()],
            page=page,
            total_pages=total_pages,
            page_callback_builder=lambda value: self._callback("group", "page", value),
            columns=2,
        )
        await self._upsert_selection_message(chat_id, text, markup, message)

    async def _select_program_title(
        self,
        chat_id: int,
        title_index: int,
        message: Message | None,
    ) -> None:
        draft = await self._get_selection_draft(chat_id)
        if draft is None or draft.semester_id is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your study program to begin.",
            )
            return

        titles = await asyncio.to_thread(
            self.api_client.get_department_program_titles,
            draft.semester_id,
            self.settings.rtu_department_code,
        )
        if title_index < 0 or title_index >= len(titles):
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="That program is no longer available. Please choose another one.",
            )
            return

        selected_title = titles[title_index]
        draft.clear_from_program()
        draft.selected_program_title = selected_title
        await self._save_selection_draft(chat_id, draft)
        LOGGER.info(
            "Program title selected: chat_id=%s title=%s title_index=%s",
            chat_id,
            selected_title,
            title_index,
        )
        await self._show_program_code_prompt(
            chat_id,
            message=message,
            notice=f"Program selected: {selected_title}",
        )

    async def _select_program(
        self,
        chat_id: int,
        program_id: int,
        message: Message | None,
    ) -> None:
        draft = await self._get_selection_draft(chat_id)
        if draft is None or draft.semester_id is None:
            await self._start_selection_prompt(
                chat_id,
                message=message,
                intro_text="Choose your study program to begin.",
            )
            return

        selected_title = draft.selected_title()
        if selected_title:
            variants = await asyncio.to_thread(
                self.api_client.get_department_program_variants_by_title,
                draft.semester_id,
                self.settings.rtu_department_code,
                selected_title,
            )
            program = next((item for item in variants if item.program_id == program_id), None)
        else:
            program = await asyncio.to_thread(
                self.api_client.get_department_program,
                draft.semester_id,
                self.settings.rtu_department_code,
                program_id,
            )

        if program is None:
            if selected_title:
                await self._show_program_code_prompt(
                    chat_id,
                    message=message,
                    notice="That RTU program code is no longer available. Please choose another one.",
                )
            else:
                await self._show_program_prompt(
                    chat_id,
                    message=message,
                    notice="That study program is no longer available. Please choose another one.",
                )
            return

        selected_program_label = program.display_label()
        LOGGER.info(
            "Study program code selected: chat_id=%s title=%s code=%s program_id=%s",
            chat_id,
            program.title,
            program.code,
            program.program_id,
        )
        draft.selected_program_title = program.title
        draft.program_family = program.title
        draft.program_id = program.program_id
        draft.program_title = program.title
        draft.program_code = program.code
        draft.department_title = self.settings.rtu_department_title
        draft.course_id = None
        await self._save_selection_draft(chat_id, draft)

        courses = await asyncio.to_thread(
            self.api_client.get_courses,
            draft.semester_id,
            draft.program_id,
        )
        if not courses:
            await self._show_program_code_prompt(
                chat_id,
                message=message,
                notice="RTU returned no course options for this program code.",
            )
            return

        if len(courses) == 1:
            draft.course_id = courses[0]
            await self._save_selection_draft(chat_id, draft)
            groups = await asyncio.to_thread(
                self.api_client.get_display_groups,
                draft.semester_id,
                draft.program_id,
                draft.course_id,
                draft.program_family,
                include_family_variants=False,
            )
            self._log_group_options(
                "Loaded group options after single-course program selection",
                chat_id,
                draft,
                groups,
            )
            if not groups:
                await self._show_course_prompt(
                    chat_id,
                    message=message,
                    notice="RTU returned no valid groups for the selected course.",
                )
                return
            if len(groups) == 1:
                await self._complete_selection(
                    chat_id,
                    groups[0].group_code,
                    message=message,
                    notice=(
                        f"Study program selected: {selected_program_label}. "
                        f"Course {courses[0]} and {self._format_group_option_label(groups[0])} were selected automatically."
                    ),
                )
                return
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice=(
                    f"Study program selected: {selected_program_label}. "
                    f"Course {courses[0]} was selected automatically."
                ),
            )
            return

        await self._show_course_prompt(
            chat_id,
            message=message,
            notice=f"Study program selected: {selected_program_label}",
        )

    async def _select_course(
        self,
        chat_id: int,
        course_id: int,
        message: Message | None,
    ) -> None:
        draft = await self._get_selection_draft(chat_id)
        if draft is None or draft.semester_id is None:
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program first.",
            )
            return

        if draft.program_id is None:
            if draft.selected_program_title:
                await self._show_program_code_prompt(
                    chat_id,
                    message=message,
                    notice="Choose the RTU program code first.",
                )
                return
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice="Choose your program first.",
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
        await self._save_selection_draft(chat_id, draft)
        LOGGER.info(
            "Course selected: chat_id=%s program=%s program_id=%s course_id=%s",
            chat_id,
            self._format_program_name(draft.program_title, draft.program_code),
            draft.program_id,
            course_id,
        )
        groups = await asyncio.to_thread(
            self.api_client.get_display_groups,
            draft.semester_id,
            draft.program_id,
            draft.course_id,
            draft.program_family,
            include_family_variants=False,
        )
        self._log_group_options("Loaded group options after course selection", chat_id, draft, groups)
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
                groups[0].group_code,
                message=message,
                notice=(
                    f"Course {course_id} selected. "
                    f"{self._format_group_option_label(groups[0])} was selected automatically."
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
        group_code: str,
        message: Message | None,
    ) -> None:
        draft = await self._get_selection_draft(chat_id)
        normalized_group_code = normalize_group_code(group_code)
        LOGGER.info(
            "Group selected: chat_id=%s group_code=%s program=%s program_id=%s course_id=%s",
            chat_id,
            normalized_group_code,
            self._format_program_name(
                draft.program_title if draft is not None else None,
                draft.program_code if draft is not None else None,
            ),
            draft.program_id if draft is not None else None,
            draft.course_id if draft is not None else None,
        )
        await self._complete_selection(chat_id, normalized_group_code, message=message)

    async def _complete_selection(
        self,
        chat_id: int,
        group_code: str,
        message: Message | None,
        notice: str | None = None,
    ) -> None:
        draft = await self._get_selection_draft(chat_id)
        normalized_group_code = normalize_group_code(group_code)
        if (
            draft is None
            or draft.semester_id is None
            or draft.program_id is None
            or draft.course_id is None
        ):
            LOGGER.warning(
                "Selection completion fallback triggered because draft is incomplete: chat_id=%s group_code=%s draft=%s",
                chat_id,
                normalized_group_code or None,
                draft,
            )
            if draft is not None and draft.semester_id is not None and draft.program_id is None and draft.selected_program_title:
                await self._show_program_code_prompt(
                    chat_id,
                    message=message,
                    notice=(
                        "The final selection step could not be completed because the exact RTU code was missing. "
                        "Choose the RTU program code again."
                    ),
                )
                return
            await self._show_program_prompt(
                chat_id,
                message=message,
                notice=(
                    "The final selection step could not be completed because the in-progress setup was incomplete. "
                    "Choose your study program again."
                ),
            )
            return

        groups = await asyncio.to_thread(
            self.api_client.get_display_groups,
            draft.semester_id,
            draft.program_id,
            draft.course_id,
            draft.program_family,
            include_family_variants=False,
        )
        self._log_group_options("Validating selected group against exact group options", chat_id, draft, groups)
        group = next(
            (item for item in groups if item.normalized_group_code() == normalized_group_code),
            None,
        )
        if group is None:
            LOGGER.warning(
                "Selected group_code was not found in current group list: chat_id=%s program=%s program_id=%s course_id=%s group_code=%s groups=%s",
                chat_id,
                self._format_program_name(draft.program_title, draft.program_code),
                draft.program_id,
                draft.course_id,
                normalized_group_code or None,
                [(item.group_code, item.group_name, item.semester_program_id) for item in groups],
            )
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice=(
                    "That group is no longer available for the selected course. "
                    "Please choose another group."
                ),
            )
            return

        try:
            target = await asyncio.to_thread(
                self.api_client.resolve_group_by_code,
                semester_id=draft.semester_id,
                program_id=draft.program_id,
                course_id=draft.course_id,
                group_code=normalized_group_code,
                semester_program_id=group.semester_program_id,
                program_family=draft.program_family,
                allow_family_fallback=False,
            )
        except RTUPublicationError as exc:
            LOGGER.warning(
                "Selected group is unpublished: chat_id=%s program=%s course_id=%s group_code=%s semester_program_id=%s error=%s",
                chat_id,
                self._format_program_name(draft.program_title, draft.program_code),
                draft.course_id,
                normalized_group_code,
                group.semester_program_id,
                exc,
            )
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice=f"The selected group is not published right now: {exc}",
            )
            return
        except RTUAPIError as exc:
            LOGGER.exception(
                "Failed to resolve selected group: chat_id=%s program=%s course_id=%s group_code=%s semester_program_id=%s",
                chat_id,
                self._format_program_name(draft.program_title, draft.program_code),
                draft.course_id,
                normalized_group_code,
                group.semester_program_id,
            )
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice=f"I couldn't activate that group right now: {exc}",
            )
            return

        LOGGER.info(
            "Final selection resolved: chat_id=%s program=%s program_id=%s course_id=%s group_code=%s selected_group=%s semester_program_id=%s",
            chat_id,
            self._format_program_name(draft.program_title, draft.program_code),
            draft.program_id,
            draft.course_id,
            target.group_code,
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
            group_code=target.group_code,
            group_name=target.group_name,
            group_id=target.group_id,
        )
        try:
            await asyncio.to_thread(self.storage.save_chat_selection, selection)
        except Exception:
            LOGGER.exception(
                "Failed to save chat selection: chat_id=%s program=%s course_id=%s group_code=%s selected_group=%s semester_program_id=%s",
                chat_id,
                self._format_program_name(selection.program_title, selection.program_code),
                selection.course_id,
                selection.resolved_group_code() or None,
                selection.display_group(),
                selection.semester_program_id,
            )
            await self._show_group_prompt(
                chat_id,
                message=message,
                notice="I couldn't save the selected group right now. Please try again.",
            )
            return

        saved_selection = await asyncio.to_thread(self.storage.get_chat_selection, chat_id)
        LOGGER.info(
            "Chat selection saved successfully: chat_id=%s program=%s course_id=%s group_code=%s selected_group=%s semester_program_id=%s is_complete=%s",
            chat_id,
            self._format_program_name(selection.program_title, selection.program_code),
            selection.course_id,
            selection.resolved_group_code() or None,
            selection.display_group(),
            selection.semester_program_id,
            self._selection_has_resolvable_target(saved_selection) if saved_selection is not None else None,
        )
        await self._clear_selection_draft(chat_id)

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
            "The main menu is ready below. Use Change selection whenever you want to switch study program, course, or group.",
            reply_markup=self._main_menu(chat_id),
        )
        LOGGER.info("Main menu shown after successful selection completion: chat_id=%s", chat_id)

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
    def _callback(action: str, mode: str, value: int | str | None = None) -> str:
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
            lines.append(f"Program: {selection.program_family}")

        if selection.program_code:
            lines.append(f"Program code: {selection.program_code}")

        if selection.program_title:
            lines.append(
                f"Underlying RTU program: {self._format_program_name(selection.program_title, selection.program_code)}"
            )

        if selection.course_id is not None:
            lines.append(f"Course: {selection.course_id}")

        group_code = selection.resolved_group_code()
        group_name = selection.display_group()
        if group_code:
            lines.append(f"Group code: {group_code}")
        if group_name and normalize_group_code(group_name) != group_code:
            lines.append(f"Group: {group_name}")
        elif not group_code and group_name:
            lines.append(f"Group: {group_name}")

        if include_resolved and selection.semester_program_id is not None:
            lines.append(f"semesterProgramId: {selection.semester_program_id}")

        return lines

    def _draft_summary_lines(self, draft: SelectionDraft) -> list[str]:
        lines = [
            f"Study period: {draft.semester_title or self.settings.rtu_semester_title}",
            f"Department: {draft.department_title or self.settings.rtu_department_title}",
        ]
        selected_title = draft.selected_title()
        if selected_title:
            lines.append(f"Program: {selected_title}")
        if draft.program_code:
            lines.append(f"Program code: {draft.program_code}")
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

    def _format_program_option_label(self, program: StudyProgram) -> str:
        return program.display_label()

    @staticmethod
    def _format_program_code_option_label(program: StudyProgram) -> str:
        code = str(program.code).strip() if program.code else ""
        if code:
            return code
        LOGGER.warning(
            "RTU program variant is missing a code: title=%s program_id=%s",
            program.title,
            program.program_id,
        )
        return "No code"

    @staticmethod
    def _format_department_label(department: StudyDepartment) -> str:
        if department.code:
            return f"{department.title} ({department.code})"
        return department.title

    @staticmethod
    def _truncate_label(text: str, max_length: int = 48) -> str:
        if len(text) <= max_length:
            return text
        suffix_start = text.rfind(" (")
        if suffix_start > 0 and text.endswith(")"):
            suffix = text[suffix_start:]
            if len(suffix) < max_length - 6:
                prefix_length = max_length - len(suffix) - 3
                return f"{text[:prefix_length].rstrip()}...{suffix}"
        return f"{text[: max_length - 3].rstrip()}..."

    @staticmethod
    def _format_group_name(group: str) -> str:
        cleaned = clean_group_label(group)
        if not cleaned:
            return "Unknown group"
        return cleaned

    @staticmethod
    def _format_group_label(group_code: str | None, group_name: str | None = None) -> str:
        normalized_group_code = normalize_group_code(group_code)
        cleaned_group_name = clean_group_label(group_name)
        if not normalized_group_code:
            return clean_group_label(group_name) or "Unknown group"
        if cleaned_group_name and normalize_group_code(cleaned_group_name) != normalized_group_code:
            return f"{normalized_group_code} — {cleaned_group_name}"
        return normalized_group_code

    @staticmethod
    def _format_group_option_text(label: str) -> str:
        cleaned_label = clean_group_label(label)
        if not cleaned_label:
            return "Unknown group"
        if cleaned_label.casefold().startswith("group "):
            return cleaned_label
        return f"Group {cleaned_label}"

    def _format_group_option_label(self, target: ResolvedSemesterProgram) -> str:
        return self._format_group_option_text(
            self._format_group_label(target.group_code, target.group_name)
        )

    def _log_group_options(
        self,
        message: str,
        chat_id: int,
        draft: SelectionDraft,
        groups: list[ResolvedSemesterProgram],
    ) -> None:
        LOGGER.info(
            "%s: chat_id=%s program_id=%s program_code=%s course_id=%s groups=%s",
            message,
            chat_id,
            draft.program_id,
            draft.program_code,
            draft.course_id,
            [
                (
                    group.program_id,
                    group.program_code or draft.program_code,
                    group.course_id,
                    group.group_code,
                    group.group_name,
                    group.semester_program_id,
                )
                for group in groups
            ],
        )

    def _selection_context_line(
        self,
        selection: ChatSelection | None,
        target: ResolvedSemesterProgram,
    ) -> str | None:
        program_family = None
        program_title = target.program_title
        program_code = target.program_code
        course_id = target.course_id
        group_code = target.group_code
        group_name = target.group_name

        if selection is not None:
            program_family = selection.program_family
            program_title = selection.program_title or program_title
            program_code = selection.program_code or program_code
            course_id = selection.course_id or course_id
            group_code = selection.resolved_group_code() or group_code
            group_name = selection.display_group() or group_name

        parts: list[str] = []
        if program_title and program_code:
            parts.append(self._format_program_name(program_title, program_code))
        elif program_family:
            parts.append(program_family)
        elif program_title:
            parts.append(self._format_program_name(program_title, program_code))
        if course_id is not None:
            parts.append(f"Course {course_id}")
        if group_code or group_name:
            parts.append(self._format_group_label(group_code, group_name))

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
