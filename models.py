"""Domain models and date range helpers."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
import re
from typing import Any
from zoneinfo import ZoneInfo

_GROUP_CODE_WHITESPACE_PATTERN = re.compile(r"\s+")


def clean_group_label(value: str | None) -> str | None:
    """Return a human-friendly group label."""
    if value is None:
        return None
    cleaned = _GROUP_CODE_WHITESPACE_PATTERN.sub(" ", str(value).strip())
    return cleaned or None


def normalize_group_code(value: str | None) -> str:
    """Return a normalized RTU group code for matching and storage."""
    if value is None:
        return ""
    cleaned = _GROUP_CODE_WHITESPACE_PATTERN.sub("", str(value).strip())
    return cleaned.upper()


def infer_group_code(value: str | None) -> str | None:
    """Infer a stable RTU group code from a legacy stored group value."""
    cleaned = clean_group_label(value)
    if not cleaned:
        return None

    lowered = cleaned.casefold()
    if lowered.startswith("group "):
        cleaned = cleaned[6:].strip()

    for delimiter in (" — ", " - "):
        if delimiter in cleaned:
            candidate = cleaned.split(delimiter, 1)[0].strip()
            if candidate:
                cleaned = candidate
                break

    normalized = normalize_group_code(cleaned)
    return normalized or None


@dataclass(slots=True, frozen=True)
class ChatSelection:
    """A per-chat RTU study selection."""

    chat_id: int
    semester_id: int | None
    semester_title: str | None
    program_family: str | None
    program_id: int | None
    program_title: str | None
    program_code: str | None
    course_id: int | None
    selected_group: str
    semester_program_id: int | None
    department_title: str | None = None
    group_code: str | None = None
    group_name: str | None = None
    group_id: int | None = None

    def resolved_group_code(self) -> str:
        """Return the normalized group code, inferring it from legacy values when possible."""
        return (
            normalize_group_code(self.group_code)
            or infer_group_code(self.selected_group)
            or infer_group_code(self.group_name)
            or ""
        )

    def display_group(self) -> str | None:
        """Return the best human-friendly group label."""
        return (
            clean_group_label(self.group_name)
            or clean_group_label(self.selected_group)
            or clean_group_label(self.group_code)
        )

    def is_complete(self) -> bool:
        """Return whether the selection has enough information to resolve a target."""
        return (
            self.semester_id is not None
            and self.program_id is not None
            and self.course_id is not None
            and bool(self.resolved_group_code())
        )

    def selection_key(self) -> tuple[int | None, int | None, int | None, str, int | None]:
        """Return a stable key for caching or de-duplicating a chat selection."""
        group_key = self.resolved_group_code()
        if not group_key and self.semester_program_id is not None:
            group_key = f"semester-program:{self.semester_program_id}"
        return (
            self.semester_id,
            self.program_id,
            self.course_id,
            group_key,
            self.semester_program_id,
        )


@dataclass(slots=True)
class SelectionDraft:
    """An in-progress per-chat selection draft."""

    semester_id: int | None = None
    semester_title: str | None = None
    department_id: int | None = None
    department_title: str | None = None
    selected_program_title: str | None = None
    program_family: str | None = None
    program_id: int | None = None
    program_title: str | None = None
    program_code: str | None = None
    course_id: int | None = None

    def clear_exact_program(self) -> None:
        """Reset the exact program-code-dependent portion of the draft."""
        self.program_family = None
        self.program_id = None
        self.program_title = None
        self.program_code = None
        self.course_id = None

    def clear_from_program(self) -> None:
        """Reset the program-dependent portion of the draft."""
        self.selected_program_title = None
        self.clear_exact_program()

    def clear_from_course(self) -> None:
        """Reset the course-dependent portion of the draft."""
        self.course_id = None

    def selected_title(self) -> str | None:
        """Return the currently selected display title, if any."""
        return self.selected_program_title or self.program_family or self.program_title


@dataclass(slots=True, frozen=True)
class StudyPeriod:
    """A study period shown on the RTU schedule website."""

    semester_id: int
    title: str
    short_name: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    active: bool = False


@dataclass(slots=True, frozen=True)
class StudyDepartment:
    """A department grouping study programs on the RTU website."""

    department_id: int
    title: str
    code: str | None = None


@dataclass(slots=True, frozen=True)
class StudyProgram:
    """A study program that can be selected for a study period."""

    program_id: int
    title: str
    code: str | None = None
    department_id: int | None = None
    department_title: str | None = None
    department_code: str | None = None

    def display_label(self) -> str:
        """Return a Telegram-friendly label for the study program."""
        title = str(self.title).strip()
        code = str(self.code).strip() if self.code else None
        if title and code:
            return f"{title} ({code})"
        if title:
            return title
        if code:
            return code
        return "Unknown program"


@dataclass(slots=True, frozen=True)
class StudyProgramFamily:
    """A legacy deduplicated program family used for saved-selection fallback."""

    family_key: str
    display_name: str
    representative_program: StudyProgram
    variants: tuple[StudyProgram, ...]


@dataclass(slots=True, frozen=True)
class Subject:
    """A study subject returned by the public RTU API."""

    subject_id: int
    code: str
    title: str
    part: int | None = None


@dataclass(slots=True, frozen=True)
class ResolvedSemesterProgram:
    """A resolved schedule target."""

    semester_program_id: int
    semester_id: int
    program_id: int
    course_id: int
    group_code: str
    group_name: str | None = None
    group_id: int | None = None
    program_code: str | None = None
    program_title: str | None = None
    published: bool | None = None

    @property
    def group(self) -> str:
        """Backward-compatible display label for the resolved group."""
        return self.display_group()

    def normalized_group_code(self) -> str:
        """Return the normalized group code."""
        return normalize_group_code(self.group_code)

    def display_group(self) -> str:
        """Return the best human-friendly label for the resolved group."""
        return clean_group_label(self.group_name) or clean_group_label(self.group_code) or "Unknown group"


@dataclass(slots=True, frozen=True)
class ScheduleEvent:
    """A normalized lesson event."""

    event_date_id: int | None
    event_id: int | None
    status_id: int | None
    title: str
    room: str
    lecturer: str
    program: str
    event_date: date
    start_time: time | None
    end_time: time | None
    room_code: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    def sort_key(self) -> tuple[date, time, time, str]:
        """Return a stable sort key for the event."""
        return (
            self.event_date,
            self.start_time or time.min,
            self.end_time or time.min,
            self.title,
        )

    def stable_id(self) -> str:
        """Return a stable identifier used for snapshot comparison."""
        if self.event_date_id is not None:
            return f"event-date:{self.event_date_id}"
        if self.event_id is not None:
            return f"event:{self.event_id}:{self.event_date.isoformat()}"
        return (
            "fallback:"
            f"{self.event_date.isoformat()}|{self.title}|{self.lecturer}|{self.room}|{self.program}"
        )

    def snapshot_payload(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of the event."""
        return {
            "event_date_id": self.event_date_id,
            "event_id": self.event_id,
            "status_id": self.status_id,
            "title": self.title,
            "room": self.room,
            "lecturer": self.lecturer,
            "program": self.program,
            "event_date": self.event_date.isoformat(),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "room_code": self.room_code,
        }

    @classmethod
    def from_snapshot_payload(cls, payload: dict[str, Any]) -> "ScheduleEvent":
        """Rebuild a ScheduleEvent from snapshot JSON data."""
        return cls(
            event_date_id=payload.get("event_date_id"),
            event_id=payload.get("event_id"),
            status_id=payload.get("status_id"),
            title=payload.get("title", ""),
            room=payload.get("room", ""),
            lecturer=payload.get("lecturer", ""),
            program=payload.get("program", ""),
            event_date=date.fromisoformat(payload["event_date"]),
            start_time=time.fromisoformat(payload["start_time"]) if payload.get("start_time") else None,
            end_time=time.fromisoformat(payload["end_time"]) if payload.get("end_time") else None,
            room_code=payload.get("room_code"),
            raw={},
        )


@dataclass(slots=True, frozen=True)
class ScheduleDiff:
    """A detected schedule change."""

    change_type: str
    event_date: date
    title: str
    description: str


@dataclass(slots=True, frozen=True)
class BotUsageStats:
    """Aggregated bot usage statistics."""

    total_chats_ever: int
    chats_with_saved_selection: int
    active_chats_last_7_days: int
    active_chats_last_30_days: int
    total_reminders_sent: int
    total_schedule_requests: int


def group_events_by_day(events: list[ScheduleEvent]) -> dict[date, list[ScheduleEvent]]:
    """Group events by calendar date."""
    grouped: dict[date, list[ScheduleEvent]] = {}
    for event in sorted(events, key=lambda item: item.sort_key()):
        grouped.setdefault(event.event_date, []).append(event)
    return grouped


def get_now(tz: ZoneInfo) -> datetime:
    """Return the current time in the given timezone."""
    return datetime.now(tz)


def get_today_range(tz: ZoneInfo, now: datetime | None = None) -> tuple[date, date]:
    """Return today's date range."""
    current = now.astimezone(tz) if now else get_now(tz)
    current_date = current.date()
    return current_date, current_date


def get_tomorrow_range(tz: ZoneInfo, now: datetime | None = None) -> tuple[date, date]:
    """Return tomorrow's date range."""
    current = now.astimezone(tz) if now else get_now(tz)
    tomorrow = current.date() + timedelta(days=1)
    return tomorrow, tomorrow


def get_week_range(tz: ZoneInfo, now: datetime | None = None) -> tuple[date, date]:
    """Return a seven-day range starting today."""
    current = now.astimezone(tz) if now else get_now(tz)
    start = current.date()
    end = start + timedelta(days=6)
    return start, end


def get_academic_week_range(tz: ZoneInfo, now: datetime | None = None) -> tuple[date, date]:
    """Return the Monday-Sunday range for the current academic week."""
    current = now.astimezone(tz) if now else get_now(tz)
    start = current.date() - timedelta(days=current.weekday())
    end = start + timedelta(days=6)
    return start, end


def get_week_key(target_date: date) -> str:
    """Return an ISO week key for storage and de-duplication."""
    iso_year, iso_week, _ = target_date.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def combine_local_datetime(
    value_date: date,
    value_time: time | None,
    tz: ZoneInfo,
) -> datetime | None:
    """Combine a local date and time into a timezone-aware datetime."""
    if value_time is None:
        return None
    return datetime.combine(value_date, value_time, tzinfo=tz)


def get_month_range(tz: ZoneInfo, now: datetime | None = None) -> tuple[date, date]:
    """Return the current calendar month range."""
    current = now.astimezone(tz) if now else get_now(tz)
    year = current.year
    month = current.month
    _, day_count = monthrange(year, month)
    return date(year, month, 1), date(year, month, day_count)


def iter_month_dates(year: int, month: int) -> list[date]:
    """Return every date in the given calendar month."""
    _, day_count = monthrange(year, month)
    return [date(year, month, day) for day in range(1, day_count + 1)]
