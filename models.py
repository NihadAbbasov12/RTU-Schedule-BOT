"""Domain models and date range helpers."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo


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

    def is_complete(self) -> bool:
        """Return whether the selection has enough information to resolve a target."""
        return (
            self.semester_id is not None
            and self.program_id is not None
            and self.course_id is not None
            and bool(self.selected_group.strip())
        )

    def selection_key(self) -> tuple[int | None, int | None, int | None, str]:
        """Return a stable key for caching or de-duplicating a chat selection."""
        return (
            self.semester_id,
            self.program_id,
            self.course_id,
            self.selected_group,
        )


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


@dataclass(slots=True, frozen=True)
class StudyProgramFamily:
    """A deduplicated program family shown to Telegram users."""

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
    group: str
    program_code: str | None = None
    program_title: str | None = None
    published: bool | None = None


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
