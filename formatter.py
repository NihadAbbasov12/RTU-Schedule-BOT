"""Telegram-friendly formatting helpers."""

from __future__ import annotations

from datetime import date

from models import ScheduleDiff, ScheduleEvent, Subject, group_events_by_day

_MARKDOWN_V2_SPECIALS = r"_*[]()~`>#+-=|{}.!"


def escape_telegram_markdown(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    return "".join(f"\\{char}" if char in _MARKDOWN_V2_SPECIALS else char for char in text)


def format_daily_schedule(label: str, target_date: date, events: list[ScheduleEvent]) -> str:
    """Format a single-day schedule message."""
    lines = [label, _format_date_header(target_date)]
    if not events:
        lines.extend(["", "No lessons scheduled."])
        return "\n".join(lines)

    for event in events:
        lines.extend(
            [
                "",
                f"{_format_time_range(event)} | {event.title}",
                f"Lecturer: {event.lecturer or 'TBA'}",
                f"Room: {event.room or 'TBA'}",
            ]
        )
    return "\n".join(lines)


def format_range_schedule(
    label: str,
    start_date: date,
    end_date: date,
    events: list[ScheduleEvent],
) -> str:
    """Format a multi-day schedule message."""
    lines = [label, f"{start_date.isoformat()} to {end_date.isoformat()}"]
    if not events:
        lines.extend(["", "No lessons scheduled."])
        return "\n".join(lines)

    for current_date, day_events in group_events_by_day(events).items():
        lines.extend(["", _format_date_header(current_date)])
        for event in day_events:
            lines.extend(
                [
                    f"{_format_time_range(event)} | {event.title}",
                    f"Lecturer: {event.lecturer or 'TBA'}",
                    f"Room: {event.room or 'TBA'}",
                    "",
                ]
            )
        if lines[-1] == "":
            lines.pop()
    return "\n".join(lines)


def format_subjects(subjects: list[Subject], heading: str = "Subjects") -> str:
    """Format the subjects list."""
    lines = [heading]
    if not subjects:
        lines.extend(["", "No subjects found."])
        return "\n".join(lines)

    for subject in subjects:
        suffix = f" (part {subject.part})" if subject.part is not None else ""
        prefix = f"{subject.code} | " if subject.code else ""
        lines.append(f"{prefix}{subject.title}{suffix}")
    return "\n".join(lines)


def format_status(
    semester_id: int,
    program_id: int,
    course_id: int,
    group: str | None,
    semester_program_id: int | None,
    scheduler_enabled: bool,
    timezone: str,
    program_title: str | None = None,
) -> str:
    """Format the bot status message."""
    selected_group = f"Group {group}" if group else "Not selected"
    resolved_value = str(semester_program_id) if semester_program_id is not None else "Not resolved"
    lines = ["Status"]
    if program_title:
        lines.append(f"program: {program_title}")
    lines.extend(
        [
            f"semesterId: {semester_id}",
            f"programId: {program_id}",
            f"courseId: {course_id}",
            f"selected group: {selected_group}",
            f"resolved semesterProgramId: {resolved_value}",
            f"scheduler: {'enabled' if scheduler_enabled else 'disabled'}",
            f"timezone: {timezone}",
        ]
    )
    return "\n".join(lines)


def format_changes(changes: list[ScheduleDiff]) -> str:
    """Format a concise change detection summary."""
    if not changes:
        return "Refresh complete.\nNo schedule changes detected."

    lines = ["Refresh complete.", "", "Changes detected:"]
    for change in sorted(changes, key=lambda item: (item.event_date, item.change_type, item.title)):
        lines.extend(
            [
                "",
                f"{change.event_date.isoformat()} | {change.change_type} | {change.title}",
                change.description,
            ]
        )
    return "\n".join(lines)


def split_message(text: str, max_length: int = 3900) -> list[str]:
    """Split long Telegram messages into safe chunks."""
    if len(text) <= max_length:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_length:
        split_at = remaining.rfind("\n\n", 0, max_length)
        if split_at == -1:
            split_at = remaining.rfind("\n", 0, max_length)
        if split_at == -1:
            split_at = max_length

        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].strip()

    if remaining:
        chunks.append(remaining)
    return chunks


def _format_time_range(event: ScheduleEvent) -> str:
    start = event.start_time.strftime("%H:%M") if event.start_time else "TBA"
    end = event.end_time.strftime("%H:%M") if event.end_time else "TBA"
    return f"{start}-{end}"


def _format_date_header(value: date) -> str:
    return value.strftime("%a, %Y-%m-%d")
