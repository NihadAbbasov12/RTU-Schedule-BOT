"""Telegram-friendly formatting helpers."""

from __future__ import annotations

from datetime import date

from models import BotUsageStats, ScheduleDiff, ScheduleEvent, Subject, group_events_by_day

_MARKDOWN_V2_SPECIALS = r"_*[]()~`>#+-=|{}.!"


def escape_telegram_markdown(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    return "".join(f"\\{char}" if char in _MARKDOWN_V2_SPECIALS else char for char in text)


def format_daily_schedule(
    label: str,
    target_date: date,
    events: list[ScheduleEvent],
    context_line: str | None = None,
) -> str:
    """Format a single-day schedule message."""
    lines = [label]
    if context_line:
        lines.append(context_line)
    lines.append(_format_date_header(target_date))
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
    context_line: str | None = None,
) -> str:
    """Format a multi-day schedule message."""
    lines = [label]
    if context_line:
        lines.append(context_line)
    lines.append(f"{start_date.isoformat()} to {end_date.isoformat()}")
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
    semester_id: int | None,
    semester_title: str | None,
    department_title: str | None,
    program_family: str | None,
    program_id: int | None,
    program_title: str | None,
    program_code: str | None,
    course_id: int | None,
    group_code: str | None,
    group_name: str | None,
    semester_program_id: int | None,
    scheduler_enabled: bool,
    timezone: str,
) -> str:
    """Format the bot status message."""
    selected_group = _format_group_label(group_code, group_name)
    selected_period = semester_title or "Not selected"
    selected_department = department_title or "Not selected"
    selected_family = program_family or program_title or "Not selected"
    selected_program_code = program_code or "Not selected"
    resolved_program = (
        _format_program_label(program_title, program_code)
        if program_title
        else "Not selected"
    )
    resolved_value = str(semester_program_id) if semester_program_id is not None else "Not resolved"
    lines = ["Status"]
    lines.extend(
        [
            f"study period: {selected_period}",
            f"semesterId: {semester_id if semester_id is not None else 'Not selected'}",
            f"department: {selected_department}",
            f"program: {selected_family}",
            f"program code: {selected_program_code}",
            f"underlying RTU program: {resolved_program}",
            f"course: {course_id if course_id is not None else 'Not selected'}",
            f"group: {selected_group}",
            f"groupCode: {group_code if group_code else 'Not selected'}",
            f"semesterProgramId: {resolved_value}",
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


def format_reminder(event: ScheduleEvent, minutes_before: int) -> str:
    """Format a lesson reminder."""
    lines = [
        "Reminder",
        "",
        f"In {minutes_before} minutes you have:",
        event.title,
        _format_time_range(event),
        f"Lecturer: {event.lecturer or 'TBA'}",
        f"Room: {event.room or 'TBA'}",
    ]
    return "\n".join(lines)


def format_admin_stats(
    stats: BotUsageStats,
    scheduler_enabled: bool,
    reminder_enabled: bool,
    timezone: str,
) -> str:
    """Format admin-only usage statistics."""
    lines = [
        "Bot Stats",
        f"total chats ever: {stats.total_chats_ever}",
        f"chats with saved selection: {stats.chats_with_saved_selection}",
        f"active chats last 7 days: {stats.active_chats_last_7_days}",
        f"active chats last 30 days: {stats.active_chats_last_30_days}",
        f"total reminders sent: {stats.total_reminders_sent}",
        f"total schedule requests: {stats.total_schedule_requests}",
        f"scheduler: {'enabled' if scheduler_enabled else 'disabled'}",
        f"reminders: {'enabled' if reminder_enabled else 'disabled'}",
        f"timezone: {timezone}",
    ]
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


def _format_program_label(program_title: str | None, program_code: str | None) -> str:
    if not program_title:
        return "Not selected"
    if program_code:
        return f"{program_title} ({program_code})"
    return program_title


def _format_group_label(group_code: str | None, group_name: str | None) -> str:
    if not group_code:
        return "Not selected"
    if group_name and group_name.strip().casefold() != group_code.strip().casefold():
        return f"{group_code} — {group_name}"
    return group_code
