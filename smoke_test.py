"""End-to-end smoke test for Full Time and Erasmus selection + schedule flows.

Runs without Telegram: mocks the RTU API and the aiogram Bot's network calls.
Exercises both flows against a temp SQLite DB and asserts on saved state and
the messages the bot would have sent.

Usage:
    python smoke_test.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import tempfile
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

# Suppress noisy bot logging during the smoke test
logging.basicConfig(level=logging.WARNING)

from config import Settings
from models import (
    ResolvedSemesterProgram,
    ScheduleEvent,
    StudyDepartment,
    StudyPeriod,
    StudyProgram,
    Subject,
)
from storage import SnapshotStorage
from bot import ScheduleBotApp


SEMESTER_ID = 29
PROGRAM_ID = 1128
PROGRAM_TITLE = "Computer Systems"
PROGRAM_CODE = "RDBC0"
MASTER_PROGRAM_ID = 1174
MASTER_PROGRAM_CODE = "ADMD0"


def _make_event(
    title: str,
    event_date: date,
    start: str,
    end: str,
    event_type: str = "Lect.",
    lecturer: str = "P.Test",
) -> ScheduleEvent:
    """Build an event title mirroring real RTU shape: "{type} {subject}, {lecturer}"."""
    full_title = f"{event_type} {title}, {lecturer}" if event_type else title
    return ScheduleEvent(
        event_date_id=None,
        event_id=None,
        status_id=None,
        title=full_title,
        room="Auditorium",
        lecturer=lecturer,
        program=PROGRAM_TITLE,
        event_date=event_date,
        start_time=time.fromisoformat(start),
        end_time=time.fromisoformat(end),
    )


class FakeRTUClient:
    """Minimal RTU API stub for smoke testing."""

    def __init__(self, today: date) -> None:
        self.today = today
        self.program = StudyProgram(
            program_id=PROGRAM_ID,
            title=PROGRAM_TITLE,
            code=PROGRAM_CODE,
            department_id=2,
            department_title="Foreign Students Department",
            department_code="02A00",
        )
        self.master_program = StudyProgram(
            program_id=MASTER_PROGRAM_ID,
            title=PROGRAM_TITLE,
            code=MASTER_PROGRAM_CODE,
            department_id=2,
            department_title="Foreign Students Department",
            department_code="02A00",
        )
        # Full Time keeps the simple Bachelor-only mapping
        self.course_to_spid = {1: 5001, 2: 5002, 3: 5003}
        # Per program-variant: course -> semester_program_id
        self.courses_by_program: dict[int, list[int]] = {
            PROGRAM_ID: [1, 2, 3],
            MASTER_PROGRAM_ID: [1],
        }
        self.spid_by_program_course: dict[tuple[int, int], int] = {
            (PROGRAM_ID, 1): 5001,
            (PROGRAM_ID, 2): 5002,
            (PROGRAM_ID, 3): 5003,
            (MASTER_PROGRAM_ID, 1): 6001,
        }
        self.subjects_by_spid: dict[int, list[Subject]] = {
            5001: [
                Subject(subject_id=101, code="DOP101", title="Programming Basics"),
                Subject(subject_id=102, code="DOP102", title="Math 1"),
            ],
            5002: [
                Subject(subject_id=201, code="DOP201", title="Algorithms"),
                Subject(subject_id=202, code="DOP202", title="Databases"),
            ],
            5003: [
                Subject(subject_id=301, code="DOP301", title="Networks"),
                Subject(subject_id=302, code="DOP302", title="Operating Systems"),
            ],
            6001: [
                Subject(subject_id=401, code="DM401", title="Machine Learning"),
                Subject(subject_id=402, code="DM402", title="Advanced Algorithms"),
            ],
        }
        self.events_by_spid: dict[int, list[ScheduleEvent]] = {
            5001: [
                _make_event("Programming Basics", today, "08:30", "10:00"),
                _make_event("Math 1", today, "10:30", "12:00"),
            ],
            5002: [
                _make_event("Algorithms", today, "13:00", "14:30"),
                _make_event("Databases", today, "15:00", "16:30"),
            ],
            5003: [
                _make_event("Networks", today, "08:30", "10:00"),
                _make_event("Operating Systems", today, "10:30", "12:00"),
            ],
            6001: [
                _make_event("Machine Learning", today, "13:00", "14:30"),
                _make_event("Advanced Algorithms", today, "15:00", "16:30"),
            ],
        }

    def get_locked_study_period(self) -> StudyPeriod:
        return StudyPeriod(
            semester_id=SEMESTER_ID,
            title="2025/2026 Spring semester (25/26-SP)",
            short_name="25/26-SP",
            active=True,
        )

    def get_locked_department(self, semester_id: int) -> StudyDepartment:
        return StudyDepartment(department_id=2, title="Foreign Students Department", code="02A00")

    def get_department_program_titles(self, semester_id: int, department_code: str) -> list[str]:
        return [self.program.title]

    def get_department_program_variants_by_title(
        self, semester_id: int, department_code: str, title: str
    ) -> list[StudyProgram]:
        if title == PROGRAM_TITLE:
            return [self.program, self.master_program]
        return []

    def _lookup_program(self, program_id: int) -> StudyProgram | None:
        if program_id == self.program.program_id:
            return self.program
        if program_id == self.master_program.program_id:
            return self.master_program
        return None

    def get_department_program(
        self, semester_id: int, department_code: str, program_id: int
    ) -> StudyProgram | None:
        return self._lookup_program(program_id)

    def get_program_family_by_representative_id(
        self, semester_id: int, department_code: str, program_id: int
    ):
        return None

    def get_study_program(self, semester_id: int, program_id: int) -> StudyProgram | None:
        return self._lookup_program(program_id)

    def get_courses(self, semester_id: int, program_id: int) -> list[int]:
        return list(self.courses_by_program.get(program_id, []))

    def get_display_groups(
        self,
        semester_id: int,
        program_id: int,
        course_id: int,
        program_family,
        include_family_variants: bool = False,
    ) -> list[ResolvedSemesterProgram]:
        spid = self.spid_by_program_course.get((program_id, course_id))
        if spid is None:
            return []
        program = self._lookup_program(program_id) or self.program
        group_code = f"{program.code}-{course_id}A"
        return [
            ResolvedSemesterProgram(
                semester_program_id=spid,
                semester_id=semester_id,
                program_id=program_id,
                course_id=course_id,
                group_code=group_code,
                group_name=f"Group {group_code}",
                group_id=spid,
                program_code=program.code,
                program_title=program.title,
                published=True,
            )
        ]

    def get_subjects(self, semester_program_id: int) -> list[Subject]:
        return list(self.subjects_by_spid.get(semester_program_id, []))

    def get_events_for_range(
        self, semester_program_id: int, start_date: date, end_date: date
    ) -> list[ScheduleEvent]:
        events = self.events_by_spid.get(semester_program_id, [])
        return [e for e in events if start_date <= e.event_date <= end_date]

    def resolve_chat_selection(self, selection):
        spid = selection.semester_program_id or self.course_to_spid[selection.course_id or 1]
        course = selection.course_id or 1
        group_code = selection.group_code or f"{PROGRAM_CODE}-{course}A"
        return ResolvedSemesterProgram(
            semester_program_id=spid,
            semester_id=selection.semester_id or SEMESTER_ID,
            program_id=selection.program_id or PROGRAM_ID,
            course_id=course,
            group_code=group_code,
            group_name=f"Group {group_code}",
            group_id=spid,
            program_code=PROGRAM_CODE,
            program_title=PROGRAM_TITLE,
            published=True,
        )

    def resolve_group_by_code(
        self,
        *,
        semester_id: int,
        program_id: int,
        course_id: int,
        group_code: str,
        semester_program_id: int | None = None,
        program_family=None,
        allow_family_fallback: bool = False,
    ) -> ResolvedSemesterProgram:
        spid = semester_program_id or self.course_to_spid.get(course_id, 5001)
        return ResolvedSemesterProgram(
            semester_program_id=spid,
            semester_id=semester_id,
            program_id=program_id,
            course_id=course_id,
            group_code=group_code,
            group_name=f"Group {group_code}",
            group_id=spid,
            program_code=PROGRAM_CODE,
            program_title=PROGRAM_TITLE,
            published=True,
        )


class Reporter:
    """Track pass/fail asserts for the smoke run."""

    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0

    def check(self, condition: bool, label: str) -> None:
        if condition:
            self.passed += 1
            print(f"  PASS: {label}")
        else:
            self.failed += 1
            print(f"  FAIL: {label}")

    def summary(self) -> int:
        total = self.passed + self.failed
        print()
        print(f"Total: {self.passed}/{total} passed, {self.failed} failed")
        return 0 if self.failed == 0 else 1


def _collected_messages(app: ScheduleBotApp) -> list[str]:
    """Extract text payloads sent through the mocked bot.send_message."""
    calls = app.bot.send_message.await_args_list  # type: ignore[attr-defined]
    payloads: list[str] = []
    for call in calls:
        # bot.send_message(chat_id, text, reply_markup=...)
        if len(call.args) >= 2:
            payloads.append(str(call.args[1]))
        elif "text" in call.kwargs:
            payloads.append(str(call.kwargs["text"]))
    return payloads


async def run_full_time(app: ScheduleBotApp, reporter: Reporter, chat_id: int) -> None:
    print(f"--- Full Time scenario (chat_id={chat_id}) ---")

    await app._show_start(chat_id)
    await app._select_study_mode(chat_id, "fulltime", message=None)
    await app._select_program_title(chat_id, 0, message=None)
    await app._select_program(chat_id, PROGRAM_ID, message=None)
    # Only one group per course in the fake, so _select_course auto-completes selection.
    await app._select_course(chat_id, 1, message=None)

    selection = await asyncio.to_thread(app.storage.get_chat_selection, chat_id)
    reporter.check(selection is not None, "Full Time selection saved")
    reporter.check(selection is not None and not selection.is_erasmus(), "study_mode = FULL_TIME")
    reporter.check(selection is not None and selection.course_id == 1, "course_id = 1")
    reporter.check(selection is not None and selection.semester_program_id == 5001, "semester_program_id = 5001")

    erasmus_rows = await asyncio.to_thread(app.storage.get_erasmus_subjects, chat_id)
    reporter.check(erasmus_rows == [], "No Erasmus subjects for Full Time chat")

    app.bot.send_message.reset_mock()  # type: ignore[attr-defined]
    await app._show_today(chat_id)
    messages = _collected_messages(app)
    schedule_text = "\n".join(messages)
    reporter.check("Programming Basics" in schedule_text, "Full Time Today contains Programming Basics")
    reporter.check("Math 1" in schedule_text, "Full Time Today contains Math 1")
    reporter.check("Algorithms" not in schedule_text, "Full Time Today does NOT leak course 2 events")


async def run_erasmus(app: ScheduleBotApp, reporter: Reporter, chat_id: int) -> None:
    print(f"--- Erasmus scenario (chat_id={chat_id}) ---")

    await app._show_start(chat_id)
    await app._select_study_mode(chat_id, "erasmus", message=None)
    # Title selection alone -> goes straight to subjects (no program-code screen)
    await app._select_program_title(chat_id, 0, message=None)

    draft = await app._get_selection_draft(chat_id)
    reporter.check(draft is not None and draft.is_erasmus(), "Draft is in Erasmus mode")
    reporter.check(
        draft is not None and draft.program_code is None,
        "Erasmus draft has no program_code (program-code screen skipped)",
    )
    reporter.check(
        draft is not None and draft.selected_program_title == PROGRAM_TITLE,
        f"Erasmus draft selected_program_title == {PROGRAM_TITLE!r}",
    )

    # Aggregation must span every program variant (Bachelor 6 + Master 2 = 8)
    options = await app._load_erasmus_subject_options(
        SEMESTER_ID, [PROGRAM_ID, MASTER_PROGRAM_ID], None
    )
    reporter.check(
        len(options) == 8,
        f"Erasmus aggregation surfaces 8 subjects across both variants (got {len(options)})",
    )
    titles = {opt.subject_title for opt in options}
    reporter.check(
        "Machine Learning" in titles and "Advanced Algorithms" in titles,
        "Aggregated subjects include Master-program variants",
    )

    # Toggle 4 subjects: 3 Bachelor + 1 Master, to exercise cross-variant save
    await app._toggle_erasmus_subject(chat_id, 101, message=None)  # Programming Basics    (B-C1)
    await app._toggle_erasmus_subject(chat_id, 202, message=None)  # Databases             (B-C2)
    await app._toggle_erasmus_subject(chat_id, 301, message=None)  # Networks              (B-C3)
    await app._toggle_erasmus_subject(chat_id, 401, message=None)  # Machine Learning      (M-C1)

    draft = await app._get_selection_draft(chat_id)
    reporter.check(
        draft is not None and draft.selected_subject_ids == {101, 202, 301, 401},
        "Draft has 4 toggled subjects spanning both variants",
    )

    # Done -> preview (NOT saved yet)
    app.bot.send_message.reset_mock()  # type: ignore[attr-defined]
    await app._show_erasmus_preview(chat_id, draft, message=None)
    preview_text = "\n".join(_collected_messages(app))
    reporter.check("Preview" in preview_text, "Preview message contains 'Preview'")
    reporter.check("Tap Confirm" in preview_text, "Preview prompts user to Tap Confirm")
    reporter.check("Programming Basics" in preview_text, "Preview shows selected Programming Basics")
    reporter.check("Math 1" not in preview_text, "Preview filters OUT unselected Math 1")
    reporter.check(
        await asyncio.to_thread(app.storage.get_chat_selection, chat_id) is None,
        "Selection NOT saved during preview step",
    )
    reporter.check(
        await asyncio.to_thread(app.storage.get_erasmus_subjects, chat_id) == [],
        "Erasmus subjects NOT saved during preview step",
    )

    # Confirm -> final save
    await app._save_erasmus_selection(chat_id, draft, message=None)

    selection = await asyncio.to_thread(app.storage.get_chat_selection, chat_id)
    reporter.check(selection is not None and selection.is_erasmus(), "Erasmus selection saved with study_mode=ERASMUS after confirm")

    saved_subjects = await asyncio.to_thread(app.storage.get_erasmus_subjects, chat_id)
    saved_ids = {s.subject_id for s in saved_subjects}
    reporter.check(
        saved_ids == {101, 202, 301, 401},
        f"Erasmus storage has subjects 101/202/301/401 (got {sorted(saved_ids)})",
    )
    courses_saved = {s.course_id for s in saved_subjects}
    reporter.check(courses_saved == {1, 2, 3}, "Saved subjects cover all 3 courses")

    app.bot.send_message.reset_mock()  # type: ignore[attr-defined]
    await app._show_today(chat_id)
    messages = _collected_messages(app)
    schedule_text = "\n".join(messages)
    reporter.check("Programming Basics" in schedule_text, "Erasmus Today shows Programming Basics")
    reporter.check("Databases" in schedule_text, "Erasmus Today shows Databases")
    reporter.check("Networks" in schedule_text, "Erasmus Today shows Networks")
    reporter.check("Machine Learning" in schedule_text, "Erasmus Today shows Master-variant Machine Learning")
    reporter.check("Math 1" not in schedule_text, "Erasmus Today filters OUT non-selected Math 1")
    reporter.check("Algorithms" not in schedule_text, "Erasmus Today filters OUT non-selected Algorithms")
    reporter.check("Operating Systems" not in schedule_text, "Erasmus Today filters OUT non-selected Operating Systems")
    reporter.check(
        "Advanced Algorithms" not in schedule_text,
        "Erasmus Today filters OUT non-selected Master-variant Advanced Algorithms",
    )

    app.bot.send_message.reset_mock()  # type: ignore[attr-defined]
    await app._show_subjects(chat_id)
    subjects_messages = _collected_messages(app)
    subjects_text = "\n".join(subjects_messages)
    reporter.check("Programming Basics" in subjects_text, "Erasmus Subjects view lists Programming Basics")
    reporter.check("Course 1:" in subjects_text, "Erasmus Subjects view groups by course")

    app.bot.send_message.reset_mock()  # type: ignore[attr-defined]
    await app._show_status(chat_id)
    status_messages = _collected_messages(app)
    status_text = "\n".join(status_messages)
    reporter.check("Mode: Erasmus" in status_text, "Status shows Mode: Erasmus")
    reporter.check("Subjects selected: 4" in status_text, "Status shows Subjects selected: 4")


async def run_mode_switch(app: ScheduleBotApp, reporter: Reporter, chat_id: int) -> None:
    """Switching from Erasmus to Full Time should purge erasmus subjects."""
    print(f"--- Mode switch scenario (chat_id={chat_id}) ---")
    # Chat starts as Erasmus from previous flow; switch to Full Time
    await app._show_start(chat_id)
    await app._select_study_mode(chat_id, "fulltime", message=None)
    await app._select_program_title(chat_id, 0, message=None)
    await app._select_program(chat_id, PROGRAM_ID, message=None)
    await app._select_course(chat_id, 2, message=None)

    selection = await asyncio.to_thread(app.storage.get_chat_selection, chat_id)
    reporter.check(selection is not None and not selection.is_erasmus(), "Switched to FULL_TIME")
    leftover = await asyncio.to_thread(app.storage.get_erasmus_subjects, chat_id)
    reporter.check(leftover == [], "Erasmus subjects purged on mode switch")


async def main() -> int:
    tz = ZoneInfo("Europe/Riga")
    today = datetime.now(tz).date()
    reporter = Reporter()

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db_path = Path(tmp) / "smoke.db"
        settings = Settings(
            telegram_bot_token="123456:smoke-test-fake-token-AAAAAAAA",
            db_path=db_path,
            enable_scheduler=False,
            reminder_enabled=False,
        )
        storage = SnapshotStorage(db_path=db_path)
        api_client = FakeRTUClient(today=today)
        app = ScheduleBotApp(settings=settings, api_client=api_client, storage=storage)
        # Stub all outbound network calls
        app.bot.send_message = AsyncMock()
        app.bot.set_my_commands = AsyncMock()
        app.bot.session = AsyncMock()

        full_time_chat = 1001
        erasmus_chat = 2002
        switch_chat = erasmus_chat  # reuse to confirm switch works

        try:
            await run_full_time(app, reporter, full_time_chat)
            print()
            await run_erasmus(app, reporter, erasmus_chat)
            print()
            await run_mode_switch(app, reporter, switch_chat)
        finally:
            storage.close()

    return reporter.summary()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
