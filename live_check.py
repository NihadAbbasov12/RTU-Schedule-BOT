"""Live end-to-end check against the real RTU API and the real SQLite database.

Unlike `smoke_test.py` (which mocks the RTU API and uses a throwaway database),
this script talks to `https://nodarbibas.rtu.lv` for real and opens the database
configured in `.env` (`DB_PATH`, by default `rtu_schedule.db`). Only the outbound
Telegram calls are stubbed, so nothing is ever sent to a real chat.

Safety rules this script follows:

* it copies the database with SQLite's own backup API before touching anything;
* it never writes to rows that belong to real chats, it only reads them;
* every row it creates itself uses a reserved chat id from `CHECK_CHAT_IDS` and
  is deleted again at the end (pass `--keep` to leave those rows in place);
* it compares the real rows before and after the run and fails if they differ.

Usage:
    python live_check.py
    python live_check.py --keep       # leave the check chats in the database
    python live_check.py --no-backup  # skip the backup copy
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

try:  # keep Latvian lecturer names printable on a cp1252 console
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

logging.basicConfig(level=logging.ERROR)

from bot import ScheduleBotApp
from config import Settings
from models import ChatSelection, ErasmusSubject
from rtu_api import RTUScheduleClient
from storage import SnapshotStorage

# Reserved chat ids for rows this script creates. Far outside the range Telegram
# hands out, so they can never collide with a real chat.
CHECK_CHAT_FULL_TIME = 999_000_001
CHECK_CHAT_STALE_FULL_TIME = 999_000_002
CHECK_CHAT_STALE_ERASMUS = 999_000_003
CHECK_CHAT_ERASMUS = 999_000_004
CHECK_CHAT_IDS = (
    CHECK_CHAT_FULL_TIME,
    CHECK_CHAT_STALE_FULL_TIME,
    CHECK_CHAT_STALE_ERASMUS,
    CHECK_CHAT_ERASMUS,
)

CHAT_SCOPED_TABLES = (
    "chat_preferences",
    "chat_erasmus_subjects",
    "selection_drafts",
    "chat_schedule_snapshots",
    "weekend_notifications",
    "chat_activity",
    "reminder_deliveries",
)

OLD_SEMESTER_ID = 29
OLD_SEMESTER_TITLE = "2025/2026 Spring semester (25/26-SP)"
PROGRAM_TITLE = "Computer Systems"
PROGRAM_ID = 1128
PROGRAM_CODE = "ADBD0"


class Reporter:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0

    def check(self, condition: bool, label: str) -> bool:
        if condition:
            self.passed += 1
            print(f"  PASS: {label}")
        else:
            self.failed += 1
            print(f"  FAIL: {label}")
        return condition

    def summary(self) -> int:
        total = self.passed + self.failed
        print(f"\nTotal: {self.passed}/{total} passed, {self.failed} failed")
        return 1 if self.failed else 0


def backup_database(db_path: Path) -> Path:
    """Copy the database with SQLite's backup API (safe even while the bot runs)."""
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target = db_path.with_name(f"{db_path.stem}-backup-{stamp}{db_path.suffix}")
    source = sqlite3.connect(db_path)
    try:
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()
    return target


def real_row_snapshot(connection: sqlite3.Connection) -> dict[str, list[tuple]]:
    """Return every row that does NOT belong to this script, table by table."""
    existing = {
        row[0]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    placeholders = ",".join("?" for _ in CHECK_CHAT_IDS)
    snapshot: dict[str, list[tuple]] = {}
    for table in CHAT_SCOPED_TABLES:
        if table not in existing:
            snapshot[table] = []
            continue
        rows = connection.execute(
            f"SELECT * FROM {table} WHERE chat_id NOT IN ({placeholders})",
            CHECK_CHAT_IDS,
        ).fetchall()
        snapshot[table] = sorted(tuple(row) for row in rows)
    return snapshot


def delete_check_rows(connection: sqlite3.Connection) -> int:
    placeholders = ",".join("?" for _ in CHECK_CHAT_IDS)
    deleted = 0
    for table in CHAT_SCOPED_TABLES:
        cursor = connection.execute(
            f"DELETE FROM {table} WHERE chat_id IN ({placeholders})",
            CHECK_CHAT_IDS,
        )
        deleted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
    connection.commit()
    return deleted


def messages_by_chat(app: ScheduleBotApp) -> dict[int, list[str]]:
    collected: dict[int, list[str]] = {}
    for call in app.bot.send_message.await_args_list:  # type: ignore[attr-defined]
        chat_id = call.kwargs.get("chat_id")
        text = call.kwargs.get("text")
        if chat_id is None and call.args:
            chat_id = call.args[0]
        if text is None and len(call.args) > 1:
            text = call.args[1]
        collected.setdefault(int(chat_id), []).append(str(text))
    return collected


def text_for(app: ScheduleBotApp, chat_id: int) -> str:
    return "\n".join(messages_by_chat(app).get(chat_id, []))


def reset(app: ScheduleBotApp) -> None:
    app.bot.send_message.reset_mock()  # type: ignore[attr-defined]


def preview(text: str, lines: int = 7) -> None:
    for line in text.splitlines()[:lines]:
        print(f"    | {line}")


async def check_live_api(app: ScheduleBotApp, rep: Reporter) -> None:
    print("\n--- 1. Live RTU API matches the configured study period ---")
    settings = app.settings
    period = await asyncio.to_thread(app.api_client.get_locked_study_period)
    rep.check(
        period.semester_id == settings.rtu_semester_id,
        f"RTU returns semester_id={settings.rtu_semester_id} for the configured period",
    )
    rep.check(
        period.title == settings.rtu_semester_title,
        f"RTU title matches the configured title ({period.title!r})",
    )
    rep.check(
        period.start_date is not None
        and period.end_date is not None
        and period.start_date <= date.today() <= period.end_date,
        f"Today falls inside the period ({period.start_date} to {period.end_date})",
    )
    department = await asyncio.to_thread(app.api_client.get_locked_department)
    rep.check(
        (department.code or "").strip() == settings.rtu_department_code,
        f"Locked department {settings.rtu_department_code} exists in this period",
    )
    titles = await asyncio.to_thread(
        app.api_client.get_department_program_titles,
        settings.rtu_semester_id,
        settings.rtu_department_code,
    )
    rep.check(PROGRAM_TITLE in titles, f"Program list contains {PROGRAM_TITLE!r} ({len(titles)} programs)")


async def audit_real_rows(app: ScheduleBotApp, rep: Reporter) -> list[ChatSelection]:
    """Read-only report on the chats that are actually stored in the database."""
    print("\n--- 2. Saved chats already in the database (read-only) ---")
    settings = app.settings
    selections = await asyncio.to_thread(app.storage.list_chat_selections)
    real = [s for s in selections if s.chat_id not in CHECK_CHAT_IDS]
    print(f"  {len(real)} real chat(s) stored")

    for selection in real:
        subjects = await asyncio.to_thread(app.storage.get_erasmus_subjects, selection.chat_id)
        mode = "Erasmus" if selection.is_erasmus() else "Full Time"
        current = selection.semester_id == settings.rtu_semester_id
        print(
            f"  chat {selection.chat_id}: {mode} | semester_id={selection.semester_id} "
            f"({selection.semester_title}) | program={selection.program_title} "
            f"| saved subjects={len(subjects)} | {'current' if current else 'STALE'}"
        )

        if not current:
            print(
                f"    -> this chat will be asked to choose again; its {len(subjects)} saved "
                f"subject(s) stay in the database until it does"
            )
        if selection.is_erasmus() and subjects and not current:
            program_ids = await app._erasmus_program_ids(
                settings.rtu_semester_id, selection.program_title, selection.program_id
            )
            options = await app._load_erasmus_subject_options(
                settings.rtu_semester_id, program_ids, None
            )
            available = {opt.normalized_title for opt in options}
            still_there = [s for s in subjects if s.normalized_title in available]
            print(
                f"    -> {len(still_there)}/{len(subjects)} of those subject titles are "
                f"offered again in {settings.rtu_semester_title}"
            )

    rep.check(True, f"Existing chats inspected without modifying them ({len(real)} chat(s))")
    return real


async def check_full_time_flow(app: ScheduleBotApp, rep: Reporter) -> None:
    chat_id = CHECK_CHAT_FULL_TIME
    print(f"\n--- 3. Full Time selection flow on the live API (chat {chat_id}) ---")
    settings = app.settings
    titles = await asyncio.to_thread(
        app.api_client.get_department_program_titles,
        settings.rtu_semester_id,
        settings.rtu_department_code,
    )
    index = titles.index(PROGRAM_TITLE)

    await app._show_start(chat_id)
    await app._select_study_mode(chat_id, "fulltime", message=None)
    await app._select_program_title(chat_id, index, message=None)
    await app._select_program(chat_id, PROGRAM_ID, message=None)
    await app._select_course(chat_id, 1, message=None)
    await app._select_group(chat_id, "1", message=None)

    selection = await asyncio.to_thread(app.storage.get_chat_selection, chat_id)
    rep.check(selection is not None, "Selection stored in the live database")
    rep.check(
        selection is not None and selection.semester_id == settings.rtu_semester_id,
        f"Stored semester_id == {settings.rtu_semester_id}",
    )
    rep.check(
        selection is not None and selection.semester_title == settings.rtu_semester_title,
        "Stored semester_title matches the configured one",
    )
    groups = await asyncio.to_thread(
        app.api_client.get_groups, settings.rtu_semester_id, PROGRAM_ID, 1
    )
    expected = next((g.semester_program_id for g in groups if g.group_code == "1"), None)
    rep.check(
        selection is not None and selection.semester_program_id == expected,
        f"Stored semester_program_id == {expected} (resolved live for {PROGRAM_CODE} course 1 group 1)",
    )

    reset(app)
    await app._show_week(chat_id)
    week_text = text_for(app, chat_id)
    rep.check("Week" in week_text, "Week view rendered")
    rep.check("No lessons" not in week_text, "Week view contains real lessons")
    preview(week_text)

    reset(app)
    await app._show_status(chat_id)
    status_text = text_for(app, chat_id)
    rep.check(settings.rtu_semester_title in status_text, "Status shows the configured study period")
    rep.check(OLD_SEMESTER_TITLE not in status_text, "Status does not mention the previous semester")


async def check_stale_guards(app: ScheduleBotApp, rep: Reporter) -> None:
    print("\n--- 4. Selections left over from the previous semester ---")
    settings = app.settings

    await asyncio.to_thread(
        app.storage.save_chat_selection,
        ChatSelection(
            chat_id=CHECK_CHAT_STALE_FULL_TIME,
            semester_id=OLD_SEMESTER_ID,
            semester_title=OLD_SEMESTER_TITLE,
            program_family=None,
            program_id=PROGRAM_ID,
            program_title=PROGRAM_TITLE,
            program_code=PROGRAM_CODE,
            course_id=1,
            selected_group="4",
            semester_program_id=28958,
            group_code="4",
        ),
    )
    reset(app)
    await app._show_today(CHECK_CHAT_STALE_FULL_TIME)
    stale_full_time = text_for(app, CHECK_CHAT_STALE_FULL_TIME)
    rep.check(
        settings.rtu_semester_title in stale_full_time,
        "Stale Full Time chat is told about the current study period",
    )
    rep.check(
        "Choose your study program" in stale_full_time,
        "Stale Full Time chat is asked to choose again instead of getting old lessons",
    )

    await asyncio.to_thread(
        app.storage.save_chat_selection,
        ChatSelection(
            chat_id=CHECK_CHAT_STALE_ERASMUS,
            semester_id=OLD_SEMESTER_ID,
            semester_title=OLD_SEMESTER_TITLE,
            program_family=None,
            program_id=PROGRAM_ID,
            program_title=PROGRAM_TITLE,
            program_code=None,
            course_id=None,
            selected_group="Erasmus",
            semester_program_id=28958,
            group_code="ERASMUS",
            study_mode="ERASMUS",
        ),
    )
    await asyncio.to_thread(
        app.storage.save_erasmus_subjects,
        CHECK_CHAT_STALE_ERASMUS,
        [
            ErasmusSubject(
                subject_id=1,
                subject_code="DST101",
                subject_title="Previous Semester Subject",
                normalized_title="previous semester subject",
                course_id=1,
                semester_program_id=28958,
            )
        ],
    )
    reset(app)
    await app._show_today(CHECK_CHAT_STALE_ERASMUS)
    stale_erasmus = text_for(app, CHECK_CHAT_STALE_ERASMUS)
    rep.check(
        settings.rtu_semester_title in stale_erasmus,
        "Stale Erasmus chat is told about the current study period",
    )
    rep.check(
        "subjects again" in stale_erasmus,
        "Stale Erasmus chat is asked to pick subjects again",
    )
    rep.check(
        "Previous Semester Subject" not in stale_erasmus,
        "No schedule is built from the previous semester's saved subjects",
    )


async def check_erasmus_flow(app: ScheduleBotApp, rep: Reporter) -> None:
    chat_id = CHECK_CHAT_ERASMUS
    print(f"\n--- 5. Erasmus selection flow on the live API (chat {chat_id}) ---")
    settings = app.settings
    titles = await asyncio.to_thread(
        app.api_client.get_department_program_titles,
        settings.rtu_semester_id,
        settings.rtu_department_code,
    )
    index = titles.index(PROGRAM_TITLE)

    await app._show_start(chat_id)
    await app._select_study_mode(chat_id, "erasmus", message=None)
    await app._select_program_title(chat_id, index, message=None)

    draft = await app._get_selection_draft(chat_id)
    rep.check(
        draft is not None and draft.semester_id == settings.rtu_semester_id,
        f"Erasmus draft uses semester_id={settings.rtu_semester_id}",
    )

    program_ids = await app._erasmus_program_ids(settings.rtu_semester_id, PROGRAM_TITLE, PROGRAM_ID)
    options = await app._load_erasmus_subject_options(settings.rtu_semester_id, program_ids, None)
    rep.check(len(options) > 0, f"Live subject list is not empty ({len(options)} subjects)")

    picked = options[:2]
    for option in picked:
        await app._toggle_erasmus_subject(chat_id, option.subject_id, message=None)
    draft = await app._get_selection_draft(chat_id)
    await app._save_erasmus_selection(chat_id, draft, message=None)

    selection = await asyncio.to_thread(app.storage.get_chat_selection, chat_id)
    rep.check(
        selection is not None and selection.semester_id == settings.rtu_semester_id,
        "Stored Erasmus selection carries the current semester_id",
    )
    saved = await asyncio.to_thread(app.storage.get_erasmus_subjects, chat_id)
    rep.check(len(saved) == len(picked), f"Erasmus subjects stored ({len(saved)})")

    reset(app)
    await app._show_month(chat_id)
    month_text = text_for(app, chat_id)
    rep.check("Month" in month_text, "Erasmus Month view rendered")
    rep.check("couldn't" not in month_text, "Erasmus Month view reports no API failure")
    print("    picked: " + ", ".join(option.subject_title for option in picked))
    preview(month_text, lines=6)


async def check_scheduled_jobs(
    app: ScheduleBotApp,
    rep: Reporter,
    real_selections: list[ChatSelection],
) -> None:
    print("\n--- 6. Scheduled jobs (Telegram calls stubbed) ---")
    settings = app.settings
    weekday = date.today()
    while weekday.weekday() > 4:  # roll forward to a teaching day
        weekday += timedelta(days=1)

    reset(app)
    await app._broadcast_schedule_for_predefined_range(
        label="Today",
        range_factory=lambda tz: (weekday, weekday),
        action="scheduled_today",
    )
    sent = messages_by_chat(app)

    current_text = "\n".join(sent.get(CHECK_CHAT_FULL_TIME, []))
    rep.check(
        bool(current_text) and "No lessons" not in current_text,
        f"Chat on the current semester receives real lessons for {weekday}",
    )
    preview(current_text)
    rep.check(
        not sent.get(CHECK_CHAT_STALE_FULL_TIME),
        "Stale Full Time chat is skipped by the scheduled broadcast",
    )
    rep.check(
        not sent.get(CHECK_CHAT_STALE_ERASMUS),
        "Stale Erasmus chat is skipped by the scheduled broadcast",
    )

    stale_real = [s for s in real_selections if s.semester_id != settings.rtu_semester_id]
    rep.check(
        all(not sent.get(s.chat_id) for s in stale_real),
        f"No scheduled message is built for the {len(stale_real)} stale real chat(s)",
    )

    reset(app)
    await app.send_weekend_notifications()
    await app.send_lesson_reminders()
    after = messages_by_chat(app)
    rep.check(
        all(not after.get(s.chat_id) for s in stale_real),
        "Weekend and reminder jobs also skip the stale real chat(s)",
    )


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--keep",
        action="store_true",
        help="leave the check chats in the database instead of deleting them",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="skip the backup copy of the database",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    db_path = Path(settings.db_path)
    print(f"Database: {db_path.resolve()}")
    print(f"Semester: id={settings.rtu_semester_id} title={settings.rtu_semester_title!r}")

    if not db_path.exists():
        print(f"ERROR: {db_path} does not exist yet - start the bot once before running this check.")
        return 1

    if args.no_backup:
        print("Backup:   skipped (--no-backup)")
    else:
        backup_path = backup_database(db_path)
        print(f"Backup:   {backup_path.name}")

    rep = Reporter()

    # Read the real rows through a plain connection first. Opening SnapshotStorage
    # the way app.py does would also run the legacy migration, which backfills NULL
    # columns (semester_id, program_id, course_id) from the RTU_* environment values
    # across every row - a write to real data this check has no business making.
    probe = sqlite3.connect(db_path)
    try:
        before = real_row_snapshot(probe)
    finally:
        probe.close()

    storage = SnapshotStorage(db_path)
    api_client = RTUScheduleClient(settings)
    app = ScheduleBotApp(settings=settings, api_client=api_client, storage=storage)
    app.bot.send_message = AsyncMock()
    app.bot.set_my_commands = AsyncMock()
    app.bot.session = AsyncMock()

    leftovers = delete_check_rows(storage.connection)
    if leftovers:
        print(f"Cleared {leftovers} leftover check row(s) from a previous run")

    try:
        await check_live_api(app, rep)
        real_selections = await audit_real_rows(app, rep)
        await check_full_time_flow(app, rep)
        await check_stale_guards(app, rep)
        await check_erasmus_flow(app, rep)
        await check_scheduled_jobs(app, rep, real_selections)

        print("\n--- 7. Real chat data survived the run untouched ---")
        after = real_row_snapshot(storage.connection)
        for table in CHAT_SCOPED_TABLES:
            rep.check(
                before[table] == after[table],
                f"{table}: {len(before[table])} real row(s) unchanged",
            )

        if args.keep:
            print(f"\nCheck chats kept in the database: {', '.join(str(c) for c in CHECK_CHAT_IDS)}")
        else:
            removed = delete_check_rows(storage.connection)
            print(f"\nRemoved {removed} row(s) created by this check")
            final = real_row_snapshot(storage.connection)
            rep.check(before == final, "Database is back to its original contents")
    finally:
        api_client.close()
        storage.close()

    return rep.summary()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
