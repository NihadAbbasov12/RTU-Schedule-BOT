
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Lock

from models import (
    BotUsageStats,
    ChatSelection,
    ScheduleDiff,
    ScheduleEvent,
    SelectionDraft,
    clean_group_label,
    infer_group_code,
    iter_month_dates,
    normalize_group_code,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class SnapshotRecord:
    """A stored daily schedule snapshot."""

    snapshot_date: date
    payload_hash: str
    events: list[ScheduleEvent]


class SnapshotStorage:
    """Persist per-chat RTU settings, diffs, and notification state in SQLite."""

    def __init__(
        self,
        db_path: Path,
        legacy_chat_id: int | None = None,
        legacy_semester_id: int | None = None,
        legacy_program_id: int | None = None,
        legacy_course_id: int | None = None,
        legacy_group: str | None = None,
        legacy_semester_program_id: int | None = None,
    ) -> None:
        self.db_path = db_path
        self._legacy_chat_id = legacy_chat_id
        self._legacy_semester_id = legacy_semester_id
        self._legacy_program_id = legacy_program_id
        self._legacy_course_id = legacy_course_id
        self._legacy_group = legacy_group
        self._legacy_semester_program_id = legacy_semester_program_id
        self._lock = Lock()
        if self.db_path.parent and str(self.db_path.parent) != ".":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._initialize()

    def close(self) -> None:
        """Close the database connection."""
        self.connection.close()

    def _initialize(self) -> None:
        with self._lock:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_preferences (
                    chat_id INTEGER PRIMARY KEY,
                    semester_id INTEGER,
                    semester_title TEXT,
                    department_title TEXT,
                    program_family TEXT,
                    program_id INTEGER,
                    program_title TEXT,
                    program_code TEXT,
                    course_id INTEGER,
                    selected_group TEXT NOT NULL,
                    group_code TEXT,
                    group_name TEXT,
                    group_id INTEGER,
                    semester_program_id INTEGER NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_schedule_snapshots (
                    chat_id INTEGER NOT NULL,
                    semester_program_id INTEGER NOT NULL,
                    group_code TEXT,
                    snapshot_date TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, semester_program_id, snapshot_date)
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS selection_drafts (
                    chat_id INTEGER PRIMARY KEY,
                    semester_id INTEGER,
                    semester_title TEXT,
                    department_id INTEGER,
                    department_title TEXT,
                    selected_program_title TEXT,
                    program_family TEXT,
                    program_id INTEGER,
                    program_title TEXT,
                    program_code TEXT,
                    course_id INTEGER,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS weekend_notifications (
                    chat_id INTEGER NOT NULL,
                    week_key TEXT NOT NULL,
                    selected_group TEXT NOT NULL,
                    group_code TEXT,
                    semester_program_id INTEGER NOT NULL,
                    sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, week_key)
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_activity (
                    chat_id INTEGER PRIMARY KEY,
                    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    interaction_count INTEGER NOT NULL DEFAULT 0,
                    schedule_request_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reminder_deliveries (
                    chat_id INTEGER NOT NULL,
                    reminder_key TEXT NOT NULL,
                    lesson_date TEXT NOT NULL,
                    lesson_start TEXT NOT NULL,
                    group_code TEXT,
                    semester_program_id INTEGER NOT NULL,
                    sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, reminder_key)
                )
                """
            )
            self._ensure_chat_preferences_columns()
            self._ensure_chat_schedule_snapshots_columns()
            self._ensure_selection_drafts_columns()
            self._ensure_weekend_notifications_columns()
            self._ensure_reminder_deliveries_columns()
            self._ensure_chat_activity_columns()
            self._ensure_chat_selections_view()
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_chat_schedule_snapshots_group_code
                ON chat_schedule_snapshots (chat_id, group_code, snapshot_date)
                """
            )
            self.connection.commit()
            self._migrate_legacy_data()
            self._migrate_group_storage()

    def _ensure_chat_preferences_columns(self) -> None:
        columns = self._table_columns("chat_preferences")
        for column_name, column_sql in (
            ("semester_id", "INTEGER"),
            ("semester_title", "TEXT"),
            ("department_title", "TEXT"),
            ("program_family", "TEXT"),
            ("program_id", "INTEGER"),
            ("program_title", "TEXT"),
            ("program_code", "TEXT"),
            ("course_id", "INTEGER"),
            ("selected_group", "TEXT NOT NULL DEFAULT ''"),
            ("group_code", "TEXT"),
            ("group_name", "TEXT"),
            ("group_id", "INTEGER"),
            ("semester_program_id", "INTEGER"),
        ):
            if column_name in columns:
                continue
            self.connection.execute(
                f"ALTER TABLE chat_preferences ADD COLUMN {column_name} {column_sql}"
            )

    def _ensure_chat_schedule_snapshots_columns(self) -> None:
        columns = self._table_columns("chat_schedule_snapshots")
        for column_name, column_type in (("group_code", "TEXT"),):
            if column_name in columns:
                continue
            self.connection.execute(
                f"ALTER TABLE chat_schedule_snapshots ADD COLUMN {column_name} {column_type}"
            )

    def _ensure_chat_activity_columns(self) -> None:
        columns = self._table_columns("chat_activity")
        for column_name, column_type, default_sql in (
            ("first_seen_at", "TEXT", "CURRENT_TIMESTAMP"),
            ("last_seen_at", "TEXT", "CURRENT_TIMESTAMP"),
            ("interaction_count", "INTEGER", "0"),
            ("schedule_request_count", "INTEGER", "0"),
        ):
            if column_name in columns:
                continue
            self.connection.execute(
                f"ALTER TABLE chat_activity ADD COLUMN {column_name} {column_type} NOT NULL DEFAULT {default_sql}"
            )

    def _ensure_selection_drafts_columns(self) -> None:
        columns = self._table_columns("selection_drafts")
        for column_name, column_type in (
            ("semester_id", "INTEGER"),
            ("semester_title", "TEXT"),
            ("department_id", "INTEGER"),
            ("department_title", "TEXT"),
            ("selected_program_title", "TEXT"),
            ("program_family", "TEXT"),
            ("program_id", "INTEGER"),
            ("program_title", "TEXT"),
            ("program_code", "TEXT"),
            ("course_id", "INTEGER"),
        ):
            if column_name in columns:
                continue
            self.connection.execute(
                f"ALTER TABLE selection_drafts ADD COLUMN {column_name} {column_type}"
            )

    def _ensure_weekend_notifications_columns(self) -> None:
        columns = self._table_columns("weekend_notifications")
        for column_name, column_type in (("group_code", "TEXT"),):
            if column_name in columns:
                continue
            self.connection.execute(
                f"ALTER TABLE weekend_notifications ADD COLUMN {column_name} {column_type}"
            )

    def _ensure_reminder_deliveries_columns(self) -> None:
        columns = self._table_columns("reminder_deliveries")
        for column_name, column_type in (("group_code", "TEXT"),):
            if column_name in columns:
                continue
            self.connection.execute(
                f"ALTER TABLE reminder_deliveries ADD COLUMN {column_name} {column_type}"
            )

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}

    def _ensure_chat_selections_view(self) -> None:
        row = self.connection.execute(
            """
            SELECT type
            FROM sqlite_master
            WHERE name = 'chat_selections'
            """
        ).fetchone()
        if row is not None:
            return

        self.connection.execute(
            """
            CREATE VIEW chat_selections AS
            SELECT
                chat_id,
                semester_id,
                semester_title,
                NULL AS department_id,
                department_title,
                program_family,
                program_id,
                program_title,
                program_code,
                course_id,
                group_code,
                selected_group,
                group_name,
                semester_program_id,
                updated_at AS created_at,
                updated_at
            FROM chat_preferences
            """
        )

    def _migrate_legacy_data(self) -> None:
        legacy_group_code = infer_group_code(self._legacy_group)
        legacy_group_name = clean_group_label(self._legacy_group)
        if self._legacy_chat_id is None:
            self.connection.commit()
        elif self._legacy_group and self._legacy_semester_program_id is not None:
            self.connection.execute(
                """
                INSERT INTO chat_preferences (
                    chat_id,
                    semester_id,
                    program_id,
                    course_id,
                    selected_group,
                    group_code,
                    group_name,
                    semester_program_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id) DO NOTHING
                """,
                (
                    self._legacy_chat_id,
                    self._legacy_semester_id,
                    self._legacy_program_id,
                    self._legacy_course_id,
                    self._legacy_group,
                    legacy_group_code,
                    legacy_group_name,
                    self._legacy_semester_program_id,
                ),
            )

        if (
            self._legacy_semester_id is not None
            or self._legacy_program_id is not None
            or self._legacy_course_id is not None
        ):
            self.connection.execute(
                """
                UPDATE chat_preferences
                SET
                    semester_id = COALESCE(semester_id, ?),
                    program_id = COALESCE(program_id, ?),
                    course_id = COALESCE(course_id, ?)
                """,
                (
                    self._legacy_semester_id,
                    self._legacy_program_id,
                    self._legacy_course_id,
                ),
            )

        if (
            self._legacy_chat_id is None
            or self._legacy_semester_program_id is None
            or not self._legacy_snapshot_table_exists()
        ):
            self.connection.execute(
                """
                UPDATE chat_preferences
                SET program_family = COALESCE(program_family, program_title)
                """
            )
            self.connection.execute(
                """
                INSERT OR IGNORE INTO chat_activity (
                    chat_id,
                    first_seen_at,
                    last_seen_at,
                    interaction_count,
                    schedule_request_count
                )
                SELECT
                    chat_id,
                    COALESCE(updated_at, CURRENT_TIMESTAMP),
                    COALESCE(updated_at, CURRENT_TIMESTAMP),
                    0,
                    0
                FROM chat_preferences
                """
            )
            self.connection.commit()
            return

        LOGGER.info(
            "Migrating legacy single-chat snapshots into per-chat storage for chat_id=%s",
            self._legacy_chat_id,
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO chat_schedule_snapshots (
                chat_id,
                semester_program_id,
                group_code,
                snapshot_date,
                payload_hash,
                payload_json,
                updated_at
            )
            SELECT
                ?,
                ?,
                ?,
                snapshot_date,
                payload_hash,
                payload_json,
                COALESCE(updated_at, CURRENT_TIMESTAMP)
            FROM schedule_snapshots
            """,
            (
                self._legacy_chat_id,
                self._legacy_semester_program_id,
                legacy_group_code,
            ),
        )
        self.connection.execute(
            """
            UPDATE chat_preferences
            SET program_family = COALESCE(program_family, program_title)
            """
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO chat_activity (
                chat_id,
                first_seen_at,
                last_seen_at,
                interaction_count,
                schedule_request_count
            )
            SELECT
                chat_id,
                COALESCE(updated_at, CURRENT_TIMESTAMP),
                COALESCE(updated_at, CURRENT_TIMESTAMP),
                0,
                0
            FROM chat_preferences
            """
        )
        self.connection.commit()

    def _migrate_group_storage(self) -> None:
        self._backfill_chat_preference_group_columns()
        self._backfill_snapshot_group_codes()
        self._backfill_weekend_notification_group_codes()
        self.connection.commit()

    def _backfill_chat_preference_group_columns(self) -> None:
        rows = self.connection.execute(
            """
            SELECT chat_id, selected_group, group_code, group_name
            FROM chat_preferences
            """
        ).fetchall()

        updated = 0
        unresolved: list[int] = []
        for row in rows:
            stored_group_code = normalize_group_code(row["group_code"])
            inferred_group_code = stored_group_code or infer_group_code(row["selected_group"]) or infer_group_code(
                row["group_name"]
            )
            selected_group = clean_group_label(row["selected_group"]) or ""
            group_name = clean_group_label(row["group_name"])

            if not inferred_group_code:
                unresolved.append(int(row["chat_id"]))
                continue

            if group_name is None and normalize_group_code(selected_group) != inferred_group_code:
                group_name = selected_group

            if (
                stored_group_code != inferred_group_code
                or (row["group_name"] != group_name and not (row["group_name"] is None and group_name is None))
            ):
                self.connection.execute(
                    """
                    UPDATE chat_preferences
                    SET group_code = ?, group_name = ?
                    WHERE chat_id = ?
                    """,
                    (inferred_group_code, group_name, row["chat_id"]),
                )
                updated += 1

        if updated:
            LOGGER.info("Backfilled chat_preferences group_code/group_name for %s rows", updated)
        if unresolved:
            LOGGER.warning(
                "Could not backfill group_code for chat_preferences rows: chat_ids=%s",
                unresolved,
            )

    def _backfill_snapshot_group_codes(self) -> None:
        cursor = self.connection.execute(
            """
            UPDATE chat_schedule_snapshots
            SET group_code = (
                SELECT chat_preferences.group_code
                FROM chat_preferences
                WHERE chat_preferences.chat_id = chat_schedule_snapshots.chat_id
                  AND chat_preferences.semester_program_id = chat_schedule_snapshots.semester_program_id
            )
            WHERE group_code IS NULL OR TRIM(group_code) = ''
            """
        )
        if cursor.rowcount:
            LOGGER.info("Backfilled chat_schedule_snapshots.group_code for %s rows", cursor.rowcount)

    def _backfill_weekend_notification_group_codes(self) -> None:
        rows = self.connection.execute(
            """
            SELECT chat_id, selected_group
            FROM weekend_notifications
            WHERE group_code IS NULL OR TRIM(group_code) = ''
            """
        ).fetchall()
        updated = 0
        for row in rows:
            group_code = infer_group_code(row["selected_group"])
            if not group_code:
                continue
            self.connection.execute(
                """
                UPDATE weekend_notifications
                SET group_code = ?
                WHERE chat_id = ? AND selected_group = ? AND (group_code IS NULL OR TRIM(group_code) = '')
                """,
                (group_code, row["chat_id"], row["selected_group"]),
            )
            updated += 1
        if updated:
            LOGGER.info("Backfilled weekend_notifications.group_code for %s rows", updated)

    def _legacy_snapshot_table_exists(self) -> bool:
        row = self.connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'schedule_snapshots'
            """
        ).fetchone()
        return row is not None

    def save_chat_selection(self, selection: ChatSelection) -> None:
        """Upsert the current RTU study selection for a chat."""
        if selection.semester_program_id is None:
            raise ValueError("semester_program_id is required when saving a chat selection")
        resolved_group_code = selection.resolved_group_code()
        if not resolved_group_code:
            raise ValueError("group_code is required when saving a chat selection")

        selected_group = selection.display_group() or selection.selected_group or resolved_group_code
        group_name = clean_group_label(selection.group_name)
        if group_name is None and normalize_group_code(selected_group) != resolved_group_code:
            group_name = selected_group
        updated_at = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

        with self._lock:
            self.connection.execute(
                """
                INSERT INTO chat_preferences (
                    chat_id,
                    semester_id,
                    semester_title,
                    department_title,
                    program_family,
                    program_id,
                    program_title,
                    program_code,
                    course_id,
                    selected_group,
                    group_code,
                    group_name,
                    group_id,
                    semester_program_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    semester_id = excluded.semester_id,
                    semester_title = excluded.semester_title,
                    department_title = excluded.department_title,
                    program_family = excluded.program_family,
                    program_id = excluded.program_id,
                    program_title = excluded.program_title,
                    program_code = excluded.program_code,
                    course_id = excluded.course_id,
                    selected_group = excluded.selected_group,
                    group_code = excluded.group_code,
                    group_name = excluded.group_name,
                    group_id = excluded.group_id,
                    semester_program_id = excluded.semester_program_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    selection.chat_id,
                    selection.semester_id,
                    selection.semester_title,
                    selection.department_title,
                    selection.program_family,
                    selection.program_id,
                    selection.program_title,
                    selection.program_code,
                    selection.course_id,
                    selected_group,
                    resolved_group_code,
                    group_name,
                    selection.group_id,
                    selection.semester_program_id,
                    updated_at,
                ),
            )
            self.connection.commit()

        LOGGER.info(
            "Saved chat selection: chat_id=%s course_id=%s group_code=%s selected_group=%s semester_program_id=%s",
            selection.chat_id,
            selection.course_id,
            resolved_group_code,
            selected_group,
            selection.semester_program_id,
        )

    def save_selection_draft(self, chat_id: int, draft: SelectionDraft) -> None:
        """Persist an in-progress selection draft for a chat."""
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO selection_drafts (
                    chat_id,
                    semester_id,
                    semester_title,
                    department_id,
                    department_title,
                    selected_program_title,
                    program_family,
                    program_id,
                    program_title,
                    program_code,
                    course_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id) DO UPDATE SET
                    semester_id = excluded.semester_id,
                    semester_title = excluded.semester_title,
                    department_id = excluded.department_id,
                    department_title = excluded.department_title,
                    selected_program_title = excluded.selected_program_title,
                    program_family = excluded.program_family,
                    program_id = excluded.program_id,
                    program_title = excluded.program_title,
                    program_code = excluded.program_code,
                    course_id = excluded.course_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    chat_id,
                    draft.semester_id,
                    draft.semester_title,
                    draft.department_id,
                    draft.department_title,
                    draft.selected_program_title,
                    draft.program_family,
                    draft.program_id,
                    draft.program_title,
                    draft.program_code,
                    draft.course_id,
                ),
            )
            self.connection.commit()

    def get_selection_draft(self, chat_id: int) -> SelectionDraft | None:
        """Load a persisted in-progress selection draft for a chat."""
        with self._lock:
            row = self.connection.execute(
                """
                SELECT
                    semester_id,
                    semester_title,
                    department_id,
                    department_title,
                    selected_program_title,
                    program_family,
                    program_id,
                    program_title,
                    program_code,
                    course_id
                FROM selection_drafts
                WHERE chat_id = ?
                """,
                (chat_id,),
            ).fetchone()

        if row is None:
            return None

        return SelectionDraft(
            semester_id=int(row["semester_id"]) if row["semester_id"] is not None else None,
            semester_title=row["semester_title"],
            department_id=int(row["department_id"]) if row["department_id"] is not None else None,
            department_title=row["department_title"],
            selected_program_title=row["selected_program_title"],
            program_family=row["program_family"],
            program_id=int(row["program_id"]) if row["program_id"] is not None else None,
            program_title=row["program_title"],
            program_code=row["program_code"],
            course_id=int(row["course_id"]) if row["course_id"] is not None else None,
        )

    def delete_selection_draft(self, chat_id: int) -> None:
        """Delete the persisted in-progress selection draft for a chat."""
        with self._lock:
            self.connection.execute(
                """
                DELETE FROM selection_drafts
                WHERE chat_id = ?
                """,
                (chat_id,),
            )
            self.connection.commit()

    def touch_chat_activity(self, chat_id: int, schedule_request: bool = False) -> None:
        """Record that a chat interacted with the bot."""
        schedule_request_increment = 1 if schedule_request else 0
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO chat_activity (
                    chat_id,
                    first_seen_at,
                    last_seen_at,
                    interaction_count,
                    schedule_request_count
                )
                VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    last_seen_at = CURRENT_TIMESTAMP,
                    interaction_count = chat_activity.interaction_count + 1,
                    schedule_request_count = (
                        chat_activity.schedule_request_count + excluded.schedule_request_count
                    )
                """,
                (chat_id, schedule_request_increment),
            )
            self.connection.commit()

        LOGGER.debug(
            "Updated chat activity: chat_id=%s schedule_request=%s",
            chat_id,
            schedule_request,
        )

    def get_chat_selection(self, chat_id: int) -> ChatSelection | None:
        """Load the current RTU study selection for a chat."""
        with self._lock:
            row = self.connection.execute(
                """
                SELECT
                    chat_id,
                    semester_id,
                    semester_title,
                    department_title,
                    program_family,
                    program_id,
                    program_title,
                    program_code,
                    course_id,
                    selected_group,
                    group_code,
                    group_name,
                    group_id,
                    semester_program_id
                FROM chat_preferences
                WHERE chat_id = ?
                """,
                (chat_id,),
            ).fetchone()

        if row is None:
            return None

        selection = self._row_to_chat_selection(row)
        LOGGER.debug(
            "Loaded saved selection: chat_id=%s course_id=%s group_code=%s selected_group=%s semester_program_id=%s",
            selection.chat_id,
            selection.course_id,
            selection.resolved_group_code() or None,
            selection.display_group(),
            selection.semester_program_id,
        )
        return selection

    def list_chat_selections(self) -> list[ChatSelection]:
        """Return all chats with an active RTU study selection."""
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT
                    chat_id,
                    semester_id,
                    semester_title,
                    department_title,
                    program_family,
                    program_id,
                    program_title,
                    program_code,
                    course_id,
                    selected_group,
                    group_code,
                    group_name,
                    group_id,
                    semester_program_id
                FROM chat_preferences
                ORDER BY chat_id
                """
            ).fetchall()

        selections = [self._row_to_chat_selection(row) for row in rows]
        LOGGER.debug("Loaded %s saved chat selections from SQLite", len(selections))
        return selections

    @staticmethod
    def _row_to_chat_selection(row: sqlite3.Row) -> ChatSelection:
        return ChatSelection(
            chat_id=int(row["chat_id"]),
            semester_id=int(row["semester_id"]) if row["semester_id"] is not None else None,
            semester_title=row["semester_title"],
            department_title=row["department_title"],
            program_family=row["program_family"],
            program_id=int(row["program_id"]) if row["program_id"] is not None else None,
            program_title=row["program_title"],
            program_code=row["program_code"],
            course_id=int(row["course_id"]) if row["course_id"] is not None else None,
            selected_group=str(row["selected_group"]),
            semester_program_id=(
                int(row["semester_program_id"])
                if row["semester_program_id"] is not None
                else None
            ),
            group_code=str(row["group_code"]) if row["group_code"] is not None else None,
            group_name=row["group_name"],
            group_id=int(row["group_id"]) if row["group_id"] is not None else None,
        )

    def try_acquire_reminder_delivery(
        self,
        chat_id: int,
        reminder_key: str,
        lesson_date: date,
        lesson_start: str,
        group_code: str | None,
        semester_program_id: int,
    ) -> bool:
        """Reserve a reminder slot if it has not been sent before."""
        normalized_group_code = normalize_group_code(group_code)
        with self._lock:
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO reminder_deliveries (
                    chat_id,
                    reminder_key,
                    lesson_date,
                    lesson_start,
                    group_code,
                    semester_program_id,
                    sent_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    chat_id,
                    reminder_key,
                    lesson_date.isoformat(),
                    lesson_start,
                    normalized_group_code or None,
                    semester_program_id,
                ),
            )
            self.connection.commit()

        inserted = cursor.rowcount == 1
        if not inserted:
            LOGGER.debug(
                "Skipped duplicate reminder reservation: chat_id=%s group_code=%s semester_program_id=%s reminder_key=%s",
                chat_id,
                normalized_group_code or None,
                semester_program_id,
                reminder_key,
            )
        return inserted

    def delete_reminder_delivery(self, chat_id: int, reminder_key: str) -> None:
        """Release a reserved reminder after a failed send."""
        with self._lock:
            self.connection.execute(
                """
                DELETE FROM reminder_deliveries
                WHERE chat_id = ? AND reminder_key = ?
                """,
                (chat_id, reminder_key),
            )
            self.connection.commit()

    def get_bot_usage_stats(self, reference_time: datetime | None = None) -> BotUsageStats:
        """Return aggregated bot usage statistics."""
        if reference_time is None:
            current_time = datetime.now(timezone.utc).replace(tzinfo=None)
        elif reference_time.tzinfo is not None:
            current_time = reference_time.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            current_time = reference_time
        active_7_days_since = (current_time - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        active_30_days_since = (current_time - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

        with self._lock:
            total_chats_ever = int(
                self.connection.execute("SELECT COUNT(*) FROM chat_activity").fetchone()[0]
            )
            chats_with_saved_selection = int(
                self.connection.execute("SELECT COUNT(*) FROM chat_preferences").fetchone()[0]
            )
            active_chats_last_7_days = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM chat_activity
                    WHERE last_seen_at >= ?
                    """,
                    (active_7_days_since,),
                ).fetchone()[0]
            )
            active_chats_last_30_days = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM chat_activity
                    WHERE last_seen_at >= ?
                    """,
                    (active_30_days_since,),
                ).fetchone()[0]
            )
            total_reminders_sent = int(
                self.connection.execute("SELECT COUNT(*) FROM reminder_deliveries").fetchone()[0]
            )
            total_schedule_requests = int(
                self.connection.execute(
                    "SELECT COALESCE(SUM(schedule_request_count), 0) FROM chat_activity"
                ).fetchone()[0]
            )

        return BotUsageStats(
            total_chats_ever=total_chats_ever,
            chats_with_saved_selection=chats_with_saved_selection,
            active_chats_last_7_days=active_chats_last_7_days,
            active_chats_last_30_days=active_chats_last_30_days,
            total_reminders_sent=total_reminders_sent,
            total_schedule_requests=total_schedule_requests,
        )

    def get_snapshot(
        self,
        chat_id: int,
        group_code: str,
        snapshot_date: date,
    ) -> SnapshotRecord | None:
        """Load a stored snapshot for a specific chat, group code, and date."""
        normalized_group_code = normalize_group_code(group_code)
        if not normalized_group_code:
            return None
        with self._lock:
            row = self.connection.execute(
                """
                SELECT snapshot_date, payload_hash, payload_json
                FROM chat_schedule_snapshots
                WHERE chat_id = ? AND group_code = ? AND snapshot_date = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (chat_id, normalized_group_code, snapshot_date.isoformat()),
            ).fetchone()

        if row is None:
            return None

        payload = json.loads(row["payload_json"])
        events = [ScheduleEvent.from_snapshot_payload(item) for item in payload]
        return SnapshotRecord(
            snapshot_date=date.fromisoformat(row["snapshot_date"]),
            payload_hash=row["payload_hash"],
            events=events,
        )

    def sync_month(
        self,
        chat_id: int,
        group_code: str,
        semester_program_id: int,
        year: int,
        month: int,
        events: list[ScheduleEvent],
    ) -> list[ScheduleDiff]:
        """Compare and store all daily snapshots for a month."""
        normalized_group_code = normalize_group_code(group_code)
        if not normalized_group_code:
            raise ValueError("group_code is required when syncing snapshots")
        changes: list[ScheduleDiff] = []
        events_by_date: dict[date, list[ScheduleEvent]] = {}
        for event in sorted(events, key=lambda item: item.sort_key()):
            events_by_date.setdefault(event.event_date, []).append(event)

        for current_date in iter_month_dates(year, month):
            current_events = events_by_date.get(current_date, [])
            current_hash, payload_json = self._serialize_events(current_events)
            previous = self.get_snapshot(chat_id, normalized_group_code, current_date)
            previous_hash = previous.payload_hash if previous else None

            if previous_hash != current_hash:
                LOGGER.info(
                    "Detected snapshot change for chat_id=%s group_code=%s semester_program_id=%s date=%s",
                    chat_id,
                    normalized_group_code,
                    semester_program_id,
                    current_date.isoformat(),
                )
                if previous is not None:
                    changes.extend(
                        self._diff_day(
                            current_date=current_date,
                            old_events=previous.events,
                            new_events=current_events,
                        )
                    )
                self._upsert_snapshot(
                    chat_id=chat_id,
                    group_code=normalized_group_code,
                    semester_program_id=semester_program_id,
                    snapshot_date=current_date,
                    payload_hash=current_hash,
                    payload_json=payload_json,
                )

        return changes

    def _upsert_snapshot(
        self,
        chat_id: int,
        group_code: str,
        semester_program_id: int,
        snapshot_date: date,
        payload_hash: str,
        payload_json: str,
    ) -> None:
        normalized_group_code = normalize_group_code(group_code)
        with self._lock:
            self.connection.execute(
                """
                DELETE FROM chat_schedule_snapshots
                WHERE chat_id = ? AND group_code = ? AND snapshot_date = ?
                """,
                (
                    chat_id,
                    normalized_group_code,
                    snapshot_date.isoformat(),
                ),
            )
            self.connection.execute(
                """
                INSERT INTO chat_schedule_snapshots (
                    chat_id,
                    semester_program_id,
                    group_code,
                    snapshot_date,
                    payload_hash,
                    payload_json,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    chat_id,
                    semester_program_id,
                    normalized_group_code,
                    snapshot_date.isoformat(),
                    payload_hash,
                    payload_json,
                ),
            )
            self.connection.commit()

    def has_weekend_notification(self, chat_id: int, week_key: str) -> bool:
        """Return whether the weekend message has already been sent for the week."""
        with self._lock:
            row = self.connection.execute(
                """
                SELECT 1
                FROM weekend_notifications
                WHERE chat_id = ? AND week_key = ?
                """,
                (chat_id, week_key),
            ).fetchone()
        return row is not None

    def mark_weekend_notification_sent(
        self,
        chat_id: int,
        week_key: str,
        group_code: str | None,
        selected_group: str,
        semester_program_id: int,
    ) -> None:
        """Persist that the weekend message has been sent for the current week."""
        normalized_group_code = normalize_group_code(group_code)
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO weekend_notifications (
                    chat_id,
                    week_key,
                    selected_group,
                    group_code,
                    semester_program_id,
                    sent_at
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id, week_key) DO UPDATE SET
                    selected_group = excluded.selected_group,
                    group_code = excluded.group_code,
                    semester_program_id = excluded.semester_program_id,
                    sent_at = CURRENT_TIMESTAMP
                """,
                (
                    chat_id,
                    week_key,
                    selected_group,
                    normalized_group_code or None,
                    semester_program_id,
                ),
            )
            self.connection.commit()

    @staticmethod
    def _serialize_events(events: list[ScheduleEvent]) -> tuple[str, str]:
        payload = [event.snapshot_payload() for event in sorted(events, key=lambda item: item.sort_key())]
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        return payload_hash, payload_json

    def _diff_day(
        self,
        current_date: date,
        old_events: list[ScheduleEvent],
        new_events: list[ScheduleEvent],
    ) -> list[ScheduleDiff]:
        changes: list[ScheduleDiff] = []
        old_map = {event.stable_id(): event for event in old_events}
        new_map = {event.stable_id(): event for event in new_events}

        for stable_id in sorted(old_map.keys() - new_map.keys()):
            event = old_map[stable_id]
            changes.append(
                ScheduleDiff(
                    change_type="removed lesson",
                    event_date=current_date,
                    title=event.title,
                    description=self._describe_event(event),
                )
            )

        for stable_id in sorted(new_map.keys() - old_map.keys()):
            event = new_map[stable_id]
            changes.append(
                ScheduleDiff(
                    change_type="added lesson",
                    event_date=current_date,
                    title=event.title,
                    description=self._describe_event(event),
                )
            )

        for stable_id in sorted(old_map.keys() & new_map.keys()):
            old_event = old_map[stable_id]
            new_event = new_map[stable_id]

            if (old_event.start_time, old_event.end_time) != (new_event.start_time, new_event.end_time):
                changes.append(
                    ScheduleDiff(
                        change_type="changed time",
                        event_date=current_date,
                        title=new_event.title,
                        description=(
                            f"{self._time_range(old_event)} -> {self._time_range(new_event)}"
                        ),
                    )
                )
            if old_event.room != new_event.room:
                changes.append(
                    ScheduleDiff(
                        change_type="changed room",
                        event_date=current_date,
                        title=new_event.title,
                        description=f"{old_event.room or 'TBA'} -> {new_event.room or 'TBA'}",
                    )
                )
            if old_event.lecturer != new_event.lecturer:
                changes.append(
                    ScheduleDiff(
                        change_type="changed lecturer",
                        event_date=current_date,
                        title=new_event.title,
                        description=(
                            f"{old_event.lecturer or 'TBA'} -> {new_event.lecturer or 'TBA'}"
                        ),
                    )
                )

        return changes

    @staticmethod
    def _describe_event(event: ScheduleEvent) -> str:
        return (
            f"{SnapshotStorage._time_range(event)} | "
            f"{event.title} | Lecturer: {event.lecturer or 'TBA'} | Room: {event.room or 'TBA'}"
        )

    @staticmethod
    def _time_range(event: ScheduleEvent) -> str:
        start = event.start_time.strftime("%H:%M") if event.start_time else "TBA"
        end = event.end_time.strftime("%H:%M") if event.end_time else "TBA"
        return f"{start}-{end}"
