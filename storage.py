"""SQLite storage for chat preferences, snapshots, and notifications."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from threading import Lock

from models import ChatSelection, ScheduleDiff, ScheduleEvent, iter_month_dates

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
        legacy_group: str | None = None,
        legacy_semester_program_id: int | None = None,
    ) -> None:
        self.db_path = db_path
        self._legacy_chat_id = legacy_chat_id
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
                    selected_group TEXT NOT NULL,
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
                CREATE TABLE IF NOT EXISTS weekend_notifications (
                    chat_id INTEGER NOT NULL,
                    week_key TEXT NOT NULL,
                    selected_group TEXT NOT NULL,
                    semester_program_id INTEGER NOT NULL,
                    sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, week_key)
                )
                """
            )
            self.connection.commit()
            self._migrate_legacy_data()

    def _migrate_legacy_data(self) -> None:
        if self._legacy_chat_id is None:
            return

        if self._legacy_group and self._legacy_semester_program_id is not None:
            self.connection.execute(
                """
                INSERT INTO chat_preferences (chat_id, selected_group, semester_program_id, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id) DO NOTHING
                """,
                (
                    self._legacy_chat_id,
                    self._legacy_group,
                    self._legacy_semester_program_id,
                ),
            )

        if self._legacy_semester_program_id is None or not self._legacy_snapshot_table_exists():
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
                snapshot_date,
                payload_hash,
                payload_json,
                updated_at
            )
            SELECT
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
            ),
        )
        self.connection.commit()

    def _legacy_snapshot_table_exists(self) -> bool:
        row = self.connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'schedule_snapshots'
            """
        ).fetchone()
        return row is not None

    def save_chat_selection(
        self,
        chat_id: int,
        selected_group: str,
        semester_program_id: int,
    ) -> None:
        """Upsert the current RTU group selection for a chat."""
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO chat_preferences (chat_id, selected_group, semester_program_id, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id) DO UPDATE SET
                    selected_group = excluded.selected_group,
                    semester_program_id = excluded.semester_program_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (chat_id, selected_group, semester_program_id),
            )
            self.connection.commit()

    def get_chat_selection(self, chat_id: int) -> ChatSelection | None:
        """Load the current RTU group selection for a chat."""
        with self._lock:
            row = self.connection.execute(
                """
                SELECT chat_id, selected_group, semester_program_id
                FROM chat_preferences
                WHERE chat_id = ?
                """,
                (chat_id,),
            ).fetchone()

        if row is None:
            return None

        return ChatSelection(
            chat_id=int(row["chat_id"]),
            selected_group=str(row["selected_group"]),
            semester_program_id=int(row["semester_program_id"]),
        )

    def list_chat_selections(self) -> list[ChatSelection]:
        """Return all chats with an active RTU group selection."""
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT chat_id, selected_group, semester_program_id
                FROM chat_preferences
                ORDER BY chat_id
                """
            ).fetchall()

        return [
            ChatSelection(
                chat_id=int(row["chat_id"]),
                selected_group=str(row["selected_group"]),
                semester_program_id=int(row["semester_program_id"]),
            )
            for row in rows
        ]

    def get_snapshot(
        self,
        chat_id: int,
        semester_program_id: int,
        snapshot_date: date,
    ) -> SnapshotRecord | None:
        """Load a stored snapshot for a specific chat, semester program, and date."""
        with self._lock:
            row = self.connection.execute(
                """
                SELECT snapshot_date, payload_hash, payload_json
                FROM chat_schedule_snapshots
                WHERE chat_id = ? AND semester_program_id = ? AND snapshot_date = ?
                """,
                (chat_id, semester_program_id, snapshot_date.isoformat()),
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
        semester_program_id: int,
        year: int,
        month: int,
        events: list[ScheduleEvent],
    ) -> list[ScheduleDiff]:
        """Compare and store all daily snapshots for a month."""
        changes: list[ScheduleDiff] = []
        events_by_date: dict[date, list[ScheduleEvent]] = {}
        for event in sorted(events, key=lambda item: item.sort_key()):
            events_by_date.setdefault(event.event_date, []).append(event)

        for current_date in iter_month_dates(year, month):
            current_events = events_by_date.get(current_date, [])
            current_hash, payload_json = self._serialize_events(current_events)
            previous = self.get_snapshot(chat_id, semester_program_id, current_date)
            previous_hash = previous.payload_hash if previous else None

            if previous_hash != current_hash:
                LOGGER.info(
                    "Detected snapshot change for chat_id=%s semester_program_id=%s date=%s",
                    chat_id,
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
                    semester_program_id=semester_program_id,
                    snapshot_date=current_date,
                    payload_hash=current_hash,
                    payload_json=payload_json,
                )

        return changes

    def _upsert_snapshot(
        self,
        chat_id: int,
        semester_program_id: int,
        snapshot_date: date,
        payload_hash: str,
        payload_json: str,
    ) -> None:
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO chat_schedule_snapshots (
                    chat_id,
                    semester_program_id,
                    snapshot_date,
                    payload_hash,
                    payload_json,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id, semester_program_id, snapshot_date) DO UPDATE SET
                    payload_hash = excluded.payload_hash,
                    payload_json = excluded.payload_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    chat_id,
                    semester_program_id,
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
        selected_group: str,
        semester_program_id: int,
    ) -> None:
        """Persist that the weekend message has been sent for the current week."""
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO weekend_notifications (
                    chat_id,
                    week_key,
                    selected_group,
                    semester_program_id,
                    sent_at
                )
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id, week_key) DO UPDATE SET
                    selected_group = excluded.selected_group,
                    semester_program_id = excluded.semester_program_id,
                    sent_at = CURRENT_TIMESTAMP
                """,
                (chat_id, week_key, selected_group, semester_program_id),
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
