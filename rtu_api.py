"""RTU public JSON API client."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, time, timezone
from threading import Lock
from typing import Any

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Settings
from models import ResolvedSemesterProgram, ScheduleEvent, Subject

LOGGER = logging.getLogger(__name__)
_NUMERIC_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")


class RTUAPIError(RuntimeError):
    """Raised when the RTU public API fails."""


class RTUResolutionError(RTUAPIError):
    """Raised when a semester program cannot be resolved."""


class RTUPublicationError(RTUAPIError):
    """Raised when a semester program is not published."""


class RTUScheduleClient:
    """Client for the RTU public schedule endpoints."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session: Session = requests.Session()
        self._bootstrap_complete = False
        self._session_lock = Lock()
        self._cache_lock = Lock()
        self._courses_cache: list[int] | None = None
        self._groups_cache: dict[str, ResolvedSemesterProgram] | None = None
        self._published_cache: dict[int, bool] = {}
        self._resolved_targets: dict[str, ResolvedSemesterProgram] = {}

        retry = Retry(
            total=settings.request_retries,
            connect=settings.request_retries,
            read=settings.request_retries,
            backoff_factor=settings.request_backoff_seconds,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": self.settings.rtu_base_url,
                "Referer": f"{self.settings.rtu_base_url}/?lang={self.settings.rtu_lang}",
            }
        )

    def close(self) -> None:
        """Close the underlying requests session."""
        self.session.close()

    def _timeout(self) -> tuple[int, int]:
        return (
            self.settings.request_connect_timeout_seconds,
            self.settings.request_timeout_seconds,
        )

    def _bootstrap(self) -> None:
        """Prime the session before POST requests."""
        if self._bootstrap_complete:
            return

        url = f"{self.settings.rtu_base_url}/?lang={self.settings.rtu_lang}"
        LOGGER.debug("Bootstrapping RTU session via %s", url)
        try:
            response = self.session.get(url, timeout=self._timeout())
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RTUAPIError(f"Failed to bootstrap RTU session: {exc}") from exc
        self._bootstrap_complete = True

    def _post_form(self, path: str, form_data: dict[str, str | int]) -> Any:
        """Send a form-encoded POST request and return parsed JSON."""
        try:
            with self._session_lock:
                self._bootstrap()
                url = f"{self.settings.rtu_base_url}/{path.lstrip('/')}"
                LOGGER.debug("POST %s with payload %s", url, form_data)
                response = self.session.post(url, data=form_data, timeout=self._timeout())
        except requests.RequestException as exc:
            raise RTUAPIError(f"RTU API request failed for {path}: {exc}") from exc

        return self._handle_json_response(response, path)

    @staticmethod
    def _handle_json_response(response: Response, path: str) -> Any:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RTUAPIError(f"RTU API request failed for {path}: {exc}") from exc

        try:
            return response.json()
        except ValueError as exc:
            raise RTUAPIError(f"RTU API returned invalid JSON for {path}") from exc

    def find_courses_by_program_id(self, semester_id: int, program_id: int) -> list[int]:
        """Return the available course IDs for a program."""
        payload = self._post_form(
            "findCourseByProgramId",
            {"semesterId": semester_id, "programId": program_id},
        )
        if not isinstance(payload, list):
            raise RTUAPIError("Unexpected response from findCourseByProgramId")
        return [int(item) for item in payload]

    def find_groups_by_course_id(
        self,
        semester_id: int,
        program_id: int,
        course_id: int,
    ) -> list[ResolvedSemesterProgram]:
        """Return the available groups for a course."""
        payload = self._post_form(
            "findGroupByCourseId",
            {
                "courseId": course_id,
                "semesterId": semester_id,
                "programId": program_id,
            },
        )
        if not isinstance(payload, list):
            raise RTUAPIError("Unexpected response from findGroupByCourseId")

        results: list[ResolvedSemesterProgram] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            program = item.get("program") or {}
            results.append(
                ResolvedSemesterProgram(
                    semester_program_id=int(item["semesterProgramId"]),
                    semester_id=int(item["semesterId"]),
                    program_id=int(item["programId"]),
                    course_id=int(item["course"]),
                    group=str(item["group"]),
                    program_code=program.get("code"),
                    program_title=program.get("titleEN") or program.get("titleLV"),
                    published=True,
                )
            )
        return results

    def is_semester_program_published(self, semester_program_id: int) -> bool:
        """Return whether the semester program is published."""
        with self._cache_lock:
            cached = self._published_cache.get(semester_program_id)
        if cached is not None:
            return cached

        payload = self._post_form(
            "isSemesterProgramPublished",
            {"semesterProgramId": semester_program_id},
        )
        if isinstance(payload, bool):
            published = payload
        elif isinstance(payload, str):
            published = payload.strip().lower() == "true"
        else:
            raise RTUAPIError("Unexpected response from isSemesterProgramPublished")

        with self._cache_lock:
            self._published_cache[semester_program_id] = published
        return published

    def get_subjects(self, semester_program_id: int) -> list[Subject]:
        """Return all subjects for the given semester program."""
        payload = self._post_form(
            "getSemProgSubjects",
            {"semesterProgramId": semester_program_id},
        )
        if not isinstance(payload, list):
            raise RTUAPIError("Unexpected response from getSemProgSubjects")

        subjects: list[Subject] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            subjects.append(
                Subject(
                    subject_id=int(item["subjectId"]),
                    code=str(item.get("code") or ""),
                    title=str(item.get("titleEN") or item.get("titleLV") or ""),
                    part=int(item["part"]) if item.get("part") is not None else None,
                )
            )
        return sorted(subjects, key=lambda subject: (subject.code, subject.title))

    def resolve_semester_program(self, group: str) -> ResolvedSemesterProgram:
        """Resolve the semester program ID for a selected group."""
        normalized_group = str(group).strip()
        if not normalized_group:
            raise RTUResolutionError("Group is empty")

        with self._cache_lock:
            cached = self._resolved_targets.get(normalized_group)
        if cached is not None:
            return cached

        courses = self._get_courses()
        if self.settings.rtu_course_id not in courses:
            raise RTUResolutionError(
                f"Course {self.settings.rtu_course_id} is not available for program "
                f"{self.settings.rtu_program_id} in semester {self.settings.rtu_semester_id}"
            )

        groups = self._get_groups()
        target = groups.get(normalized_group)
        if target is None:
            raise RTUResolutionError(
                f"Group {normalized_group} was not found for course "
                f"{self.settings.rtu_course_id}, semester {self.settings.rtu_semester_id}, "
                f"program {self.settings.rtu_program_id}"
            )

        published = self.is_semester_program_published(target.semester_program_id)
        if not published:
            raise RTUPublicationError(
                f"semesterProgramId {target.semester_program_id} for group {normalized_group} is not published"
            )

        resolved = ResolvedSemesterProgram(
            semester_program_id=target.semester_program_id,
            semester_id=target.semester_id,
            program_id=target.program_id,
            course_id=target.course_id,
            group=target.group,
            program_code=target.program_code,
            program_title=target.program_title,
            published=True,
        )
        with self._cache_lock:
            self._resolved_targets[normalized_group] = resolved
        return resolved

    def get_month_events(self, semester_program_id: int, year: int, month: int) -> list[ScheduleEvent]:
        """Fetch normalized events for a single calendar month."""
        payload = self._post_form(
            "getSemesterProgEventList",
            {
                "semesterProgramId": semester_program_id,
                "year": year,
                "month": month,
            },
        )
        if not isinstance(payload, list):
            raise RTUAPIError("Unexpected response from getSemesterProgEventList")

        events = [self._normalize_event(item) for item in payload if isinstance(item, dict)]
        return sorted(events, key=lambda event: event.sort_key())

    def get_events_for_range(
        self,
        semester_program_id: int,
        start_date: date,
        end_date: date,
    ) -> list[ScheduleEvent]:
        """Fetch events spanning one or more calendar months."""
        if end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date")

        events: list[ScheduleEvent] = []
        for year, month in self._iter_year_months(start_date, end_date):
            events.extend(self.get_month_events(semester_program_id, year, month))

        filtered = [
            event
            for event in events
            if start_date <= event.event_date <= end_date
        ]
        return sorted(filtered, key=lambda event: event.sort_key())

    def _get_courses(self) -> list[int]:
        with self._cache_lock:
            if self._courses_cache is not None:
                return list(self._courses_cache)

        courses = self.find_courses_by_program_id(
            semester_id=self.settings.rtu_semester_id,
            program_id=self.settings.rtu_program_id,
        )
        with self._cache_lock:
            self._courses_cache = list(courses)
        return courses

    def _get_groups(self) -> dict[str, ResolvedSemesterProgram]:
        with self._cache_lock:
            if self._groups_cache is not None:
                return dict(self._groups_cache)

        groups = self.find_groups_by_course_id(
            semester_id=self.settings.rtu_semester_id,
            program_id=self.settings.rtu_program_id,
            course_id=self.settings.rtu_course_id,
        )
        group_map = {item.group: item for item in groups}
        with self._cache_lock:
            self._groups_cache = dict(group_map)
        return group_map

    @staticmethod
    def _iter_year_months(start_date: date, end_date: date) -> list[tuple[int, int]]:
        months: list[tuple[int, int]] = []
        current_year = start_date.year
        current_month = start_date.month
        while (current_year, current_month) <= (end_date.year, end_date.month):
            months.append((current_year, current_month))
            if current_month == 12:
                current_year += 1
                current_month = 1
            else:
                current_month += 1
        return months

    def _normalize_event(self, payload: dict[str, Any]) -> ScheduleEvent:
        """Normalize a raw event payload into a ScheduleEvent."""
        try:
            event_date = self._parse_date(payload.get("eventDate"))
            start_time = self._parse_time(payload.get("customStart"), event_date)
            end_time = self._parse_time(payload.get("customEnd"), event_date)
            room_payload = payload.get("room")

            return ScheduleEvent(
                event_date_id=self._parse_optional_int(payload.get("eventDateId")),
                event_id=self._parse_optional_int(payload.get("eventId")),
                status_id=self._parse_optional_int(payload.get("statusId")),
                title=self._prefer_language(payload, "eventTempNameEn", "eventTempName"),
                room=self._extract_room_text(payload, room_payload),
                lecturer=self._prefer_language(
                    payload,
                    "lecturerInfoTextEn",
                    "lecturerInfoText",
                    fallback="TBA",
                ),
                program=self._prefer_language(
                    payload,
                    "programInfoTextEn",
                    "programInfoText",
                    fallback="",
                ),
                event_date=event_date,
                start_time=start_time,
                end_time=end_time,
                room_code=self._extract_room_code(room_payload),
                raw=payload,
            )
        except RTUAPIError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise RTUAPIError(f"Failed to normalize RTU event payload: {exc}") from exc

    @staticmethod
    def _prefer_language(
        payload: dict[str, Any],
        english_key: str,
        local_key: str,
        fallback: str = "",
    ) -> str:
        value = payload.get(english_key) or payload.get(local_key) or fallback
        return str(value).strip()

    @staticmethod
    def _parse_optional_int(value: Any) -> int | None:
        if value is None or value == "":
            return None
        return int(value)

    def _parse_date(self, value: Any) -> date:
        if value is None:
            raise RTUAPIError("Event is missing eventDate")
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return self._to_local_datetime(value).date()

        timestamp_dt = self._parse_unix_timestamp(value)
        if timestamp_dt is not None:
            return timestamp_dt.date()

        raw = str(value).strip()
        try:
            return date.fromisoformat(raw)
        except ValueError:
            pass
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return self._to_local_datetime(parsed).date()
        except ValueError as exc:
            raise RTUAPIError(f"Unable to parse eventDate: {raw}") from exc

    def _parse_time(self, value: Any, event_date: date) -> time | None:
        if value is None or value == "":
            return None
        if isinstance(value, dict):
            return self._parse_time_object(value)
        if isinstance(value, time):
            return value
        if isinstance(value, datetime):
            return self._to_local_datetime(value).time().replace(tzinfo=None)

        timestamp_dt = self._parse_unix_timestamp(value)
        if timestamp_dt is not None:
            return timestamp_dt.time().replace(tzinfo=None)

        raw = str(value).strip()
        if len(raw) >= 5 and raw[2] == ":" and "T" not in raw:
            parsed_time = time.fromisoformat(raw)
            if parsed_time.tzinfo is None:
                return parsed_time
            parsed_datetime = datetime.combine(event_date, parsed_time)
            return self._to_local_datetime(parsed_datetime).time().replace(tzinfo=None)

        normalized = raw.replace("Z", "+00:00")
        for candidate in (normalized, f"{event_date.isoformat()}T{normalized}"):
            try:
                parsed = datetime.fromisoformat(candidate)
                return self._to_local_datetime(parsed).time().replace(tzinfo=None)
            except ValueError:
                continue

        raise RTUAPIError(f"Unable to parse event time: {raw}")

    @staticmethod
    def _parse_time_object(value: dict[str, Any]) -> time:
        try:
            hour = int(value.get("hour", 0))
            minute = int(value.get("minute", 0))
            second = int(value.get("second", 0))
            nano = int(value.get("nano", 0))
        except (TypeError, ValueError) as exc:
            raise RTUAPIError(f"Unable to parse RTU time object: {value}") from exc

        try:
            return time(
                hour=hour,
                minute=minute,
                second=second,
                microsecond=max(nano, 0) // 1000,
            )
        except ValueError as exc:
            raise RTUAPIError(f"Invalid RTU time object: {value}") from exc

    def _extract_room_text(self, payload: dict[str, Any], room_payload: Any) -> str:
        room_text = self._prefer_language(payload, "roomInfoTextEn", "roomInfoText", fallback="")
        if room_text:
            return room_text

        if isinstance(room_payload, dict):
            room_name = room_payload.get("roomNameEN") or room_payload.get("roomName")
            if room_name:
                return str(room_name).strip()

        if not isinstance(room_payload, dict) and room_payload is not None and room_payload != "":
            return str(room_payload).strip()

        return "TBA"

    @staticmethod
    def _extract_room_code(room_payload: Any) -> str | None:
        if isinstance(room_payload, dict):
            room_number = room_payload.get("roomNumber")
            if room_number is None or room_number == "":
                return None
            return str(room_number).strip()

        if room_payload is None or room_payload == "":
            return None

        return str(room_payload).strip()

    def _to_local_datetime(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=self.settings.zoneinfo)
        return value.astimezone(self.settings.zoneinfo)

    def _parse_unix_timestamp(self, value: Any) -> datetime | None:
        if isinstance(value, bool):
            return None

        number: float | None = None
        if isinstance(value, (int, float)):
            number = float(value)
        else:
            raw = str(value).strip()
            if _NUMERIC_PATTERN.fullmatch(raw):
                number = float(raw)

        if number is None or abs(number) < 1_000_000_000:
            return None

        if abs(number) >= 1_000_000_000_000:
            number /= 1000.0

        try:
            return datetime.fromtimestamp(number, tz=timezone.utc).astimezone(self.settings.zoneinfo)
        except (OverflowError, OSError, ValueError) as exc:
            raise RTUAPIError(f"Unable to parse Unix timestamp: {value}") from exc
