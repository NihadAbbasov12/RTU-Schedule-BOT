"""RTU public JSON API client."""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from datetime import date, datetime, time, timezone
from html import unescape
from html.parser import HTMLParser
from threading import Lock
from typing import Any

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Settings
from models import (
    ChatSelection,
    ResolvedSemesterProgram,
    ScheduleEvent,
    StudyDepartment,
    StudyPeriod,
    StudyProgram,
    StudyProgramFamily,
    Subject,
    clean_group_label,
    infer_group_code,
    normalize_group_code,
)

LOGGER = logging.getLogger(__name__)
_NUMERIC_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")
_SEMESTER_SHORT_NAME_PATTERN = re.compile(r"^(?P<title>.+?)\s*\((?P<short>[^()]+)\)$")
_WHITESPACE_PATTERN = re.compile(r"\s+")
_SMALL_TITLE_WORDS = {"and", "of", "in", "to", "for"}
_PROGRAM_ID_FIELD_CANDIDATES = ("programId", "program_id", "id")
_PROGRAM_CODE_FIELD_CANDIDATES = (
    "code",
    "programCode",
    "program_code",
    "studyProgramCode",
    "study_program_code",
    "shortName",
    "short_name",
)
_PROGRAM_TITLE_FIELD_CANDIDATES = (
    "titleEN",
    "titleLV",
    "title",
    "name",
    "programTitle",
    "program_title",
    "displayName",
    "display_name",
)
_GROUP_CODE_FIELD_CANDIDATES = ("groupCode", "group_code", "code", "group", "name", "title")
_GROUP_NAME_FIELD_CANDIDATES = (
    "groupName",
    "group_name",
    "displayName",
    "display_name",
    "titleEN",
    "titleLV",
    "name",
    "title",
)
_GROUP_ID_FIELD_CANDIDATES = ("groupId", "group_id", "id")
_LOCKED_DEPARTMENT_CODE = "02A00"


class RTUAPIError(RuntimeError):
    """Raised when the RTU public API fails."""


class RTUResolutionError(RTUAPIError):
    """Raised when a semester program cannot be resolved."""


class RTUPublicationError(RTUAPIError):
    """Raised when a semester program is not published."""


class _StudyPeriodParser(HTMLParser):
    """Parse the study period select from the RTU homepage."""

    def __init__(self) -> None:
        super().__init__()
        self.periods: list[StudyPeriod] = []
        self._inside_semester_select = False
        self._inside_option = False
        self._option_attrs: dict[str, str | None] = {}
        self._option_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = dict(attrs)
        if tag == "select" and attrs_map.get("id") == "semester-id":
            self._inside_semester_select = True
            return

        if self._inside_semester_select and tag == "option":
            self._inside_option = True
            self._option_attrs = attrs_map
            self._option_chunks = []

    def handle_data(self, data: str) -> None:
        if self._inside_option:
            self._option_chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "option" and self._inside_option:
            self._finish_option()
            return
        if tag == "select" and self._inside_semester_select:
            self._inside_semester_select = False

    def _finish_option(self) -> None:
        option_attrs = dict(self._option_attrs)
        raw_value = option_attrs.get("value")
        raw_title = unescape("".join(self._option_chunks).strip())
        self._inside_option = False
        self._option_attrs = {}
        self._option_chunks = []

        if raw_value is None or raw_title == "":
            return

        try:
            semester_id = int(raw_value)
        except ValueError:
            return

        match = _SEMESTER_SHORT_NAME_PATTERN.match(raw_title)
        short_name = match.group("short") if match else None
        self.periods.append(
            StudyPeriod(
                semester_id=semester_id,
                title=raw_title,
                short_name=short_name,
                active=("selected" in option_attrs),
            )
        )


class RTUScheduleClient:
    """Client for the RTU public schedule endpoints."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session: Session = requests.Session()
        self._bootstrap_complete = False
        self._homepage_html: str | None = None
        self._session_lock = Lock()
        self._cache_lock = Lock()
        self._study_periods_cache: list[StudyPeriod] | None = None
        self._study_period_details_cache: dict[int, StudyPeriod] = {}
        self._departments_cache: dict[int, list[StudyDepartment]] = {}
        self._programs_cache: dict[int, list[StudyProgram]] = {}
        self._program_families_cache: dict[tuple[int, str], list[StudyProgramFamily]] = {}
        self._courses_cache: dict[tuple[int, int], list[int]] = {}
        self._groups_cache: dict[tuple[int, int, int], list[ResolvedSemesterProgram]] = {}
        self._published_cache: dict[int, bool] = {}
        self._resolved_targets: dict[
            tuple[int, int, int, str, int | None, str | None, bool],
            ResolvedSemesterProgram,
        ] = {}

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

    def get_study_periods(self) -> list[StudyPeriod]:
        """Return all study periods shown on the RTU homepage."""
        with self._cache_lock:
            if self._study_periods_cache is not None:
                return list(self._study_periods_cache)

        parser = _StudyPeriodParser()
        parser.feed(self._get_homepage_html())
        periods = parser.periods
        if not periods:
            raise RTUAPIError("Unable to parse study periods from the RTU homepage")

        with self._cache_lock:
            self._study_periods_cache = list(periods)
        return periods

    def get_study_period_details(self, semester_id: int) -> StudyPeriod:
        """Return detailed information for one study period."""
        with self._cache_lock:
            cached = self._study_period_details_cache.get(semester_id)
        if cached is not None:
            return cached

        payload = self._post_form("getChousenSemesterStartEndDate", {"semesterId": semester_id})
        if not isinstance(payload, dict):
            raise RTUAPIError("Unexpected response from getChousenSemesterStartEndDate")

        title = self._prefer_language(payload, "titleEN", "titleLV")
        short_name = self._prefer_language(payload, "shortNameEN", "shortNameLV", fallback="") or None
        period = StudyPeriod(
            semester_id=int(payload.get("semesterId", semester_id)),
            title=self._compose_study_period_title(title, short_name),
            short_name=short_name,
            start_date=self._parse_date(payload.get("startDate")) if payload.get("startDate") is not None else None,
            end_date=self._parse_date(payload.get("endDate")) if payload.get("endDate") is not None else None,
            active=bool(payload.get("active")),
        )
        with self._cache_lock:
            self._study_period_details_cache[semester_id] = period
        return period

    def get_departments(self, semester_id: int) -> list[StudyDepartment]:
        """Return departments that contain study programs for the given study period."""
        _, departments = self._load_programs_and_departments(semester_id)
        return departments

    def get_locked_study_period(self) -> StudyPeriod:
        """Return the study period that the bot is locked to."""
        period = self.get_study_period_details(self.settings.rtu_semester_id)
        if period.title != self.settings.rtu_semester_title:
            LOGGER.warning(
                "Locked semester title differs from RTU response: configured=%s actual=%s",
                self.settings.rtu_semester_title,
                period.title,
            )
        LOGGER.info(
            "Using locked study period: semester_id=%s title=%s",
            period.semester_id,
            period.title,
        )
        return period

    def get_locked_department(self, semester_id: int | None = None) -> StudyDepartment:
        """Return the Foreign Students department for the locked semester."""
        resolved_semester_id = semester_id or self.settings.rtu_semester_id
        departments = self.get_departments(resolved_semester_id)
        for department in departments:
            if (department.code or "").strip().casefold() == _LOCKED_DEPARTMENT_CODE.casefold():
                LOGGER.info(
                    "Using locked department: semester_id=%s department_id=%s department=%s (%s)",
                    resolved_semester_id,
                    department.department_id,
                    department.title,
                    department.code,
                )
                return department
        raise RTUAPIError(
            f"Department {_LOCKED_DEPARTMENT_CODE} was not found in semester {resolved_semester_id}"
        )

    def get_study_programs(self, semester_id: int) -> list[StudyProgram]:
        """Return study programs for the given study period."""
        programs, _ = self._load_programs_and_departments(semester_id)
        return programs

    def get_study_program(self, semester_id: int, program_id: int) -> StudyProgram | None:
        """Return a single study program if it exists."""
        for program in self.get_study_programs(semester_id):
            if program.program_id == program_id:
                return program
        return None

    def get_department_programs(
        self,
        semester_id: int,
        department_code: str,
    ) -> list[StudyProgram]:
        """Return non-deduplicated RTU study programs for one department."""
        requested_department_code = str(department_code).strip()
        if requested_department_code.casefold() != _LOCKED_DEPARTMENT_CODE.casefold():
            LOGGER.warning(
                "Ignoring requested department_code=%s because this bot is locked to %s",
                requested_department_code,
                _LOCKED_DEPARTMENT_CODE,
            )
        normalized_department_code = _LOCKED_DEPARTMENT_CODE.casefold()
        programs = [
            program
            for program in self.get_study_programs(semester_id)
            if (program.department_code or "").strip().casefold() == normalized_department_code
        ]
        if not programs:
            raise RTUAPIError(
                f"No study programs were returned for department {_LOCKED_DEPARTMENT_CODE} in semester {semester_id}"
            )

        programs = sorted(
            programs,
            key=lambda program: (
                self._normalize_program_family_title(program.title).casefold(),
                (program.code or "").casefold(),
                program.program_id,
            ),
        )
        LOGGER.info(
            "Fetched RTU program options: semester_id=%s department_code=%s count=%s",
            semester_id,
            department_code,
            len(programs),
        )
        for program in programs:
            LOGGER.debug(
                "Program option prepared: title=%r code=%r id=%s",
                program.title,
                program.code,
                program.program_id,
            )
        return programs

    def get_department_program_titles(
        self,
        semester_id: int,
        department_code: str,
    ) -> list[str]:
        """Return unique study program titles for one department, without RTU codes."""
        titles_by_key: dict[str, str] = {}
        for program in self.get_department_programs(semester_id, department_code):
            normalized_title = self._normalize_program_family_title(program.title)
            if not normalized_title:
                continue
            titles_by_key.setdefault(normalized_title.casefold(), normalized_title)

        titles = sorted(titles_by_key.values(), key=lambda title: title.casefold())
        LOGGER.info(
            "Fetched RTU program title options: semester_id=%s department_code=%s count=%s titles=%s",
            semester_id,
            department_code,
            len(titles),
            titles,
        )
        return titles

    def get_department_program_variants_by_title(
        self,
        semester_id: int,
        department_code: str,
        title: str,
    ) -> list[StudyProgram]:
        """Return exact RTU program variants for one selected display title."""
        title_key = self._normalize_program_family_title(title).casefold()
        variants = [
            program
            for program in self.get_department_programs(semester_id, department_code)
            if self._normalize_program_family_title(program.title).casefold() == title_key
        ]
        variants = sorted(
            variants,
            key=lambda program: ((program.code or "").casefold(), program.program_id),
        )
        LOGGER.info(
            "Fetched RTU program code variants: semester_id=%s department_code=%s title=%r variants=%s",
            semester_id,
            department_code,
            title,
            [(program.code, program.program_id) for program in variants],
        )
        return variants

    def get_department_program(
        self,
        semester_id: int,
        department_code: str,
        program_id: int,
    ) -> StudyProgram | None:
        """Return one exact RTU study program inside the selected department."""
        for program in self.get_department_programs(semester_id, department_code):
            if program.program_id == program_id:
                return program
        return None

    def get_courses(self, semester_id: int, program_id: int) -> list[int]:
        """Return available course numbers for a study program."""
        cache_key = (semester_id, program_id)
        with self._cache_lock:
            cached = self._courses_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        payload = self._post_form(
            "findCourseByProgramId",
            {"semesterId": semester_id, "programId": program_id},
        )
        if not isinstance(payload, list):
            raise RTUAPIError("Unexpected response from findCourseByProgramId")

        courses: list[int] = []
        for item in payload:
            try:
                courses.append(int(item))
            except (TypeError, ValueError):
                LOGGER.warning(
                    "Skipping malformed RTU course item for semester_id=%s program_id=%s: %r",
                    semester_id,
                    program_id,
                    item,
                )

        courses = sorted(set(courses))
        with self._cache_lock:
            self._courses_cache[cache_key] = list(courses)
        return courses

    def get_program_families(
        self,
        semester_id: int,
        department_code: str,
    ) -> list[StudyProgramFamily]:
        """Return deduplicated program families for one department."""
        cache_key = (semester_id, department_code)
        with self._cache_lock:
            cached = self._program_families_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        programs = [
            program
            for program in self.get_study_programs(semester_id)
            if program.department_code == department_code
        ]
        LOGGER.debug(
            "Raw RTU program rows: semester_id=%s department_code=%s rows=%s",
            semester_id,
            department_code,
            [
                f"{program.program_id}:{self._normalize_program_family_title(program.title)} ({program.code or 'no-code'})"
                for program in programs
            ],
        )
        if not programs:
            raise RTUAPIError(
                f"No study programs were returned for department {department_code} in semester {semester_id}"
            )

        grouped: dict[str, list[StudyProgram]] = {}
        for program in programs:
            family_key = self._program_family_key(program.title)
            grouped.setdefault(family_key, []).append(program)

        families: list[StudyProgramFamily] = []
        for family_key, variants in grouped.items():
            display_name = self._choose_program_family_display_name(variants)
            representative = self._choose_representative_program(semester_id, variants)
            families.append(
                StudyProgramFamily(
                    family_key=family_key,
                    display_name=display_name,
                    representative_program=representative,
                    variants=tuple(
                        sorted(
                            variants,
                            key=lambda item: (self._normalize_program_family_title(item.title), item.program_id),
                        )
                    ),
                )
            )

        families = sorted(families, key=lambda item: item.display_name.casefold())
        LOGGER.debug(
            "Grouped RTU program families: semester_id=%s department_code=%s groups=%s",
            semester_id,
            department_code,
            [
                (
                    family.display_name,
                    [
                        f"{program.program_id}:{program.code or 'no-code'}"
                        for program in family.variants
                    ],
                    f"{family.representative_program.program_id}:{family.representative_program.code or 'no-code'}",
                )
                for family in families
            ],
        )
        with self._cache_lock:
            self._program_families_cache[cache_key] = list(families)
        return families

    def get_program_family_by_representative_id(
        self,
        semester_id: int,
        department_code: str,
        representative_program_id: int,
    ) -> StudyProgramFamily | None:
        """Return one deduplicated program family by its representative program ID."""
        for family in self.get_program_families(semester_id, department_code):
            if family.representative_program.program_id == representative_program_id:
                return family
        return None

    def get_program_family_by_program_id(
        self,
        semester_id: int,
        department_code: str,
        program_id: int,
    ) -> StudyProgramFamily | None:
        """Return one deduplicated program family by any program ID contained in it."""
        for family in self.get_program_families(semester_id, department_code):
            if any(variant.program_id == program_id for variant in family.variants):
                return family
        return None

    def get_family_courses(
        self,
        semester_id: int,
        program_id: int,
        *,
        program_family: str | None = None,
        department_code: str | None = None,
    ) -> list[int]:
        """Return all available course numbers across the selected program family."""
        family = self._resolve_program_family(
            semester_id=semester_id,
            program_id=program_id,
            program_family=program_family,
            department_code=department_code,
        )
        if family is None:
            return self.get_courses(semester_id, program_id)

        courses: set[int] = set()
        for variant in self._ordered_family_variants(family, program_id):
            courses.update(self.get_courses(semester_id, variant.program_id))

        resolved_courses = sorted(courses)
        LOGGER.debug(
            "Loaded RTU family courses: semester_id=%s family=%s representative_program_id=%s courses=%s",
            semester_id,
            family.display_name,
            family.representative_program.program_id,
            resolved_courses,
        )
        return resolved_courses

    def get_family_groups(
        self,
        semester_id: int,
        program_id: int,
        course_id: int,
        *,
        program_family: str | None = None,
        department_code: str | None = None,
    ) -> list[ResolvedSemesterProgram]:
        """Return deduplicated groups across all variants in the selected program family."""
        family = self._resolve_program_family(
            semester_id=semester_id,
            program_id=program_id,
            program_family=program_family,
            department_code=department_code,
        )
        if family is None:
            return self.get_groups(semester_id, program_id, course_id)

        groups_by_code: dict[str, ResolvedSemesterProgram] = {}
        for variant in self._ordered_family_variants(family, program_id):
            courses = self.get_courses(semester_id, variant.program_id)
            if course_id not in courses:
                continue

            for group in self.get_groups(semester_id, variant.program_id, course_id):
                group_code = group.normalized_group_code()
                existing = groups_by_code.get(group_code)
                if existing is None:
                    groups_by_code[group_code] = group
                    continue

                LOGGER.debug(
                    "Duplicate RTU group_code across family variants: semester_id=%s family=%s course_id=%s group_code=%s existing_program_id=%s replacement_program_id=%s",
                    semester_id,
                    family.display_name,
                    course_id,
                    group_code,
                    existing.program_id,
                    group.program_id,
                )

        resolved_groups = sorted(
            groups_by_code.values(),
            key=lambda item: (self._group_sort_key(item.group_code), item.semester_program_id),
        )
        LOGGER.debug(
            "Loaded RTU family groups: semester_id=%s family=%s course_id=%s groups=%s",
            semester_id,
            family.display_name,
            course_id,
            [(group.group_code, group.group_name, group.program_id, group.semester_program_id) for group in resolved_groups],
        )
        return resolved_groups

    def get_groups(
        self,
        semester_id: int,
        program_id: int,
        course_id: int,
    ) -> list[ResolvedSemesterProgram]:
        """Return available groups for a study program course."""
        cache_key = (semester_id, program_id, course_id)
        with self._cache_lock:
            cached = self._groups_cache.get(cache_key)
        if cached is not None:
            return list(cached)

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
        raw_example = next((item for item in payload if isinstance(item, dict)), None)
        if raw_example is not None:
            LOGGER.debug(
                "Raw RTU group object example: semester_id=%s program_id=%s course_id=%s raw_group=%r",
                semester_id,
                program_id,
                course_id,
                raw_example,
            )

        results: list[ResolvedSemesterProgram] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            try:
                program_payload = item.get("program") or {}
                group_code, group_name, group_id, debug_fields = self._extract_group_metadata(
                    item,
                    semester_id=semester_id,
                    program_id=program_id,
                    course_id=course_id,
                )
                results.append(
                    ResolvedSemesterProgram(
                        semester_program_id=int(item["semesterProgramId"]),
                        semester_id=int(item.get("semesterId", semester_id)),
                        program_id=int(item.get("programId", program_id)),
                        course_id=int(item.get("course", course_id)),
                        group_code=group_code,
                        group_name=group_name,
                        group_id=group_id,
                        program_code=program_payload.get("code"),
                        program_title=(
                            program_payload.get("titleEN")
                            or program_payload.get("titleLV")
                            or None
                        ),
                        published=None,
                    )
                )
                LOGGER.debug(
                    "Parsed RTU group item: semester_id=%s program_id=%s course_id=%s semester_program_id=%s group_code=%s group_name=%s group_id=%s code_field=%s name_field=%s group_id_field=%s keys=%s",
                    semester_id,
                    program_id,
                    course_id,
                    item.get("semesterProgramId"),
                    group_code,
                    group_name,
                    group_id,
                    debug_fields.get("group_code_field"),
                    debug_fields.get("group_name_field"),
                    debug_fields.get("group_id_field"),
                    sorted(item.keys()),
                )
            except (KeyError, TypeError, ValueError):
                LOGGER.warning(
                    "Skipping malformed RTU group item for semester_id=%s program_id=%s course_id=%s: %r",
                    semester_id,
                    program_id,
                    course_id,
                    item,
                )

        results = sorted(
            results,
            key=lambda item: (self._group_sort_key(item.group_code), item.semester_program_id),
        )
        LOGGER.info(
            "Fetched RTU group list: semester_id=%s program_id=%s course_id=%s count=%s",
            semester_id,
            program_id,
            course_id,
            len(results),
        )
        LOGGER.debug(
            "Fetched RTU groups detail: semester_id=%s program_id=%s course_id=%s groups=%s",
            semester_id,
            program_id,
            course_id,
            [
                (
                    group.group_code,
                    group.group_name,
                    group.group_id,
                    group.semester_program_id,
                )
                for group in results
            ],
        )
        with self._cache_lock:
            self._groups_cache[cache_key] = list(results)
        return results

    def get_display_groups(
        self,
        semester_id: int,
        program_id: int,
        course_id: int,
        program_family: str | None = None,
        *,
        include_family_variants: bool = False,
    ) -> list[ResolvedSemesterProgram]:
        """Return filtered groups suitable for Telegram selection."""
        raw_groups = self._load_group_candidates(
            semester_id=semester_id,
            program_id=program_id,
            course_id=course_id,
            program_family=program_family,
            include_family_variants=include_family_variants,
        )
        LOGGER.debug(
            "Loaded raw RTU groups: semester_id=%s program_id=%s course_id=%s include_family_variants=%s groups=%s",
            semester_id,
            program_id,
            course_id,
            include_family_variants,
            [(group.group_code, group.group_name, group.semester_program_id) for group in raw_groups],
        )
        filtered_groups = [
            group
            for group in raw_groups
            if self._parse_group_number(group.group_code) is None
            or self._is_display_group(group.group_code)
        ]
        if not filtered_groups:
            filtered_groups = list(raw_groups)
        elif all(self._parse_group_number(group.group_code) is not None for group in filtered_groups):
            visible_numbers = [
                number
                for number in (self._parse_group_number(group.group_code) for group in filtered_groups)
                if number is not None
            ]
            contiguous_visible_groups = self._contiguous_prefix_length(visible_numbers)
            if 2 <= contiguous_visible_groups < len(visible_numbers):
                allowed_numbers = set(range(1, contiguous_visible_groups + 1))
                filtered_groups = [
                    group
                    for group in filtered_groups
                    if self._parse_group_number(group.group_code) in allowed_numbers
                ]
                LOGGER.debug(
                    "Applied contiguous prefix group filter: semester_id=%s program_id=%s course_id=%s prefix=%s",
                    semester_id,
                    program_id,
                    course_id,
                    contiguous_visible_groups,
                )

        filtered_groups = sorted(
            filtered_groups,
            key=lambda item: (self._group_sort_key(item.group_code), item.semester_program_id),
        )
        LOGGER.debug(
            "Filtered RTU groups: semester_id=%s program_id=%s course_id=%s groups=%s",
            semester_id,
            program_id,
            course_id,
            [(group.group_code, group.group_name, group.semester_program_id) for group in filtered_groups],
        )
        return filtered_groups

    def resolve_chat_selection(self, selection: ChatSelection) -> ResolvedSemesterProgram:
        """Resolve a saved chat selection into an active semester program target."""
        if selection.semester_id is None or selection.program_id is None or selection.course_id is None:
            raise RTUResolutionError("Saved selection is incomplete")

        resolved_group_code = selection.resolved_group_code()
        LOGGER.info(
            "Resolving saved selection: chat_id=%s semester_id=%s program_id=%s course_id=%s saved_group_code=%s saved_group=%s saved_semester_program_id=%s",
            selection.chat_id,
            selection.semester_id,
            selection.program_id,
            selection.course_id,
            resolved_group_code or None,
            selection.display_group(),
            selection.semester_program_id,
        )

        if resolved_group_code:
            return self.resolve_group_by_code(
                semester_id=selection.semester_id,
                program_id=selection.program_id,
                course_id=selection.course_id,
                group_code=resolved_group_code,
                semester_program_id=selection.semester_program_id,
                program_family=selection.program_family,
                allow_family_fallback=self._allows_legacy_family_fallback(selection),
            )

        if selection.semester_program_id is not None:
            LOGGER.warning(
                "Saved selection is missing group_code; attempting semester_program_id fallback: chat_id=%s semester_id=%s program_id=%s course_id=%s semester_program_id=%s selected_group=%s",
                selection.chat_id,
                selection.semester_id,
                selection.program_id,
                selection.course_id,
                selection.semester_program_id,
                selection.selected_group,
            )
            return self._resolve_group_by_semester_program_id(
                semester_id=selection.semester_id,
                program_id=selection.program_id,
                course_id=selection.course_id,
                semester_program_id=selection.semester_program_id,
                program_family=selection.program_family,
            )

        raise RTUResolutionError(
            "Saved selection has no group_code and the legacy group value could not be recovered"
        )

    def resolve_group_by_code(
        self,
        semester_id: int | None,
        program_id: int | None,
        course_id: int | None,
        group_code: str | None,
        semester_program_id: int | None = None,
        program_family: str | None = None,
        allow_family_fallback: bool = True,
    ) -> ResolvedSemesterProgram:
        """Resolve a semester program from a study period, program, course, and stable RTU group code."""
        if semester_id is None or program_id is None or course_id is None:
            raise RTUResolutionError("Study period, study program, and course are required")

        normalized_group_code = normalize_group_code(group_code)
        if not normalized_group_code:
            raise RTUResolutionError("Group code is required")

        family_cache_key = self._program_family_key(program_family) if program_family else None
        cache_key = (
            semester_id,
            program_id,
            course_id,
            normalized_group_code,
            semester_program_id,
            family_cache_key,
            allow_family_fallback,
        )
        with self._cache_lock:
            cached = self._resolved_targets.get(cache_key)
        if cached is not None:
            return cached

        LOGGER.info(
            "Resolving RTU group_code=%s for semester_id=%s program_id=%s course_id=%s saved_semester_program_id=%s program_family=%s allow_family_fallback=%s",
            normalized_group_code,
            semester_id,
            program_id,
            course_id,
            semester_program_id,
            program_family,
            allow_family_fallback,
        )

        target: ResolvedSemesterProgram | None = None
        available_group_codes: list[str] = []
        exact_courses = self.get_courses(semester_id, program_id)
        if course_id in exact_courses:
            exact_groups = self.get_groups(semester_id, program_id, course_id)
            available_group_codes = [group.group_code for group in exact_groups]
            target = self._find_group_target(exact_groups, normalized_group_code, semester_program_id)
            if target is None:
                LOGGER.warning(
                    "Exact RTU program lookup did not resolve group_code=%s for semester_id=%s program_id=%s course_id=%s available_group_codes=%s",
                    normalized_group_code,
                    semester_id,
                    program_id,
                    course_id,
                    available_group_codes,
                )
        else:
            LOGGER.warning(
                "Exact RTU program lookup skipped because course_id=%s is not available for semester_id=%s program_id=%s exact_courses=%s",
                course_id,
                semester_id,
                program_id,
                exact_courses,
            )

        if target is None and allow_family_fallback:
            courses = self.get_family_courses(
                semester_id,
                program_id,
                program_family=program_family,
                department_code=_LOCKED_DEPARTMENT_CODE,
            )
            if course_id not in courses:
                raise RTUResolutionError(
                    f"Course {course_id} is not available for program {program_id} in study period {semester_id}"
                )

            groups = self._load_group_candidates(
                semester_id=semester_id,
                program_id=program_id,
                course_id=course_id,
                program_family=program_family,
                include_family_variants=True,
            )
            available_group_codes = [group.group_code for group in groups]
            target = self._find_group_target(groups, normalized_group_code, semester_program_id)
            if target is not None:
                LOGGER.warning(
                    "Resolved RTU group_code via family fallback: semester_id=%s program_id=%s course_id=%s group_code=%s semester_program_id=%s",
                    semester_id,
                    program_id,
                    course_id,
                    normalized_group_code,
                    target.semester_program_id,
                )

        if target is None:
            LOGGER.warning(
                "Unable to resolve RTU group_code=%s for semester_id=%s program_id=%s course_id=%s allow_family_fallback=%s available_group_codes=%s",
                normalized_group_code,
                semester_id,
                program_id,
                course_id,
                allow_family_fallback,
                available_group_codes,
            )
            if not allow_family_fallback and course_id not in exact_courses:
                raise RTUResolutionError(
                    f"Course {course_id} is not available for exact program {program_id} in study period {semester_id}"
                )
            raise RTUResolutionError(
                f"Group code {normalized_group_code} was not found for course {course_id}, study period {semester_id}, program {program_id}"
            )

        if not self.is_semester_program_published(target.semester_program_id):
            raise RTUPublicationError(
                "Schedule is not published for "
                f"{target.program_title or 'the selected program'}, course {target.course_id}, group {target.group}"
            )

        resolved = replace(target, published=True)
        with self._cache_lock:
            self._resolved_targets[cache_key] = resolved
        return resolved

    def resolve_semester_program(
        self,
        semester_id: int | None,
        program_id: int | None,
        course_id: int | None,
        group: str | None = None,
        semester_program_id: int | None = None,
        program_family: str | None = None,
    ) -> ResolvedSemesterProgram:
        """Backward-compatible resolver that prefers normalized RTU group codes."""
        normalized_group_code = normalize_group_code(group) or infer_group_code(group)
        if normalized_group_code:
            return self.resolve_group_by_code(
                semester_id=semester_id,
                program_id=program_id,
                course_id=course_id,
                group_code=normalized_group_code,
                semester_program_id=semester_program_id,
                program_family=program_family,
            )
        if semester_program_id is None:
            raise RTUResolutionError("Group code is required")
        if semester_id is None or program_id is None or course_id is None:
            raise RTUResolutionError("Study period, study program, and course are required")
        return self._resolve_group_by_semester_program_id(
            semester_id=semester_id,
            program_id=program_id,
            course_id=course_id,
            semester_program_id=semester_program_id,
            program_family=program_family,
        )

    @staticmethod
    def _allows_legacy_family_fallback(selection: ChatSelection) -> bool:
        """Return whether a saved selection looks old enough to need family fallback."""
        return not selection.program_code

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
            try:
                subjects.append(
                    Subject(
                        subject_id=int(item["subjectId"]),
                        code=str(item.get("code") or ""),
                        title=str(item.get("titleEN") or item.get("titleLV") or ""),
                        part=int(item["part"]) if item.get("part") is not None else None,
                    )
                )
            except (KeyError, TypeError, ValueError):
                LOGGER.warning("Skipping malformed RTU subject item: %r", item)
        return sorted(subjects, key=lambda subject: (subject.code, subject.title))

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

        self._homepage_html = response.text
        self._bootstrap_complete = True

    def _get_homepage_html(self) -> str:
        with self._session_lock:
            self._bootstrap()
            if self._homepage_html is None:
                raise RTUAPIError("RTU homepage is unavailable")
            return self._homepage_html

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

    def _load_programs_and_departments(
        self,
        semester_id: int,
    ) -> tuple[list[StudyProgram], list[StudyDepartment]]:
        with self._cache_lock:
            cached_programs = self._programs_cache.get(semester_id)
            cached_departments = self._departments_cache.get(semester_id)
        if cached_programs is not None and cached_departments is not None:
            return list(cached_programs), list(cached_departments)

        payload = self._post_form("findProgramsBySemesterId", {"semesterId": semester_id})
        if not isinstance(payload, list):
            raise RTUAPIError("Unexpected response from findProgramsBySemesterId")

        programs: list[StudyProgram] = []
        departments: list[StudyDepartment] = []
        logged_program_example = False
        for index, item in enumerate(payload):
            if not isinstance(item, dict):
                continue

            department_id = self._parse_optional_int(item.get("departmentId"))
            if department_id is None:
                department_id = -(index + 1)

            department_title = self._prefer_language(item, "titleEN", "titleLV")
            department_code = str(item.get("code") or "").strip() or None
            department_programs: list[StudyProgram] = []
            for program_item in item.get("program") or []:
                if not isinstance(program_item, dict):
                    continue
                if not logged_program_example:
                    LOGGER.debug(
                        "Raw RTU program object example: semester_id=%s department_code=%s raw_program=%r",
                        semester_id,
                        department_code,
                        program_item,
                    )
                    logged_program_example = True
                try:
                    parsed_program_id, parsed_title, parsed_code, debug_fields = self._extract_program_metadata(
                        program_item
                    )
                    department_programs.append(
                        StudyProgram(
                            program_id=parsed_program_id,
                            title=parsed_title,
                            code=parsed_code,
                            department_id=department_id,
                            department_title=department_title,
                            department_code=department_code,
                        )
                    )
                    LOGGER.debug(
                        "Parsed RTU program item: semester_id=%s department_code=%s title=%r code=%r program_id=%s title_field=%s code_field=%s id_field=%s",
                        semester_id,
                        department_code,
                        parsed_title,
                        parsed_code,
                        parsed_program_id,
                        debug_fields.get("program_title_field"),
                        debug_fields.get("program_code_field"),
                        debug_fields.get("program_id_field"),
                    )
                except (KeyError, TypeError, ValueError):
                    LOGGER.warning(
                        "Skipping malformed RTU study program item for semester_id=%s: %r",
                        semester_id,
                        program_item,
                    )

            if not department_programs:
                continue

            departments.append(
                StudyDepartment(
                    department_id=department_id,
                    title=department_title,
                    code=department_code,
                )
            )
            programs.extend(department_programs)

        with self._cache_lock:
            self._programs_cache[semester_id] = list(programs)
            self._departments_cache[semester_id] = list(departments)
        return programs, departments

    def _choose_representative_program(
        self,
        semester_id: int,
        variants: list[StudyProgram],
    ) -> StudyProgram:
        configured_match = next(
            (program for program in variants if program.program_id == self.settings.rtu_program_id),
            None,
        )
        if configured_match is not None:
            LOGGER.debug(
                "Representative RTU program chosen from configured default: semester_id=%s family=%s representative=%s (%s)",
                semester_id,
                self._normalize_program_family_title(configured_match.title),
                configured_match.program_id,
                configured_match.code,
            )
            return configured_match

        ranked = sorted(
            variants,
            key=lambda program: self._representative_sort_key(semester_id, program),
        )
        representative = ranked[0]
        LOGGER.debug(
            "Representative RTU program chosen: semester_id=%s family=%s representative=%s (%s)",
            semester_id,
            self._normalize_program_family_title(representative.title),
            representative.program_id,
            representative.code,
        )
        return representative

    def _representative_sort_key(
        self,
        semester_id: int,
        program: StudyProgram,
    ) -> tuple[int, int, int, int, int]:
        courses = self.get_courses(semester_id, program.program_id)
        special_groups = 0
        visible_groups = 0
        contiguous_visible_groups = 0

        for course_id in courses:
            groups = self.get_groups(semester_id, program.program_id, course_id)
            visible_numbers = [
                number
                for number in (self._parse_group_number(group.group_code) for group in groups)
                if number is not None and self._is_display_group_number(number)
            ]
            visible_numbers = sorted(set(visible_numbers))
            visible_groups += len(visible_numbers)
            contiguous_visible_groups += self._contiguous_prefix_length(visible_numbers)
            special_groups += len(groups) - len(
                [
                    group
                    for group in groups
                    if self._is_display_group(group.group_code)
                ]
            )

        return (
            -contiguous_visible_groups,
            special_groups,
            -visible_groups,
            -len(courses),
            program.program_id,
        )

    def _load_group_candidates(
        self,
        *,
        semester_id: int,
        program_id: int,
        course_id: int,
        program_family: str | None = None,
        include_family_variants: bool = True,
    ) -> list[ResolvedSemesterProgram]:
        if include_family_variants:
            family_groups = self.get_family_groups(
                semester_id,
                program_id,
                course_id,
                program_family=program_family,
                department_code=_LOCKED_DEPARTMENT_CODE,
            )
            if family_groups:
                return family_groups
        return self.get_groups(semester_id, program_id, course_id)

    def _resolve_program_family(
        self,
        *,
        semester_id: int,
        program_id: int,
        program_family: str | None = None,
        department_code: str | None = None,
    ) -> StudyProgramFamily | None:
        resolved_department_code = department_code or _LOCKED_DEPARTMENT_CODE
        families = self.get_program_families(semester_id, resolved_department_code)
        if program_family:
            family_key = self._program_family_key(program_family)
            for family in families:
                if family.family_key == family_key:
                    return family

        by_representative = self.get_program_family_by_representative_id(
            semester_id,
            resolved_department_code,
            program_id,
        )
        if by_representative is not None:
            return by_representative

        return self.get_program_family_by_program_id(
            semester_id,
            resolved_department_code,
            program_id,
        )

    @staticmethod
    def _ordered_family_variants(
        family: StudyProgramFamily,
        preferred_program_id: int,
    ) -> tuple[StudyProgram, ...]:
        return tuple(
            sorted(
                family.variants,
                key=lambda variant: (0 if variant.program_id == preferred_program_id else 1, variant.program_id),
            )
        )

    def _resolve_group_by_semester_program_id(
        self,
        semester_id: int,
        program_id: int,
        course_id: int,
        semester_program_id: int,
        program_family: str | None = None,
    ) -> ResolvedSemesterProgram:
        groups = self._load_group_candidates(
            semester_id=semester_id,
            program_id=program_id,
            course_id=course_id,
            program_family=program_family,
        )
        for candidate in groups:
            if candidate.semester_program_id == semester_program_id:
                if not self.is_semester_program_published(candidate.semester_program_id):
                    raise RTUPublicationError(
                        "Schedule is not published for "
                        f"{candidate.program_title or 'the selected program'}, course {candidate.course_id}, group {candidate.group}"
                    )
                return replace(candidate, published=True)

        LOGGER.warning(
            "semester_program_id fallback failed for semester_id=%s program_id=%s course_id=%s semester_program_id=%s available_groups=%s",
            semester_id,
            program_id,
            course_id,
            semester_program_id,
            [(group.group_code, group.semester_program_id) for group in groups],
        )
        raise RTUResolutionError(
            f"semesterProgramId {semester_program_id} was not found for course {course_id}, study period {semester_id}, program {program_id}"
        )

    @staticmethod
    def _find_group_target(
        groups: list[ResolvedSemesterProgram],
        normalized_group_code: str,
        semester_program_id: int | None,
    ) -> ResolvedSemesterProgram | None:
        matches = [
            candidate
            for candidate in groups
            if candidate.normalized_group_code() == normalized_group_code
        ]
        if not matches:
            return None

        if semester_program_id is not None:
            for candidate in matches:
                if candidate.semester_program_id == semester_program_id:
                    return candidate

        if len(matches) > 1:
            LOGGER.warning(
                "Multiple RTU groups matched group_code=%s semester_program_id=%s candidates=%s",
                normalized_group_code,
                semester_program_id,
                [(candidate.group_code, candidate.semester_program_id) for candidate in matches],
            )
        return matches[0]

    def _extract_program_metadata(
        self,
        item: dict[str, Any],
    ) -> tuple[int, str, str | None, dict[str, str | None]]:
        program_id: int | None = None
        program_id_field: str | None = None
        for field_name in _PROGRAM_ID_FIELD_CANDIDATES:
            raw_value = item.get(field_name)
            try:
                if raw_value not in (None, ""):
                    program_id = int(raw_value)
                    program_id_field = field_name
                    break
            except (TypeError, ValueError):
                continue

        if program_id is None:
            raise ValueError("RTU program item is missing a valid program ID")

        program_code: str | None = None
        program_code_field: str | None = None
        for field_name in _PROGRAM_CODE_FIELD_CANDIDATES:
            raw_value = item.get(field_name)
            if raw_value in (None, ""):
                continue
            candidate_code = str(raw_value).strip()
            if candidate_code:
                program_code = candidate_code
                program_code_field = field_name
                break

        program_title: str | None = None
        program_title_field: str | None = None
        for field_name in _PROGRAM_TITLE_FIELD_CANDIDATES:
            raw_value = item.get(field_name)
            if raw_value in (None, ""):
                continue
            candidate_title = self._normalize_program_family_title(str(raw_value))
            if candidate_title:
                program_title = candidate_title
                program_title_field = field_name
                break

        if not program_title and program_code:
            program_title = program_code
            program_title_field = program_code_field
        if not program_title:
            raise ValueError("RTU program item is missing a readable title")

        return program_id, program_title, program_code, {
            "program_id_field": program_id_field,
            "program_code_field": program_code_field,
            "program_title_field": program_title_field,
        }

    def _extract_group_metadata(
        self,
        item: dict[str, Any],
        *,
        semester_id: int,
        program_id: int,
        course_id: int,
    ) -> tuple[str, str | None, int | None, dict[str, str | None]]:
        group_payload = item.get("group")
        candidate_values = self._collect_group_candidate_values(item, group_payload)

        group_code: str | None = None
        group_code_field: str | None = None
        for field_name, raw_value in candidate_values["code"]:
            candidate_code = infer_group_code(raw_value)
            if candidate_code:
                group_code = candidate_code
                group_code_field = field_name
                break

        if not group_code:
            LOGGER.warning(
                "Skipping RTU group item without a resolvable group code: semester_id=%s program_id=%s course_id=%s keys=%s item=%r",
                semester_id,
                program_id,
                course_id,
                sorted(item.keys()),
                item,
            )
            raise ValueError("RTU group item is missing a resolvable group code")

        group_name: str | None = None
        group_name_field: str | None = None
        for field_name, raw_value in candidate_values["name"]:
            candidate_name = clean_group_label(str(raw_value)) if raw_value not in (None, "") else None
            if candidate_name:
                group_name = candidate_name
                group_name_field = field_name
                break

        if group_name is None:
            raw_group_name = clean_group_label(group_payload if not isinstance(group_payload, dict) else None)
            if raw_group_name:
                group_name = raw_group_name
                group_name_field = "group"

        if group_name and normalize_group_code(group_name) == group_code:
            group_name = None

        group_id: int | None = None
        group_id_field: str | None = None
        for field_name, raw_value in candidate_values["id"]:
            try:
                if raw_value not in (None, ""):
                    group_id = int(raw_value)
                    group_id_field = field_name
                    break
            except (TypeError, ValueError):
                continue

        return group_code, group_name, group_id, {
            "group_code_field": group_code_field,
            "group_name_field": group_name_field,
            "group_id_field": group_id_field,
        }

    @staticmethod
    def _collect_group_candidate_values(
        item: dict[str, Any],
        group_payload: Any,
    ) -> dict[str, list[tuple[str, Any]]]:
        candidates: dict[str, list[tuple[str, Any]]] = {
            "code": [],
            "name": [],
            "id": [],
        }
        for field_name in _GROUP_CODE_FIELD_CANDIDATES:
            if field_name in item:
                candidates["code"].append((field_name, item.get(field_name)))
        for field_name in _GROUP_NAME_FIELD_CANDIDATES:
            if field_name in item:
                candidates["name"].append((field_name, item.get(field_name)))
        for field_name in _GROUP_ID_FIELD_CANDIDATES:
            if field_name in item:
                candidates["id"].append((field_name, item.get(field_name)))

        if isinstance(group_payload, dict):
            for field_name in _GROUP_CODE_FIELD_CANDIDATES:
                if field_name in group_payload:
                    candidates["code"].append((f"group.{field_name}", group_payload.get(field_name)))
            for field_name in _GROUP_NAME_FIELD_CANDIDATES:
                if field_name in group_payload:
                    candidates["name"].append((f"group.{field_name}", group_payload.get(field_name)))
            for field_name in _GROUP_ID_FIELD_CANDIDATES:
                if field_name in group_payload:
                    candidates["id"].append((f"group.{field_name}", group_payload.get(field_name)))

        return candidates

    @staticmethod
    def _normalize_program_family_title(title: str) -> str:
        cleaned = _WHITESPACE_PATTERN.sub(" ", str(title).strip())
        cleaned = cleaned.replace(" ,", ",")
        cleaned = re.sub(r"\s*,\s*", ", ", cleaned)
        return cleaned.strip()

    def _program_family_key(self, title: str) -> str:
        return self._normalize_program_family_title(title).casefold()

    def _choose_program_family_display_name(self, variants: list[StudyProgram]) -> str:
        cleaned_titles = [self._normalize_program_family_title(item.title) for item in variants]
        prettified = [self._prettify_program_family_title(title) for title in cleaned_titles]
        return sorted(
            prettified,
            key=lambda title: (
                -self._title_case_score(title),
                len(title),
                title.casefold(),
            ),
        )[0]

    @staticmethod
    def _prettify_program_family_title(title: str) -> str:
        parts = title.split()
        prettified: list[str] = []
        for index, part in enumerate(parts):
            word = part if part.isupper() else part.capitalize()
            if index > 0 and word.lower().strip(",") in _SMALL_TITLE_WORDS:
                if word.endswith(","):
                    word = f"{word[:-1].lower()},"
                else:
                    word = word.lower()
            prettified.append(word)
        return " ".join(prettified)

    @staticmethod
    def _title_case_score(title: str) -> int:
        score = 0
        for word in title.split():
            plain = word.strip(",")
            if plain and plain[0].isupper():
                score += 1
        return score

    @staticmethod
    def _parse_group_number(group: str) -> int | None:
        cleaned = normalize_group_code(group)
        if not cleaned.isdigit():
            return None
        return int(cleaned)

    @classmethod
    def _is_display_group_number(cls, number: int) -> bool:
        return 1 <= number < 100 and number != 800

    @classmethod
    def _is_display_group(cls, group: str) -> bool:
        number = cls._parse_group_number(group)
        return number is not None and cls._is_display_group_number(number)

    @classmethod
    def _group_sort_key(cls, group: str) -> tuple[int, int, str]:
        number = cls._parse_group_number(group)
        if number is None:
            return (2, 0, str(group))
        if cls._is_display_group_number(number):
            return (0, number, str(group))
        return (1, number, str(group))

    @staticmethod
    def _contiguous_prefix_length(numbers: list[int]) -> int:
        expected = 1
        for number in numbers:
            if number != expected:
                break
            expected += 1
        return expected - 1

    @staticmethod
    def _compose_study_period_title(title: str, short_name: str | None) -> str:
        if not short_name:
            return title
        if short_name in title:
            return title
        return f"{title} ({short_name})"

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

    def _parse_date(self, value: Any) -> date:
        if value is None:
            raise RTUAPIError("Value is missing a date")
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
            raise RTUAPIError(f"Unable to parse date value: {raw}") from exc

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
