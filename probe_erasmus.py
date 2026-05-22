"""Throwaway probe: compare RTU subject titles vs event titles for ADBD0."""
from __future__ import annotations

import sys
from datetime import date, timedelta

from config import Settings
from rtu_api import RTUScheduleClient
from models import (
    event_matches_picked_subject,
    extract_event_subject_title,
    normalize_subject_title,
)

# Force UTF-8 stdout for Windows console (so Latvian chars don't crash prints)
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def main() -> None:
    settings = Settings.from_env()
    client = RTUScheduleClient(settings)

    sem_id = settings.rtu_semester_id
    dept = settings.rtu_department_code

    # find ADBD0
    variants = client.get_department_program_variants_by_title(sem_id, dept, "Computer Systems")
    print("Computer Systems variants:")
    for v in variants:
        print(f"  program_id={v.program_id} code={v.code!r} title={v.title!r}")

    target = next((v for v in variants if (v.code or "").upper() == "ADBD0"), None)
    if target is None:
        # try other commonly-named Erasmus programs to find one
        print("ADBD0 not in Computer Systems, scanning all titles...")
        for title in client.get_department_program_titles(sem_id, dept):
            vs = client.get_department_program_variants_by_title(sem_id, dept, title)
            for v in vs:
                if (v.code or "").upper() == "ADBD0":
                    target = v
                    print(f"  found ADBD0 under title={title!r} program_id={v.program_id}")
                    break
            if target:
                break
    if target is None:
        print("ADBD0 NOT FOUND")
        client.close()
        return

    print(f"\nUsing ADBD0 program_id={target.program_id}")
    courses = client.get_courses(sem_id, target.program_id)
    print(f"Courses: {courses}")

    today = date.today()
    span_start = today - timedelta(days=30)
    span_end = today + timedelta(days=14)

    for cid in courses:
        groups = client.get_display_groups(sem_id, target.program_id, cid, None, include_family_variants=False)
        if not groups:
            print(f"  course {cid}: no groups")
            continue
        g = groups[0]
        print(f"\n--- Course {cid} | semester_program_id={g.semester_program_id} group={g.group_code} ---")
        subjects = client.get_subjects(g.semester_program_id)
        subj_titles_norm = {normalize_subject_title(s.title): s.title for s in subjects}
        print(f"  Subjects ({len(subjects)}):")
        for s in subjects:
            print(f"    code={s.code!r} title={s.title!r}")

        events = client.get_events_for_range(g.semester_program_id, span_start, span_end)
        print(f"  Events in {span_start}..{span_end}: {len(events)}")
        event_title_set: set[str] = set()
        for e in events:
            event_title_set.add(e.title)
        picked_norms = list(subj_titles_norm.keys())
        for et in sorted(event_title_set):
            # find the corresponding lecturer text for this event (first occurrence)
            lec = next((e.lecturer for e in events if e.title == et), None)
            extracted = extract_event_subject_title(et, lec)
            ext_norm = normalize_subject_title(extracted)
            matched = any(event_matches_picked_subject(ext_norm, p) for p in picked_norms)
            naive = normalize_subject_title(et) in subj_titles_norm
            print(
                f"    event_title={et!r}\n"
                f"      extracted={extracted!r} -> match(new)={matched}  match(old)={naive}"
            )

    client.close()


if __name__ == "__main__":
    main()
