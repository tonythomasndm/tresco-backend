from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.core.config import get_settings
from app.models.platform_models import (
    LinkedInEducationModel,
    LinkedInExperienceModel,
    LinkedInModel,
    LinkedInSkillModel,
)
from app.utils.helpers import coerce_bool, coerce_int, safe_float


LINKEDIN_COMPANY_PRESTIGE = {
    "openai": 100,
    "deepmind": 100,
    "google": 100,
    "alphabet": 100,
    "meta": 97,
    "facebook": 97,
    "apple": 96,
    "microsoft": 95,
    "amazon": 94,
    "netflix": 94,
    "nvidia": 94,
    "tesla": 92,
    "uber": 88,
    "atlassian": 88,
    "salesforce": 88,
    "adobe": 87,
    "oracle": 84,
    "ibm": 82,
    "deloitte": 78,
    "accenture": 75,
    "infosys": 72,
    "tcs": 70,
    "wipro": 68,
    "capgemini": 68,
}


def linkedin_text(value: Any, separator: str = ", ") -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        items = [linkedin_text(item, separator=separator) for item in value]
        return separator.join(item for item in items if item)
    if isinstance(value, dict):
        for key in ("name", "title", "job_title", "company_name", "authority", "organization"):
            text = linkedin_text(value.get(key))
            if text:
                return text
        return json.dumps(value, default=str, sort_keys=True)
    return str(value).strip()


def dedupe_non_empty(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = linkedin_text(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def parse_linkedin_date(value: Any, today: datetime | None = None) -> datetime | None:
    today = today or datetime.now(timezone.utc)
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, dict):
        year = coerce_int(value.get("year"))
        if year <= 0:
            return None
        month = min(max(coerce_int(value.get("month"), 1), 1), 12)
        day = min(max(coerce_int(value.get("day"), 1), 1), 28)
        return datetime(year, month, day, tzinfo=timezone.utc)

    text = str(value).strip()
    if not text:
        return None

    lowered = text.lower()
    if lowered in {"present", "current", "now", "ongoing", "today", "till date"}:
        return today

    normalized = text.replace("/", "-")
    for candidate in (normalized, text):
        for fmt in ("%m-%Y", "%Y-%m", "%Y-%m-%d", "%m-%d-%Y", "%b %Y", "%B %Y", "%Y"):
            try:
                parsed = datetime.strptime(candidate, fmt)
                if fmt in {"%m-%Y", "%Y-%m", "%b %Y", "%B %Y", "%Y"}:
                    parsed = parsed.replace(day=1)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def linkedin_month_index(dt: datetime) -> int:
    return dt.year * 12 + dt.month


def linkedin_months_between(start_dt: datetime | None, end_dt: datetime | None) -> int:
    if not start_dt or not end_dt:
        return 0
    if end_dt < start_dt:
        end_dt = start_dt
    return (linkedin_month_index(end_dt) - linkedin_month_index(start_dt)) + 1


def format_linkedin_date(value: Any) -> str:
    dt = parse_linkedin_date(value) if not isinstance(value, datetime) else value
    return dt.date().isoformat() if dt else ""


def normalize_linkedin_experience_rows(
    linkedin_raw_data: dict[str, Any],
    today: datetime | None = None,
) -> list[dict[str, Any]]:
    today = today or datetime.now(timezone.utc)
    experiences = linkedin_raw_data.get("experience") or linkedin_raw_data.get("experiences") or []
    rows: list[dict[str, Any]] = []

    for raw_entry in experiences:
        if not isinstance(raw_entry, dict):
            continue

        start_dt = parse_linkedin_date(raw_entry.get("job_started_on") or raw_entry.get("start_date"), today=today)
        end_source = raw_entry.get("job_ended_on") or raw_entry.get("end_date")
        is_current = coerce_bool(raw_entry.get("job_still_working")) or (bool(start_dt) and not end_source)
        end_dt = parse_linkedin_date(end_source, today=today)
        if is_current and start_dt:
            end_dt = today
        if start_dt and end_dt and end_dt < start_dt:
            end_dt = start_dt

        rows.append(
            {
                "experience_index": 0,
                "job_title": linkedin_text(
                    raw_entry.get("job_title") or raw_entry.get("title") or raw_entry.get("raw_job_title")
                ),
                "company_name": linkedin_text(
                    raw_entry.get("company_name") or raw_entry.get("company") or raw_entry.get("raw_company_name")
                ),
                "employment_type": linkedin_text(raw_entry.get("employment_type")),
                "start_date": format_linkedin_date(start_dt),
                "end_date": "" if is_current else format_linkedin_date(end_dt),
                "start_year": start_dt.year if start_dt else None,
                "start_month": start_dt.month if start_dt else None,
                "end_year": None if (is_current or not end_dt) else end_dt.year,
                "end_month": None if (is_current or not end_dt) else end_dt.month,
                "duration_months": linkedin_months_between(start_dt, end_dt) if start_dt and end_dt else 0,
                "is_current": is_current,
                "company_industry": linkedin_text(raw_entry.get("company_industry")),
                "company_headcount_range": linkedin_text(raw_entry.get("company_headcount_range")),
                "company_id": linkedin_text(raw_entry.get("company_id")),
                "company_url": linkedin_text(raw_entry.get("company_url")),
                "company_website": linkedin_text(raw_entry.get("company_website")),
                "job_location": linkedin_text(raw_entry.get("job_location")),
                "job_location_city": linkedin_text(raw_entry.get("job_location_city")),
                "job_location_state": linkedin_text(raw_entry.get("job_location_state")),
                "job_location_country": linkedin_text(raw_entry.get("job_location_country")),
                "job_description": linkedin_text(raw_entry.get("job_description")),
                "raw_job_title": linkedin_text(raw_entry.get("raw_job_title")),
                "raw_company_name": linkedin_text(raw_entry.get("raw_company_name")),
            }
        )

    rows.sort(
        key=lambda row: (
            1 if row["is_current"] else 0,
            row["start_year"] or 0,
            row["start_month"] or 0,
            row["end_year"] or 0,
            row["end_month"] or 0,
        ),
        reverse=True,
    )

    for index, row in enumerate(rows, start=1):
        row["experience_index"] = index

    return rows


def normalize_linkedin_education_rows(
    linkedin_raw_data: dict[str, Any],
    today: datetime | None = None,
) -> list[dict[str, Any]]:
    today = today or datetime.now(timezone.utc)
    education_rows = linkedin_raw_data.get("education") or []
    rows: list[dict[str, Any]] = []

    for raw_entry in education_rows:
        if not isinstance(raw_entry, dict):
            continue

        start_dt = parse_linkedin_date(raw_entry.get("started_on") or raw_entry.get("start_date"), today=today)
        end_dt = parse_linkedin_date(raw_entry.get("ended_on") or raw_entry.get("end_date"), today=today)
        if start_dt and end_dt and end_dt < start_dt:
            end_dt = start_dt
        is_current = bool(start_dt and not end_dt)

        fields = raw_entry.get("fields_of_study") or raw_entry.get("degree") or []
        if isinstance(fields, str):
            fields = [fields]

        rows.append(
            {
                "education_index": 0,
                "university_name": linkedin_text(raw_entry.get("university_name")),
                "fields_of_study": ", ".join(dedupe_non_empty(fields)),
                "start_date": format_linkedin_date(start_dt),
                "end_date": "" if is_current else format_linkedin_date(end_dt),
                "start_year": start_dt.year if start_dt else None,
                "start_month": start_dt.month if start_dt else None,
                "end_year": None if (is_current or not end_dt) else end_dt.year,
                "end_month": None if (is_current or not end_dt) else end_dt.month,
                "duration_months": linkedin_months_between(start_dt, end_dt) if start_dt and end_dt else 0,
                "is_current": is_current,
                "grade": linkedin_text(raw_entry.get("grade")),
                "description": linkedin_text(raw_entry.get("description")),
                "social_url": linkedin_text(raw_entry.get("social_url")),
                "university_id": linkedin_text(raw_entry.get("university_id")),
                "logo": linkedin_text(raw_entry.get("logo")),
            }
        )

    rows.sort(
        key=lambda row: (
            row["end_year"] or 0,
            row["end_month"] or 0,
            row["start_year"] or 0,
            row["start_month"] or 0,
        ),
        reverse=True,
    )

    for index, row in enumerate(rows, start=1):
        row["education_index"] = index

    return rows


def normalize_linkedin_skill_rows(linkedin_raw_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_skill in linkedin_raw_data.get("skills") or []:
        if isinstance(raw_skill, dict):
            skill_name = linkedin_text(raw_skill.get("name") or raw_skill.get("skill"))
            endorsement_count = raw_skill.get("endorsement_count") or raw_skill.get("endorsements")
        else:
            skill_name = linkedin_text(raw_skill)
            endorsement_count = None
        if not skill_name or skill_name in seen:
            continue
        seen.add(skill_name)
        rows.append(
            {
                "skill_index": 0,
                "skill_name": skill_name,
                "endorsement_count": coerce_int(endorsement_count, 0) if endorsement_count is not None else None,
            }
        )

    for index, row in enumerate(rows, start=1):
        row["skill_index"] = index

    return rows


def linkedin_title_level(title: str) -> int:
    lowered = (title or "").lower()
    if not lowered:
        return 0

    level = 0
    keyword_groups = (
        (1, ("intern", "student", "ambassador", "apprentice", "trainee", "volunteer")),
        (2, ("assistant", "associate", "junior", "analyst", "fellow")),
        (3, ("engineer", "developer", "consultant", "scientist", "specialist", "researcher", "teacher", "advisor", "producer", "member")),
        (4, ("senior", "lead", "manager", "architect", "owner", "founder")),
        (5, ("staff", "principal", "director", "head", "vice president", "vp", "chief", "president")),
    )
    for score, keywords in keyword_groups:
        if any(keyword in lowered for keyword in keywords):
            level = max(level, score)
    return level or 3


def compute_linkedin_timeline_metrics(
    experience_rows: list[dict[str, Any]],
    education_rows: list[dict[str, Any]],
    today: datetime | None = None,
) -> dict[str, Any]:
    today = today or datetime.now(timezone.utc)

    experience_ranges: list[tuple[datetime, datetime, dict[str, Any]]] = []
    for row in experience_rows:
        start_dt = parse_linkedin_date(row.get("start_date"), today=today)
        end_dt = today if row.get("is_current") else parse_linkedin_date(row.get("end_date"), today=today)
        if not start_dt:
            continue
        if not end_dt or end_dt < start_dt:
            end_dt = start_dt
        experience_ranges.append((start_dt, end_dt, row))

    covered_months: set[int] = set()
    sorted_experience = sorted(experience_ranges, key=lambda item: item[0])
    role_durations: list[int] = []
    title_levels: list[int] = []
    company_names: set[str] = set()

    for start_dt, end_dt, row in sorted_experience:
        role_durations.append(linkedin_months_between(start_dt, end_dt))
        title_levels.append(linkedin_title_level(row.get("job_title", "")))
        if row.get("company_name"):
            company_names.add(row["company_name"])
        for month_idx in range(linkedin_month_index(start_dt), linkedin_month_index(end_dt) + 1):
            covered_months.add(month_idx)

    gaps: list[int] = []
    for (_, previous_end, _), (next_start, _, _) in zip(sorted_experience, sorted_experience[1:]):
        gap_months = linkedin_month_index(next_start) - linkedin_month_index(previous_end) - 1
        if gap_months > 0:
            gaps.append(gap_months)

    education_ranges: list[tuple[datetime | None, datetime | None, dict[str, Any]]] = []
    for row in education_rows:
        start_dt = parse_linkedin_date(row.get("start_date"), today=today)
        end_dt = today if row.get("is_current") else parse_linkedin_date(row.get("end_date"), today=today)
        if start_dt and (not end_dt or end_dt < start_dt):
            end_dt = start_dt
        education_ranges.append((start_dt, end_dt, row))

    dated_education = [entry for entry in education_ranges if entry[0] or entry[1]]
    valid_education = [entry for entry in dated_education if not entry[0] or not entry[1] or entry[1] >= entry[0]]
    reasonable_education = [
        entry
        for entry in valid_education
        if not entry[0] or not entry[1] or 3 <= linkedin_months_between(entry[0], entry[1]) <= 96
    ]

    progression_steps = sum(
        1
        for previous_level, next_level in zip(title_levels, title_levels[1:])
        if next_level > previous_level
    )

    return {
        "total_experience_months": len(covered_months),
        "average_tenure_months": (sum(role_durations) / len(role_durations)) if role_durations else 0.0,
        "longest_gap_months": max(gaps, default=0),
        "total_gap_months": sum(gaps),
        "current_roles": sum(1 for _, _, row in experience_ranges if row.get("is_current")),
        "positions_count": len(experience_ranges),
        "unique_companies": len(company_names),
        "seniority_delta": max(0, title_levels[-1] - title_levels[0]) if len(title_levels) >= 2 else 0,
        "progression_steps": progression_steps,
        "dated_education_count": len(dated_education),
        "valid_education_ratio": (len(valid_education) / len(dated_education)) if dated_education else 0.0,
        "reasonable_education_ratio": (len(reasonable_education) / len(dated_education)) if dated_education else 0.0,
    }


def estimate_linkedin_company_prestige(experience_rows: list[dict[str, Any]]) -> float:
    best_score = 0.0
    for row in experience_rows:
        company_name = (row.get("company_name") or "").lower()
        matched_score = max(
            (score for keyword, score in LINKEDIN_COMPANY_PRESTIGE.items() if keyword in company_name),
            default=0,
        )
        if matched_score:
            best_score = max(best_score, matched_score)
            continue

        headcount = row.get("company_headcount_range") or ""
        if headcount.startswith(("10001", "5001", "1001")):
            best_score = max(best_score, 65.0)
        elif headcount:
            best_score = max(best_score, 55.0)

    if best_score:
        return best_score
    return 50.0 if experience_rows else 0.0


def prepare_linkedin_profile(
    linkedin_raw_data: dict[str, Any],
    fallback_profile_url: str = "",
    today: datetime | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    today = today or datetime.now(timezone.utc)
    experience_rows = normalize_linkedin_experience_rows(linkedin_raw_data, today=today)
    education_rows = normalize_linkedin_education_rows(linkedin_raw_data, today=today)
    skill_rows = normalize_linkedin_skill_rows(linkedin_raw_data)
    timeline_metrics = compute_linkedin_timeline_metrics(experience_rows, education_rows, today=today)

    certs = linkedin_raw_data.get("certification") or linkedin_raw_data.get("certifications") or []
    recs = linkedin_raw_data.get("recommendations_received") or linkedin_raw_data.get("recommendations") or []

    experience_titles = dedupe_non_empty(row.get("job_title") for row in experience_rows)
    experience_companies = dedupe_non_empty(row.get("company_name") for row in experience_rows)
    education_details: list[str] = []
    for row in education_rows:
        school = row.get("university_name", "")
        field = row.get("fields_of_study", "")
        detail = f"{school} ({field})" if school and field else school or field
        if detail:
            education_details.append(detail)

    current_companies = [
        row["company_name"]
        for row in experience_rows
        if row.get("is_current") and row.get("company_name")
    ]
    skill_names = [row["skill_name"] for row in skill_rows if row.get("skill_name")]

    linkedin_profile = {
        "li_name": linkedin_raw_data.get("full_name") or linkedin_raw_data.get("display_name"),
        "li_headline": linkedin_raw_data.get("profile_headline") or linkedin_raw_data.get("headline"),
        "li_summary": linkedin_raw_data.get("description") or linkedin_raw_data.get("summary") or linkedin_raw_data.get("about"),
        "li_location": linkedin_raw_data.get("location"),
        "li_profile_url": linkedin_raw_data.get("profile_link") or fallback_profile_url,
        "li_followers": linkedin_raw_data.get("followers") or linkedin_raw_data.get("followers_count"),
        "li_connections": linkedin_raw_data.get("connections") or linkedin_raw_data.get("connections_count"),
        "li_num_positions": len(experience_rows),
        "li_total_exp_months": timeline_metrics["total_experience_months"],
        "li_avg_tenure_months": round(timeline_metrics["average_tenure_months"], 1),
        "li_longest_gap_months": timeline_metrics["longest_gap_months"],
        "li_num_current_roles": timeline_metrics["current_roles"],
        "li_exp_titles": " | ".join(experience_titles),
        "li_exp_companies": " | ".join(experience_companies),
        "li_num_education": len(education_rows),
        "li_edu_details": " | ".join(education_details),
        "li_num_skills": len(skill_names),
        "li_skills": ", ".join(skill_names[:30]),
        "li_num_certs": len(certs),
        "li_cert_names": ", ".join(
            dedupe_non_empty((cert.get("name") if isinstance(cert, dict) else cert) for cert in certs)
        ),
        "li_num_recommendations": len(recs),
        "li_has_photo": bool(linkedin_raw_data.get("avatar_url") or linkedin_raw_data.get("profile_picture")),
        "li_has_summary": bool(linkedin_raw_data.get("description") or linkedin_raw_data.get("summary") or linkedin_raw_data.get("about")),
        "li_has_experience": bool(experience_rows),
        "li_has_education": bool(education_rows),
        "li_has_skills": bool(skill_rows),
        "li_has_certs": bool(certs),
        "li_has_recommendations": bool(recs),
        "li_current_company": current_companies[0] if current_companies else linkedin_raw_data.get("current_company_name"),
        "li_country": linkedin_raw_data.get("country"),
    }

    return linkedin_profile, experience_rows, education_rows, skill_rows, timeline_metrics


def scrape_linkedin_apify(
    profile_url: str,
    apify_token: str,
    actor_id: str,
) -> dict[str, Any]:
    if not profile_url or not apify_token:
        return {}

    try:
        from apify_client import ApifyClient
    except Exception:
        return {}

    try:
        client = ApifyClient(apify_token)
        run = client.actor(actor_id).call(
            run_input={
                "urls": [profile_url],
                "resolveEmails": False,
            }
        )
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            return {}
        for item in client.dataset(dataset_id).iterate_items():
            if isinstance(item, dict):
                return item
    except Exception:
        return {}

    return {}


def normalize_apify_to_datamagnet(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict) or not raw:
        return {}

    experiences: list[dict[str, Any]] = []
    for exp in (raw.get("experiences") or []):
        if not isinstance(exp, dict):
            continue
        start = exp.get("startedOn") or exp.get("startDate") or {}
        end = exp.get("endedOn") or exp.get("endDate") or {}
        experiences.append(
            {
                "job_title": exp.get("title"),
                "company_name": exp.get("companyName"),
                "employment_type": exp.get("employmentType"),
                "job_started_on": start,
                "job_ended_on": end,
                "job_still_working": exp.get("isCurrent", False),
                "company_industry": exp.get("companyIndustry"),
                "company_headcount_range": exp.get("companyStaffCountRange"),
                "company_id": exp.get("companyId"),
                "company_url": exp.get("companyLinkedinUrl"),
                "company_website": exp.get("companyWebsite"),
                "job_location": exp.get("location"),
                "job_description": exp.get("description"),
            }
        )

    educations: list[dict[str, Any]] = []
    for edu in (raw.get("educations") or raw.get("education") or []):
        if not isinstance(edu, dict):
            continue
        start = edu.get("startedOn") or edu.get("startDate") or {}
        end = edu.get("endedOn") or edu.get("endDate") or {}
        field = edu.get("fieldOfStudy") or edu.get("degreeName")
        fields = [field] if isinstance(field, str) and field.strip() else field
        educations.append(
            {
                "university_name": edu.get("schoolName"),
                "fields_of_study": fields or [],
                "started_on": start,
                "ended_on": end,
                "grade": edu.get("grade"),
                "description": edu.get("description"),
                "social_url": edu.get("schoolUrl"),
                "university_id": edu.get("schoolId"),
                "logo": edu.get("schoolLogo"),
            }
        )

    skills: list[dict[str, Any]] = []
    for raw_skill in (raw.get("skills") or []):
        if isinstance(raw_skill, dict):
            name = raw_skill.get("name") or raw_skill.get("skill")
            if not name:
                continue
            skills.append(
                {
                    "name": name,
                    "endorsement_count": raw_skill.get("endorsementCount") or raw_skill.get("endorsements"),
                }
            )
        elif isinstance(raw_skill, str) and raw_skill.strip():
            skills.append({"name": raw_skill.strip(), "endorsement_count": None})

    certifications: list[dict[str, Any]] = []
    for cert in (raw.get("certifications") or []):
        if not isinstance(cert, dict):
            continue
        certifications.append(
            {
                "name": cert.get("name"),
                "authority": cert.get("issuingOrganization") or cert.get("authority"),
            }
        )

    recommendations = raw.get("recommendations") or raw.get("recommendationsReceived") or []
    current_company = next(
        (
            exp.get("company_name")
            for exp in experiences
            if exp.get("job_still_working") and exp.get("company_name")
        ),
        None,
    )
    full_name = raw.get("fullName")
    if not full_name:
        full_name = " ".join(part for part in [raw.get("firstName"), raw.get("lastName")] if part) or None

    return {
        "full_name": full_name,
        "display_name": full_name,
        "profile_headline": raw.get("headline"),
        "description": raw.get("about") or raw.get("summary"),
        "location": raw.get("location"),
        "profile_link": raw.get("linkedinUrl") or raw.get("profileUrl"),
        "followers": raw.get("followersCount") or raw.get("followers"),
        "followers_count": raw.get("followersCount") or raw.get("followers"),
        "connections": raw.get("connectionsCount") or raw.get("connections"),
        "connections_count": raw.get("connectionsCount") or raw.get("connections"),
        "avatar_url": raw.get("profilePicture") or raw.get("profilePic"),
        "country": raw.get("country"),
        "current_company_name": current_company,
        "experience": experiences,
        "education": educations,
        "skills": skills,
        "certification": certifications,
        "certifications": certifications,
        "recommendations_received": recommendations,
        "recommendations": recommendations,
        "featured": raw.get("featured") or [],
        "publication": raw.get("publications") or [],
        "project": raw.get("projects") or [],
        "volunteering": raw.get("volunteeringExperiences") or raw.get("volunteering") or [],
    }


class LinkedInService:
    def __init__(self) -> None:
        self.settings = get_settings()

    def fetch_profile(self, profile_url: str) -> LinkedInModel | None:
        if not profile_url or not self.settings.apify_token:
            return None

        raw_apify = scrape_linkedin_apify(
            profile_url=profile_url,
            apify_token=self.settings.apify_token,
            actor_id=self.settings.apify_linkedin_actor_id,
        )
        data = normalize_apify_to_datamagnet(raw_apify)
        if not isinstance(data, dict) or not data:
            return None

        normalized_profile, experience_rows, education_rows, skill_rows, timeline_metrics = prepare_linkedin_profile(
            data,
            fallback_profile_url=profile_url,
        )

        return LinkedInModel(
            profile_url=normalized_profile.get("li_profile_url") or profile_url,
            name=normalized_profile.get("li_name"),
            headline=normalized_profile.get("li_headline"),
            summary=normalized_profile.get("li_summary"),
            location=normalized_profile.get("li_location"),
            followers=coerce_int(normalized_profile.get("li_followers")),
            connections=coerce_int(normalized_profile.get("li_connections")),
            current_company=normalized_profile.get("li_current_company"),
            country=normalized_profile.get("li_country"),
            total_experience_months=coerce_int(normalized_profile.get("li_total_exp_months")),
            certifications=data.get("certification") or data.get("certifications") or [],
            recommendations=data.get("recommendations_received") or data.get("recommendations") or [],
            publications=data.get("publication") or data.get("publications") or [],
            projects=data.get("project") or data.get("projects") or [],
            featured=data.get("featured") or [],
            volunteering=data.get("volunteering") or [],
            experience=[LinkedInExperienceModel(**row) for row in experience_rows],
            education=[LinkedInEducationModel(**row) for row in education_rows],
            skills=[LinkedInSkillModel(**row) for row in skill_rows],
            timeline_metrics=timeline_metrics,
            normalized_profile=normalized_profile,
            source_payload=data,
        )
