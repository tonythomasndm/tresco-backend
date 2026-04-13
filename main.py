
# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — INSTALL DEPENDENCIES                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

import builtins
import sys
import traceback

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
import time
import random
from typing import Any, Dict
from datetime import datetime, timezone
import fitz          # PyMuPDF
import pdfplumber
import requests
import re
import time
import json
import math
import pandas as pd
from urllib.parse import urlparse
from datetime import datetime, timezone
from pydantic import BaseModel


# 🔥 Supabase credentials
SUPABASE_URL = "https://uankwdgpnouwmtgcainy.supabase.co"
SUPABASE_KEY = "sb_publishable_8a7DY7P5uPa8zZmQF9OKSQ_JLLM_aJt"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()
origins = [
    "http://localhost:5173",  # your frontend (Vite)
    "http://localhost:3000",  # if React
    "https://tresco.vercel.app"  # (for testing only)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],   # IMPORTANT
    allow_headers=["*"],
)

def default_error_code(status_code: int) -> str:
    return {
        400: "bad_request",
        404: "not_found",
        422: "invalid_request",
        500: "internal_server_error",
        502: "bad_gateway",
        503: "service_unavailable",
    }.get(status_code, "request_failed")


def build_error_payload(error_code: str, message: str, details: Any = None) -> dict:
    error = {
        "code": error_code,
        "message": message,
    }
    if details is not None:
        error["details"] = details
    return {
        "status": "error",
        "error": error,
    }


def raise_api_error(status_code: int, error_code: str, message: str, details: Any = None):
    payload = {
        "code": error_code,
        "message": message,
    }
    if details is not None:
        payload["details"] = details
    raise HTTPException(status_code=status_code, detail=payload)


@app.exception_handler(HTTPException)
async def http_exception_handler(_request: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, dict):
        error_code = detail.get("code") or default_error_code(exc.status_code)
        message = detail.get("message") or "Request failed."
        details = detail.get("details")
    else:
        error_code = default_error_code(exc.status_code)
        message = str(detail)
        details = None

    return JSONResponse(
        status_code=exc.status_code,
        content=build_error_payload(error_code, message, details),
    )


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(_request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content=build_error_payload(
            "invalid_request",
            "Request validation failed.",
            exc.errors(),
        ),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception):
    traceback.print_exception(type(exc), exc, exc.__traceback__)
    return JSONResponse(
        status_code=500,
        content=build_error_payload(
            "internal_server_error",
            "An unexpected error occurred while processing the request.",
        ),
    )


def safe_log(*args, **kwargs):
    try:
        builtins.print(*args, **kwargs)
    except UnicodeEncodeError:
        sep = kwargs.get("sep")
        end = kwargs.get("end")
        stream = kwargs.get("file") or sys.stdout
        flush = kwargs.get("flush", False)

        if sep is None:
            sep = " "
        if end is None:
            end = "\n"

        encoding = getattr(stream, "encoding", None) or sys.getdefaultencoding() or "utf-8"
        message = sep.join(str(arg) for arg in args)
        try:
            safe_message = message.encode(encoding, errors="backslashreplace").decode(encoding)
        except LookupError:
            safe_message = message.encode("utf-8", errors="backslashreplace").decode("utf-8")

        builtins.print(safe_message, end=end, file=stream, flush=flush)


print = safe_log
# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — INPUT PARAMETERS  (edit these before running)                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
def get_platform_links_by_user(user_id: str):
    normalized_user_id = (user_id or "").strip()
    if not normalized_user_id:
        raise_api_error(
            400,
            "invalid_user_id",
            "`user_id` must not be blank.",
            {"field": "user_id"},
        )

    get_user_or_error(normalized_user_id)
    accounts = get_platform_accounts_by_user(normalized_user_id, include_profile_url=True)

    platform_links = {}
    for acc in accounts:
        platform_name = normalize_platform_name(acc.get("platform_name", ""))
        if platform_name == "stack_overflow":
            platform_name = "stackoverflow"

        profile_url = acc.get("profile_url")
        if platform_name and profile_url:
            platform_links[platform_name] = profile_url

    if not platform_links:
        raise_api_error(
            404,
            "platform_links_not_found",
            "No valid platform links found for this user.",
        )

    return platform_links


def normalize_platform_name(platform_name: str) -> str:
    return re.sub(r"[\s\-]+", "_", (platform_name or "").strip().lower())


def get_platform_score_key(platform_name: str) -> str:
    normalized_name = normalize_platform_name(platform_name)
    if normalized_name == "stackoverflow":
        return "stack_overflow"
    return normalized_name


def get_user_or_error(user_id: str):
    try:
        user_res = supabase.table("users").select("id").eq("id", user_id).execute()
    except Exception:
        traceback.print_exc()
        raise_api_error(
            503,
            "supabase_user_lookup_failed",
            "Failed to fetch the user from Supabase.",
        )

    if not user_res.data:
        raise_api_error(404, "user_not_found", "User not found.")

    return user_res.data[0]


def get_platform_accounts_by_user(user_id: str, include_profile_url: bool = False):
    columns = "id, platform_name, profile_url" if include_profile_url else "id, platform_name"

    try:
        accounts_res = supabase.table("platform_accounts") \
            .select(columns) \
            .eq("user_id", user_id) \
            .execute()
    except Exception:
        traceback.print_exc()
        raise_api_error(
            503,
            "supabase_platform_lookup_failed",
            "Failed to fetch platform accounts from Supabase.",
        )

    accounts = accounts_res.data or []
    if not accounts:
        raise_api_error(
            404,
            "platform_accounts_not_found",
            "No platform accounts found for this user.",
        )

    return accounts


def persist_score_results(user_id: str, result: Dict):
    normalized_user_id = (user_id or "").strip()
    if not normalized_user_id:
        raise_api_error(
            400,
            "invalid_user_id",
            "`user_id` must not be blank.",
            {"field": "user_id"},
        )

    get_user_or_error(normalized_user_id)
    accounts = get_platform_accounts_by_user(normalized_user_id)

    current_time = datetime.now(timezone.utc).isoformat()
    candidate_score = result.get("score", 0) or 0

    try:
        analysis_insert = supabase.table("candidate_score_analysis").insert({
            "user_id": normalized_user_id,
            "score": candidate_score,
            "pros": result.get("pros", ""),
            "cons": result.get("cons", ""),
            "improvements": result.get("improvements", ""),
            "is_fraud": False,
            "created_at": current_time,
        }).execute()
    except Exception:
        traceback.print_exc()
        raise_api_error(
            503,
            "candidate_score_persistence_failed",
            "Failed to store candidate score analysis in Supabase.",
        )

    if not analysis_insert.data:
        raise_api_error(
            503,
            "candidate_score_persistence_failed",
            "Supabase did not confirm candidate score analysis storage.",
        )

    print("💾 Stored candidate_score_analysis")

    platform_scores = result.get("platform_scores") or {}
    for account in accounts:
        score_key = get_platform_score_key(account.get("platform_name", ""))
        platform_score = platform_scores.get(score_key, 0) or 0

        try:
            insert_res = supabase.table("platform_score").insert({
                "platform_account_id": account["id"],
                "score": platform_score,
                "created_at": current_time,
            }).execute()
        except Exception:
            traceback.print_exc()
            raise_api_error(
                503,
                "platform_score_persistence_failed",
                f"Failed to store the platform score for {account.get('platform_name', 'unknown platform')}.",
            )

        if not insert_res.data:
            raise_api_error(
                503,
                "platform_score_persistence_failed",
                f"Supabase did not confirm platform score storage for {account.get('platform_name', 'unknown platform')}.",
            )

    print("💾 Stored platform scores")
    

LINKEDIN_EXPERIENCE_COLUMNS = [
    "experience_index",
    "job_title",
    "company_name",
    "employment_type",
    "start_date",
    "end_date",
    "start_year",
    "start_month",
    "end_year",
    "end_month",
    "duration_months",
    "is_current",
    "company_industry",
    "company_headcount_range",
    "company_id",
    "company_url",
    "company_website",
    "job_location",
    "job_location_city",
    "job_location_state",
    "job_location_country",
    "job_description",
    "raw_job_title",
    "raw_company_name",
]

LINKEDIN_EDUCATION_COLUMNS = [
    "education_index",
    "university_name",
    "fields_of_study",
    "start_date",
    "end_date",
    "start_year",
    "start_month",
    "end_year",
    "end_month",
    "duration_months",
    "is_current",
    "grade",
    "description",
    "social_url",
    "university_id",
    "logo",
]

LINKEDIN_SKILL_COLUMNS = [
    "skill_index",
    "skill_name",
    "endorsement_count",
]

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


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "present", "current"}


def _linkedin_text(value, separator: str = ", ") -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        items = []
        for item in value:
            text = _linkedin_text(item, separator=separator)
            if text:
                items.append(text)
        return separator.join(items)
    if isinstance(value, dict):
        for key in ("name", "title", "job_title", "company_name", "authority", "organization"):
            text = _linkedin_text(value.get(key))
            if text:
                return text
        return json.dumps(value, default=str, sort_keys=True)
    return str(value).strip()


def _dedupe_non_empty(values):
    seen = set()
    result = []
    for value in values:
        text = _linkedin_text(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def parse_linkedin_date(value, today=None):
    today = today or datetime.now(timezone.utc)
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, dict):
        year = _coerce_int(value.get("year"))
        if year <= 0:
            return None
        month = min(max(_coerce_int(value.get("month"), 1), 1), 12)
        day = min(max(_coerce_int(value.get("day"), 1), 1), 28)
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


def _linkedin_month_index(dt: datetime) -> int:
    return dt.year * 12 + dt.month


def linkedin_months_between(start_dt, end_dt) -> int:
    if not start_dt or not end_dt:
        return 0
    if end_dt < start_dt:
        end_dt = start_dt
    return (_linkedin_month_index(end_dt) - _linkedin_month_index(start_dt)) + 1


def format_linkedin_date(value) -> str:
    dt = parse_linkedin_date(value) if not isinstance(value, datetime) else value
    return dt.date().isoformat() if dt else ""


def normalize_linkedin_experience_rows(linkedin_raw_data, today=None):
    today = today or datetime.now(timezone.utc)
    experiences = []
    if isinstance(linkedin_raw_data, dict):
        experiences = linkedin_raw_data.get("experience") or linkedin_raw_data.get("experiences") or []

    rows = []
    for raw_entry in experiences:
        if not isinstance(raw_entry, dict):
            continue

        start_dt = parse_linkedin_date(raw_entry.get("job_started_on") or raw_entry.get("start_date"), today=today)
        end_source = raw_entry.get("job_ended_on") or raw_entry.get("end_date")
        is_current = _coerce_bool(raw_entry.get("job_still_working")) or (bool(start_dt) and not end_source)
        end_dt = parse_linkedin_date(end_source, today=today)
        if is_current and start_dt:
            end_dt = today
        if start_dt and end_dt and end_dt < start_dt:
            end_dt = start_dt

        rows.append({
            "experience_index": 0,
            "job_title": _linkedin_text(raw_entry.get("job_title") or raw_entry.get("title") or raw_entry.get("raw_job_title")),
            "company_name": _linkedin_text(raw_entry.get("company_name") or raw_entry.get("company") or raw_entry.get("raw_company_name")),
            "employment_type": _linkedin_text(raw_entry.get("employment_type")),
            "start_date": format_linkedin_date(start_dt),
            "end_date": "" if is_current else format_linkedin_date(end_dt),
            "start_year": start_dt.year if start_dt else None,
            "start_month": start_dt.month if start_dt else None,
            "end_year": None if (is_current or not end_dt) else end_dt.year,
            "end_month": None if (is_current or not end_dt) else end_dt.month,
            "duration_months": linkedin_months_between(start_dt, end_dt) if start_dt and end_dt else 0,
            "is_current": is_current,
            "company_industry": _linkedin_text(raw_entry.get("company_industry")),
            "company_headcount_range": _linkedin_text(raw_entry.get("company_headcount_range")),
            "company_id": _linkedin_text(raw_entry.get("company_id")),
            "company_url": _linkedin_text(raw_entry.get("company_url")),
            "company_website": _linkedin_text(raw_entry.get("company_website")),
            "job_location": _linkedin_text(raw_entry.get("job_location")),
            "job_location_city": _linkedin_text(raw_entry.get("job_location_city")),
            "job_location_state": _linkedin_text(raw_entry.get("job_location_state")),
            "job_location_country": _linkedin_text(raw_entry.get("job_location_country")),
            "job_description": _linkedin_text(raw_entry.get("job_description")),
            "raw_job_title": _linkedin_text(raw_entry.get("raw_job_title")),
            "raw_company_name": _linkedin_text(raw_entry.get("raw_company_name")),
        })

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
    for idx, row in enumerate(rows, start=1):
        row["experience_index"] = idx
    return rows


def normalize_linkedin_education_rows(linkedin_raw_data, today=None):
    today = today or datetime.now(timezone.utc)
    education = []
    if isinstance(linkedin_raw_data, dict):
        education = linkedin_raw_data.get("education") or []

    rows = []
    for raw_entry in education:
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

        rows.append({
            "education_index": 0,
            "university_name": _linkedin_text(raw_entry.get("university_name")),
            "fields_of_study": ", ".join(_dedupe_non_empty(fields)),
            "start_date": format_linkedin_date(start_dt),
            "end_date": "" if is_current else format_linkedin_date(end_dt),
            "start_year": start_dt.year if start_dt else None,
            "start_month": start_dt.month if start_dt else None,
            "end_year": None if (is_current or not end_dt) else end_dt.year,
            "end_month": None if (is_current or not end_dt) else end_dt.month,
            "duration_months": linkedin_months_between(start_dt, end_dt) if start_dt and end_dt else 0,
            "is_current": is_current,
            "grade": _linkedin_text(raw_entry.get("grade")),
            "description": _linkedin_text(raw_entry.get("description")),
            "social_url": _linkedin_text(raw_entry.get("social_url")),
            "university_id": _linkedin_text(raw_entry.get("university_id")),
            "logo": _linkedin_text(raw_entry.get("logo")),
        })

    rows.sort(
        key=lambda row: (
            row["end_year"] or 0,
            row["end_month"] or 0,
            row["start_year"] or 0,
            row["start_month"] or 0,
        ),
        reverse=True,
    )
    for idx, row in enumerate(rows, start=1):
        row["education_index"] = idx
    return rows


def normalize_linkedin_skill_rows(linkedin_raw_data):
    skills = []
    if isinstance(linkedin_raw_data, dict):
        skills = linkedin_raw_data.get("skills") or []

    rows = []
    seen = set()
    for raw_skill in skills:
        if isinstance(raw_skill, dict):
            skill_name = _linkedin_text(raw_skill.get("name") or raw_skill.get("skill"))
            endorsement_count = raw_skill.get("endorsement_count") or raw_skill.get("endorsements")
        else:
            skill_name = _linkedin_text(raw_skill)
            endorsement_count = None
        if not skill_name or skill_name in seen:
            continue
        seen.add(skill_name)
        rows.append({
            "skill_index": 0,
            "skill_name": skill_name,
            "endorsement_count": endorsement_count,
        })

    for idx, row in enumerate(rows, start=1):
        row["skill_index"] = idx
    return rows


def compute_linkedin_timeline_metrics(experience_rows, education_rows, today=None):
    today = today or datetime.now(timezone.utc)

    experience_ranges = []
    for row in experience_rows:
        start_dt = parse_linkedin_date(row.get("start_date"), today=today)
        end_dt = today if row.get("is_current") else parse_linkedin_date(row.get("end_date"), today=today)
        if not start_dt:
            continue
        if not end_dt or end_dt < start_dt:
            end_dt = start_dt
        experience_ranges.append((start_dt, end_dt, row))

    covered_months = set()
    sorted_experience = sorted(experience_ranges, key=lambda item: item[0])
    role_durations = []
    title_levels = []
    company_names = set()

    for start_dt, end_dt, row in sorted_experience:
        role_durations.append(linkedin_months_between(start_dt, end_dt))
        title_levels.append(linkedin_title_level(row.get("job_title", "")))
        if row.get("company_name"):
            company_names.add(row["company_name"])
        for month_idx in range(_linkedin_month_index(start_dt), _linkedin_month_index(end_dt) + 1):
            covered_months.add(month_idx)

    gaps = []
    for (_, prev_end, _), (next_start, _, _) in zip(sorted_experience, sorted_experience[1:]):
        gap_months = _linkedin_month_index(next_start) - _linkedin_month_index(prev_end) - 1
        if gap_months > 0:
            gaps.append(gap_months)

    education_ranges = []
    for row in education_rows:
        start_dt = parse_linkedin_date(row.get("start_date"), today=today)
        end_dt = today if row.get("is_current") else parse_linkedin_date(row.get("end_date"), today=today)
        if start_dt and (not end_dt or end_dt < start_dt):
            end_dt = start_dt
        education_ranges.append((start_dt, end_dt, row))

    dated_education = [entry for entry in education_ranges if entry[0] or entry[1]]
    valid_education = [entry for entry in dated_education if not entry[0] or not entry[1] or entry[1] >= entry[0]]
    reasonable_education = [
        entry for entry in valid_education
        if not entry[0] or not entry[1] or 3 <= linkedin_months_between(entry[0], entry[1]) <= 96
    ]

    progression_steps = sum(
        1 for previous_level, next_level in zip(title_levels, title_levels[1:]) if next_level > previous_level
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


def estimate_linkedin_company_prestige(experience_rows) -> float:
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


def prepare_linkedin_profile(linkedin_raw_data, fallback_profile_url: str = "", today=None):
    today = today or datetime.now(timezone.utc)
    linkedin_raw_data = linkedin_raw_data or {}

    experience_rows = normalize_linkedin_experience_rows(linkedin_raw_data, today=today)
    education_rows = normalize_linkedin_education_rows(linkedin_raw_data, today=today)
    skill_rows = normalize_linkedin_skill_rows(linkedin_raw_data)
    timeline_metrics = compute_linkedin_timeline_metrics(experience_rows, education_rows, today=today)

    certs = linkedin_raw_data.get("certification") or linkedin_raw_data.get("certifications") or []
    recs = linkedin_raw_data.get("recommendations_received") or linkedin_raw_data.get("recommendations") or []

    experience_titles = _dedupe_non_empty(row.get("job_title") for row in experience_rows)
    experience_companies = _dedupe_non_empty(row.get("company_name") for row in experience_rows)
    education_details = []
    for row in education_rows:
        school = row.get("university_name", "")
        field = row.get("fields_of_study", "")
        detail = f"{school} ({field})" if school and field else school or field
        if detail:
            education_details.append(detail)

    current_companies = [row["company_name"] for row in experience_rows if row.get("is_current") and row.get("company_name")]
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
        "li_cert_names": ", ".join(_dedupe_non_empty(
            (cert.get("name") if isinstance(cert, dict) else cert) for cert in certs
        )),
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


def score_linkedin_profile(
    linkedin_profile,
    linkedin_raw_data,
    experience_rows,
    education_rows,
    skill_rows,
    llm_scores=None,
    today=None,
):
    today = today or datetime.now(timezone.utc)
    llm_scores = llm_scores or {}
    timeline_metrics = compute_linkedin_timeline_metrics(experience_rows, education_rows, today=today)

    total_exp_months = timeline_metrics["total_experience_months"]
    avg_tenure_months = timeline_metrics["average_tenure_months"]
    longest_gap_months = timeline_metrics["longest_gap_months"]
    total_gap_months = timeline_metrics["total_gap_months"]
    current_roles = timeline_metrics["current_roles"]
    positions_count = timeline_metrics["positions_count"]

    skills_count = len(skill_rows)
    recommendations_count = _coerce_int(linkedin_profile.get("li_num_recommendations"))
    connections = _safe_float(linkedin_profile.get("li_connections"))
    followers = _safe_float(linkedin_profile.get("li_followers"))

    experience_text = " ".join(_dedupe_non_empty([
        linkedin_profile.get("li_headline"),
        linkedin_profile.get("li_summary"),
        *(row.get("job_title") for row in experience_rows),
    ])).lower()
    relevant_skills = 0
    for row in skill_rows:
        skill_name = row.get("skill_name", "").lower()
        tokens = [token for token in re.split(r"[^a-z0-9+.#]+", skill_name) if len(token) > 2]
        if skill_name and (skill_name in experience_text or any(token in experience_text for token in tokens)):
            relevant_skills += 1
    skill_relevance_ratio = (relevant_skills / skills_count) if skills_count else 0.0

    featured = linkedin_raw_data.get("featured") or []
    publications = linkedin_raw_data.get("publication") or linkedin_raw_data.get("publications") or []
    projects = linkedin_raw_data.get("project") or linkedin_raw_data.get("projects") or []
    activity_items = [item for item in [*featured, *publications, *projects] if isinstance(item, dict)]
    recent_activity_count = 0
    for item in activity_items:
        activity_date = parse_linkedin_date(
            item.get("startedOn") or item.get("started_on") or item.get("publishedOn") or item.get("date"),
            today=today,
        )
        if activity_date and (today - activity_date).days <= 730:
            recent_activity_count += 1

    summary_word_count = len(str(linkedin_profile.get("li_summary") or "").split())
    profile_sections = [
        bool(linkedin_profile.get("li_name")),
        bool(linkedin_profile.get("li_headline")),
        bool(linkedin_profile.get("li_summary")),
        bool(linkedin_profile.get("li_location")),
        bool(linkedin_profile.get("li_has_photo")),
        bool(experience_rows),
        bool(education_rows),
        bool(skill_rows),
        bool(linkedin_profile.get("li_num_certs")),
        bool(linkedin_profile.get("li_num_recommendations")),
        bool(linkedin_profile.get("li_current_company")),
    ]

    education_rows_with_fields = [row for row in education_rows if row.get("fields_of_study")]
    dated_education_ratio = (
        timeline_metrics["dated_education_count"] / len(education_rows)
        if education_rows else 0.0
    )

    rule_scores = {
        "employment_consistency_score": max(0.0, min(100.0,
            42.0
            + min(total_exp_months / 72.0, 1.0) * 25.0
            + min(avg_tenure_months / 24.0, 1.0) * 20.0
            + (8.0 if current_roles else 0.0)
            - min(longest_gap_months / 12.0, 1.0) * 25.0
            - min(total_gap_months / 24.0, 1.0) * 10.0
        )),
        "career_progression_trajectory": max(0.0, min(100.0,
            35.0
            + min(timeline_metrics["seniority_delta"], 4) * 11.0
            + min(timeline_metrics["progression_steps"], 3) * 9.0
            + min(positions_count / 5.0, 1.0) * 12.0
            + min(avg_tenure_months / 24.0, 1.0) * 12.0
            + (8.0 if current_roles else 0.0)
        )),
        "company_prestige_score": estimate_linkedin_company_prestige(experience_rows),
        "skill_endorsement_credibility": max(0.0, min(100.0,
            min(skills_count / 20.0, 1.0) * 70.0
            + skill_relevance_ratio * 20.0
            + (10.0 if skills_count >= 5 else 0.0)
        )),
        "recommendation_authenticity": max(0.0, min(100.0,
            min(recommendations_count / 5.0, 1.0) * 75.0
            + (15.0 if recommendations_count else 0.0)
            + min(current_roles, 2) * 5.0
        )),
        "profile_completeness": max(0.0, min(100.0, (sum(profile_sections) / len(profile_sections)) * 100.0)),
        "network_size_quality": max(0.0, min(100.0, min(((connections * 0.75) + (followers * 0.25)) / 500.0, 1.0) * 100.0)),
        "education_verification_score": max(0.0, min(100.0,
            (25.0 if education_rows else 0.0)
            + dated_education_ratio * 25.0
            + timeline_metrics["valid_education_ratio"] * 25.0
            + timeline_metrics["reasonable_education_ratio"] * 15.0
            + min(len(education_rows_with_fields) / 2.0, 1.0) * 10.0
        )),
        "activity_frequency_score": max(0.0, min(100.0,
            min(len(activity_items) / 6.0, 1.0) * 70.0
            + min(recent_activity_count / 3.0, 1.0) * 30.0
        )),
        "content_quality_score": max(0.0, min(100.0,
            min(summary_word_count / 120.0, 1.0) * 55.0
            + min(len(publications) / 3.0, 1.0) * 25.0
            + min(len(featured) / 3.0, 1.0) * 10.0
            + (10.0 if linkedin_profile.get("li_headline") else 0.0)
        )),
    }

    final_scores = {}
    date_driven_keys = {
        "employment_consistency_score",
        "career_progression_trajectory",
        "education_verification_score",
    }
    for key, rule_score in rule_scores.items():
        llm_value = llm_scores.get(key)
        try:
            llm_value = float(llm_value)
        except (TypeError, ValueError):
            llm_value = -1

        if llm_value >= 0:
            llm_weight = 0.2 if key in date_driven_keys else 0.35
            blended = (rule_score * (1.0 - llm_weight)) + (max(0.0, min(100.0, llm_value)) * llm_weight)
            final_scores[key] = round(max(0.0, min(100.0, blended)), 2)
        else:
            final_scores[key] = round(max(0.0, min(100.0, rule_score)), 2)

    return final_scores


# path to the candidate's resume PDF
def simulated_ml_model(platform_links:dict[str, str]):
  print("🚀 ML Model started...")
  platform_links = {k.lower(): v for k, v in platform_links.items()}
  time.sleep(random.randint(30, 60))
  RESUME_PDF_PATH = ""
  # ── OPTIONAL PROFILE LINKS  (leave as "" to skip that platform) ───────────────
  MANUAL_LINKEDIN      = platform_links.get("linkedin", "")
  MANUAL_GITHUB        = platform_links.get("github", "")
  MANUAL_LEETCODE      = platform_links.get("leetcode", "")
  MANUAL_HACKERRANK    = platform_links.get("hackerrank", "")
  MANUAL_STACKOVERFLOW = platform_links.get("stackoverflow", "")

  # ── API CREDENTIALS ───────────────────────────────────────────────────────────
  # Option A: set directly here
  AZURE_OPENAI_ENDPOINT   = "https://linked.openai.azure.com/openai/v1/"
  AZURE_OPENAI_DEPLOYMENT = "gpt-5.4-mini"
  AZURE_OPENAI_API_KEY    = "CWcXBMalpGqTsxcKFF3movpCCR0xGwpQtMFEfIZEDTEd6oTsFtdlJQQJ99CDACYeBjFXJ3w3AAABACOGYHIY"   # paste your Azure OpenAI key

  GITHUB_TOKEN       = ""   # optional — raises rate limit
  DATAMAGNET_TOKEN   = "c74d3048f38115659b73c81a276f0f91b172111cc06d26544130caad99280838"   # required for LinkedIn
  KAGGLE_USERNAME    = ""   # required for Kaggle
  KAGGLE_KEY         = ""   # required for Kaggle
  SO_API_KEY         = ""   # optional — raises quota

  # Option B: read from environment variables (safer for shared notebooks)
  import os
  if not AZURE_OPENAI_API_KEY:  AZURE_OPENAI_API_KEY  = os.getenv("AZURE_OPENAI_API_KEY", "")
  if not GITHUB_TOKEN:           GITHUB_TOKEN           = os.getenv("GITHUB_TOKEN", "")
  if not DATAMAGNET_TOKEN:       DATAMAGNET_TOKEN       = os.getenv("DATAMAGNET_TOKEN", "")
  if not KAGGLE_USERNAME:        KAGGLE_USERNAME        = os.getenv("KAGGLE_USERNAME", "")
  if not KAGGLE_KEY:             KAGGLE_KEY             = os.getenv("KAGGLE_KEY", "")
  if not SO_API_KEY:             SO_API_KEY             = os.getenv("SO_API_KEY", "")

  print("✅ Input parameters set")
  print(f"   Resume PDF       : {RESUME_PDF_PATH}")
  print(f"   LinkedIn         : {MANUAL_LINKEDIN or '(auto-detect from PDF)'}")
  print(f"   GitHub           : {MANUAL_GITHUB or '(auto-detect from PDF)'}")
  print(f"   LeetCode         : {MANUAL_LEETCODE or '(auto-detect from PDF)'}")
  print(f"   HackerRank       : {MANUAL_HACKERRANK or '(auto-detect from PDF)'}")
  print(f"   StackOverflow    : {MANUAL_STACKOVERFLOW or '(auto-detect from PDF)'}")
  print(f"   Azure OpenAI     : {'✓ set' if AZURE_OPENAI_API_KEY else '✗ not set — LLM scoring will be skipped'}")
  print(f"   GitHub Token     : {'✓ set' if GITHUB_TOKEN else 'not set (60 req/hr limit)'}")
  print(f"   DataMagnet       : {'✓ set' if DATAMAGNET_TOKEN else 'not set — LinkedIn will be skipped'}")

  """## 📄 Stage 1 — Scraper Pipeline"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 3 — SHARED IMPORTS & UTILITIES                                        ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  
  # ── Retry GET / POST ──────────────────────────────────────────────────────────
  def _get(url, headers=None, params=None, retries=3, delay=1.5):
      for attempt in range(retries):
          try:
              r = requests.get(url, headers=headers, params=params, timeout=15)
              if r.status_code == 429:
                  wait = int(r.headers.get("Retry-After", delay * (attempt + 1) * 2))
                  print(f"  ⏳ Rate limited — waiting {wait}s")
                  time.sleep(wait)
                  continue
              return r
          except requests.exceptions.Timeout:
              print(f"  ⏳ Timeout (attempt {attempt+1}/{retries})")
              time.sleep(delay * (attempt + 1))
          except Exception as e:
              print(f"  ✗ Request error: {e}")
              break
      return None

  def _post(url, json_body, headers=None, retries=3, delay=1.5):
      for attempt in range(retries):
          try:
              r = requests.post(url, json=json_body, headers=headers, timeout=15)
              if r.status_code == 429:
                  time.sleep(delay * (attempt + 1) * 2)
                  continue
              return r
          except Exception as e:
              print(f"  ✗ Request error: {e}")
              break
      return None

  def _slug(url, fallback=None):
      if not url:
          return fallback
      parts = [p for p in url.rstrip("/").split("/") if p]
      slug = parts[-1].split("?")[0].split("#")[0] if parts else None
      return slug or fallback

  print("✅ Utilities ready")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 4 — RESUME PARSER (extract links, email, raw text)                   ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝

  linkedin      = MANUAL_LINKEDIN
  github        = MANUAL_GITHUB
  leetcode      = MANUAL_LEETCODE
  hackerrank    = MANUAL_HACKERRANK
  stackoverflow = MANUAL_STACKOVERFLOW

  print(f"   github        = {github}")
  print(f"   leetcode      = {leetcode}")
  print(f"   hackerrank    = {hackerrank}")
  print(f"   stackoverflow = {stackoverflow}")
  print(f"   linkedin      = {linkedin}")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 5 — GITHUB SCRAPER                                                   ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  github_profile = {}
  github_repos   = []

  if github:
      try:
          gh_user = _slug(github)
          GH_HDR  = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
          GH_HDR["Accept"] = "application/vnd.github+json"

          res  = _get(f"https://api.github.com/users/{gh_user}", headers=GH_HDR)
          prof = res.json() if res else {}

          all_repos, url = [], f"https://api.github.com/users/{gh_user}/repos?per_page=100&sort=pushed"
          while url:
              r = _get(url, headers=GH_HDR)
              if not r: break
              data = r.json()
              if isinstance(data, list):
                  all_repos.extend(data)
              nxt = re.search(r'<([^>]+)>;\s*rel="next"', r.headers.get("Link", ""))
              url = nxt.group(1) if nxt else None
              time.sleep(0.3)

          original_repos  = [r for r in all_repos if not r.get("fork")]
          total_stars     = sum(r.get("stargazers_count", 0) for r in original_repos)
          total_forks_got = sum(r.get("forks_count", 0) for r in original_repos)
          languages_used  = list({r["language"] for r in original_repos if r.get("language")})

          github_profile = {
              "gh_username":        prof.get("login"),
              "gh_name":            prof.get("name"),
              "gh_bio":             prof.get("bio"),
              "gh_company":         prof.get("company"),
              "gh_blog":            prof.get("blog"),
              "gh_location":        prof.get("location"),
              "gh_followers":       prof.get("followers"),
              "gh_following":       prof.get("following"),
              "gh_public_repos":    prof.get("public_repos"),
              "gh_account_created": prof.get("created_at"),
              "gh_total_stars":     total_stars,
              "gh_total_forks_got": total_forks_got,
              "gh_original_repos":  len(original_repos),
              "gh_languages":       ", ".join(languages_used),
              "gh_top_repo_stars":  max((r.get("stargazers_count", 0) for r in original_repos), default=0),
          }

          for r in original_repos:
              github_repos.append({
                  "repo_name":   r.get("name"),
                  "repo_url":    r.get("html_url"),
                  "stars":       r.get("stargazers_count", 0),
                  "forks":       r.get("forks_count", 0),
                  "language":    r.get("language"),
                  "description": r.get("description"),
                  "updated_at":  r.get("pushed_at"),
                  "size_kb":     r.get("size", 0),
                  "open_issues": r.get("open_issues_count", 0),
                  "topics":      ", ".join(r.get("topics", [])),
              })

          print(f"✅ GitHub — {len(original_repos)} original repos | {total_stars} stars | languages: {', '.join(languages_used[:5])}")
      except Exception as e:
          print(f"✗ GitHub error: {e}")
  else:
      print("— GitHub: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 6 — LEETCODE SCRAPER                                                 ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  leetcode_profile = {}

  LC_HDR = {
      "Content-Type": "application/json",
      "Referer":      "https://leetcode.com",
      "User-Agent":   "Mozilla/5.0",
  }

  LC_QUERY = """
  query getUserProfile($username: String!) {
    matchedUser(username: $username) {
      profile { realName ranking reputation starRating }
      submitStats { acSubmissionNum { difficulty count } }
      badges { name icon }
      languageProblemCount { languageName problemsSolved }
      tagProblemCounts {
        advanced  { tagName problemsSolved }
        intermediate { tagName problemsSolved }
        fundamental { tagName problemsSolved }
      }
    }
    userContestRanking(username: $username) {
      rating globalRanking totalParticipants topPercentage attendedContestsCount
    }
  }
  """

  if leetcode:
      try:
          lc_user = _slug(leetcode)
          if not lc_user or lc_user == "u":
              parts = [p for p in leetcode.rstrip("/").split("/") if p and p != "u"]
              lc_user = parts[-1] if parts else None

          r = _post(
              "https://leetcode.com/graphql",
              json_body={"query": LC_QUERY, "variables": {"username": lc_user}},
              headers=LC_HDR
          )

          data    = r.json().get("data", {}) if r else {}
          user    = data.get("matchedUser") or {}
          contest = data.get("userContestRanking") or {}
          profile = user.get("profile") or {}
          stats   = user.get("submitStats", {}).get("acSubmissionNum", [])

          solved = {s["difficulty"]: s["count"] for s in stats}

          tag_counts   = user.get("tagProblemCounts", {})
          advanced_tags = [(t["tagName"], t["problemsSolved"]) for t in tag_counts.get("advanced", [])]
          advanced_tags.sort(key=lambda x: x[1], reverse=True)

          langs     = user.get("languageProblemCount", [])
          lang_str  = ", ".join(f"{l['languageName']}({l['problemsSolved']})" for l in langs[:5])
          badges    = [b["name"] for b in user.get("badges", [])]

          leetcode_profile = {
              "lc_username":          lc_user,
              "lc_ranking":           profile.get("ranking"),
              "lc_total_solved":      solved.get("All", 0),
              "lc_easy_solved":       solved.get("Easy", 0),
              "lc_medium_solved":     solved.get("Medium", 0),
              "lc_hard_solved":       solved.get("Hard", 0),
              "lc_contest_rating":    contest.get("rating"),
              "lc_contest_rank":      contest.get("globalRanking"),
              "lc_top_percentage":    contest.get("topPercentage"),
              "lc_contests_attended": contest.get("attendedContestsCount"),
              "lc_star_rating":       profile.get("starRating"),
              "lc_reputation":        profile.get("reputation"),
              "lc_badges":            ", ".join(badges),
              "lc_languages":         lang_str,
              "lc_top_topics":        ", ".join(t[0] for t in advanced_tags[:5]),
          }

          print(f"✅ LeetCode — total: {solved.get('All',0)} | E:{solved.get('Easy',0)} M:{solved.get('Medium',0)} H:{solved.get('Hard',0)} | contest rating: {contest.get('rating','—')}")
      except Exception as e:
          print(f"✗ LeetCode error: {e}")
  else:
      print("— LeetCode: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 7 — HACKERRANK SCRAPER                                               ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  hackerrank_profile = {}
  HR_HDR = {"User-Agent": "Mozilla/5.0"}

  HR_DOMAINS = [
      "Problem Solving", "Python", "Java", "C", "C++",
      "SQL", "Databases", "Linux Shell", "Regex",
      "JavaScript", "Rest API", "Go", "Ruby",
  ]

  def _hr_stars(badges, domain):
      for b in badges:
          if domain.lower() in b.get("badge_name", "").lower():
              return b.get("stars", 0)
      return 0

  if hackerrank:
      try:
          hr_user = _slug(hackerrank)
          res_prof   = _get(f"https://www.hackerrank.com/rest/hackers/{hr_user}/profile", headers=HR_HDR)
          prof_data  = res_prof.json().get("model", {}) if res_prof else {}
          res_badges = _get(f"https://www.hackerrank.com/rest/hackers/{hr_user}/badges", headers=HR_HDR)
          badges     = res_badges.json().get("models", []) if res_badges else []

          hr_skills_list = [
              f"{b['badge_name']} ({b.get('stars', 0)} stars)"
              for b in badges if b.get("stars", 0) > 0
          ]

          domain_scores = {f"hr_{d.lower().replace(' ', '_')}_stars": _hr_stars(badges, d)
                          for d in HR_DOMAINS}

          hackerrank_profile = {
              "hr_username":    hr_user,
              "hr_rank":        prof_data.get("level"),
              "hr_score":       prof_data.get("score"),
              "hr_country":     prof_data.get("country"),
              "hr_skills_raw":  ", ".join(hr_skills_list),
              "hr_total_badges":len([b for b in badges if b.get("stars", 0) > 0]),
              **domain_scores,
          }

          print(f"✅ HackerRank — {hackerrank_profile['hr_total_badges']} active badges | {', '.join(hr_skills_list[:4])}")
      except Exception as e:
          print(f"✗ HackerRank error: {e}")
  else:
      print("— HackerRank: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 8 — LINKEDIN SCRAPER (DataMagnet API)                                ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  linkedin_profile = {}
  linkedin_raw_data = {}   # kept for LLM stage
  linkedin_experience_rows = []
  linkedin_education_rows = []
  linkedin_skill_rows = []
  linkedin_timeline_metrics = {}

  if linkedin and DATAMAGNET_TOKEN:
      try:
          r = requests.post(
              "https://api.datamagnet.co/api/v1/linkedin/person",
              headers={"Authorization": f"Bearer {DATAMAGNET_TOKEN}", "Content-Type": "application/json"},
              json={"url": linkedin}, timeout=30
          )
          raw = r.json()
          data = raw.get("message", raw) if isinstance(raw, dict) else {}
          linkedin_raw_data = data
          linkedin_profile, linkedin_experience_rows, linkedin_education_rows, linkedin_skill_rows, linkedin_timeline_metrics = prepare_linkedin_profile(
              data,
              fallback_profile_url=linkedin,
          )

          print(
              f"✅ LinkedIn — {linkedin_profile.get('li_name')} | "
              f"{len(linkedin_experience_rows)} positions | "
              f"{len(linkedin_skill_rows)} skills | "
              f"{linkedin_profile.get('li_total_exp_months', 0)} months exp"
          )
      except Exception as e:
          print(f"✗ LinkedIn error: {e}")
  elif not DATAMAGNET_TOKEN:
      print("— LinkedIn: DATAMAGNET_TOKEN not set — skipping")
  else:
      print("— LinkedIn: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 9 — STACK OVERFLOW SCRAPER                                           ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  stackoverflow_profile = {}

  def _so_user_id(url_or_id):
      if not url_or_id: return None
      s = str(url_or_id).strip()
      if s.isdigit(): return s
      m = re.search(r'/users/(\d+)', s)
      return m.group(1) if m else None

  so_id = _so_user_id(stackoverflow)

  if so_id:
      try:
          SO_BASE   = "https://api.stackexchange.com/2.3"
          SO_PARAMS = {"site": "stackoverflow"}
          if SO_API_KEY: SO_PARAMS["key"] = SO_API_KEY

          r_prof = _get(f"{SO_BASE}/users/{so_id}", params=SO_PARAMS)
          items  = r_prof.json().get("items", []) if r_prof else []
          prof   = items[0] if items else {}

          r_tags = _get(f"{SO_BASE}/users/{so_id}/tags",
                        params={**SO_PARAMS, "pagesize": 20, "sort": "activity"})
          tags   = r_tags.json().get("items", []) if r_tags else []
          time.sleep(0.5)

          r_ans   = _get(f"{SO_BASE}/users/{so_id}/answers",
                        params={**SO_PARAMS, "pagesize": 30, "sort": "votes", "filter": "withbody"})
          answers = r_ans.json().get("items", []) if r_ans else []
          time.sleep(0.5)

          badge_counts  = prof.get("badge_counts", {})
          top_tags      = [t["name"] for t in tags]
          accepted_ans  = sum(1 for a in answers if a.get("is_accepted"))
          avg_ans_score = round(sum(a.get("score", 0) for a in answers) / max(len(answers), 1), 1)

          stackoverflow_profile = {
              "so_user_id":         so_id,
              "so_display_name":    prof.get("display_name"),
              "so_reputation":      prof.get("reputation", 0),
              "so_answer_count":    prof.get("answer_count", 0),
              "so_question_count":  prof.get("question_count", 0),
              "so_gold_badges":     badge_counts.get("gold", 0),
              "so_silver_badges":   badge_counts.get("silver", 0),
              "so_bronze_badges":   badge_counts.get("bronze", 0),
              "so_accepted_answers":accepted_ans,
              "so_avg_answer_score":avg_ans_score,
              "so_top_tags":        ", ".join(top_tags[:10]),
              "so_account_created": prof.get("creation_date"),
              "so_last_access":     prof.get("last_access_date"),
              "so_profile_url":     prof.get("link"),
          }

          print(f"✅ Stack Overflow — rep: {prof.get('reputation',0):,} | answers: {prof.get('answer_count',0)} | top tags: {', '.join(top_tags[:5])}")
      except Exception as e:
          print(f"✗ Stack Overflow error: {e}")
  else:
      print("— Stack Overflow: no user ID/URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 10 — BUILD MASTER PROFILE & SAVE INTERMEDIATE CSVs                  ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import math as _math

  def _is_numeric(val):
      if val is None: return False
      if isinstance(val, bool): return True
      if isinstance(val, (int, float)):
          return not (isinstance(val, float) and _math.isnan(val))
      try:
          float(str(val).strip()); return True
      except (ValueError, TypeError):
          return False

  def split_platform(profile_dict):
      numeric, contextual = {}, {}
      for k, v in profile_dict.items():
          if _is_numeric(v): numeric[k] = v
          else: contextual[k] = v
      return numeric, contextual

  def save_platform_csvs(platform, profile_dict, extra_numeric=None, extra_contextual=None):
      if not profile_dict:
          return {platform: {'numeric': 0, 'contextual': 0, 'status': 'skipped'}}
      num_d, ctx_d = split_platform(profile_dict)
      if extra_numeric:
          for k, v in extra_numeric.items():
              num_d[k] = v; ctx_d.pop(k, None)
      if extra_contextual:
          for k, v in extra_contextual.items():
              ctx_d[k] = v; num_d.pop(k, None)
      pd.DataFrame([num_d]).to_csv(f'{platform}_numeric.csv', index=False)
      pd.DataFrame([ctx_d]).to_csv(f'{platform}_contextual.csv', index=False)
      return {platform: {'numeric': len(num_d), 'contextual': len(ctx_d), 'status': 'saved'}}

  # ── Save per-platform CSVs ─────────────────────────────────────────────────────
  save_platform_csvs('linkedin', linkedin_profile,
      extra_numeric={k: v for k, v in linkedin_profile.items() if k in {
          'li_followers','li_connections','li_num_positions','li_total_exp_months',
          'li_num_education','li_num_skills','li_num_certs','li_num_recommendations',
          'li_has_photo','li_has_summary','li_has_experience','li_has_education',
          'li_has_skills','li_has_certs','li_has_recommendations'}},
      extra_contextual={k: v for k, v in linkedin_profile.items() if k in {
          'li_name','li_headline','li_summary','li_location','li_profile_url',
          'li_exp_titles','li_exp_companies','li_edu_details','li_skills',
          'li_cert_names','li_current_company','li_country'}})

  save_platform_csvs('github', github_profile,
      extra_numeric={k: v for k, v in github_profile.items() if k in {
          'gh_followers','gh_following','gh_public_repos','gh_total_stars',
          'gh_total_forks_got','gh_original_repos','gh_top_repo_stars'}},
      extra_contextual={k: v for k, v in github_profile.items() if k in {
          'gh_bio','gh_company','gh_blog','gh_location','gh_username',
          'gh_name','gh_languages','gh_account_created'}})

  save_platform_csvs('leetcode', leetcode_profile,
      extra_numeric={k: v for k, v in leetcode_profile.items() if k in {
          'lc_total_solved','lc_easy_solved','lc_medium_solved','lc_hard_solved',
          'lc_contest_rating','lc_contest_rank','lc_top_percentage',
          'lc_contests_attended','lc_star_rating','lc_reputation','lc_ranking'}},
      extra_contextual={k: v for k, v in leetcode_profile.items() if k in {
          'lc_username','lc_badges','lc_languages','lc_top_topics'}})

  save_platform_csvs('hackerrank', hackerrank_profile,
      extra_numeric={k: v for k, v in hackerrank_profile.items()
                    if k.endswith('_stars') or k in {'hr_rank','hr_score','hr_total_badges'}},
      extra_contextual={k: v for k, v in hackerrank_profile.items() if k in {
          'hr_username','hr_skills_raw','hr_country'}})

  save_platform_csvs('stackoverflow', stackoverflow_profile,
      extra_numeric={k: v for k, v in stackoverflow_profile.items() if k in {
          'so_reputation','so_answer_count','so_question_count',
          'so_gold_badges','so_silver_badges','so_bronze_badges',
          'so_accepted_answers','so_avg_answer_score',
          'so_account_created','so_last_access'}},
      extra_contextual={k: v for k, v in stackoverflow_profile.items() if k in {
          'so_user_id','so_display_name','so_top_tags','so_profile_url'}})

  # Resume contextual
  pd.DataFrame([{'email': "none", 'resume_text': "none",
                'resume_raw': "none"}]).to_csv('resume_contextual.csv', index=False)

  # Master profile CSV
  all_data = {
      'email': "email", 'linkedin_url': linkedin, 'github_url': github,
      'leetcode_url': leetcode, 'hackerrank_url': hackerrank,
      'stackoverflow_url': stackoverflow, 'kaggle_url': "kaggle",
      'resume_text': "resume_content['clean']",
      **github_profile, **leetcode_profile, **hackerrank_profile,
      **linkedin_profile, **stackoverflow_profile,
  }
  pd.DataFrame([all_data]).to_csv('candidate_profile.csv', index=False)

  # GitHub repos
  df_github_repos = pd.DataFrame(github_repos) if github_repos else pd.DataFrame()
  df_github_repos.to_csv('github_repos.csv', index=False)

  # LinkedIn sub-tables
  if linkedin_raw_data:
      pd.json_normalize(linkedin_raw_data).to_csv('linkedin_profile_full.csv', index=False)
      pd.DataFrame(linkedin_experience_rows, columns=LINKEDIN_EXPERIENCE_COLUMNS).to_csv('linkedin_experience.csv', index=False)
      pd.DataFrame(linkedin_education_rows, columns=LINKEDIN_EDUCATION_COLUMNS).to_csv('linkedin_education.csv', index=False)
      pd.DataFrame(linkedin_skill_rows, columns=LINKEDIN_SKILL_COLUMNS).to_csv('linkedin_skills.csv', index=False)

  print(f"✅ Stage 1 complete — {len(all_data)} columns in master profile")
  print(f"   Platforms scraped: GitHub={bool(github_profile)}, LeetCode={bool(leetcode_profile)}, HackerRank={bool(hackerrank_profile)}, LinkedIn={bool(linkedin_profile)}, StackOverflow={bool(stackoverflow_profile)}")

  """## 🤖 Stage 2 — Sub-Metrics Scoring (LLM-assisted per-platform scores)"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 11 — AZURE OPENAI CLIENT SETUP                                       ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  from openai import OpenAI

  NOW = datetime.now(timezone.utc)

  def safe(val, default=0):
      return default if (val is None or (isinstance(val, float) and math.isnan(val))) else val

  def clamp(val, lo=0.0, hi=100.0):
      return max(lo, min(hi, float(val)))

  def stars_to_score(stars, max_stars=5):
      return clamp((stars / max_stars) * 100)

  def iso_to_years_ago(iso):
      try:
          dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
          return (NOW - dt.replace(tzinfo=timezone.utc)).days / 365.25
      except Exception:
          return 0.0

  llm_client = None
  if AZURE_OPENAI_API_KEY:
      llm_client = OpenAI(base_url=AZURE_OPENAI_ENDPOINT, api_key=AZURE_OPENAI_API_KEY)
      print("✅ Azure OpenAI client ready")
  else:
      print("⚠️  No Azure OpenAI key — LLM scoring stages will use rule-based fallback scores")

  def llm_score(system_prompt, user_content):
      if not llm_client:
          return {}
      try:
          response = llm_client.chat.completions.create(
              model=AZURE_OPENAI_DEPLOYMENT,
              temperature=0.1,
              response_format={"type": "json_object"},
              messages=[
                  {"role": "system", "content": system_prompt},
                  {"role": "user",   "content": user_content},
              ],
          )
          return json.loads(response.choices[0].message.content)
      except Exception as e:
          print(f"  LLM call error: {e}")
          return {}

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 12 — GITHUB SUB-METRIC SCORER                                        ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  github_scores = {}

  def score_github(profile, repos):
      p = profile
      if not p:
          return {}

      n_repos  = safe(p.get('gh_original_repos'), 0)
      stars    = safe(p.get('gh_total_stars'), 0)
      forks    = safe(p.get('gh_total_forks_got'), 0)
      followers= safe(p.get('gh_followers'), 0)
      langs    = str(p.get('gh_languages', '')).split(', ') if p.get('gh_languages') else []

      account_age_years = iso_to_years_ago(p.get('gh_account_created', ''))

      # Rule-based scores
      repository_originality           = clamp(min(n_repos / 30, 1) * 100)
      stars_received_score             = clamp(min(stars / 50, 1) * 100)
      forks_received_score             = clamp(min(forks / 20, 1) * 100)
      language_diversity               = clamp(min(len(langs) / 8, 1) * 100)
      collaboration_network            = clamp(min(followers / 100, 1) * 100)
      repository_count_score           = clamp(min(n_repos / 20, 1) * 100)
      project_longevity                = clamp(min(account_age_years / 5, 1) * 100)

      # LLM-scored (needs contextual interpretation)
      llm_prompt = """You are a GitHub profile evaluator. Given a candidate's GitHub profile and repo list,
  score ONLY these sub-metrics (0-100 scale). Return ONLY a JSON object with these exact keys. Set -1 if data is missing.
  Keys: commit_frequency_score, contribution_graph_density_score, documentation_quality, ci_cd_usage"""

      user_content = json.dumps({
          'profile': {k: v for k, v in p.items() if k not in ('gh_bio',)},
          'top_repos': (repos[:10] if repos else [])
      }, default=str)

      llm_result = llm_score(llm_prompt, user_content)

      return {
          'repository_originality':            repository_originality,
          'stars_received_score':              stars_received_score,
          'forks_received_score':              forks_received_score,
          'language_diversity':                language_diversity,
          'collaboration_network':             collaboration_network,
          'repository_count_score':            repository_count_score,
          'project_longevity':                 project_longevity,
          'account_age_years':                 round(account_age_years, 2),
          'commit_frequency_score':            clamp(llm_result.get('commit_frequency_score', 30)),
          'contribution_graph_density_score':  clamp(llm_result.get('contribution_graph_density_score', 30)),
          'documentation_quality':             clamp(llm_result.get('documentation_quality', 30)),
          'ci_cd_usage':                       clamp(llm_result.get('ci_cd_usage', 10)),
      }

  if github_profile:
      github_scores = score_github(github_profile, github_repos)
      pd.DataFrame([github_scores]).to_csv('github_scores.csv', index=False)
      print(f"✅ GitHub sub-metrics scored → github_scores.csv")
  else:
      print("— GitHub: no profile data, skipping sub-metric scoring")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 13 — LEETCODE SUB-METRIC SCORER                                      ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  leetcode_scores = {}

  def score_leetcode(profile):
      p = profile
      if not p: return {}

      total   = safe(p.get('lc_total_solved'), 0)
      easy    = safe(p.get('lc_easy_solved'), 0)
      medium  = safe(p.get('lc_medium_solved'), 0)
      hard    = safe(p.get('lc_hard_solved'), 0)
      ranking = safe(p.get('lc_ranking'), 0)
      cr      = safe(p.get('lc_contest_rating'), 0)
      top_pct = safe(p.get('lc_top_percentage'), 100)
      attended= safe(p.get('lc_contests_attended'), 0)

      problems_solved_score    = clamp(min(total / 500, 1) * 100)
      global_ranking_score     = clamp((1 - min(ranking / 500000, 1)) * 100) if ranking else 0
      top_percentage_score     = clamp((1 - top_pct / 100) * 100) if top_pct < 100 else 0
      contest_rating_score     = clamp(min((cr - 1200) / 1300, 1) * 100) if cr > 1200 else 0
      contest_participation_score = clamp(min(attended / 20, 1) * 100)
      total_solved             = max(total, 1)
      acceptance_rate_score    = clamp(((easy + medium * 1.5 + hard * 2.5) / (total_solved * 2.5)) * 100)
      difficulty_distribution  = clamp((hard / max(total_solved, 1)) * 500)

      langs  = str(p.get('lc_languages', '')).split(', ')
      topics = str(p.get('lc_top_topics', '')).split(', ')
      language_diversity  = clamp(min(len([l for l in langs if l]) / 5, 1) * 100)
      category_coverage   = clamp(min(len([t for t in topics if t]) / 10, 1) * 100)

      return {
          'problems_solved_score':       problems_solved_score,
          'global_ranking_score':        global_ranking_score,
          'top_percentage_score':        top_percentage_score,
          'contest_rating_score':        contest_rating_score,
          'contest_participation_score': contest_participation_score,
          'acceptance_rate_score':       acceptance_rate_score,
          'difficulty_distribution':     difficulty_distribution,
          'language_diversity':          language_diversity,
          'category_coverage':           category_coverage,
      }

  if leetcode_profile:
      leetcode_scores = score_leetcode(leetcode_profile)
      pd.DataFrame([leetcode_scores]).to_csv('leetcode_scores.csv', index=False)
      print(f"✅ LeetCode sub-metrics scored → leetcode_scores.csv")
  else:
      print("— LeetCode: no profile data, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 14 — HACKERRANK SUB-METRIC SCORER                                    ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  hackerrank_scores = {}

  def score_hackerrank(profile):
      p = profile
      if not p: return {}

      total_badges = safe(p.get('hr_total_badges'), 0)
      ps_stars  = safe(p.get('hr_problem_solving_stars'), 0)
      py_stars  = safe(p.get('hr_python_stars'), 0)
      java_stars= safe(p.get('hr_java_stars'), 0)
      sql_stars = safe(p.get('hr_sql_stars'), 0)
      c_stars   = safe(p.get('hr_c_stars'), 0)
      cpp_stars = safe(p.get('hr_c++_stars') or p.get('hr_c___stars'), 0)

      all_domain_keys = [k for k in p if k.endswith('_stars')]
      all_star_vals   = [safe(p[k], 0) for k in all_domain_keys]
      avg_stars = sum(all_star_vals) / max(len([v for v in all_star_vals if v > 0]), 1)

      skill_certificates_score = clamp(stars_to_score(ps_stars))
      avg_stars_score          = clamp(stars_to_score(avg_stars))
      badges_count_score       = clamp(min(total_badges / 10, 1) * 100)
      domain_score_quality     = clamp(max(stars_to_score(ps_stars),
                                          stars_to_score(py_stars),
                                          stars_to_score(java_stars),
                                          stars_to_score(sql_stars)))

      hr_rank  = safe(p.get('hr_rank'), None)
      hr_score = safe(p.get('hr_score'), None)
      rank_score = clamp(min(hr_rank / 1000, 1) * 100) if hr_rank else 30

      return {
          'skill_certificates_score': skill_certificates_score,
          'avg_stars_score':          avg_stars_score,
          'badges_count_score':       badges_count_score,
          'domain_score_quality':     domain_score_quality,
          'rank_score':               rank_score,
      }

  if hackerrank_profile:
      hackerrank_scores = score_hackerrank(hackerrank_profile)
      pd.DataFrame([hackerrank_scores]).to_csv('hackerrank_scores.csv', index=False)
      print(f"✅ HackerRank sub-metrics scored → hackerrank_scores.csv")
  else:
      print("— HackerRank: no profile data, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 15 — LINKEDIN & STACKOVERFLOW SUB-METRIC SCORER (LLM)                ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  linkedin_scores     = {}
  stackoverflow_scores= {}

  # ── LinkedIn ──────────────────────────────────────────────────────────────────
  LINKEDIN_LLM_PROMPT = """You are an expert HR analyst. Given a LinkedIn profile (JSON), score ONLY the
  following sub-metrics on a 0-100 scale (100 = best possible). Return ONLY a JSON object with these
  exact keys. If data is missing set the value to -1.
  Keys to score:
  - employment_consistency_score (job gaps, tenure, logical career progression)
  - career_progression_trajectory (promotions, seniority growth)
  - company_prestige_score (tier of employers: FAANG=100, startup=30)
  - skill_endorsement_credibility (breadth, relevance of skills)
  - recommendation_authenticity (count + perceived quality)
  - profile_completeness (all sections filled)
  - network_size_quality (connections + follower count)
  - education_verification_score (degree relevance, institution prestige)
  - activity_frequency_score (posts, publications, featured items)
  - content_quality_score (quality of shared content)"""

  if linkedin_profile:
      li_llm = llm_score(LINKEDIN_LLM_PROMPT, json.dumps({
          'profile': linkedin_profile,
          'experience_rows': linkedin_experience_rows[:8],
          'education_rows':  linkedin_education_rows[:5],
          'skill_rows':      linkedin_skill_rows[:20],
          'timeline_metrics': linkedin_timeline_metrics,
      }, default=str))

      linkedin_scores = score_linkedin_profile(
          linkedin_profile,
          linkedin_raw_data,
          linkedin_experience_rows,
          linkedin_education_rows,
          linkedin_skill_rows,
          llm_scores=li_llm,
      )
      pd.DataFrame([linkedin_scores]).to_csv('linkedin_scores.csv', index=False)
      print("✅ LinkedIn sub-metrics scored → linkedin_scores.csv")
  else:
      print("— LinkedIn: no profile data, skipping")

  # ── Stack Overflow ─────────────────────────────────────────────────────────────
  def score_stackoverflow(profile):
      p = profile
      if not p: return {}
      rep       = safe(p.get('so_reputation'), 0)
      answers   = safe(p.get('so_answer_count'), 0)
      accepted  = safe(p.get('so_accepted_answers'), 0)
      gold      = safe(p.get('so_gold_badges'), 0)
      silver    = safe(p.get('so_silver_badges'), 0)
      avg_score = safe(p.get('so_avg_answer_score'), 0)

      reputation_score       = clamp(min(rep / 10000, 1) * 100)
      answer_volume_score    = clamp(min(answers / 100, 1) * 100)
      acceptance_rate_score  = clamp((accepted / max(answers, 1)) * 100)
      badge_quality_score    = clamp(min((gold * 20 + silver * 5) / 200, 1) * 100)
      answer_quality_score   = clamp(min(avg_score / 10, 1) * 100)
      tags = str(p.get('so_top_tags', '')).split(', ')
      expertise_breadth      = clamp(min(len([t for t in tags if t]) / 10, 1) * 100)

      return {
          'reputation_score':      reputation_score,
          'answer_volume_score':   answer_volume_score,
          'acceptance_rate_score': acceptance_rate_score,
          'badge_quality_score':   badge_quality_score,
          'answer_quality_score':  answer_quality_score,
          'expertise_breadth':     expertise_breadth,
      }

  if stackoverflow_profile:
      stackoverflow_scores = score_stackoverflow(stackoverflow_profile)
      pd.DataFrame([stackoverflow_scores]).to_csv('stackoverflow_scores.csv', index=False)
      print("✅ Stack Overflow sub-metrics scored → stackoverflow_scores.csv")
  else:
      print("— Stack Overflow: no profile data, skipping")

  """## 📊 Stage 3 — Mathematical Scoring (Weighted Platform Scores)"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 16 — PLATFORM WEIGHTS & SCORING ENGINE                               ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import numpy as np
  import warnings
  warnings.filterwarnings('ignore')

  # ── Per-platform sub-metric weights ───────────────────────────────────────────
  LINKEDIN_WEIGHTS = {
      'employment_consistency_score':   0.20,
      'career_progression_trajectory':  0.18,
      'company_prestige_score':         0.12,
      'skill_endorsement_credibility':  0.10,
      'recommendation_authenticity':    0.10,
      'profile_completeness':           0.08,
      'network_size_quality':           0.08,
      'education_verification_score':   0.07,
      'activity_frequency_score':       0.04,
      'content_quality_score':          0.03,
  }

  GITHUB_WEIGHTS = {
      'repository_originality':           0.22,
      'commit_frequency_score':           0.20,
      'contribution_graph_density_score': 0.15,
      'project_longevity':                0.12,
      'stars_received_score':             0.07,
      'forks_received_score':             0.03,
      'documentation_quality':            0.06,
      'ci_cd_usage':                      0.02,
      'language_diversity':               0.06,
      'collaboration_network':            0.05,
      'repository_count_score':           0.02,
  }

  LEETCODE_WEIGHTS = {
      'problems_solved_score':       0.25,
      'global_ranking_score':        0.20,
      'top_percentage_score':        0.15,
      'contest_rating_score':        0.12,
      'contest_participation_score': 0.08,
      'acceptance_rate_score':       0.10,
      'difficulty_distribution':     0.05,
      'language_diversity':          0.03,
      'category_coverage':           0.02,
  }

  HACKERRANK_WEIGHTS = {
      'skill_certificates_score': 0.30,
      'avg_stars_score':          0.25,
      'domain_score_quality':     0.25,
      'badges_count_score':       0.12,
      'rank_score':               0.08,
  }

  STACKOVERFLOW_WEIGHTS = {
      'reputation_score':      0.30,
      'answer_volume_score':   0.20,
      'acceptance_rate_score': 0.20,
      'badge_quality_score':   0.15,
      'answer_quality_score':  0.10,
      'expertise_breadth':     0.05,
  }

  OVERALL_PLATFORM_WEIGHTS = {
      'LinkedIn':      0.30,
      'GitHub':        0.25,
      'LeetCode':      0.20,
      'HackerRank':    0.15,
      'StackOverflow': 0.10,
  }

  def _clamp_score(value):
      if pd.isna(value) or value < 0: return 0.0
      return float(np.clip(value, 0, 100))

  def weighted_platform_score(scores_dict, weights):
      present = {col: w for col, w in weights.items() if col in scores_dict}
      missing = [col for col in weights if col not in scores_dict]
      if not present:
          return {'platform_score': 0.0, 'breakdown': {}, 'warnings': missing}

      total_weight = sum(present.values())
      breakdown, weighted_sum = {}, 0.0
      for col, raw_w in present.items():
          norm_w       = raw_w / total_weight
          sub_score    = _clamp_score(scores_dict[col])
          contribution = sub_score * norm_w
          weighted_sum += contribution
          breakdown[col] = {
              'raw_score': round(sub_score, 2),
              'weight': round(norm_w * 100, 2),
              'contribution': round(contribution, 2),
          }
      return {'platform_score': round(weighted_sum, 2), 'breakdown': breakdown, 'warnings': missing}

  def score_to_grade(s):
      if s >= 850: return 'A+', '🟢 Excellent'
      if s >= 750: return 'A',  '🟢 Very Good'
      if s >= 650: return 'B+', '🟡 Good'
      if s >= 550: return 'B',  '🟡 Above Average'
      if s >= 450: return 'C+', '🟠 Average'
      if s >= 350: return 'C',  '🟠 Below Average'
      return 'D', '🔴 Needs Improvement'

  print("✅ Scoring engine loaded")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 17 — COMPUTE PLATFORM SCORES & OVERALL SCORE                         ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  PLATFORM_SCORES_MAP = {
      'LinkedIn':      (linkedin_scores,     LINKEDIN_WEIGHTS),
      'GitHub':        (github_scores,       GITHUB_WEIGHTS),
      'LeetCode':      (leetcode_scores,     LEETCODE_WEIGHTS),
      'HackerRank':    (hackerrank_scores,   HACKERRANK_WEIGHTS),
      'StackOverflow': (stackoverflow_scores,STACKOVERFLOW_WEIGHTS),
  }

  platform_results = {}
  platform_scores  = {}

  for platform, (scores_dict, weights) in PLATFORM_SCORES_MAP.items():
      if scores_dict:
          result = weighted_platform_score(scores_dict, weights)
          platform_results[platform] = result
          platform_scores[platform]  = result['platform_score']
      else:
          platform_results[platform] = None
          platform_scores[platform]  = None

  # ── Overall score with dynamic weight redistribution ─────────────────────────
  present_platforms = {p: s for p, s in platform_scores.items() if s is not None}
  absent_platforms  = {p: s for p, s in platform_scores.items() if s is None}

  present_weight_sum = sum(OVERALL_PLATFORM_WEIGHTS[p] for p in present_platforms)
  adjusted_weights = {
      p: (OVERALL_PLATFORM_WEIGHTS[p] / present_weight_sum if p in present_platforms else 0.0)
      for p in OVERALL_PLATFORM_WEIGHTS
  }

  overall_score = round(sum(present_platforms[p] * adjusted_weights[p] * 10 for p in present_platforms), 2)
  grade, lbl    = score_to_grade(overall_score)

  print('\n' + '═'*62)
  print(f'  🏅  OVERALL PLATFORM SCORE  :  {overall_score}/1000')
  print(f'  📋  Grade                   :  {grade}  {lbl}')
  if absent_platforms:
      print(f'  ⚠️   Missing platforms       :  {", ".join(absent_platforms.keys())}')
  print('═'*62)
  print()
  print(f"  {'Platform':<15} {'Score':>6}  {'Adj Wt':>8}  Status")
  print('  ' + '─'*45)
  for p in OVERALL_PLATFORM_WEIGHTS:
      s = platform_scores[p]
      adj_w = adjusted_weights[p]
      if s is not None:
          print(f"  {p:<15} {s*10:>6.1f}  {adj_w*100:>7.1f}%  ✅")
      else:
          print(f"  {p:<15} {'N/A':>6}  {'0.0%':>8}  ❌ missing")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 18 — SAVE SCORE REPORT CSV                                           ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  summary_rows = []
  for platform in OVERALL_PLATFORM_WEIGHTS:
      base_w = OVERALL_PLATFORM_WEIGHTS[platform]
      adj_w  = adjusted_weights[platform]
      ps     = platform_scores[platform]
      g, l   = score_to_grade(ps * 10) if ps is not None else ('N/A', '❌ Missing')
      summary_rows.append({
          'Platform':         platform,
          'Status':           'Present' if ps is not None else 'Missing',
          'Platform Score':   round(ps * 10, 2) if ps is not None else 'N/A',
          'Grade':            g,
          'Verdict':          l,
          'Base Weight':      f'{base_w*100:.0f}%',
          'Adjusted Weight':  f'{adj_w*100:.1f}%',
          'Weighted Contrib': round(ps * adj_w * 10, 2) if ps is not None else 0.0,
      })

  summary_df = pd.DataFrame(summary_rows)
  overall_row = pd.DataFrame([{
      'Platform': '🏅 OVERALL', 'Status': f"{len(present_platforms)}/{len(OVERALL_PLATFORM_WEIGHTS)} platforms",
      'Platform Score': overall_score, 'Grade': grade, 'Verdict': lbl,
      'Base Weight': '100%', 'Adjusted Weight': '100%', 'Weighted Contrib': overall_score,
  }])
  final_df = pd.concat([summary_df, overall_row], ignore_index=True)
  final_df.to_csv('overall_score_report.csv', index=False)
  print(final_df.to_string(index=False))
  print("\n✅ Stage 3 complete → overall_score_report.csv")

  """## 🧠 Stage 4 — LLM Scoring Pipeline (Holistic Analysis)"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 19 — PREPARE DATA PACKAGES FOR LLM ANALYSIS                         ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import textwrap

  def safe_float(val, default=0.0):
      try:
          v = float(val)
          return 0.0 if (v != v) else v
      except (TypeError, ValueError):
          return default

  candidate = all_data   # master profile dict built in Cell 10

  # Package A: Skill evidence
  pkg_skill = {
      'github': {
          'public_repos':         safe_float(candidate.get('gh_public_repos')),
          'total_stars':          safe_float(candidate.get('gh_total_stars')),
          'languages':            candidate.get('gh_languages', ''),
          'followers':            safe_float(candidate.get('gh_followers')),
          'commit_frequency':     safe_float(github_scores.get('commit_frequency_score', 0)),
          'documentation':        safe_float(github_scores.get('documentation_quality', 0)),
          'top_repos':            github_repos[:5] if github_repos else [],
      },
      'leetcode': {
          'total_solved':   safe_float(candidate.get('lc_total_solved')),
          'easy':           safe_float(candidate.get('lc_easy_solved')),
          'medium':         safe_float(candidate.get('lc_medium_solved')),
          'hard':           safe_float(candidate.get('lc_hard_solved')),
          'contest_rating': safe_float(candidate.get('lc_contest_rating')),
          'top_topics':     candidate.get('lc_top_topics', ''),
      },
      'hackerrank': {
          'total_badges':   safe_float(candidate.get('hr_total_badges')),
          'skills_raw':     candidate.get('hr_skills_raw', ''),
          'ps_stars':       safe_float(candidate.get('hr_problem_solving_stars')),
          'python_stars':   safe_float(candidate.get('hr_python_stars')),
      },
      'certifications': linkedin_profile.get('li_cert_names', ''),
  }

  # Package B: Professional identity
  pkg_identity = {
      'name':        linkedin_profile.get('li_name', ''),
      'headline':    linkedin_profile.get('li_headline', ''),
      'location':    linkedin_profile.get('li_location', ''),
      'linkedin': {
          'connections':   safe_float(linkedin_profile.get('li_connections')),
          'followers':     safe_float(linkedin_profile.get('li_followers')),
          'positions':     safe_float(linkedin_profile.get('li_num_positions')),
          'exp_months':    safe_float(linkedin_profile.get('li_total_exp_months')),
          'skills':        linkedin_profile.get('li_skills', ''),
          'exp_titles':    linkedin_profile.get('li_exp_titles', ''),
          'exp_companies': linkedin_profile.get('li_exp_companies', ''),
          'edu_details':   linkedin_profile.get('li_edu_details', ''),
      },
      'stackoverflow': {
          'reputation': safe_float(stackoverflow_profile.get('so_reputation')),
          'answers':    safe_float(stackoverflow_profile.get('so_answer_count')),
          'top_tags':   stackoverflow_profile.get('so_top_tags', ''),
      },
      'community_activities': [
          f"{e.get('role', '')} at {e.get('organization', '')}"
          for e in (linkedin_raw_data.get('volunteering') or [])
      ] or ['No volunteering data found'],
  }

  # Package C: Career behaviour
  pkg_behavior = {
      'career_history': [
          {
              'title': row.get('job_title', ''),
              'company': row.get('company_name', ''),
              'type': row.get('employment_type', ''),
              'started': row.get('start_date', ''),
              'ended': row.get('end_date') or 'Present',
              'current': row.get('is_current', False),
              'duration_months': row.get('duration_months', 0),
          }
          for row in linkedin_experience_rows[:10]
      ],
      'education_history': [
          {
              'school': row.get('university_name', ''),
              'degree': row.get('fields_of_study', ''),
              'started': row.get('start_date', ''),
              'ended': row.get('end_date') or 'Present',
              'duration_months': row.get('duration_months', 0),
          }
          for row in linkedin_education_rows[:5]
      ],
      'certifications_list': [
          f"{c.get('name', '')} – {c.get('authority', '')}"
          for c in ((linkedin_raw_data.get('certification') or linkedin_raw_data.get('certifications') or []))
      ] or ['No certifications found'],
      'platform_scores': {p: platform_scores[p] for p in platform_scores if platform_scores[p]},
      'overall_score': overall_score,
  }

  print("✅ Data packages ready for LLM analysis")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 20 — LLM HOLISTIC ANALYSIS (generates candidate_analysis.json)       ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import re as _re

  SYSTEM_HOLISTIC = textwrap.dedent("""
  You are an expert HR evaluation AI. Analyse the candidate data above for recuirtment purpose and return
  ONLY a valid JSON object (no markdown fences, no extra text).

  The JSON must contain exactly these two top-level keys:
    "candidate_report"  and  "recruiter_report"

  ────────────────────────────────────────────────────────────────
  CANDIDATE REPORT keys:
  ────────────────────────────────────────────────────────────────
  "overall_trust_score"  : integer 0-1000  (computed from multi-platform evidence)
  "trust_score_breakdown": {
      "technical_skills"  : int 0-250,   // coding platforms
      "experience"        : int 0-200,   // work history quality
      "education"         : int 0-150,   // degree relevance
      "community_impact"  : int 0-150,   // SO, GitHub stars/forks
      "consistency"       : int 0-150,   // cross-platform alignment
      "fraud_penalty"     : int 0-(-100) // deduct if anomalies detected
  }
  "score_label"          : string  e.g. "Promising Candidate"
  "what_this_score_means": string  2-3 sentence plain-English explanation
  "platform_breakdown"   : list of objects:
      { "platform": str, "score": float, "max": 100, "grade": str,
        "key_finding": str, "improvement_tip": str }
  "top_3_strengths"      : list of 3 strings
  "areas_to_improve"     : list of 3-5 strings
  "score_improvement_simulator": list of objects:
      { "action": str, "estimated_score_gain": int, "difficulty": "Easy|Medium|Hard",
        "time_estimate": str }
  "salary_intelligence"  : {
      "estimated_range_usd_annual": {"min": int, "max": int},
      "market_percentile"         : int,
      "justification"             : str,
      "comparable_roles"          : list of str
  }
  "verified_credential_badge": {
      "badge_level"    : "Bronze|Silver|Gold|Platinum",
      "verified_items" : list of str,
      "unverified_items": list of str,
      "badge_explanation": str
  }

  ────────────────────────────────────────────────────────────────
  RECRUITER REPORT keys:
  ────────────────────────────────────────────────────────────────
  "trust_score_summary"  : { "score": int, "label": str, "one_liner": str }
  "reliability_assessment": {
      "overall_reliability" : "Low|Medium|High|Very High",
      "tenure_consistency"  : str,
      "platform_activity_trend": str,
      "red_flags"           : list of str,
      "positive_signals"    : list of str
  }
  "skill_verification_summary": list of objects:
      { "skill": str, "verified_by": list of str, "confidence": "Low|Medium|High" }
  "fraud_risk_indicator"  : {
      "risk_level"          : "Low|Medium|High|Critical",
      "risk_score"          : int (0-100, 0=no risk),
      "anomalies_detected"  : list of str,
      "suspicious_patterns" : list of str,
      "recommendation"      : str
  }
  "platform_evidence_cards": list of objects per platform:
      { "platform": str, "account_age_days": int, "activity_level": str,
        "authenticity_signals": list of str, "concern_signals": list of str }
  "salary_intelligence"   : {
      "recommended_offer_range_usd": {"min": int, "max": int},
      "negotiation_advice"         : str,
      "market_benchmark"           : str
  }
  "Recruiter_interview_probe_suggestions": list of objects:
      { "category": str, "question": str, "Answer": str,
        "follow_up": str }
    (provide at least 6 probes covering: technical depth, experience gaps,
    fraud verification, behavioral, motivation, culture fit)

  Be analytical, fair, and base everything strictly on the data provided.
  Flag any inconsistency (e.g. short tenure listed as long, no repo activity,
  very low SO reputation despite listed skills).
  """
  )

  USER_HOLISTIC = json.dumps({
      'skill_package':      pkg_skill,
      'identity_package':   pkg_identity,
      'behavior_package':   pkg_behavior,
      'mathematical_scores': {
          'overall_score':    overall_score,
          'grade':            grade,
          'platform_scores':  {p: round(s * 10, 1) if s else None for p, s in platform_scores.items()},
      },
  }, default=str)

  analysis = {}

  if llm_client:
      print("🔄 Calling LLM for holistic analysis...")
      try:
          response = llm_client.chat.completions.create(
              model=AZURE_OPENAI_DEPLOYMENT,
              temperature=0.2,
              response_format={'type': 'json_object'},
              messages=[
                  {'role': 'system', 'content': SYSTEM_HOLISTIC},
                  {'role': 'user',   'content': USER_HOLISTIC},
              ]
          )
          raw_output = response.choices[0].message.content
          analysis   = json.loads(raw_output)
          print("✅ LLM holistic analysis complete")
          print(analysis)
      except Exception as e:
          print(f"✗ LLM error: {e}")
          # Try regex extraction as fallback
          try:
              m = _re.search(r'\{.*\}', raw_output, _re.DOTALL)
              if m: analysis = json.loads(m.group(0))
          except Exception:
              pass
  else:
      print("⚠️  No LLM client — building rule-based analysis summary")
      analysis = {
          'candidate_name':         linkedin_profile.get('li_name') or 'Unknown',
          'contact_info':           'Not provided',
          'summary':                'Analysis generated without LLM. See mathematical scores for details.',
          'years_of_experience':    round(safe_float(linkedin_profile.get('li_total_exp_months', 0)) / 12, 1),
          'top_5_skills':           (linkedin_profile.get('li_skills', '') or '').split(', ')[:5],
          'platform_insights':      {p.lower(): f'Score: {round(s*10,1)}/100' if s else 'No data'
                                    for p, s in platform_scores.items()},
          'strengths':              ['Mathematical scoring complete — LLM assessment skipped'],
          'areas_for_improvement':  ['Configure AZURE_OPENAI_API_KEY for full LLM analysis'],
          'recommended_roles':      ['See detailed_assessment for role recommendations'],
          'mathematical_score':     overall_score,
          'overall_score':          overall_score,
          'score_breakdown':        {p: round(s * 10, 1) if s else 0 for p, s in platform_scores.items()},
          'hire_recommendation':    grade if grade in ('A+', 'A') else ('Yes' if grade in ('B+', 'B') else 'Neutral'),
          'detailed_assessment':    {
              'technical_depth':     f"LeetCode: {leetcode_profile.get('lc_total_solved', 0)} problems. GitHub: {github_profile.get('gh_original_repos', 0)} repos.",
              'domain_expertise':    f"HackerRank: {hackerrank_profile.get('hr_total_badges', 0)} badges. Skills: {linkedin_profile.get('li_num_skills', 0)}.",
              'competitive_ability': f"LeetCode Hard: {leetcode_profile.get('lc_hard_solved', 0)}. Contest rating: {leetcode_profile.get('lc_contest_rating', 'N/A')}.",
              'professional_brand':  f"LinkedIn connections: {linkedin_profile.get('li_connections', 'N/A')}. SO reputation: {stackoverflow_profile.get('so_reputation', 0)}.",
              'career_trajectory':   f"Experience: {round(safe_float(linkedin_profile.get('li_total_exp_months',0))/12,1)} years. Positions: {linkedin_profile.get('li_num_positions',0)}.",
          },
      }

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 21 — ASSEMBLE & SAVE candidate_analysis.json                         ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  def format_interview_qa(probes):
    if not probes:
        return []

    formatted = []
    for i, p in enumerate(probes, 1):
        q = p.get("question", "")
        a = p.get("Answer", "")
        
        formatted.append(f"Q{i}. {q} Ans{i}: {a}")

    return formatted

  def list_to_paragraph(items):
    if not items:
        return ""
    return " ".join([f"{i+1}. {item}." for i, item in enumerate(items)])
  # Ensure mathematical score is always present
  analysis['mathematical_score'] = overall_score
  analysis['score_breakdown'] = {
      p: round(s * 10, 1) if s is not None else None
      for p, s in platform_scores.items()
  }
  analysis['grade']  = grade
  analysis['verdict']= lbl
  def safe_score(val):
    if val is None:
        return -1   # 🔥 missing platform
    try:
        return int(val * 10)
    except:
        return -1
    # --- Add the specific format requested by user ---
  # Mapping internal keys to user-requested keys
  analysis['user_formatted_result'] = { 
    "score": analysis.get("candidate_report", {}).get("overall_trust_score", 0),

    "pros": list_to_paragraph(
        analysis.get("recruiter_report", {})
        .get("reliability_assessment", {})
        .get("positive_signals", ["Good technical presence"])
    ),

    "cons": list_to_paragraph(
        analysis.get("recruiter_report", {})
        .get("reliability_assessment", {})
        .get("red_flags", ["No major risks identified"])
    ),
      
    "interview_question": format_interview_qa(
    analysis.get("recruiter_report", {})
    .get("Recruiter_interview_probe_suggestions", [])
    ),

    "improvements": list_to_paragraph(
        analysis.get("candidate_report", {})
        .get("areas_to_improve", ["Improve platform activity"])
    ),

    "platform_scores": {
        "github": safe_score(platform_scores.get('GitHub')),
        "leetcode": safe_score(platform_scores.get('LeetCode')),
        "hackerrank": safe_score(platform_scores.get('HackerRank')),
        "linkedin": safe_score(platform_scores.get('LinkedIn')),
        "stack_overflow": safe_score(platform_scores.get('StackOverflow'))
    }
}
# Scraper metadata
  analysis['scraper_metadata'] = {
      'linkedin_url':     linkedin,
      'github_url':       github,
      'leetcode_url':     leetcode,
      'hackerrank_url':   hackerrank,
      'stackoverflow_url':stackoverflow,
      'platforms_available': list(present_platforms.keys()),
      'platforms_missing':   list(absent_platforms.keys()),
      'analysis_timestamp':  datetime.now().isoformat(),
  }

  # Raw platform profiles
  analysis['raw_profiles'] = {
      'github':        github_profile,
      'leetcode':      leetcode_profile,
      'hackerrank':    hackerrank_profile,
      'linkedin':      linkedin_profile,
      'stackoverflow': stackoverflow_profile,
  }

  # Sub-metric scores
  analysis['sub_metric_scores'] = {
      'github':        github_scores,
      'leetcode':      leetcode_scores,
      'hackerrank':    hackerrank_scores,
      'linkedin':      linkedin_scores,
      'stackoverflow': stackoverflow_scores,
  }

  # Save final output
  with open('candidate_analysis.json', 'w', encoding='utf-8') as f:
      json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)

  print("\n" + "═"*62)
  print("  ✅  PIPELINE COMPLETE")
  print("═"*62)
  print(f"  Candidate       : {analysis.get('candidate_name', 'N/A')}")
  print(f"  Overall Score   : {overall_score} / 1000  ({grade} — {lbl})")
  print(f"  User Result     : {json.dumps(analysis['user_formatted_result'], indent=2)}")
  print("═"*62)

  return analysis['user_formatted_result']

class ScoreRequest(BaseModel):
    user_id: str

@app.post("/generate-score")
def generate_score(req: ScoreRequest):
    user_id = (req.user_id or "").strip()
    if not user_id:
        raise_api_error(
            400,
            "invalid_user_id",
            "`user_id` must not be blank.",
            {"field": "user_id"},
        )

    print("👉 Received user_id:", user_id)

    # 1️⃣ Fetch links
    platform_links = get_platform_links_by_user(user_id)

    print("📦 Fetched links:", platform_links)

    # 2️⃣ Run ML model
    try:
        result = simulated_ml_model(platform_links)
    except HTTPException:
        raise
    except Exception:
        traceback.print_exc()
        raise_api_error(
            502,
            "model_execution_failed",
            "The scoring pipeline failed while generating the candidate score.",
        )

    if not result:
        raise_api_error(
            502,
            "model_execution_failed",
            "The scoring pipeline returned no result.",
        )

    if not isinstance(result, dict):
        raise_api_error(
            502,
            "model_response_invalid",
            "The scoring pipeline returned an invalid response payload.",
        )

    # 3️⃣ Store candidate and platform scores in Supabase
    persist_score_results(user_id, result)

    return {
        "status": "success",
        "data": result
    }


        
'''@app.post("/test-links")
def test_links(platform_links: dict[str, str]):
    try:
        print("📦 Received:", platform_links)

        result = simulated_ml_model(platform_links)

        return {
            "status": "success",
            "data": result
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}'''
    
