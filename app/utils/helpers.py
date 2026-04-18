from __future__ import annotations

import builtins
import json
import math
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from fastapi import HTTPException


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
]


def default_error_code(status_code: int) -> str:
    return {
        400: "bad_request",
        404: "not_found",
        422: "invalid_request",
        500: "internal_server_error",
        502: "bad_gateway",
        503: "service_unavailable",
    }.get(status_code, "request_failed")


def build_error_payload(error_code: str, message: str, details: Any = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"status": "error", "error": {"code": error_code, "message": message}}
    if details is not None:
        payload["error"]["details"] = details
    return payload


def raise_api_error(status_code: int, error_code: str, message: str, details: Any = None) -> None:
    detail = {"code": error_code, "message": message}
    if details is not None:
        detail["details"] = details
    raise HTTPException(status_code=status_code, detail=detail)


def safe_log(*args: Any, **kwargs: Any) -> None:
    try:
        builtins.print(*args, **kwargs)
    except UnicodeEncodeError:
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        stream = kwargs.get("file") or sys.stdout
        flush = kwargs.get("flush", False)
        encoding = getattr(stream, "encoding", None) or sys.getdefaultencoding() or "utf-8"
        message = sep.join(str(arg) for arg in args)
        try:
            safe_message = message.encode(encoding, errors="backslashreplace").decode(encoding)
        except LookupError:
            safe_message = message.encode("utf-8", errors="backslashreplace").decode("utf-8")
        builtins.print(safe_message, end=end, file=stream, flush=flush)


def normalize_platform_name(platform_name: str) -> str:
    return re.sub(r"[\s\-]+", "_", (platform_name or "").strip().lower())


def get_platform_score_key(platform_name: str) -> str:
    normalized_name = normalize_platform_name(platform_name)
    if normalized_name == "stackoverflow":
        return "stack_overflow"
    return normalized_name


def slug_from_url(url: str, fallback: str | None = None) -> str | None:
    if not url:
        return fallback
    parts = [part for part in url.rstrip("/").split("/") if part]
    slug = parts[-1].split("?")[0].split("#")[0] if parts else None
    return slug or fallback


def stackoverflow_user_id(url_or_id: str | int | None) -> str | None:
    if url_or_id is None:
        return None
    value = str(url_or_id).strip()
    if value.isdigit():
        return value
    match = re.search(r"/users/(\d+)", value)
    return match.group(1) if match else None


def safe_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    retries: int = 3,
    delay: float = 1.5,
    timeout: int = 20,
) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", delay * (attempt + 1) * 2))
                time.sleep(retry_after)
                continue
            return response
        except requests.exceptions.Timeout:
            time.sleep(delay * (attempt + 1))
        except Exception:
            traceback.print_exc()
            break
    return None


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return default if math.isnan(result) else result
    except (TypeError, ValueError):
        return default


def coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "present", "current"}
    return bool(value)


def is_numeric(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return not (isinstance(value, float) and math.isnan(value))
    try:
        float(str(value).strip())
        return True
    except (TypeError, ValueError):
        return False


def split_platform_profile(profile_dict: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    numeric: dict[str, Any] = {}
    contextual: dict[str, Any] = {}
    for key, value in profile_dict.items():
        if is_numeric(value):
            numeric[key] = value
        else:
            contextual[key] = value
    return numeric, contextual


def save_platform_csv(
    platform: str,
    profile_dict: dict[str, Any],
    *,
    extra_numeric: dict[str, Any] | None = None,
    extra_contextual: dict[str, Any] | None = None,
    output_dir: Path | None = None,
) -> None:
    if not profile_dict:
        return

    output_dir = output_dir or Path.cwd()
    numeric, contextual = split_platform_profile(profile_dict)

    if extra_numeric:
        for key, value in extra_numeric.items():
            numeric[key] = value
            contextual.pop(key, None)

    if extra_contextual:
        for key, value in extra_contextual.items():
            contextual[key] = value
            numeric.pop(key, None)

    pd.DataFrame([numeric]).to_csv(output_dir / f"{platform}_numeric.csv", index=False)
    pd.DataFrame([contextual]).to_csv(output_dir / f"{platform}_contextual.csv", index=False)


def write_json_artifact(name: str, payload: dict[str, Any], output_dir: Path | None = None) -> None:
    output_dir = output_dir or Path.cwd()
    with open(output_dir / name, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, default=str)


def list_to_paragraph(items: list[str] | None) -> str:
    if not items:
        return ""
    return " ".join(f"{index + 1}. {item}." for index, item in enumerate(items) if item)


def format_interview_qa(probes: list[dict[str, Any]] | None) -> str:
    if not probes:
        return ""
    formatted: list[str] = []
    for index, probe in enumerate(probes, start=1):
        question = str(probe.get("question", "")).strip()
        answer = str(probe.get("Answer") or probe.get("expected_answer") or "").strip()
        if question and answer:
            formatted.append(f"Q{index}. {question} Ans{index}: {answer}")
    return " ".join(formatted)


def activity_level_from_timestamps(timestamps: list[str | int | None]) -> str:
    valid_dates: list[datetime] = []
    for value in timestamps:
        if value in (None, ""):
            continue
        try:
            if isinstance(value, int):
                valid_dates.append(datetime.fromtimestamp(value))
            else:
                valid_dates.append(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except Exception:
            continue

    if not valid_dates:
        return "low"

    latest = max(valid_dates)
    age_days = (datetime.utcnow() - latest.replace(tzinfo=None)).days
    if age_days <= 14:
        return "high"
    if age_days <= 60:
        return "moderate"
    return "low"


def extract_domain(url: str | None) -> str:
    if not url:
        return ""
    return urlparse(url).netloc.lower()
