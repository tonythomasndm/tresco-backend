from __future__ import annotations

import ast
import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
ENV_LOCAL_PATH = BASE_DIR / ".env.local"
ENV_PATH = BASE_DIR / ".env"

if ENV_LOCAL_PATH.exists():
    load_dotenv(ENV_LOCAL_PATH, override=False)
elif ENV_PATH.exists():
    load_dotenv(ENV_PATH, override=False)


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default

    def _clean_item(item: str) -> str:
        cleaned = item.strip()
        if not cleaned:
            return ""
        cleaned = cleaned.removeprefix("[").removesuffix("]").strip()
        if len(cleaned) >= 2 and (
            (cleaned[0] == '"' and cleaned[-1] == '"')
            or (cleaned[0] == "'" and cleaned[-1] == "'")
        ):
            cleaned = cleaned[1:-1].strip()
        if cleaned.startswith(("http://", "https://")):
            cleaned = cleaned.rstrip("/")
        return cleaned

    raw = value.strip()
    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(raw)
        except Exception:
            continue
        if isinstance(parsed, (list, tuple, set)):
            items = [_clean_item(str(item)) for item in parsed]
            cleaned_items = [item for item in items if item]
            if cleaned_items:
                return cleaned_items

    items = [_clean_item(item) for item in raw.split(",")]
    cleaned_items = [item for item in items if item]
    return cleaned_items or default


def _get_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    cleaned = value.strip()
    if len(cleaned) >= 2 and (
        (cleaned[0] == '"' and cleaned[-1] == '"')
        or (cleaned[0] == "'" and cleaned[-1] == "'")
    ):
        cleaned = cleaned[1:-1].strip()
    return cleaned


def _get_first_str(names: list[str], default: str = "") -> str:
    for name in names:
        value = _get_str(name, "")
        if value:
            return value

    # Fallback: tolerate env keys with different case or accidental spaces.
    normalized_env: dict[str, str] = {}
    for key, value in os.environ.items():
        normalized_key = key.strip().upper()
        if normalized_key and normalized_key not in normalized_env:
            normalized_env[normalized_key] = value

    for name in names:
        raw_value = normalized_env.get(name.strip().upper())
        if raw_value is None:
            continue
        cleaned = str(raw_value).strip()
        if len(cleaned) >= 2 and (
            (cleaned[0] == '"' and cleaned[-1] == '"')
            or (cleaned[0] == "'" and cleaned[-1] == "'")
        ):
            cleaned = cleaned[1:-1].strip()
        if cleaned:
            return cleaned

    return default


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    cors_origins: list[str]
    supabase_url: str
    supabase_key: str
    azure_openai_endpoint: str
    azure_openai_deployment: str
    azure_openai_api_key: str
    github_token: str
    apify_token: str
    apify_linkedin_actor_id: str
    stackoverflow_api_key: str
    write_pipeline_artifacts: bool


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        app_name=_get_str("APP_NAME", "TrustScore ML Backend"),
        app_env=_get_str("APP_ENV", "development"),
        cors_origins=_get_list(
            "CORS_ORIGINS",
            [
                "http://localhost:5173",
                "http://localhost:3000",
                "https://tresco.vercel.app",
            ],
        ),
        supabase_url=_get_first_str(["SUPABASE_URL", "NEXT_PUBLIC_SUPABASE_URL"], ""),
        supabase_key=_get_first_str(
            ["SUPABASE_KEY", "SUPABASE_ANON_KEY", "NEXT_PUBLIC_SUPABASE_ANON_KEY"],
            "",
        ),
        azure_openai_endpoint=_get_first_str(["AZURE_OPENAI_ENDPOINT", "OPENAI_BASE_URL"], ""),
        azure_openai_deployment=_get_str("AZURE_OPENAI_DEPLOYMENT", "gpt-5.4-mini"),
        azure_openai_api_key=_get_first_str(["AZURE_OPENAI_API_KEY", "OPENAI_API_KEY"], ""),
        github_token=_get_str("GITHUB_TOKEN", ""),
        apify_token=_get_first_str(
            ["APIFY_TOKEN", "APIFY_API_TOKEN", "APIFY_API_KEY", "APIFY_CLIENT_TOKEN"],
            "",
        ),
        apify_linkedin_actor_id=_get_str("APIFY_LINKEDIN_ACTOR_ID", "EacyHlzi4GOX8oMge"),
        stackoverflow_api_key=_get_str("SO_API_KEY", ""),
        write_pipeline_artifacts=_get_bool("WRITE_PIPELINE_ARTIFACTS", True),
    )
