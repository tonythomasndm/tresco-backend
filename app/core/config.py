from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env.local", override=False)


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


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
    datamagnet_token: str
    stackoverflow_api_key: str
    write_pipeline_artifacts: bool


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("APP_NAME", "TrustScore ML Backend"),
        app_env=os.getenv("APP_ENV", "development"),
        cors_origins=_get_list(
            "CORS_ORIGINS",
            [
                "http://localhost:5173",
                "http://localhost:3000",
                "https://tresco.vercel.app",
            ],
        ),
        supabase_url=os.getenv("SUPABASE_URL", ""),
        supabase_key=os.getenv("SUPABASE_KEY", ""),
        azure_openai_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", ""),
        azure_openai_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-5.4-mini"),
        azure_openai_api_key=os.getenv("AZURE_OPENAI_API_KEY", ""),
        github_token=os.getenv("GITHUB_TOKEN", ""),
        datamagnet_token=os.getenv("DATAMAGNET_TOKEN", ""),
        stackoverflow_api_key=os.getenv("SO_API_KEY", ""),
        write_pipeline_artifacts=_get_bool("WRITE_PIPELINE_ARTIFACTS", True),
    )
