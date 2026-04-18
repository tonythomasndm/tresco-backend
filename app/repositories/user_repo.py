from __future__ import annotations

import traceback
from typing import Any

from app.external.supabase_client import get_supabase_client
from app.utils.helpers import normalize_platform_name, raise_api_error


class UserRepository:
    def __init__(self) -> None:
        try:
            self.client = get_supabase_client()
        except RuntimeError as exc:
            raise_api_error(503, "supabase_not_configured", str(exc))

    def get_user_or_error(self, user_id: str) -> dict[str, Any]:
        try:
            response = self.client.table("users").select("id").eq("id", user_id).execute()
        except Exception:
            traceback.print_exc()
            raise_api_error(
                503,
                "supabase_user_lookup_failed",
                "Failed to fetch the user from Supabase.",
            )

        if not response.data:
            raise_api_error(404, "user_not_found", "User not found.")

        return response.data[0]

    def get_platform_accounts_by_user(
        self,
        user_id: str,
        *,
        include_profile_url: bool = False,
    ) -> list[dict[str, Any]]:
        columns = "id, platform_name, profile_url" if include_profile_url else "id, platform_name"

        try:
            response = (
                self.client.table("platform_accounts")
                .select(columns)
                .eq("user_id", user_id)
                .execute()
            )
        except Exception:
            traceback.print_exc()
            raise_api_error(
                503,
                "supabase_platform_lookup_failed",
                "Failed to fetch platform accounts from Supabase.",
            )

        accounts = response.data or []
        if not accounts:
            raise_api_error(
                404,
                "platform_accounts_not_found",
                "No platform accounts found for this user.",
            )

        return accounts

    def get_platform_links_by_user(self, user_id: str) -> dict[str, str]:
        normalized_user_id = (user_id or "").strip()
        if not normalized_user_id:
            raise_api_error(
                400,
                "invalid_user_id",
                "`user_id` must not be blank.",
                {"field": "user_id"},
            )

        self.get_user_or_error(normalized_user_id)
        accounts = self.get_platform_accounts_by_user(
            normalized_user_id,
            include_profile_url=True,
        )

        platform_links: dict[str, str] = {}
        for account in accounts:
            platform_name = normalize_platform_name(account.get("platform_name", ""))
            if platform_name == "stack_overflow":
                platform_name = "stackoverflow"

            profile_url = account.get("profile_url")
            if platform_name and profile_url:
                platform_links[platform_name] = profile_url

        if not platform_links:
            raise_api_error(
                404,
                "platform_links_not_found",
                "No valid platform links found for this user.",
            )

        return platform_links
