from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import Any

from app.external.supabase_client import get_supabase_client
from app.utils.helpers import get_platform_score_key, raise_api_error


class ScoreRepository:
    def __init__(self) -> None:
        try:
            self.client = get_supabase_client()
        except RuntimeError as exc:
            raise_api_error(503, "supabase_not_configured", str(exc))

    def persist_score_results(
        self,
        user_id: str,
        result: dict[str, Any],
        platform_accounts: list[dict[str, Any]],
    ) -> None:
        normalized_user_id = (user_id or "").strip()
        if not normalized_user_id:
            raise_api_error(
                400,
                "invalid_user_id",
                "`user_id` must not be blank.",
                {"field": "user_id"},
            )

        current_time = datetime.now(timezone.utc).isoformat()
        candidate_score = result.get("score", 0) or 0

        try:
            analysis_insert = self.client.table("candidate_score_analysis").insert(
                {
                    "user_id": normalized_user_id,
                    "score": candidate_score,
                    "pros": result.get("pros", ""),
                    "cons": result.get("cons", ""),
                    "improvements": result.get("improvements", ""),
                    "is_fraud": False,
                    "created_at": current_time,
                }
            ).execute()
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

        platform_scores = result.get("platform_scores") or {}
        for account in platform_accounts:
            score_key = get_platform_score_key(account.get("platform_name", ""))
            platform_score = platform_scores.get(score_key, 0) or 0

            try:
                insert_res = self.client.table("platform_score").insert(
                    {
                        "platform_account_id": account["id"],
                        "score": platform_score,
                        "created_at": current_time,
                    }
                ).execute()
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
