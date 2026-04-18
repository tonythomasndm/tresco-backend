from __future__ import annotations

from app.core.config import get_settings
from app.models.platform_models import StackOverflowModel
from app.utils.helpers import safe_request, stackoverflow_user_id


class StackOverflowService:
    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.stackoverflow_api_key
        self.base_url = "https://api.stackexchange.com/2.3"

    def _params(self) -> dict[str, str]:
        params = {"site": "stackoverflow"}
        if self.api_key:
            params["key"] = self.api_key
        return params

    def fetch_profile(self, profile_url: str) -> StackOverflowModel | None:
        user_id = stackoverflow_user_id(profile_url)
        if not user_id:
            return None

        params = self._params()
        profile_res = safe_request("GET", f"{self.base_url}/users/{user_id}", params=params)
        if not profile_res or not profile_res.ok:
            return None

        tags_res = safe_request(
            "GET",
            f"{self.base_url}/users/{user_id}/tags",
            params={**params, "pagesize": 20, "sort": "activity"},
        )
        answers_res = safe_request(
            "GET",
            f"{self.base_url}/users/{user_id}/answers",
            params={**params, "pagesize": 30, "sort": "votes", "filter": "withbody"},
        )

        profile_items = (profile_res.json() or {}).get("items", [])
        tags = ((tags_res.json() or {}).get("items", []) if tags_res and tags_res.ok else [])
        answers = (
            (answers_res.json() or {}).get("items", [])
            if answers_res and answers_res.ok
            else []
        )
        profile_data = profile_items[0] if profile_items else {}

        badge_counts = profile_data.get("badge_counts", {})
        top_tags = [tag.get("name") for tag in tags if tag.get("name")]
        accepted_answers = sum(1 for answer in answers if answer.get("is_accepted"))
        avg_answer_score = round(
            sum(int(answer.get("score", 0) or 0) for answer in answers) / max(len(answers), 1),
            1,
        )

        return StackOverflowModel(
            profile_url=profile_data.get("link") or profile_url,
            user_id=user_id,
            display_name=profile_data.get("display_name"),
            reputation=int(profile_data.get("reputation", 0) or 0),
            answer_count=int(profile_data.get("answer_count", 0) or 0),
            question_count=int(profile_data.get("question_count", 0) or 0),
            gold_badges=int(badge_counts.get("gold", 0) or 0),
            silver_badges=int(badge_counts.get("silver", 0) or 0),
            bronze_badges=int(badge_counts.get("bronze", 0) or 0),
            accepted_answers=accepted_answers,
            avg_answer_score=avg_answer_score,
            top_tags=top_tags[:10],
            account_created=profile_data.get("creation_date"),
            last_access=profile_data.get("last_access_date"),
        )
