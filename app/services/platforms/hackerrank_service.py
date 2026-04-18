from __future__ import annotations

from app.models.platform_models import HackerRankModel
from app.utils.helpers import coerce_int, safe_float, safe_request, slug_from_url


HR_DOMAINS = [
    "Problem Solving",
    "Python",
    "Java",
    "C",
    "C++",
    "SQL",
    "Databases",
    "Linux Shell",
    "Regex",
    "JavaScript",
    "Rest API",
    "Go",
    "Ruby",
]


class HackerRankService:
    def __init__(self) -> None:
        self.headers = {"User-Agent": "Mozilla/5.0"}

    def _stars_for_domain(self, badges: list[dict], domain: str) -> int:
        for badge in badges:
            if domain.lower() in str(badge.get("badge_name", "")).lower():
                return int(badge.get("stars", 0) or 0)
        return 0

    def fetch_profile(self, profile_url: str) -> HackerRankModel | None:
        username = slug_from_url(profile_url)
        if not username:
            return None

        profile_res = safe_request(
            "GET",
            f"https://www.hackerrank.com/rest/hackers/{username}/profile",
            headers=self.headers,
        )
        badges_res = safe_request(
            "GET",
            f"https://www.hackerrank.com/rest/hackers/{username}/badges",
            headers=self.headers,
        )
        if not profile_res or not profile_res.ok or not badges_res or not badges_res.ok:
            return None

        profile_data = (profile_res.json() or {}).get("model", {})
        badges = (badges_res.json() or {}).get("models", [])

        skill_lines = [
            f"{badge.get('badge_name')} ({badge.get('stars', 0)} stars)"
            for badge in badges
            if int(badge.get("stars", 0) or 0) > 0
        ]
        domain_stars = {
            domain.lower().replace(" ", "_"): self._stars_for_domain(badges, domain)
            for domain in HR_DOMAINS
        }

        return HackerRankModel(
            profile_url=profile_url,
            username=username,
            rank=coerce_int(profile_data.get("level"), 0) or None,
            score=safe_float(profile_data.get("score"), 0.0) or None,
            country=profile_data.get("country"),
            skills=skill_lines,
            total_badges=len(skill_lines),
            domain_stars=domain_stars,
        )
