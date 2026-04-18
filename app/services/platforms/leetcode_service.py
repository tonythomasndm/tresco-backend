from __future__ import annotations

from app.models.platform_models import LeetCodeModel
from app.utils.helpers import coerce_int, safe_float, safe_request, slug_from_url


LC_QUERY = """
query getUserProfile($username: String!) {
  matchedUser(username: $username) {
    profile { ranking reputation starRating }
    submitStats { acSubmissionNum { difficulty count } }
    badges { name icon }
    languageProblemCount { languageName problemsSolved }
    tagProblemCounts {
      advanced { tagName problemsSolved }
      intermediate { tagName problemsSolved }
      fundamental { tagName problemsSolved }
    }
  }
  userContestRanking(username: $username) {
    rating globalRanking totalParticipants topPercentage attendedContestsCount
  }
}
"""


class LeetCodeService:
    def __init__(self) -> None:
        self.headers = {
            "Content-Type": "application/json",
            "Referer": "https://leetcode.com",
            "User-Agent": "Mozilla/5.0",
        }

    def fetch_profile(self, profile_url: str) -> LeetCodeModel | None:
        username = slug_from_url(profile_url)
        if not username or username == "u":
            parts = [part for part in profile_url.rstrip("/").split("/") if part and part != "u"]
            username = parts[-1] if parts else None
        if not username:
            return None

        response = safe_request(
            "POST",
            "https://leetcode.com/graphql",
            headers=self.headers,
            json_body={"query": LC_QUERY, "variables": {"username": username}},
        )
        if not response or not response.ok:
            return None

        data = (response.json() or {}).get("data", {})
        user = data.get("matchedUser") or {}
        contest = data.get("userContestRanking") or {}
        profile = user.get("profile") or {}
        stats = ((user.get("submitStats") or {}).get("acSubmissionNum") or [])
        solved_map = {item.get("difficulty"): item.get("count", 0) for item in stats}

        advanced_tags = [
            (tag.get("tagName"), tag.get("problemsSolved", 0))
            for tag in ((user.get("tagProblemCounts") or {}).get("advanced") or [])
        ]
        advanced_tags.sort(key=lambda item: item[1], reverse=True)

        languages = [
            f"{item.get('languageName')}({item.get('problemsSolved', 0)})"
            for item in (user.get("languageProblemCount") or [])
        ]
        badges = [badge.get("name") for badge in (user.get("badges") or []) if badge.get("name")]

        return LeetCodeModel(
            profile_url=profile_url,
            username=username,
            ranking=coerce_int(profile.get("ranking"), 0) or None,
            problems_solved=int(solved_map.get("All", 0) or 0),
            easy_solved=int(solved_map.get("Easy", 0) or 0),
            medium_solved=int(solved_map.get("Medium", 0) or 0),
            hard_solved=int(solved_map.get("Hard", 0) or 0),
            contest_rating=safe_float(contest.get("rating"), 0.0) or None,
            contest_rank=coerce_int(contest.get("globalRanking"), 0) or None,
            top_percentage=safe_float(contest.get("topPercentage"), 0.0) if contest.get("topPercentage") is not None else None,
            contests_attended=int(contest.get("attendedContestsCount", 0) or 0),
            star_rating=safe_float(profile.get("starRating"), 0.0) if profile.get("starRating") is not None else None,
            reputation=coerce_int(profile.get("reputation"), 0) or None,
            badges=badges,
            languages=languages[:5],
            top_topics=[tag_name for tag_name, _ in advanced_tags[:5] if tag_name],
        )
