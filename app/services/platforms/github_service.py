from __future__ import annotations

import re
from typing import Any

from app.core.config import get_settings
from app.models.platform_models import GitHubModel, GitHubRepoModel
from app.utils.helpers import activity_level_from_timestamps, safe_request, slug_from_url


class GitHubService:
    def __init__(self) -> None:
        settings = get_settings()
        self.token = settings.github_token

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/vnd.github+json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def fetch_profile(self, profile_url: str) -> GitHubModel | None:
        username = slug_from_url(profile_url)
        if not username:
            return None

        headers = self._headers()
        profile_res = safe_request("GET", f"https://api.github.com/users/{username}", headers=headers)
        if not profile_res or not profile_res.ok:
            return None

        profile_data = profile_res.json()
        all_repos: list[dict[str, Any]] = []
        next_url = f"https://api.github.com/users/{username}/repos?per_page=100&sort=pushed"

        while next_url:
            repo_res = safe_request("GET", next_url, headers=headers)
            if not repo_res or not repo_res.ok:
                break
            payload = repo_res.json()
            if isinstance(payload, list):
                all_repos.extend(payload)
            match = re.search(r'<([^>]+)>;\s*rel="next"', repo_res.headers.get("Link", ""))
            next_url = match.group(1) if match else None

        events_res = safe_request(
            "GET",
            f"https://api.github.com/users/{username}/events/public?per_page=100",
            headers=headers,
        )
        events = events_res.json() if events_res and events_res.ok and isinstance(events_res.json(), list) else []
        recent_commits = sum(
            int((event.get("payload") or {}).get("size", 0))
            for event in events
            if event.get("type") == "PushEvent"
        )

        original_repos = [repo for repo in all_repos if not repo.get("fork")]
        languages = sorted({repo.get("language") for repo in original_repos if repo.get("language")})
        total_stars = sum(int(repo.get("stargazers_count", 0)) for repo in original_repos)
        total_forks = sum(int(repo.get("forks_count", 0)) for repo in original_repos)

        repo_models = [
            GitHubRepoModel(
                repo_name=repo.get("name", ""),
                repo_url=repo.get("html_url", ""),
                stars=int(repo.get("stargazers_count", 0)),
                forks=int(repo.get("forks_count", 0)),
                language=repo.get("language"),
                description=repo.get("description"),
                updated_at=repo.get("pushed_at"),
                size_kb=int(repo.get("size", 0)),
                open_issues=int(repo.get("open_issues_count", 0)),
                topics=", ".join(repo.get("topics", [])),
            )
            for repo in original_repos
        ]

        activity_timestamps = [repo.get("pushed_at") for repo in original_repos]
        activity_timestamps.extend(event.get("created_at") for event in events)

        return GitHubModel(
            profile_url=profile_url,
            username=profile_data.get("login", username),
            name=profile_data.get("name"),
            bio=profile_data.get("bio"),
            company=profile_data.get("company"),
            blog=profile_data.get("blog"),
            location=profile_data.get("location"),
            followers=int(profile_data.get("followers", 0) or 0),
            following=int(profile_data.get("following", 0) or 0),
            public_repos=int(profile_data.get("public_repos", 0) or 0),
            original_repos=len(original_repos),
            stars=total_stars,
            forks_received=total_forks,
            top_repo_stars=max((repo.stars for repo in repo_models), default=0),
            commits=recent_commits,
            languages=languages,
            account_created=profile_data.get("created_at"),
            activity_level=activity_level_from_timestamps(activity_timestamps),
            repos_data=repo_models,
        )
