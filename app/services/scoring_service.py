from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from app.models.platform_models import (
    GitHubModel,
    HackerRankModel,
    LeetCodeModel,
    LinkedInModel,
    StackOverflowModel,
)
from app.utils.helpers import coerce_int, safe_float
from app.utils.mathematical import (
    clamp,
    compute_overall_score,
    iso_to_years_ago,
    score_to_grade,
    stars_to_score,
    weighted_platform_score,
)
from app.services.platforms.linkedin_service import (
    compute_linkedin_timeline_metrics,
    estimate_linkedin_company_prestige,
    parse_linkedin_date,
)


LINKEDIN_WEIGHTS = {
    "employment_consistency_score": 0.20,
    "career_progression_trajectory": 0.18,
    "company_prestige_score": 0.12,
    "skill_endorsement_credibility": 0.10,
    "recommendation_authenticity": 0.10,
    "profile_completeness": 0.08,
    "network_size_quality": 0.08,
    "education_verification_score": 0.07,
    "activity_frequency_score": 0.04,
    "content_quality_score": 0.03,
}

GITHUB_WEIGHTS = {
    "repository_originality": 0.22,
    "commit_frequency_score": 0.20,
    "contribution_graph_density_score": 0.15,
    "project_longevity": 0.12,
    "stars_received_score": 0.07,
    "forks_received_score": 0.03,
    "documentation_quality": 0.06,
    "ci_cd_usage": 0.02,
    "language_diversity": 0.06,
    "collaboration_network": 0.05,
    "repository_count_score": 0.02,
}

LEETCODE_WEIGHTS = {
    "problems_solved_score": 0.25,
    "global_ranking_score": 0.20,
    "top_percentage_score": 0.15,
    "contest_rating_score": 0.12,
    "contest_participation_score": 0.08,
    "acceptance_rate_score": 0.10,
    "difficulty_distribution": 0.05,
    "language_diversity": 0.03,
    "category_coverage": 0.02,
}

HACKERRANK_WEIGHTS = {
    "skill_certificates_score": 0.30,
    "avg_stars_score": 0.25,
    "domain_score_quality": 0.25,
    "badges_count_score": 0.12,
    "rank_score": 0.08,
}

STACKOVERFLOW_WEIGHTS = {
    "reputation_score": 0.30,
    "answer_volume_score": 0.20,
    "acceptance_rate_score": 0.20,
    "badge_quality_score": 0.15,
    "answer_quality_score": 0.10,
    "expertise_breadth": 0.05,
}


class ScoringService:
    def build_platform_context(
        self,
        *,
        github: GitHubModel | None,
        leetcode: LeetCodeModel | None,
        hackerrank: HackerRankModel | None,
        linkedin: LinkedInModel | None,
        stackoverflow: StackOverflowModel | None,
    ) -> dict[str, Any]:
        return {
            "github_profile": self.github_profile_dict(github),
            "github_repos": [repo.model_dump() for repo in github.repos_data] if github else [],
            "leetcode_profile": self.leetcode_profile_dict(leetcode),
            "hackerrank_profile": self.hackerrank_profile_dict(hackerrank),
            "linkedin_profile": self.linkedin_profile_dict(linkedin),
            "linkedin_raw_data": linkedin.source_payload if linkedin else {},
            "linkedin_experience_rows": [row.model_dump() for row in linkedin.experience] if linkedin else [],
            "linkedin_education_rows": [row.model_dump() for row in linkedin.education] if linkedin else [],
            "linkedin_skill_rows": [row.model_dump() for row in linkedin.skills] if linkedin else [],
            "linkedin_timeline_metrics": linkedin.timeline_metrics if linkedin else {},
            "stackoverflow_profile": self.stackoverflow_profile_dict(stackoverflow),
        }

    def github_profile_dict(self, github: GitHubModel | None) -> dict[str, Any]:
        if not github:
            return {}
        return {
            "gh_username": github.username,
            "gh_name": github.name,
            "gh_bio": github.bio,
            "gh_company": github.company,
            "gh_blog": github.blog,
            "gh_location": github.location,
            "gh_followers": github.followers,
            "gh_following": github.following,
            "gh_public_repos": github.public_repos,
            "gh_account_created": github.account_created,
            "gh_total_stars": github.stars,
            "gh_total_forks_got": github.forks_received,
            "gh_original_repos": github.original_repos,
            "gh_languages": ", ".join(github.languages),
            "gh_top_repo_stars": github.top_repo_stars,
            "gh_recent_commits": github.commits,
            "gh_activity_level": github.activity_level,
            "gh_profile_url": github.profile_url,
        }

    def leetcode_profile_dict(self, leetcode: LeetCodeModel | None) -> dict[str, Any]:
        if not leetcode:
            return {}
        return {
            "lc_username": leetcode.username,
            "lc_ranking": leetcode.ranking,
            "lc_total_solved": leetcode.problems_solved,
            "lc_easy_solved": leetcode.easy_solved,
            "lc_medium_solved": leetcode.medium_solved,
            "lc_hard_solved": leetcode.hard_solved,
            "lc_contest_rating": leetcode.contest_rating,
            "lc_contest_rank": leetcode.contest_rank,
            "lc_top_percentage": leetcode.top_percentage,
            "lc_contests_attended": leetcode.contests_attended,
            "lc_star_rating": leetcode.star_rating,
            "lc_reputation": leetcode.reputation,
            "lc_badges": ", ".join(leetcode.badges),
            "lc_languages": ", ".join(leetcode.languages),
            "lc_top_topics": ", ".join(leetcode.top_topics),
            "lc_profile_url": leetcode.profile_url,
        }

    def hackerrank_profile_dict(self, hackerrank: HackerRankModel | None) -> dict[str, Any]:
        if not hackerrank:
            return {}
        domain_scores = {f"hr_{key}_stars": value for key, value in hackerrank.domain_stars.items()}
        return {
            "hr_username": hackerrank.username,
            "hr_rank": hackerrank.rank,
            "hr_score": hackerrank.score,
            "hr_country": hackerrank.country,
            "hr_skills_raw": ", ".join(hackerrank.skills),
            "hr_total_badges": hackerrank.total_badges,
            "hr_profile_url": hackerrank.profile_url,
            **domain_scores,
        }

    def linkedin_profile_dict(self, linkedin: LinkedInModel | None) -> dict[str, Any]:
        if not linkedin:
            return {}
        return dict(linkedin.normalized_profile)

    def stackoverflow_profile_dict(self, stackoverflow: StackOverflowModel | None) -> dict[str, Any]:
        if not stackoverflow:
            return {}
        return {
            "so_user_id": stackoverflow.user_id,
            "so_display_name": stackoverflow.display_name,
            "so_reputation": stackoverflow.reputation,
            "so_answer_count": stackoverflow.answer_count,
            "so_question_count": stackoverflow.question_count,
            "so_gold_badges": stackoverflow.gold_badges,
            "so_silver_badges": stackoverflow.silver_badges,
            "so_bronze_badges": stackoverflow.bronze_badges,
            "so_accepted_answers": stackoverflow.accepted_answers,
            "so_avg_answer_score": stackoverflow.avg_answer_score,
            "so_top_tags": ", ".join(stackoverflow.top_tags),
            "so_account_created": stackoverflow.account_created,
            "so_last_access": stackoverflow.last_access,
            "so_profile_url": stackoverflow.profile_url,
        }

    def score_github(self, profile: dict[str, Any], repos: list[dict[str, Any]]) -> dict[str, Any]:
        if not profile:
            return {}

        repo_count = safe_float(profile.get("gh_original_repos"), 0)
        stars = safe_float(profile.get("gh_total_stars"), 0)
        forks = safe_float(profile.get("gh_total_forks_got"), 0)
        followers = safe_float(profile.get("gh_followers"), 0)
        recent_commits = safe_float(profile.get("gh_recent_commits"), 0)
        languages = str(profile.get("gh_languages", "")).split(", ") if profile.get("gh_languages") else []
        account_age_years = iso_to_years_ago(profile.get("gh_account_created"))

        documentation_signals = sum(
            1
            for repo in repos
            if (repo.get("description") or "").strip() or (repo.get("topics") or "").strip()
        )
        ci_cd_signals = sum(
            1
            for repo in repos
            if any(keyword in str(repo.get("topics", "")).lower() for keyword in ("ci", "cd", "github-actions"))
        )
        dense_activity = sum(1 for repo in repos if safe_float(repo.get("stars")) >= 3 or safe_float(repo.get("forks")) >= 1)

        return {
            "repository_originality": clamp(min(repo_count / 30, 1) * 100),
            "stars_received_score": clamp(min(stars / 50, 1) * 100),
            "forks_received_score": clamp(min(forks / 20, 1) * 100),
            "language_diversity": clamp(min(len([lang for lang in languages if lang]) / 8, 1) * 100),
            "collaboration_network": clamp(min(followers / 100, 1) * 100),
            "repository_count_score": clamp(min(repo_count / 20, 1) * 100),
            "project_longevity": clamp(min(account_age_years / 5, 1) * 100),
            "commit_frequency_score": clamp(min(recent_commits / 200, 1) * 100),
            "contribution_graph_density_score": clamp(min(dense_activity / max(len(repos), 1), 1) * 100),
            "documentation_quality": clamp(min(documentation_signals / max(len(repos), 1), 1) * 100),
            "ci_cd_usage": clamp(min(ci_cd_signals / max(len(repos), 1), 1) * 100),
        }

    def score_leetcode(self, profile: dict[str, Any]) -> dict[str, Any]:
        if not profile:
            return {}

        total = safe_float(profile.get("lc_total_solved"), 0)
        easy = safe_float(profile.get("lc_easy_solved"), 0)
        medium = safe_float(profile.get("lc_medium_solved"), 0)
        hard = safe_float(profile.get("lc_hard_solved"), 0)
        ranking = safe_float(profile.get("lc_ranking"), 0)
        contest_rating = safe_float(profile.get("lc_contest_rating"), 0)
        top_pct = safe_float(profile.get("lc_top_percentage"), 100)
        attended = safe_float(profile.get("lc_contests_attended"), 0)
        total_solved = max(total, 1)
        languages = [item for item in str(profile.get("lc_languages", "")).split(", ") if item]
        topics = [item for item in str(profile.get("lc_top_topics", "")).split(", ") if item]

        return {
            "problems_solved_score": clamp(min(total / 500, 1) * 100),
            "global_ranking_score": clamp((1 - min(ranking / 500000, 1)) * 100) if ranking else 0,
            "top_percentage_score": clamp((1 - top_pct / 100) * 100) if top_pct < 100 else 0,
            "contest_rating_score": clamp(min((contest_rating - 1200) / 1300, 1) * 100) if contest_rating > 1200 else 0,
            "contest_participation_score": clamp(min(attended / 20, 1) * 100),
            "acceptance_rate_score": clamp(((easy + medium * 1.5 + hard * 2.5) / (total_solved * 2.5)) * 100),
            "difficulty_distribution": clamp((hard / total_solved) * 500),
            "language_diversity": clamp(min(len(languages) / 5, 1) * 100),
            "category_coverage": clamp(min(len(topics) / 10, 1) * 100),
        }

    def score_hackerrank(self, profile: dict[str, Any]) -> dict[str, Any]:
        if not profile:
            return {}

        total_badges = safe_float(profile.get("hr_total_badges"), 0)
        all_domain_keys = [key for key in profile if key.endswith("_stars")]
        all_star_values = [safe_float(profile.get(key), 0) for key in all_domain_keys]
        avg_stars = sum(all_star_values) / max(len([value for value in all_star_values if value > 0]), 1)

        problem_solving_stars = safe_float(profile.get("hr_problem_solving_stars"), 0)
        python_stars = safe_float(profile.get("hr_python_stars"), 0)
        java_stars = safe_float(profile.get("hr_java_stars"), 0)
        sql_stars = safe_float(profile.get("hr_sql_stars"), 0)
        rank = safe_float(profile.get("hr_rank"), 0)

        return {
            "skill_certificates_score": clamp(stars_to_score(problem_solving_stars)),
            "avg_stars_score": clamp(stars_to_score(avg_stars)),
            "badges_count_score": clamp(min(total_badges / 10, 1) * 100),
            "domain_score_quality": clamp(
                max(
                    stars_to_score(problem_solving_stars),
                    stars_to_score(python_stars),
                    stars_to_score(java_stars),
                    stars_to_score(sql_stars),
                )
            ),
            "rank_score": clamp(min(rank / 1000, 1) * 100) if rank else 30,
        }

    def score_linkedin(
        self,
        profile: dict[str, Any],
        raw_data: dict[str, Any],
        experience_rows: list[dict[str, Any]],
        education_rows: list[dict[str, Any]],
        skill_rows: list[dict[str, Any]],
        llm_scores: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not profile:
            return {}

        today = datetime.now(timezone.utc)
        llm_scores = llm_scores or {}
        timeline_metrics = compute_linkedin_timeline_metrics(experience_rows, education_rows, today=today)

        total_exp_months = timeline_metrics["total_experience_months"]
        avg_tenure_months = timeline_metrics["average_tenure_months"]
        longest_gap_months = timeline_metrics["longest_gap_months"]
        total_gap_months = timeline_metrics["total_gap_months"]
        current_roles = timeline_metrics["current_roles"]
        positions_count = timeline_metrics["positions_count"]

        skills_count = len(skill_rows)
        recommendations_count = coerce_int(profile.get("li_num_recommendations"))
        connections = safe_float(profile.get("li_connections"))
        followers = safe_float(profile.get("li_followers"))

        experience_text = " ".join(
            filter(
                None,
                [
                    str(profile.get("li_headline") or ""),
                    str(profile.get("li_summary") or ""),
                    *[str(row.get("job_title") or "") for row in experience_rows],
                ],
            )
        ).lower()
        relevant_skills = 0
        for row in skill_rows:
            skill_name = str(row.get("skill_name") or "").lower()
            tokens = [token for token in re.split(r"[^a-z0-9+.#]+", skill_name) if len(token) > 2]
            if skill_name and (skill_name in experience_text or any(token in experience_text for token in tokens)):
                relevant_skills += 1
        skill_relevance_ratio = (relevant_skills / skills_count) if skills_count else 0.0

        featured = raw_data.get("featured") or []
        publications = raw_data.get("publication") or raw_data.get("publications") or []
        projects = raw_data.get("project") or raw_data.get("projects") or []
        activity_items = [item for item in [*featured, *publications, *projects] if isinstance(item, dict)]
        recent_activity_count = 0
        for item in activity_items:
            activity_date = parse_linkedin_date(
                item.get("startedOn") or item.get("started_on") or item.get("publishedOn") or item.get("date"),
                today=today,
            )
            if activity_date and (today - activity_date).days <= 730:
                recent_activity_count += 1

        summary_word_count = len(str(profile.get("li_summary") or "").split())
        profile_sections = [
            bool(profile.get("li_name")),
            bool(profile.get("li_headline")),
            bool(profile.get("li_summary")),
            bool(profile.get("li_location")),
            bool(profile.get("li_has_photo")),
            bool(experience_rows),
            bool(education_rows),
            bool(skill_rows),
            bool(profile.get("li_num_certs")),
            bool(profile.get("li_num_recommendations")),
            bool(profile.get("li_current_company")),
        ]

        education_rows_with_fields = [row for row in education_rows if row.get("fields_of_study")]
        dated_education_ratio = (
            timeline_metrics["dated_education_count"] / len(education_rows)
            if education_rows
            else 0.0
        )

        rule_scores = {
            "employment_consistency_score": max(
                0.0,
                min(
                    100.0,
                    42.0
                    + min(total_exp_months / 72.0, 1.0) * 25.0
                    + min(avg_tenure_months / 24.0, 1.0) * 20.0
                    + (8.0 if current_roles else 0.0)
                    - min(longest_gap_months / 12.0, 1.0) * 25.0
                    - min(total_gap_months / 24.0, 1.0) * 10.0,
                ),
            ),
            "career_progression_trajectory": max(
                0.0,
                min(
                    100.0,
                    35.0
                    + min(timeline_metrics["seniority_delta"], 4) * 11.0
                    + min(timeline_metrics["progression_steps"], 3) * 9.0
                    + min(positions_count / 5.0, 1.0) * 12.0
                    + min(avg_tenure_months / 24.0, 1.0) * 12.0
                    + (8.0 if current_roles else 0.0),
                ),
            ),
            "company_prestige_score": estimate_linkedin_company_prestige(experience_rows),
            "skill_endorsement_credibility": max(
                0.0,
                min(
                    100.0,
                    min(skills_count / 20.0, 1.0) * 70.0
                    + skill_relevance_ratio * 20.0
                    + (10.0 if skills_count >= 5 else 0.0),
                ),
            ),
            "recommendation_authenticity": max(
                0.0,
                min(
                    100.0,
                    min(recommendations_count / 5.0, 1.0) * 75.0
                    + (15.0 if recommendations_count else 0.0)
                    + min(current_roles, 2) * 5.0,
                ),
            ),
            "profile_completeness": max(
                0.0,
                min(100.0, (sum(profile_sections) / len(profile_sections)) * 100.0),
            ),
            "network_size_quality": max(
                0.0,
                min(100.0, min(((connections * 0.75) + (followers * 0.25)) / 500.0, 1.0) * 100.0),
            ),
            "education_verification_score": max(
                0.0,
                min(
                    100.0,
                    (25.0 if education_rows else 0.0)
                    + dated_education_ratio * 25.0
                    + timeline_metrics["valid_education_ratio"] * 25.0
                    + timeline_metrics["reasonable_education_ratio"] * 15.0
                    + min(len(education_rows_with_fields) / 2.0, 1.0) * 10.0,
                ),
            ),
            "activity_frequency_score": max(
                0.0,
                min(
                    100.0,
                    min(len(activity_items) / 6.0, 1.0) * 70.0
                    + min(recent_activity_count / 3.0, 1.0) * 30.0,
                ),
            ),
            "content_quality_score": max(
                0.0,
                min(
                    100.0,
                    min(summary_word_count / 120.0, 1.0) * 55.0
                    + min(len(publications) / 3.0, 1.0) * 25.0
                    + min(len(featured) / 3.0, 1.0) * 10.0
                    + (10.0 if profile.get("li_headline") else 0.0),
                ),
            ),
        }

        final_scores: dict[str, Any] = {}
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
                blended = (rule_score * (1.0 - llm_weight)) + (clamp(llm_value) * llm_weight)
                final_scores[key] = round(clamp(blended), 2)
            else:
                final_scores[key] = round(clamp(rule_score), 2)

        return final_scores

    def score_stackoverflow(self, profile: dict[str, Any]) -> dict[str, Any]:
        if not profile:
            return {}

        reputation = safe_float(profile.get("so_reputation"), 0)
        answers = safe_float(profile.get("so_answer_count"), 0)
        accepted = safe_float(profile.get("so_accepted_answers"), 0)
        gold = safe_float(profile.get("so_gold_badges"), 0)
        silver = safe_float(profile.get("so_silver_badges"), 0)
        avg_score = safe_float(profile.get("so_avg_answer_score"), 0)
        tags = [tag for tag in str(profile.get("so_top_tags", "")).split(", ") if tag]

        return {
            "reputation_score": clamp(min(reputation / 10000, 1) * 100),
            "answer_volume_score": clamp(min(answers / 100, 1) * 100),
            "acceptance_rate_score": clamp((accepted / max(answers, 1)) * 100),
            "badge_quality_score": clamp(min((gold * 20 + silver * 5) / 200, 1) * 100),
            "answer_quality_score": clamp(min(avg_score / 10, 1) * 100),
            "expertise_breadth": clamp(min(len(tags) / 10, 1) * 100),
        }

    def score_all_platforms(
        self,
        context: dict[str, Any],
        *,
        linkedin_llm_scores: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        github_scores = self.score_github(context["github_profile"], context["github_repos"])
        leetcode_scores = self.score_leetcode(context["leetcode_profile"])
        hackerrank_scores = self.score_hackerrank(context["hackerrank_profile"])
        linkedin_scores = self.score_linkedin(
            context["linkedin_profile"],
            context["linkedin_raw_data"],
            context["linkedin_experience_rows"],
            context["linkedin_education_rows"],
            context["linkedin_skill_rows"],
            llm_scores=linkedin_llm_scores,
        )
        stackoverflow_scores = self.score_stackoverflow(context["stackoverflow_profile"])

        platform_score_inputs = {
            "LinkedIn": (linkedin_scores, LINKEDIN_WEIGHTS),
            "GitHub": (github_scores, GITHUB_WEIGHTS),
            "LeetCode": (leetcode_scores, LEETCODE_WEIGHTS),
            "HackerRank": (hackerrank_scores, HACKERRANK_WEIGHTS),
            "StackOverflow": (stackoverflow_scores, STACKOVERFLOW_WEIGHTS),
        }

        platform_results: dict[str, Any] = {}
        platform_scores: dict[str, float | None] = {}
        for platform, (scores, weights) in platform_score_inputs.items():
            if scores:
                result = weighted_platform_score(scores, weights)
                platform_results[platform] = result
                platform_scores[platform] = result["platform_score"]
            else:
                platform_results[platform] = None
                platform_scores[platform] = None

        overall_score, adjusted_weights = compute_overall_score(platform_scores)
        grade, verdict = score_to_grade(overall_score)

        return {
            "github_scores": github_scores,
            "leetcode_scores": leetcode_scores,
            "hackerrank_scores": hackerrank_scores,
            "linkedin_scores": linkedin_scores,
            "stackoverflow_scores": stackoverflow_scores,
            "platform_results": platform_results,
            "platform_scores": platform_scores,
            "overall_score": overall_score,
            "adjusted_weights": adjusted_weights,
            "grade": grade,
            "verdict": verdict,
        }
