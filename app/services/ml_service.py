from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.core.config import get_settings
from app.models.response_models import FinalResponseModel
from app.services.llm.final_scoring_llm import FinalScoringLLM
from app.services.llm.platform_scoring_llm import PlatformScoringLLM
from app.services.platforms.github_service import GitHubService
from app.services.platforms.hackerrank_service import HackerRankService
from app.services.platforms.leetcode_service import LeetCodeService
from app.services.platforms.linkedin_service import LinkedInService
from app.services.platforms.stackoverflow_service import StackOverflowService
from app.services.scoring_service import ScoringService
from app.utils.helpers import (
    LINKEDIN_EDUCATION_COLUMNS,
    LINKEDIN_EXPERIENCE_COLUMNS,
    LINKEDIN_SKILL_COLUMNS,
    format_interview_qa,
    list_to_paragraph,
    save_platform_csv,
    write_json_artifact,
)
from app.utils.mathematical import safe_score
from app.utils.mathematical import score_to_grade


class MLService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.github_service = GitHubService()
        self.leetcode_service = LeetCodeService()
        self.hackerrank_service = HackerRankService()
        self.linkedin_service = LinkedInService()
        self.stackoverflow_service = StackOverflowService()
        self.scoring_service = ScoringService()
        self.platform_scoring_llm = PlatformScoringLLM()
        self.final_scoring_llm = FinalScoringLLM()

    def generate_score(self, platform_links: dict[str, str]) -> FinalResponseModel:
        normalized_links = {key.lower(): value for key, value in (platform_links or {}).items()}

        github = self.github_service.fetch_profile(normalized_links.get("github", ""))
        leetcode = self.leetcode_service.fetch_profile(normalized_links.get("leetcode", ""))
        hackerrank = self.hackerrank_service.fetch_profile(normalized_links.get("hackerrank", ""))
        linkedin = self.linkedin_service.fetch_profile(normalized_links.get("linkedin", ""))
        stackoverflow = self.stackoverflow_service.fetch_profile(normalized_links.get("stackoverflow", ""))

        context = self.scoring_service.build_platform_context(
            github=github,
            leetcode=leetcode,
            hackerrank=hackerrank,
            linkedin=linkedin,
            stackoverflow=stackoverflow,
        )

        scoring = self.scoring_service.score_all_platforms(context)
        platform_outputs = self._build_platform_outputs(context)
        platform_llm_scores = self._score_platform_layers(context, scoring)
        payload = self._build_final_llm_payload(context, scoring, platform_llm_scores)
        final_analysis = self.final_scoring_llm.generate(
            payload=payload,
            platform_outputs=platform_outputs,
            platform_llm_scores={key: value.model_dump() for key, value in platform_llm_scores.items()},
            math_summary={
                "overall_score": scoring["overall_score"],
                "grade": scoring["grade"],
                "verdict": scoring["verdict"],
                "platform_scores": scoring["platform_scores"],
                "platforms_missing": [
                    platform for platform, score in scoring["platform_scores"].items() if score is None
                ],
            },
            resume_data={"resume_text": "", "resume_raw": ""},
        )

        response = self._build_final_response(
            final_analysis=final_analysis,
            scoring=scoring,
            context=context,
            platform_links=normalized_links,
        )

        if self.settings.write_pipeline_artifacts:
            self._write_pipeline_artifacts(
                context=context,
                scoring=scoring,
                final_analysis=response.analysis,
                platform_links=normalized_links,
            )

        return response

    def _score_platform_layers(
        self,
        context: dict[str, Any],
        scoring: dict[str, Any],
    ) -> dict[str, Any]:
        platform_map = {
            "GitHub": (context["github_profile"], scoring["github_scores"], scoring["platform_scores"]["GitHub"]),
            "LeetCode": (context["leetcode_profile"], scoring["leetcode_scores"], scoring["platform_scores"]["LeetCode"]),
            "HackerRank": (
                context["hackerrank_profile"],
                scoring["hackerrank_scores"],
                scoring["platform_scores"]["HackerRank"],
            ),
            "LinkedIn": (
                {
                    **context["linkedin_profile"],
                    "experience_rows": context["linkedin_experience_rows"],
                    "education_rows": context["linkedin_education_rows"],
                    "skill_rows": context["linkedin_skill_rows"],
                },
                scoring["linkedin_scores"],
                scoring["platform_scores"]["LinkedIn"],
            ),
            "StackOverflow": (
                context["stackoverflow_profile"],
                scoring["stackoverflow_scores"],
                scoring["platform_scores"]["StackOverflow"],
            ),
        }

        platform_llm_scores = {}
        for platform, (platform_data, rule_metrics, rule_score) in platform_map.items():
            if not platform_data:
                continue
            platform_llm_scores[platform] = self.platform_scoring_llm.score_platform(
                platform=platform,
                platform_data=platform_data,
                rule_metrics=rule_metrics,
                rule_score=rule_score,
            )
        return platform_llm_scores

    def _build_platform_outputs(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "github": {
                **context["github_profile"],
                "repos_data": context["github_repos"],
            },
            "leetcode": context["leetcode_profile"],
            "hackerrank": context["hackerrank_profile"],
            "linkedin": {
                **context["linkedin_profile"],
                "experience_rows": context["linkedin_experience_rows"],
                "education_rows": context["linkedin_education_rows"],
                "skill_rows": context["linkedin_skill_rows"],
                "timeline_metrics": context["linkedin_timeline_metrics"],
            },
            "stackoverflow": context["stackoverflow_profile"],
        }

    def _build_final_llm_payload(
        self,
        context: dict[str, Any],
        scoring: dict[str, Any],
        platform_llm_scores: dict[str, Any],
    ) -> dict[str, Any]:
        github_profile = context["github_profile"]
        leetcode_profile = context["leetcode_profile"]
        hackerrank_profile = context["hackerrank_profile"]
        linkedin_profile = context["linkedin_profile"]
        linkedin_raw_data = context["linkedin_raw_data"]
        stackoverflow_profile = context["stackoverflow_profile"]

        skill_package = {
            "github": {
                "public_repos": github_profile.get("gh_public_repos", 0),
                "total_stars": github_profile.get("gh_total_stars", 0),
                "languages": github_profile.get("gh_languages", ""),
                "followers": github_profile.get("gh_followers", 0),
                "commit_frequency": scoring["github_scores"].get("commit_frequency_score", 0),
                "documentation": scoring["github_scores"].get("documentation_quality", 0),
                "top_repos": context["github_repos"][:5],
            },
            "leetcode": {
                "total_solved": leetcode_profile.get("lc_total_solved", 0),
                "easy": leetcode_profile.get("lc_easy_solved", 0),
                "medium": leetcode_profile.get("lc_medium_solved", 0),
                "hard": leetcode_profile.get("lc_hard_solved", 0),
                "contest_rating": leetcode_profile.get("lc_contest_rating", 0),
                "top_topics": leetcode_profile.get("lc_top_topics", ""),
            },
            "hackerrank": {
                "total_badges": hackerrank_profile.get("hr_total_badges", 0),
                "skills_raw": hackerrank_profile.get("hr_skills_raw", ""),
                "ps_stars": hackerrank_profile.get("hr_problem_solving_stars", 0),
                "python_stars": hackerrank_profile.get("hr_python_stars", 0),
            },
            "certifications": linkedin_profile.get("li_cert_names", ""),
        }

        identity_package = {
            "name": linkedin_profile.get("li_name", ""),
            "headline": linkedin_profile.get("li_headline", ""),
            "location": linkedin_profile.get("li_location", ""),
            "linkedin": {
                "connections": linkedin_profile.get("li_connections", 0),
                "followers": linkedin_profile.get("li_followers", 0),
                "positions": linkedin_profile.get("li_num_positions", 0),
                "exp_months": linkedin_profile.get("li_total_exp_months", 0),
                "skills": linkedin_profile.get("li_skills", ""),
                "exp_titles": linkedin_profile.get("li_exp_titles", ""),
                "exp_companies": linkedin_profile.get("li_exp_companies", ""),
                "edu_details": linkedin_profile.get("li_edu_details", ""),
            },
            "stackoverflow": {
                "reputation": stackoverflow_profile.get("so_reputation", 0),
                "answers": stackoverflow_profile.get("so_answer_count", 0),
                "top_tags": stackoverflow_profile.get("so_top_tags", ""),
            },
            "community_activities": [
                f"{entry.get('role', '')} at {entry.get('organization', '')}"
                for entry in (linkedin_raw_data.get("volunteering") or [])
                if isinstance(entry, dict)
            ]
            or ["No volunteering data found"],
        }

        behavior_package = {
            "career_history": [
                {
                    "title": row.get("job_title", ""),
                    "company": row.get("company_name", ""),
                    "type": row.get("employment_type", ""),
                    "started": row.get("start_date", ""),
                    "ended": row.get("end_date") or "Present",
                    "current": row.get("is_current", False),
                    "duration_months": row.get("duration_months", 0),
                }
                for row in context["linkedin_experience_rows"][:10]
            ],
            "education_history": [
                {
                    "school": row.get("university_name", ""),
                    "degree": row.get("fields_of_study", ""),
                    "started": row.get("start_date", ""),
                    "ended": row.get("end_date") or "Present",
                    "duration_months": row.get("duration_months", 0),
                }
                for row in context["linkedin_education_rows"][:5]
            ],
            "certifications_list": [
                f"{entry.get('name', '')} - {entry.get('authority', '')}"
                for entry in (linkedin_raw_data.get("certification") or linkedin_raw_data.get("certifications") or [])
                if isinstance(entry, dict)
            ]
            or ["No certifications found"],
            "platform_scores": {
                platform: data.model_dump() for platform, data in platform_llm_scores.items()
            },
            "overall_score": scoring["overall_score"],
        }

        return {
            "skill_package": skill_package,
            "identity_package": identity_package,
            "behavior_package": behavior_package,
            "mathematical_scores": {
                "overall_score": scoring["overall_score"],
                "grade": scoring["grade"],
                "platform_scores": {
                    platform: round(score, 1) if score is not None else None
                    for platform, score in scoring["platform_scores"].items()
                },
            },
        }

    def _build_final_response(
        self,
        *,
        final_analysis: dict[str, Any],
        scoring: dict[str, Any],
        context: dict[str, Any],
        platform_links: dict[str, str],
    ) -> FinalResponseModel:
        candidate_report = self._ensure_mapping(final_analysis.get("candidate_report"))
        recruiter_report = self._ensure_mapping(final_analysis.get("recruiter_report"))
        reliability_assessment = self._ensure_mapping(recruiter_report.get("reliability_assessment"))
        candidate_areas_to_improve = self._ensure_string_list(candidate_report.get("areas_to_improve"))
        candidate_strengths = self._ensure_string_list(candidate_report.get("top_3_strengths"))
        pros_items = self._build_pros(candidate_report, recruiter_report, reliability_assessment)
        cons_items = self._build_cons(
            candidate_report,
            recruiter_report,
            reliability_assessment,
            candidate_areas_to_improve,
        )
        probes = (
            final_analysis.get("personalized_probe_suggestions")
            or recruiter_report.get("personalized_probe_suggestions")
            or recruiter_report.get("Personalized_probe_suggestions_realted to uploaded data")
            or []
        )
        probes = self._ensure_probe_list(probes)

        analysis = dict(final_analysis)
        analysis["candidate_report"] = candidate_report
        analysis["recruiter_report"] = recruiter_report
        analysis["personalized_probe_suggestions"] = probes
        analysis["mathematical_score"] = scoring["overall_score"]
        analysis["score_breakdown"] = {
            platform: round(score * 10, 1) if score is not None else None
            for platform, score in scoring["platform_scores"].items()
        }
        analysis["grade"] = scoring["grade"]
        analysis["verdict"] = scoring["verdict"]
        analysis["user_formatted_result"] = {
            "score": int(candidate_report.get("overall_trust_score") or round(scoring["overall_score"])),
            "pros": list_to_paragraph(pros_items or candidate_strengths),
            "cons": list_to_paragraph(cons_items),
            "interview_questions": format_interview_qa(probes),
            "improvements": list_to_paragraph(candidate_areas_to_improve),
            "platform_scores": {
                "github": safe_score(scoring["platform_scores"].get("GitHub")),
                "leetcode": safe_score(scoring["platform_scores"].get("LeetCode")),
                "hackerrank": safe_score(scoring["platform_scores"].get("HackerRank")),
                "linkedin": safe_score(scoring["platform_scores"].get("LinkedIn")),
                "stack_overflow": safe_score(scoring["platform_scores"].get("StackOverflow")),
            },
        }
        analysis["scraper_metadata"] = {
            "linkedin_url": platform_links.get("linkedin", ""),
            "github_url": platform_links.get("github", ""),
            "leetcode_url": platform_links.get("leetcode", ""),
            "hackerrank_url": platform_links.get("hackerrank", ""),
            "stackoverflow_url": platform_links.get("stackoverflow", ""),
            "platforms_available": [
                platform for platform, score in scoring["platform_scores"].items() if score is not None
            ],
            "platforms_missing": [
                platform for platform, score in scoring["platform_scores"].items() if score is None
            ],
            "analysis_timestamp": datetime.now().isoformat(),
        }
        analysis["raw_profiles"] = {
            "github": context["github_profile"],
            "leetcode": context["leetcode_profile"],
            "hackerrank": context["hackerrank_profile"],
            "linkedin": context["linkedin_profile"],
            "stackoverflow": context["stackoverflow_profile"],
        }
        analysis["sub_metric_scores"] = {
            "github": scoring["github_scores"],
            "leetcode": scoring["leetcode_scores"],
            "hackerrank": scoring["hackerrank_scores"],
            "linkedin": scoring["linkedin_scores"],
            "stackoverflow": scoring["stackoverflow_scores"],
        }

        user_result = analysis["user_formatted_result"]
        return FinalResponseModel(
            score=user_result["score"],
            pros=user_result["pros"],
            cons=user_result["cons"],
            interview_questions=user_result["interview_questions"],
            improvements=user_result["improvements"],
            platform_scores=user_result["platform_scores"],
            candidate_report=candidate_report,
            recruiter_report=recruiter_report,
            personalized_probe_suggestions=probes,
            analysis=analysis,
        )

    def _ensure_mapping(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        return {}

    def _ensure_string_list(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item) for item in value if str(item).strip()]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed if str(item).strip()]
            except Exception:
                if value.strip():
                    return [value.strip()]
        return []

    def _ensure_probe_list(self, value: Any) -> list[dict[str, Any]]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [item for item in parsed if isinstance(item, dict)]
            except Exception:
                pass
        return []

    def _build_pros(
        self,
        candidate_report: dict[str, Any],
        recruiter_report: dict[str, Any],
        reliability_assessment: dict[str, Any],
    ) -> list[str]:
        candidates = [
            self._ensure_string_list(reliability_assessment.get("positive_signals")),
            self._ensure_string_list(candidate_report.get("top_3_strengths")),
            self._ensure_string_list(recruiter_report.get("skill_verification_summary")),
            self._ensure_string_list(recruiter_report.get("trust_score_summary")),
            self._ensure_string_list(recruiter_report.get("reliability_assessment")),
        ]

        for items in candidates:
            cleaned = [item for item in items if item]
            if cleaned:
                return cleaned[:5]

        return ["No strong positive signals were extracted from the current report payload."]

    def _build_cons(
        self,
        candidate_report: dict[str, Any],
        recruiter_report: dict[str, Any],
        reliability_assessment: dict[str, Any],
        candidate_areas_to_improve: list[str],
    ) -> list[str]:
        fraud_risk = self._ensure_mapping(recruiter_report.get("fraud_risk_indicator"))
        candidates = [
            self._ensure_string_list(reliability_assessment.get("red_flags")),
            candidate_areas_to_improve,
            self._ensure_string_list(fraud_risk.get("anomalies_detected")),
            self._ensure_string_list(fraud_risk.get("suspicious_patterns")),
            self._ensure_string_list(recruiter_report.get("fraud_risk_indicator")),
        ]

        for items in candidates:
            cleaned = [item for item in items if item]
            if cleaned:
                return cleaned[:5]

        return ["No major negative signals were extracted from the current report payload."]

    def _write_pipeline_artifacts(
        self,
        *,
        context: dict[str, Any],
        scoring: dict[str, Any],
        final_analysis: dict[str, Any],
        platform_links: dict[str, str],
    ) -> None:
        output_dir = Path.cwd()

        self._write_stage_one_artifacts(context, platform_links, output_dir)
        self._write_stage_two_artifacts(scoring, output_dir)
        self._write_stage_three_artifacts(scoring, output_dir)
        write_json_artifact("candidate_analysis.json", final_analysis, output_dir=output_dir)

    def _write_stage_one_artifacts(
        self,
        context: dict[str, Any],
        platform_links: dict[str, str],
        output_dir: Path,
    ) -> None:
        linkedin_profile = context["linkedin_profile"]
        github_profile = context["github_profile"]
        leetcode_profile = context["leetcode_profile"]
        hackerrank_profile = context["hackerrank_profile"]
        stackoverflow_profile = context["stackoverflow_profile"]

        save_platform_csv(
            "linkedin",
            linkedin_profile,
            extra_numeric={key: value for key, value in linkedin_profile.items() if key in {
                "li_followers", "li_connections", "li_num_positions", "li_total_exp_months",
                "li_num_education", "li_num_skills", "li_num_certs", "li_num_recommendations",
                "li_has_photo", "li_has_summary", "li_has_experience", "li_has_education",
                "li_has_skills", "li_has_certs", "li_has_recommendations",
            }},
            extra_contextual={key: value for key, value in linkedin_profile.items() if key in {
                "li_name", "li_headline", "li_summary", "li_location", "li_profile_url",
                "li_exp_titles", "li_exp_companies", "li_edu_details", "li_skills",
                "li_cert_names", "li_current_company", "li_country",
            }},
            output_dir=output_dir,
        )
        save_platform_csv(
            "github",
            github_profile,
            extra_numeric={key: value for key, value in github_profile.items() if key in {
                "gh_followers", "gh_following", "gh_public_repos", "gh_total_stars",
                "gh_total_forks_got", "gh_original_repos", "gh_top_repo_stars", "gh_recent_commits",
            }},
            extra_contextual={key: value for key, value in github_profile.items() if key in {
                "gh_bio", "gh_company", "gh_blog", "gh_location", "gh_username",
                "gh_name", "gh_languages", "gh_account_created", "gh_activity_level", "gh_profile_url",
            }},
            output_dir=output_dir,
        )
        save_platform_csv(
            "leetcode",
            leetcode_profile,
            extra_numeric={key: value for key, value in leetcode_profile.items() if key in {
                "lc_total_solved", "lc_easy_solved", "lc_medium_solved", "lc_hard_solved",
                "lc_contest_rating", "lc_contest_rank", "lc_top_percentage",
                "lc_contests_attended", "lc_star_rating", "lc_reputation", "lc_ranking",
            }},
            extra_contextual={key: value for key, value in leetcode_profile.items() if key in {
                "lc_username", "lc_badges", "lc_languages", "lc_top_topics", "lc_profile_url",
            }},
            output_dir=output_dir,
        )
        save_platform_csv(
            "hackerrank",
            hackerrank_profile,
            extra_numeric={key: value for key, value in hackerrank_profile.items() if key.endswith("_stars") or key in {
                "hr_rank", "hr_score", "hr_total_badges"
            }},
            extra_contextual={key: value for key, value in hackerrank_profile.items() if key in {
                "hr_username", "hr_skills_raw", "hr_country", "hr_profile_url",
            }},
            output_dir=output_dir,
        )
        save_platform_csv(
            "stackoverflow",
            stackoverflow_profile,
            extra_numeric={key: value for key, value in stackoverflow_profile.items() if key in {
                "so_reputation", "so_answer_count", "so_question_count",
                "so_gold_badges", "so_silver_badges", "so_bronze_badges",
                "so_accepted_answers", "so_avg_answer_score",
                "so_account_created", "so_last_access",
            }},
            extra_contextual={key: value for key, value in stackoverflow_profile.items() if key in {
                "so_user_id", "so_display_name", "so_top_tags", "so_profile_url",
            }},
            output_dir=output_dir,
        )

        pd.DataFrame([{"email": "none", "resume_text": "none", "resume_raw": "none"}]).to_csv(
            output_dir / "resume_contextual.csv",
            index=False,
        )

        all_data = {
            "email": "email",
            "linkedin_url": platform_links.get("linkedin", ""),
            "github_url": platform_links.get("github", ""),
            "leetcode_url": platform_links.get("leetcode", ""),
            "hackerrank_url": platform_links.get("hackerrank", ""),
            "stackoverflow_url": platform_links.get("stackoverflow", ""),
            "kaggle_url": "kaggle",
            "resume_text": "resume_content['clean']",
            **github_profile,
            **leetcode_profile,
            **hackerrank_profile,
            **linkedin_profile,
            **stackoverflow_profile,
        }
        pd.DataFrame([all_data]).to_csv(output_dir / "candidate_profile.csv", index=False)
        pd.DataFrame(context["github_repos"]).to_csv(output_dir / "github_repos.csv", index=False)

        if context["linkedin_raw_data"]:
            pd.json_normalize(context["linkedin_raw_data"]).to_csv(
                output_dir / "linkedin_profile_full.csv",
                index=False,
            )
            pd.DataFrame(context["linkedin_experience_rows"], columns=LINKEDIN_EXPERIENCE_COLUMNS).to_csv(
                output_dir / "linkedin_experience.csv",
                index=False,
            )
            pd.DataFrame(context["linkedin_education_rows"], columns=LINKEDIN_EDUCATION_COLUMNS).to_csv(
                output_dir / "linkedin_education.csv",
                index=False,
            )
            pd.DataFrame(context["linkedin_skill_rows"], columns=LINKEDIN_SKILL_COLUMNS).to_csv(
                output_dir / "linkedin_skills.csv",
                index=False,
            )

    def _write_stage_two_artifacts(self, scoring: dict[str, Any], output_dir: Path) -> None:
        pd.DataFrame([scoring["github_scores"]]).to_csv(output_dir / "github_scores.csv", index=False)
        pd.DataFrame([scoring["leetcode_scores"]]).to_csv(output_dir / "leetcode_scores.csv", index=False)
        pd.DataFrame([scoring["hackerrank_scores"]]).to_csv(output_dir / "hackerrank_scores.csv", index=False)
        pd.DataFrame([scoring["linkedin_scores"]]).to_csv(output_dir / "linkedin_scores.csv", index=False)
        pd.DataFrame([scoring["stackoverflow_scores"]]).to_csv(output_dir / "stackoverflow_scores.csv", index=False)

    def _write_stage_three_artifacts(self, scoring: dict[str, Any], output_dir: Path) -> None:
        summary_rows = []
        for platform, score in scoring["platform_scores"].items():
            adjusted_weight = scoring["adjusted_weights"].get(platform, 0.0)
            grade, verdict = score_to_grade((score or 0) * 10) if score is not None else ("N/A", "Missing")
            summary_rows.append(
                {
                    "Platform": platform,
                    "Status": "Present" if score is not None else "Missing",
                    "Platform Score": round(score * 10, 2) if score is not None else "N/A",
                    "Grade": grade,
                    "Verdict": verdict,
                    "Adjusted Weight": f"{adjusted_weight * 100:.1f}%",
                    "Weighted Contrib": round(score * adjusted_weight * 10, 2) if score is not None else 0.0,
                }
            )
        summary_df = pd.DataFrame(summary_rows)
        overall_row = pd.DataFrame(
            [
                {
                    "Platform": "OVERALL",
                    "Status": f"{len([score for score in scoring['platform_scores'].values() if score is not None])}/{len(scoring['platform_scores'])} platforms",
                    "Platform Score": scoring["overall_score"],
                    "Grade": scoring["grade"],
                    "Verdict": scoring["verdict"],
                    "Adjusted Weight": "100%",
                    "Weighted Contrib": scoring["overall_score"],
                }
            ]
        )
        pd.concat([summary_df, overall_row], ignore_index=True).to_csv(
            output_dir / "overall_score_report.csv",
            index=False,
        )
