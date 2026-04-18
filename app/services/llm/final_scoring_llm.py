from __future__ import annotations

import json
from typing import Any

from app.external.azure_openai import AzureOpenAIClient
from app.utils.mathematical import clamp, score_label_from_100


SYSTEM_HOLISTIC = """
You are a technical hiring evaluation AI specializing in candidate verification.
Return only valid JSON.

Required top-level keys:
- candidate_report
- recruiter_report
- personalized_probe_suggestions

Candidate report requirements:
- overall_trust_score: integer 0-1000
- trust_score_breakdown with technical_skills, experience, education, community_impact, consistency, fraud_penalty
- score_label
- what_this_score_means
- platform_breakdown
- top_3_strengths
- areas_to_improve
- score_improvement_simulator
- salary_intelligence
- verified_credential_badge

Recruiter report requirements:
- trust_score_summary
- reliability_assessment
- skill_verification_summary
- fraud_risk_indicator
- platform_evidence_cards
- salary_intelligence

Probe requirements:
- At least 6 personalized probes
- Every probe must reference actual profile evidence
- Each probe must contain category, question, Answer, follow_up

Scoring rules:
- Base platform weighting on LinkedIn 30%, GitHub 25%, LeetCode 20%, HackerRank 15%, StackOverflow 10%
- If a platform is missing, redistribute its weight proportionally across present platforms
- Do not hallucinate missing data
- Use concise, recruiter-focused language
"""


class FinalScoringLLM:
    def __init__(self) -> None:
        self.client = AzureOpenAIClient()

    def generate(
        self,
        *,
        payload: dict[str, Any],
        platform_outputs: dict[str, Any],
        platform_llm_scores: dict[str, Any],
        math_summary: dict[str, Any],
        resume_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        fallback = self._fallback_analysis(
            payload=payload,
            platform_outputs=platform_outputs,
            platform_llm_scores=platform_llm_scores,
            math_summary=math_summary,
            resume_data=resume_data or {},
        )

        if not self.client.is_configured:
            return fallback

        user_content = json.dumps(
            {
                "packages": payload,
                "platform_outputs": platform_outputs,
                "platform_llm_scores": platform_llm_scores,
                "math_summary": math_summary,
                "resume_data": resume_data or {},
            },
            default=str,
        )
        result = self.client.json_completion(
            system_prompt=SYSTEM_HOLISTIC,
            user_content=user_content,
            temperature=0.2,
        )
        if not result:
            return fallback

        if "candidate_report" not in result or "recruiter_report" not in result:
            merged = dict(fallback)
            merged.update(result)
            return self._sanitize_result(merged, fallback)

        if "personalized_probe_suggestions" not in result:
            result["personalized_probe_suggestions"] = fallback["personalized_probe_suggestions"]

        return self._sanitize_result(result, fallback)

    def _sanitize_result(self, result: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(result)
        sanitized["candidate_report"] = self._ensure_dict(
            sanitized.get("candidate_report"),
            fallback.get("candidate_report", {}),
        )
        sanitized["recruiter_report"] = self._ensure_dict(
            sanitized.get("recruiter_report"),
            fallback.get("recruiter_report", {}),
        )
        sanitized["personalized_probe_suggestions"] = self._ensure_list(
            sanitized.get("personalized_probe_suggestions"),
            fallback.get("personalized_probe_suggestions", []),
        )
        return sanitized

    def _ensure_dict(self, value: Any, fallback: dict[str, Any]) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        return dict(fallback)

    def _ensure_list(self, value: Any, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
        return list(fallback)

    def _fallback_analysis(
        self,
        *,
        payload: dict[str, Any],
        platform_outputs: dict[str, Any],
        platform_llm_scores: dict[str, Any],
        math_summary: dict[str, Any],
        resume_data: dict[str, Any],
    ) -> dict[str, Any]:
        linkedin = platform_outputs.get("linkedin", {}) or {}
        github = platform_outputs.get("github", {}) or {}
        leetcode = platform_outputs.get("leetcode", {}) or {}
        hackerrank = platform_outputs.get("hackerrank", {}) or {}
        stackoverflow = platform_outputs.get("stackoverflow", {}) or {}

        overall_score = int(round(math_summary.get("overall_score", 0)))
        overall_score_100 = clamp(overall_score / 10.0)
        label = score_label_from_100(overall_score_100)
        years_exp = round((linkedin.get("li_total_exp_months", 0) or 0) / 12, 1)

        platform_breakdown = []
        positive_signals: list[str] = []
        red_flags: list[str] = []
        skill_verification_summary: list[dict[str, Any]] = []
        evidence_cards: list[dict[str, Any]] = []

        for platform, score_payload in platform_llm_scores.items():
            score = int(round(score_payload.get("score", 0)))
            if score >= 70:
                positive_signals.append(f"{platform} shows strong verified activity ({score}/100).")
            elif 0 < score < 45:
                red_flags.append(f"{platform} evidence is relatively weak at {score}/100.")

            platform_breakdown.append(
                {
                    "platform": platform,
                    "score": score,
                    "max": 100,
                    "grade": self._platform_grade(score),
                    "key_finding": score_payload.get("reason", ""),
                    "improvement_tip": self._platform_tip(platform, score),
                }
            )
            evidence_cards.append(
                {
                    "platform": platform,
                    "account_age_days": self._account_age_days(platform_outputs, platform),
                    "activity_level": self._activity_level(platform_outputs, platform),
                    "authenticity_signals": score_payload.get("key_findings", []),
                    "concern_signals": [] if score >= 45 else [self._platform_tip(platform, score)],
                }
            )

        missing_platforms = math_summary.get("platforms_missing", [])
        if missing_platforms:
            red_flags.append(
                "Missing platform evidence for "
                + ", ".join(missing_platforms)
                + "; weights were redistributed rather than treated as zero."
            )

        if github.get("gh_total_stars", 0):
            skill_verification_summary.append(
                {"skill": "Project delivery", "verified_by": ["GitHub"], "confidence": "High"}
            )
        if leetcode.get("lc_total_solved", 0):
            skill_verification_summary.append(
                {"skill": "Problem solving", "verified_by": ["LeetCode"], "confidence": "High"}
            )
        if hackerrank.get("hr_total_badges", 0):
            skill_verification_summary.append(
                {"skill": "Skill badge validation", "verified_by": ["HackerRank"], "confidence": "Medium"}
            )
        if linkedin.get("li_num_positions", 0):
            skill_verification_summary.append(
                {"skill": "Professional experience", "verified_by": ["LinkedIn"], "confidence": "High"}
            )
        if stackoverflow.get("so_answer_count", 0):
            skill_verification_summary.append(
                {"skill": "Community support", "verified_by": ["StackOverflow"], "confidence": "Medium"}
            )

        top_strengths = positive_signals[:3] or [
            "Multi-platform evidence is available for the candidate.",
            "The profile shows at least one verifiable technical signal.",
            "Scoring combines deterministic math with cross-platform context.",
        ]
        areas_to_improve = self._build_improvement_areas(
            platform_llm_scores=platform_llm_scores,
            missing_platforms=missing_platforms,
        )
        probes = self._build_personalized_probes(platform_outputs)
        fraud_risk = self._fraud_risk(red_flags)
        salary_range = self._salary_range(overall_score_100, years_exp)

        candidate_report = {
            "overall_trust_score": overall_score,
            "trust_score_breakdown": {
                "technical_skills": min(250, int(round((platform_llm_scores.get("GitHub", {}).get("score", 0) + platform_llm_scores.get("LeetCode", {}).get("score", 0) + platform_llm_scores.get("HackerRank", {}).get("score", 0)) * 0.9))),
                "experience": min(200, int(round(platform_llm_scores.get("LinkedIn", {}).get("score", 0) * 2))),
                "education": min(150, int(round(max(30, platform_llm_scores.get("LinkedIn", {}).get("score", 0) * 1.2 if linkedin.get("li_num_education", 0) else 20)))),
                "community_impact": min(150, int(round((platform_llm_scores.get("GitHub", {}).get("score", 0) + platform_llm_scores.get("StackOverflow", {}).get("score", 0)) * 0.75))),
                "consistency": min(150, int(round(overall_score_100 * 1.5))),
                "fraud_penalty": -min(100, fraud_risk["risk_score"] // 2),
            },
            "score_label": label,
            "what_this_score_means": self._score_explanation(overall_score_100, label),
            "platform_breakdown": platform_breakdown,
            "top_3_strengths": top_strengths,
            "areas_to_improve": areas_to_improve,
            "score_improvement_simulator": self._improvement_simulator(areas_to_improve),
            "salary_intelligence": {
                "estimated_range_usd_annual": salary_range,
                "market_percentile": int(min(95, max(20, overall_score_100))),
                "justification": "Estimate is anchored to observed platform strength, community impact, and visible experience depth.",
                "comparable_roles": self._comparable_roles(overall_score_100),
            },
            "verified_credential_badge": {
                "badge_level": self._badge_level(len(platform_llm_scores), fraud_risk["risk_level"]),
                "verified_items": [item["skill"] for item in skill_verification_summary],
                "unverified_items": missing_platforms,
                "badge_explanation": "Badge strength reflects how much of the candidate narrative is independently supported across platforms.",
            },
        }

        recruiter_report = {
            "trust_score_summary": {
                "score": overall_score,
                "label": label,
                "one_liner": self._one_liner(platform_llm_scores, label),
            },
            "reliability_assessment": {
                "overall_reliability": self._reliability_label(len(platform_llm_scores), fraud_risk["risk_level"]),
                "tenure_consistency": self._tenure_consistency(linkedin),
                "platform_activity_trend": self._platform_activity_trend(platform_llm_scores),
                "red_flags": red_flags[:5],
                "positive_signals": positive_signals[:5],
            },
            "skill_verification_summary": skill_verification_summary,
            "fraud_risk_indicator": fraud_risk,
            "platform_evidence_cards": evidence_cards,
            "salary_intelligence": {
                "recommended_offer_range_usd": salary_range,
                "negotiation_advice": "Anchor the offer to verified capability and use probes to validate depth in the strongest visible signals.",
                "market_benchmark": f"{label} tier candidate based on current observable evidence.",
            },
            "Personalized_probe_suggestions_realted to uploaded data": probes,
            "personalized_probe_suggestions": probes,
        }

        return {
            "candidate_report": candidate_report,
            "recruiter_report": recruiter_report,
            "personalized_probe_suggestions": probes,
            "candidate_name": linkedin.get("li_name") or github.get("gh_name") or github.get("gh_username") or "Unknown",
            "years_of_experience": years_exp,
            "top_5_skills": str(linkedin.get("li_skills", "")).split(", ")[:5] if linkedin else [],
            "summary": self._score_explanation(overall_score_100, label),
        }

    def _build_improvement_areas(
        self,
        *,
        platform_llm_scores: dict[str, Any],
        missing_platforms: list[str],
    ) -> list[str]:
        areas: list[str] = []
        for platform, payload in platform_llm_scores.items():
            if payload.get("score", 0) < 60:
                areas.append(f"Strengthen measurable evidence on {platform} with more recent and higher-impact activity.")
        for platform in missing_platforms:
            areas.append(f"Add verifiable {platform} activity so recruiters have more balanced evidence.")
        return areas[:5] or ["Increase cross-platform evidence to improve trust and verification depth."]

    def _build_personalized_probes(self, platform_outputs: dict[str, Any]) -> list[dict[str, Any]]:
        linkedin = platform_outputs.get("linkedin", {}) or {}
        github = platform_outputs.get("github", {}) or {}
        leetcode = platform_outputs.get("leetcode", {}) or {}
        hackerrank = platform_outputs.get("hackerrank", {}) or {}
        stackoverflow = platform_outputs.get("stackoverflow", {}) or {}
        probes: list[dict[str, Any]] = []

        github_repos = github.get("repos_data", []) or []
        if github_repos:
            top_repo = sorted(github_repos, key=lambda repo: repo.get("stars", 0), reverse=True)[0]
            probes.append(
                {
                    "category": "technical_depth",
                    "question": f"I saw `{top_repo.get('repo_name')}` is one of your stronger GitHub projects. What architecture decisions did you make there and why?",
                    "Answer": "A strong answer should explain the system design, core trade-offs, implementation details, and at least one concrete challenge solved in that repository.",
                    "follow_up": f"Walk me through one bug or refactor you personally handled in `{top_repo.get('repo_name')}`.",
                }
            )

        if leetcode.get("lc_total_solved", 0):
            probes.append(
                {
                    "category": "problem_solving",
                    "question": f"Your LeetCode profile shows {leetcode.get('lc_total_solved', 0)} solved problems with {leetcode.get('lc_hard_solved', 0)} hard questions. Which patterns became most useful in interviews?",
                    "Answer": "A strong answer should connect solved problems to algorithmic patterns, explain complexity trade-offs, and show how those patterns transfer to real engineering tasks.",
                    "follow_up": "Describe one hard problem you initially solved the wrong way and how you corrected it.",
                }
            )

        if hackerrank.get("hr_total_badges", 0):
            probes.append(
                {
                    "category": "skill_validation",
                    "question": f"HackerRank shows {hackerrank.get('hr_total_badges', 0)} active badges. Which badge best reflects your day-to-day engineering work and why?",
                    "Answer": "A strong answer should tie the badge to real production skills, explain depth beyond the test itself, and reference situations where that skill was applied.",
                    "follow_up": "What kind of tasks would quickly reveal whether someone only memorized solutions for that domain?",
                }
            )

        experience_rows = linkedin.get("experience_rows", []) or []
        if experience_rows:
            latest_role = experience_rows[0]
            probes.append(
                {
                    "category": "experience_validation",
                    "question": f"Your latest LinkedIn role is listed as {latest_role.get('job_title', 'your current position')} at {latest_role.get('company_name', 'your company')}. What outcomes were you directly accountable for there?",
                    "Answer": "A strong answer should describe ownership, scope, measurable outcomes, collaborators, and the candidate's personal contribution instead of only team-level work.",
                    "follow_up": "Which decision in that role best demonstrates your judgment under constraints?",
                }
            )

        if linkedin.get("li_longest_gap_months", 0):
            probes.append(
                {
                    "category": "timeline_consistency",
                    "question": f"I noticed a LinkedIn career gap of about {linkedin.get('li_longest_gap_months', 0)} months. What were you focused on during that period?",
                    "Answer": "A strong answer should give a direct, time-bounded explanation with specific learning, work, or personal context and should remain consistent with the rest of the profile.",
                    "follow_up": "What evidence from that period would help validate the work or learning you just described?",
                }
            )

        if stackoverflow.get("so_answer_count", 0):
            probes.append(
                {
                    "category": "community_impact",
                    "question": f"Your StackOverflow profile shows activity around {stackoverflow.get('so_top_tags', '') or 'your top tags'}. How do those topics connect to problems you solve professionally?",
                    "Answer": "A strong answer should connect public community contributions to real engineering experience, explain why those topics matter, and mention examples of helping others with nuance.",
                    "follow_up": "Tell me about one answer you wrote that required more than a textbook response.",
                }
            )

        while len(probes) < 6:
            probes.append(
                {
                    "category": "motivation_alignment",
                    "question": "Several parts of your public profile show uneven activity. Which platform best represents your actual strengths today, and why?",
                    "Answer": "A strong answer should identify the most representative platform, explain the mismatch with other platforms honestly, and give credible reasons backed by actual work.",
                    "follow_up": "What additional evidence would you share if a recruiter wanted to verify that claim?",
                }
            )

        return probes[:8]

    def _platform_grade(self, score: float) -> str:
        if score >= 85:
            return "A"
        if score >= 70:
            return "B"
        if score >= 55:
            return "C"
        if score >= 40:
            return "D"
        return "E"

    def _platform_tip(self, platform: str, score: float) -> str:
        if platform == "GitHub":
            return "Ship more visible projects, improve documentation, and increase recent contribution depth."
        if platform == "LeetCode":
            return "Improve hard-problem coverage and maintain stronger contest consistency."
        if platform == "HackerRank":
            return "Earn more advanced badges in domains aligned with target roles."
        if platform == "LinkedIn":
            return "Complete profile sections and add clearer evidence of progression, impact, and activity."
        if platform == "StackOverflow":
            return "Increase answer quality and reputation in a narrower set of high-value tags."
        return "Add more verifiable platform evidence."

    def _score_explanation(self, overall_score_100: float, label: str) -> str:
        if overall_score_100 >= 80:
            return f"{label} with strong multi-platform verification and consistent technical signals."
        if overall_score_100 >= 60:
            return f"{label} with credible evidence, though some areas still need deeper proof of impact."
        return f"{label} because the available evidence is limited, uneven, or not yet strong enough across platforms."

    def _improvement_simulator(self, areas: list[str]) -> list[dict[str, Any]]:
        actions = []
        for index, area in enumerate(areas[:3], start=1):
            actions.append(
                {
                    "action": area,
                    "estimated_score_gain": max(15, 30 - (index * 3)),
                    "difficulty": "Medium" if index == 1 else "Easy",
                    "time_estimate": "2-6 weeks",
                }
            )
        return actions or [
            {
                "action": "Add stronger public technical evidence across the weakest platforms.",
                "estimated_score_gain": 20,
                "difficulty": "Medium",
                "time_estimate": "2-6 weeks",
            }
        ]

    def _salary_range(self, overall_score_100: float, years_exp: float) -> dict[str, int]:
        base = 45000 + int(years_exp * 12000)
        multiplier = 1.0 + (overall_score_100 / 100.0)
        low = int(base * multiplier)
        high = int(low * 1.25)
        return {"min": low, "max": high}

    def _comparable_roles(self, overall_score_100: float) -> list[str]:
        if overall_score_100 >= 80:
            return ["Software Engineer II", "Backend Engineer", "Platform Engineer"]
        if overall_score_100 >= 60:
            return ["Software Engineer I", "Junior Backend Engineer", "Full-Stack Engineer"]
        return ["Associate Software Engineer", "Trainee Engineer", "Developer Intern"]

    def _badge_level(self, platform_count: int, risk_level: str) -> str:
        if platform_count >= 4 and risk_level in {"Low", "Medium"}:
            return "Gold"
        if platform_count >= 3:
            return "Silver"
        return "Bronze"

    def _one_liner(self, platform_llm_scores: dict[str, Any], label: str) -> str:
        best = max(platform_llm_scores.items(), key=lambda item: item[1].get("score", 0), default=(None, {}))
        if best[0]:
            return f"{label} led by the strongest visible evidence on {best[0]}."
        return f"{label} based on the currently available platform evidence."

    def _reliability_label(self, platform_count: int, risk_level: str) -> str:
        if risk_level == "Critical":
            return "Low"
        if platform_count >= 4:
            return "Very High"
        if platform_count >= 3:
            return "High"
        if platform_count >= 2:
            return "Medium"
        return "Low"

    def _tenure_consistency(self, linkedin: dict[str, Any]) -> str:
        longest_gap = linkedin.get("li_longest_gap_months", 0) or 0
        if longest_gap == 0:
            return "No meaningful gap detected from the available LinkedIn timeline."
        return f"Largest visible LinkedIn gap is about {longest_gap} months and should be validated in interview."

    def _platform_activity_trend(self, platform_llm_scores: dict[str, Any]) -> str:
        if not platform_llm_scores:
            return "No reliable platform activity was available."
        average_score = sum(item.get("score", 0) for item in platform_llm_scores.values()) / len(platform_llm_scores)
        if average_score >= 70:
            return "Cross-platform activity is broadly healthy and consistent."
        if average_score >= 50:
            return "Platform activity is mixed, with some stronger surfaces than others."
        return "Most available platforms show limited or low-confidence activity."

    def _fraud_risk(self, red_flags: list[str]) -> dict[str, Any]:
        risk_score = min(100, len(red_flags) * 18)
        if risk_score >= 70:
            risk_level = "Critical"
        elif risk_score >= 45:
            risk_level = "High"
        elif risk_score >= 20:
            risk_level = "Medium"
        else:
            risk_level = "Low"
        return {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "anomalies_detected": red_flags[:5],
            "suspicious_patterns": red_flags[:3],
            "recommendation": "Use targeted probe questions to validate any weak or inconsistent public signals.",
        }

    def _account_age_days(self, platform_outputs: dict[str, Any], platform: str) -> int:
        github = platform_outputs.get("github", {}) or {}
        stackoverflow = platform_outputs.get("stackoverflow", {}) or {}
        if platform == "GitHub" and github.get("gh_account_created"):
            return self._days_since_iso(github.get("gh_account_created"))
        if platform == "StackOverflow" and stackoverflow.get("so_account_created"):
            return self._days_since_timestamp(stackoverflow.get("so_account_created"))
        return 0

    def _days_since_iso(self, value: Any) -> int:
        from datetime import datetime, timezone

        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return max(0, (datetime.now(timezone.utc) - parsed.replace(tzinfo=timezone.utc)).days)
        except Exception:
            return 0

    def _days_since_timestamp(self, value: Any) -> int:
        from datetime import datetime, timezone

        try:
            parsed = datetime.fromtimestamp(int(value), tz=timezone.utc)
            return max(0, (datetime.now(timezone.utc) - parsed).days)
        except Exception:
            return 0

    def _activity_level(self, platform_outputs: dict[str, Any], platform: str) -> str:
        github = platform_outputs.get("github", {}) or {}
        if platform == "GitHub":
            return github.get("gh_activity_level", "unknown")
        return "moderate"
