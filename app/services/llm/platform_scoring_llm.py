from __future__ import annotations

import json
from typing import Any

from app.external.azure_openai import AzureOpenAIClient
from app.models.response_models import PlatformScoreModel
from app.utils.mathematical import clamp


PLATFORM_SCORING_PROMPT = """
You are evaluating a single candidate platform profile.
Return only valid JSON with exactly these keys:
- platform: string
- score: integer from 0 to 100
- reason: short evidence-based explanation
- key_findings: array of 2 to 4 concise findings

Rules:
- Base the score strictly on the provided profile data and rule-based metrics.
- Do not hallucinate missing evidence.
- Keep the explanation recruiter-friendly and factual.
"""


class PlatformScoringLLM:
    def __init__(self) -> None:
        self.client = AzureOpenAIClient()

    def score_platform(
        self,
        *,
        platform: str,
        platform_data: dict[str, Any],
        rule_metrics: dict[str, Any],
        rule_score: float | None,
    ) -> PlatformScoreModel:
        fallback = self._fallback(platform=platform, platform_data=platform_data, rule_score=rule_score)
        if not self.client.is_configured:
            return fallback

        payload = json.dumps(
            {
                "platform": platform,
                "platform_data": platform_data,
                "rule_metrics": rule_metrics,
                "rule_score": rule_score,
            },
            default=str,
        )
        result = self.client.json_completion(
            system_prompt=PLATFORM_SCORING_PROMPT,
            user_content=payload,
            temperature=0.1,
        )
        if not result:
            return fallback

        return PlatformScoreModel(
            platform=str(result.get("platform") or platform),
            score=clamp(result.get("score", rule_score or 0)),
            reason=str(result.get("reason") or fallback.reason),
            key_findings=[
                str(item) for item in (result.get("key_findings") or fallback.key_findings) if str(item).strip()
            ],
            rule_score=rule_score,
        )

    def _fallback(
        self,
        *,
        platform: str,
        platform_data: dict[str, Any],
        rule_score: float | None,
    ) -> PlatformScoreModel:
        findings: list[str] = []
        if platform == "GitHub":
            findings.append(f"{platform_data.get('gh_original_repos', 0)} original repos detected.")
            findings.append(f"{platform_data.get('gh_total_stars', 0)} total stars across public work.")
        elif platform == "LeetCode":
            findings.append(f"{platform_data.get('lc_total_solved', 0)} problems solved.")
            findings.append(f"Contest rating: {platform_data.get('lc_contest_rating') or 'N/A'}.")
        elif platform == "HackerRank":
            findings.append(f"{platform_data.get('hr_total_badges', 0)} active badges.")
            findings.append("Domain stars available for skills verification.")
        elif platform == "LinkedIn":
            findings.append(f"{platform_data.get('li_num_positions', 0)} roles in the public profile.")
            findings.append(f"{platform_data.get('li_num_skills', 0)} listed skills.")
        elif platform == "StackOverflow":
            findings.append(f"Reputation: {platform_data.get('so_reputation', 0)}.")
            findings.append(f"Answers posted: {platform_data.get('so_answer_count', 0)}.")

        score = clamp(rule_score or 0)
        if score >= 80:
            reason = "Strong evidence of consistent platform activity and measurable impact."
        elif score >= 60:
            reason = "Moderate platform strength with some credible signals, but limited standout impact."
        elif score > 0:
            reason = "Platform shows partial evidence, though the profile remains relatively light."
        else:
            reason = "Not enough reliable platform evidence was available to score strongly."

        return PlatformScoreModel(
            platform=platform,
            score=score,
            reason=reason,
            key_findings=findings[:4],
            rule_score=rule_score,
        )
