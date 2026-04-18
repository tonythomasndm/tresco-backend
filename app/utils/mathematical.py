from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.utils.helpers import safe_float


OVERALL_PLATFORM_WEIGHTS: dict[str, float] = {
    "LinkedIn": 0.30,
    "GitHub": 0.25,
    "LeetCode": 0.20,
    "HackerRank": 0.15,
    "StackOverflow": 0.10,
}


def clamp(value: Any, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, safe_float(value, lower)))


def normalize_score(value: Any, source_max: float = 100.0, target_max: float = 100.0) -> float:
    if source_max <= 0:
        return 0.0
    normalized = (safe_float(value) / source_max) * target_max
    return round(clamp(normalized, 0.0, target_max), 2)


def safe_score(value: Any, *, multiplier: int = 10, missing_value: int = -1) -> int:
    if value is None:
        return missing_value
    return int(round(clamp(value) * multiplier))


def stars_to_score(stars: Any, max_stars: float = 5.0) -> float:
    return clamp((safe_float(stars) / max_stars) * 100.0)


def iso_to_years_ago(iso_value: str | None) -> float:
    if not iso_value:
        return 0.0
    try:
        dt = datetime.fromisoformat(str(iso_value).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt.replace(tzinfo=timezone.utc)).days / 365.25
    except Exception:
        return 0.0


def weighted_platform_score(scores_dict: dict[str, Any], weights: dict[str, float]) -> dict[str, Any]:
    present = {metric: weight for metric, weight in weights.items() if metric in scores_dict}
    missing = [metric for metric in weights if metric not in scores_dict]
    if not present:
        return {"platform_score": 0.0, "breakdown": {}, "warnings": missing}

    total_weight = sum(present.values())
    weighted_sum = 0.0
    breakdown: dict[str, Any] = {}

    for metric, raw_weight in present.items():
        normalized_weight = raw_weight / total_weight
        sub_score = clamp(scores_dict.get(metric))
        contribution = sub_score * normalized_weight
        weighted_sum += contribution
        breakdown[metric] = {
            "raw_score": round(sub_score, 2),
            "weight": round(normalized_weight * 100, 2),
            "contribution": round(contribution, 2),
        }

    return {
        "platform_score": round(weighted_sum, 2),
        "breakdown": breakdown,
        "warnings": missing,
    }


def redistribute_platform_weights(platform_scores: dict[str, float | None]) -> dict[str, float]:
    present = {platform: score for platform, score in platform_scores.items() if score is not None}
    if not present:
        return {platform: 0.0 for platform in OVERALL_PLATFORM_WEIGHTS}

    total_weight = sum(OVERALL_PLATFORM_WEIGHTS[platform] for platform in present)
    return {
        platform: (
            OVERALL_PLATFORM_WEIGHTS[platform] / total_weight if platform in present else 0.0
        )
        for platform in OVERALL_PLATFORM_WEIGHTS
    }


def compute_overall_score(platform_scores: dict[str, float | None]) -> tuple[float, dict[str, float]]:
    adjusted_weights = redistribute_platform_weights(platform_scores)
    overall_score = round(
        sum(
            safe_float(score) * adjusted_weights[platform] * 10.0
            for platform, score in platform_scores.items()
            if score is not None
        ),
        2,
    )
    return overall_score, adjusted_weights


def score_to_grade(score_out_of_1000: float) -> tuple[str, str]:
    score = safe_float(score_out_of_1000)
    if score >= 850:
        return "A+", "Excellent"
    if score >= 750:
        return "A", "Very Good"
    if score >= 650:
        return "B+", "Good"
    if score >= 550:
        return "B", "Above Average"
    if score >= 450:
        return "C+", "Average"
    if score >= 350:
        return "C", "Below Average"
    return "D", "Needs Improvement"


def score_label_from_100(score_out_of_100: float) -> str:
    score = clamp(score_out_of_100)
    if score >= 85:
        return "Excellent Candidate"
    if score >= 70:
        return "Strong Candidate"
    if score >= 55:
        return "Promising Candidate"
    if score >= 40:
        return "Average Candidate"
    return "Needs Improvement"
