from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PlatformScoreModel(BaseModel):
    platform: str
    score: float
    reason: str
    key_findings: list[str] = Field(default_factory=list)
    rule_score: float | None = None


class FinalResponseModel(BaseModel):
    score: int = 0
    pros: str = ""
    cons: str = ""
    interview_questions: str = ""
    improvements: str = ""
    platform_scores: dict[str, int] = Field(default_factory=dict)
    candidate_report: dict[str, Any] = Field(default_factory=dict)
    recruiter_report: dict[str, Any] = Field(default_factory=dict)
    personalized_probe_suggestions: list[dict[str, Any]] = Field(default_factory=list)
    analysis: dict[str, Any] = Field(default_factory=dict)
