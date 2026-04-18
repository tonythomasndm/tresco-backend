from __future__ import annotations

import traceback

from fastapi import APIRouter, HTTPException

from app.models.request_models import ScoreRequest
from app.repositories.score_repo import ScoreRepository
from app.repositories.user_repo import UserRepository
from app.services.ml_service import MLService
from app.utils.helpers import raise_api_error


router = APIRouter(tags=["score"])


@router.post("/generate-score")
def generate_score(request: ScoreRequest) -> dict:
    user_id = (request.user_id or "").strip()
    if not user_id:
        raise_api_error(
            400,
            "invalid_user_id",
            "`user_id` must not be blank.",
            {"field": "user_id"},
        )

    user_repo = UserRepository()
    score_repo = ScoreRepository()
    ml_service = MLService()

    platform_links = user_repo.get_platform_links_by_user(user_id)
    platform_accounts = user_repo.get_platform_accounts_by_user(user_id)

    try:
        result = ml_service.generate_score(platform_links)
    except HTTPException:
        raise
    except Exception:
        traceback.print_exc()
        raise_api_error(
            502,
            "model_execution_failed",
            "The scoring pipeline failed while generating the candidate score.",
        )

    result_payload = result.model_dump()
    score_repo.persist_score_results(user_id, result_payload, platform_accounts)

    return {
        "status": "success",
        "data": {
            "score": result.score,
            "pros": result.pros,
            "cons": result.cons,
            "interview_questions": result.interview_questions,
            "improvements": result.improvements,
            "platform_scores": result.platform_scores,
        },
    }
