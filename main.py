from app.main import app
from app.services.ml_service import MLService


def simulated_ml_model(platform_links: dict[str, str]) -> dict:
    return MLService().generate_score(platform_links).model_dump()
