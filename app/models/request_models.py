from pydantic import BaseModel


class ScoreRequest(BaseModel):
    user_id: str
