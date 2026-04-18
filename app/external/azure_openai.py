from __future__ import annotations

import json
import re
from typing import Any

from openai import OpenAI

from app.core.config import get_settings


class AzureOpenAIClient:
    def __init__(self) -> None:
        settings = get_settings()
        self.deployment = settings.azure_openai_deployment
        self._client = None
        if settings.azure_openai_api_key and settings.azure_openai_endpoint:
            self._client = OpenAI(
                base_url=settings.azure_openai_endpoint,
                api_key=settings.azure_openai_api_key,
            )

    @property
    def is_configured(self) -> bool:
        return self._client is not None

    def json_completion(
        self,
        *,
        system_prompt: str,
        user_content: str,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        if not self._client:
            return {}

        raw_output = ""
        try:
            response = self._client.chat.completions.create(
                model=self.deployment,
                temperature=temperature,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
            )
            raw_output = response.choices[0].message.content or "{}"
            return json.loads(raw_output)
        except Exception:
            if raw_output:
                match = re.search(r"\{.*\}", raw_output, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(0))
                    except Exception:
                        return {}
            return {}
