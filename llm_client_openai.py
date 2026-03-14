import os
from dataclasses import dataclass
from typing import Optional

from openai import OpenAI


@dataclass
class LLMConfig:
    model: str = os.getenv("OPENAI_MODEL", "gpt-4.1")
    temperature: float = 0.1
    max_tokens: int = 4096


class LLMClientOpenAI:

    def __init__(self, config: Optional[LLMConfig] = None):

        self.config = config or LLMConfig()

        api_key = os.getenv("OPENAI_API_KEY")

        if not api_key:
            raise RuntimeError("OPENAI_API_KEY ausente.")

        self.client = OpenAI(api_key=api_key)

    def gerar_texto(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:

        messages = []

        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({"role": "user", "content": prompt})

        temp = temperature or self.config.temperature
        mtok = max_tokens or self.config.max_tokens

        resp = self.client.chat.completions.create(
            model=self.config.model,
            messages=messages,
            temperature=temp,
            max_tokens=mtok
        )

        return resp.choices[0].message.content.strip()