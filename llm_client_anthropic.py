import os
from dataclasses import dataclass
from typing import Optional

import anthropic


@dataclass
class AnthropicConfig:
    model: str = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    max_tokens: int = 4096
    temperature: float = 0.1


class LLMClientAnthropic:

    def __init__(self, config: Optional[AnthropicConfig] = None):
        self.config = config or AnthropicConfig()
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY ausente.")
        self.client = anthropic.Anthropic(api_key=api_key)

    def gerar_texto(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        msg = self.client.messages.create(
            model=self.config.model,
            max_tokens=max_tokens or self.config.max_tokens,
            temperature=temperature or self.config.temperature,
            system=system_prompt or "Você é um assistente analítico da CAMIM.",
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
