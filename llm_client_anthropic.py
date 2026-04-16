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
        self.last_finish_reason: Optional[str] = None
        self.last_usage: dict = {}
        self.last_model: Optional[str] = None

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
        self.last_finish_reason = getattr(msg, "stop_reason", None)
        usage = getattr(msg, "usage", None)
        if usage is not None:
            pin  = getattr(usage, "input_tokens",  None)
            pout = getattr(usage, "output_tokens", None)
            tot  = None
            if pin is not None and pout is not None:
                tot = pin + pout
            self.last_usage = {
                "prompt_tokens":     pin,
                "completion_tokens": pout,
                "total_tokens":      tot,
            }
        else:
            self.last_usage = {}
        self.last_model = getattr(msg, "model", None) or self.config.model
        return msg.content[0].text.strip()
