# llm_client.py
"""
Este módulo é o único ponto de contato do backend com a API de LLM (Groq).
O objetivo é encapsular completamente a chamada de modelo em um lugar só.
Dessa forma, o restante do código (orquestrador, resumidor, pedagogo, formatter)
não conhece detalhes de SDK, nomes de modelos ou chaves de API.
Também facilita trocar de modelo ou ajustar temperatura/max_tokens sem refatorar
todo o projeto: basta mexer na configuração deste cliente.
Por fim, ele padroniza a interface: sempre recebe um prompt (e opcionalmente
um system_prompt) e devolve uma string já “limpa” de espaços supérfluos.

"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, Literal

from groq import Groq


@dataclass
class LLMConfig:
    """
    Configuração padrão de chamadas ao LLM via Groq.
    Os valores podem ser sobrepostos em cada chamada, se necessário.
    """
    # Nome do modelo principal – segue o mesmo padrão do analyze_groq.py
    model: str = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

    # Parâmetros default (podem ser override por chamada)
    temperature: float = 0.1
    max_tokens: int = 4096

    # Timeout “lógico” em segundos (apenas para controle externo, se quiser)
    timeout_s: int = 60


class LLMClient:
    """
    Cliente de alto nível para Groq (chat.completions).
    Toda chamada de IA do projeto deve passar por `gerar_texto`.
    Assim você:
      - centraliza autenticação (GROQ_API_KEY),
      - garante padrões de temperatura/max_tokens,
      - e tem um ponto único para logs e ajustes futuros.
    """

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError("GROQ_API_KEY ausente no ambiente.")
        self._client = Groq(api_key=api_key)

    def gerar_texto(
        self,
        prompt: str,
        *,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        # opcional, caso no futuro você queira reaproveitar em modo JSON
        response_format: Optional[Literal["json_object", "text"]] = "text",
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Gera texto a partir de um prompt, usando Groq.

        Parâmetros:
          - prompt: texto principal enviado como mensagem do usuário.
          - system_prompt: instrução de sistema (papel do agente).
          - temperature: override da temperatura padrão.
          - max_tokens: override de max_tokens padrão.
          - response_format:
              * "text"        → saída livre (texto normal).
              * "json_object" → força o modelo a responder com JSON (raw string).
          - extra: dicionário livre para metadados/log (não utilizado aqui, mas pronto p/ expansão).

        Retorno:
          - string com o conteúdo da primeira escolha (`choices[0].message.content`), stripado.
        """
        if not prompt or not prompt.strip():
            return ""

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        temp = float(temperature if temperature is not None else self.config.temperature)
        mtok = int(max_tokens if max_tokens is not None else self.config.max_tokens)

        kwargs: Dict[str, Any] = dict(
            model=self.config.model,
            messages=messages,
            temperature=temp,
            max_tokens=mtok,
            tool_choice="none",
        )

        if response_format == "json_object":
            kwargs["response_format"] = {"type": "json_object"}

        resp = self._client.chat.completions.create(**kwargs)
        out = (resp.choices[0].message.content or "").strip()
        return out
