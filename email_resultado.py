"""email_resultado.py — o que a coluna Erro da vw_cad_email quer dizer.

Sem dependência nenhuma de propósito: o camim-auth (sem pyodbc) também lê.
"""

import re

# ── Resultado do envio (coluna Erro da vw_cad_email) ─────────────────────────
# Levantado em 17/09/2026 em todos os postos (desde 01/08): SÓ o
# CamimCancelaConsultas ("Pré agendamento Cancelado") grava a coluna Erro —
# com o ID da mensagem na Amazon SES quando deu certo, ou "EMAIL EXCECAO HTTP:
# …" quando falhou. Os outros programas (boleto, exame, prescrição, boas-vindas)
# deixam Erro sempre vazio: o e-mail está na tabela, mas o resultado não.
# Quem usa: sync_email (grava ind_email.falhou), export_indicadores_painel e a
# lista do monitor de robôs (auth_routes) — a regra mora SÓ aqui.

_RE_SES_ID = re.compile(r"^[0-9a-f]{16}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[0-9]{6}$", re.I)


def email_falhou(erro: str) -> int:
    """1 = o envio falhou. Vazio (programa não grava) e ID da Amazon SES
    (aceito para entrega) contam como enviado."""
    e = (erro or "").strip()
    if not e or _RE_SES_ID.match(e):
        return 0
    return 1


def explicar_erro_email(erro: str) -> str:
    """O conteúdo da coluna Erro em português de gente."""
    e = (erro or "").strip()
    if not e:
        return "enviado (o programa não grava o resultado)"
    if _RE_SES_ID.match(e):
        return "aceito pela Amazon SES para entrega (o código é o ID da mensagem lá)"
    if "500" in e and "Internal Server Error" in e:
        return "falhou: a API de e-mail deu erro interno (HTTP 500) — não saiu"
    if "12002" in e and "sending" in e.lower():
        return "falhou: a API de e-mail não respondeu a tempo no envio (WinHTTP 12002) — não saiu"
    if "12002" in e and "receiving" in e.lower():
        return "falhou: enviado, mas a resposta não voltou a tempo (WinHTTP 12002) — pode ter saído"
    m = re.search(r"\((12\d{3})\)", e)
    if m:
        return f"falhou: erro de comunicação com a API de e-mail (WinHTTP {m.group(1)})"
    return "falhou: " + re.sub(r"^EMAIL EXCECAO HTTP:\s*", "", e)[:120]
