#!/usr/bin/env python3
"""
export_custos_ia.py — ETL do dashboard "Custos com IA".

Roda no cron (1x/hora). Busca o custo REAL da OpenAI por projeto (mês corrente)
via Costs API da organização e grava o snapshot em CUSTOS_IA_DIR/openai.json.

NÃO toca na Groq (que não tem API — é atualizada sob demanda pela própria página,
via print/visão ou digitação manual).

É somente-leitura no provedor: faz apenas GET na Costs API (não gera custo).
"""
import json
import sys

from dotenv import load_dotenv

# Mesmo .env do app (caminho da VM). Em dev local, ajuste/exporte as vars.
load_dotenv("/opt/relatorio_h_t/.env")

import custos_ia  # noqa: E402


def main() -> int:
    snap = custos_ia.save_openai_snapshot()
    status = "OK" if snap.get("ok") else f"ERRO: {snap.get('error')}"
    print(
        f"[custos_ia] OpenAI {snap.get('month')} → "
        f"total US$ {snap.get('total_usd')} | "
        f"{len(snap.get('projects', []))} projeto(s) | {status}"
    )
    # Sai 0 mesmo em erro de credencial: o front mostra o erro; não queremos
    # spammar alertas de cron por falta de chave (estado esperado até configurar).
    return 0


if __name__ == "__main__":
    sys.exit(main())
