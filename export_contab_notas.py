#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_contab_notas.py — coleta do app "Contabilidade - Notas".

Uma nota por linha, com o máximo de dados que o ERP guarda (número, RPS, chave
da NFS-e, tomador, matrícula, código do serviço, nome CAMIM do serviço,
discriminação, ISS, cancelamento...). Somente SELECT.

Saída: json_contab_notas/<POSTO>_<AAAA-MM>.json — um arquivo por posto e mês.
Quem junta os 13 postos POR EMPRESA (cada clínica é uma empresa; as duas
operadoras emitem em vários postos) é a API, em contabilidade_notas_routes.py.

Esta pasta NÃO é publicada em /var/www: o dado só sai pela API, que confere a
permissão da página. Nota fiscal com nome e CPF não fica em arquivo estático.

Uso:
    python export_contab_notas.py                    # mês atual + anterior, e os meses que faltam desde 2026-01
    python export_contab_notas.py --meses 2026-07,2026-08
    python export_contab_notas.py --postos AC --dry-run   # só conta, não grava

Posto que falha mantém o arquivo anterior (com a data de coleta antiga) e fica
registrado em _etl_meta_export_contab_notas.json — falha não vira "zero nota".
"""
import argparse
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

from etl_meta import ETLMeta
# Só o encanamento (conexão, retry, calendário). A query é própria.
from export_notas_rps import (
    EARLIEST_ALLOWED,
    build_conns_from_env,
    load_sql_strip_go,
    month_bounds,
    month_iter,
    previous_month_bounds,
    run_query,
    try_build_engine,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_PATH = os.path.join(BASE_DIR, "sql_contab_notas", "notas.sql")
OUT_DIR = os.path.join(BASE_DIR, "json_contab_notas")


def out_path(posto: str, ym: str) -> str:
    return os.path.join(OUT_DIR, f"{posto}_{ym}.json")


def _gravar(posto: str, ym: str, df) -> int:
    df = df.copy()
    df["data_emissao"] = df["data_emissao"].astype(str)
    # Nada de NaN no arquivo: troca por None e ainda recusa na serialização.
    df = df.astype(object).where(df.notna(), None)
    payload = {
        "posto": posto,
        "ym": ym,
        "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "notas": df.to_dict(orient="records"),
    }
    txt = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    destino = out_path(posto, ym)
    tmp = destino + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(txt)
    os.replace(tmp, destino)   # troca atômica: a API nunca lê arquivo pela metade
    return len(payload["notas"])


def _posto(posto: str, odbc: str, meses: list, sql: str, forcados: set, dry_run: bool):
    """→ (posto, [linhas de log], erro ou None)."""
    log = []
    pendentes = [(ini, fim, ym) for ini, fim, ym in meses
                 if ym in forcados or not os.path.exists(out_path(posto, ym))]
    if not pendentes:
        return posto, log, None
    try:
        engine = try_build_engine(odbc, retries=3)
    except Exception as e:
        return posto, log, f"conexão: {e}"
    for ini, fim, ym in pendentes:
        try:
            df = run_query(engine, sql, ini, fim, retries=3)
        except Exception as e:
            return posto, log, f"query {ym}: {e}"
        if dry_run:
            log.append(f"[{posto}] DRY-RUN {ym}: {len(df)} notas (nada gravado)")
            continue
        try:
            n = _gravar(posto, ym, df)
        except Exception as e:
            return posto, log, f"gravar {ym}: {e}"
        log.append(f"[{posto}] OK {ym}: {n} notas")
    return posto, log, None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--postos", default="", help="Subconjunto de postos, ex.: AC. Vazio = todos do .env.")
    ap.add_argument("--meses", default="", help="Meses a regravar além do atual e do anterior. Ex.: 2026-07,2026-08")
    ap.add_argument("--dry-run", action="store_true", help="Roda as queries e só conta; não grava nada.")
    args = ap.parse_args()

    postos = [c for c in (args.postos or "").upper() if c.isalpha()]
    meses_extra = {m.strip() for m in (args.meses or "").split(",") if m.strip()}
    invalidos = sorted(m for m in meses_extra if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", m))
    if invalidos:
        raise SystemExit(f"--meses inválido (use AAAA-MM): {', '.join(invalidos)}")

    sql = load_sql_strip_go(SQL_PATH)
    if not sql:
        raise SystemExit(f"SQL vazio: {SQL_PATH}")
    conns = build_conns_from_env(postos=postos or None)
    if not conns:
        raise SystemExit("Nenhum posto no .env (DB_HOST_<P> / DB_BASE_<P>).")

    hoje = date.today()
    _, fim_atual, ym_atual = month_bounds(hoje)
    _, _, ym_ant = previous_month_bounds(hoje)
    forcados = meses_extra | {ym_atual, ym_ant}
    meses = [month_bounds(m) for m in month_iter(EARLIEST_ALLOWED, fim_atual)]

    if not args.dry_run:
        os.makedirs(OUT_DIR, exist_ok=True)
    meta = ETLMeta("export_contab_notas", OUT_DIR)

    with ThreadPoolExecutor(max_workers=min(13, len(conns))) as ex:
        resultados = list(ex.map(lambda kv: _posto(kv[0], kv[1], meses, sql, forcados, args.dry_run),
                                 conns.items()))
    falhas = 0
    for posto, log, erro in sorted(resultados):
        for linha in log:
            print(linha)
        if erro:
            falhas += 1
            print(f"[{posto}] ERRO {erro} (arquivo anterior mantido)")
            meta.error(posto, str(erro)[:500])
        else:
            meta.ok(posto)
    if not args.dry_run:
        meta.save()
    print(f"fim: {len(resultados) - falhas} postos ok, {falhas} com erro")


if __name__ == "__main__":
    main()
