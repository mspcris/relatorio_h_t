# export_vendas.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
#
# Função:
#   - Executa o SQL de vendas (fin_receita_vendas.sql) por posto e mês
#   - Salva CSVs em /dados_vendas
#   - Gera JSON agregado simples em /json_vendas:
#       json_vendas/vendas_mensal.json
#     Estrutura:
#       {
#         "2025-10": {
#           "valor_total": ...,
#           "qtd_vendas": ...,
#           "ticket_medio": ...,
#           "por_posto": {
#             "A": { "valor_total": ..., "qtd_vendas": ..., "ticket_medio": ... },
#             ...
#           }
#         },
#         ...
#         "_meta": { "gerado_em": "...iso..." }
#       }
#
import os
import re
import sys
import json
import argparse
from datetime import date, datetime, timezone

import pandas as pd
from sqlalchemy import text

# Reuso de utilitários do export_governanca (sem afetar o pipeline atual)
from export_governanca import (
    build_conns_from_env,
    ensure_dir,
    make_engine,
    POSTOS,
    ym_to_date,
    month_bounds,
    month_iter,
    current_month_bounds,
    EARLIEST_ALLOWED,
    load_sql_strip_go,
)

# =========================
# Constantes específicas de VENDAS
# =========================
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
SQL_VENDAS_DIR    = os.path.join(BASE_DIR, "sql_vendas")
DADOS_VENDAS_DIR  = os.path.join(BASE_DIR, "dados_vendas")
JSON_VENDAS_DIR   = os.path.join(BASE_DIR, "json_vendas")

SQL_VENDAS_FILE   = os.path.join(SQL_VENDAS_DIR, "fin_receita_vendas.sql")


def _set_mtime(path: str) -> None:
    """Ajusta atime/mtime para 'agora' com timezone local."""
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))


# =========================
# Execução de consultas
# =========================
def _ensure_nocount(sql_text: str) -> str:
    """Garante SET NOCOUNT ON; no início do SQL para não sujar o resultado."""
    return sql_text if sql_text.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_text


def run_query(engine, sql_txt, ini, fim):
    """Executa o SQL de vendas com parâmetros de data ini/fim."""
    body = _ensure_nocount(sql_txt)
    with engine.connect() as con:
        df = pd.read_sql_query(
            text(body),
            con,
            params={"ini": ini, "fim": fim},
        )
    return df


def target_csv_path(posto: str, ym: str) -> str:
    """Nome padrão do CSV de vendas por posto/mês."""
    return os.path.join(DADOS_VENDAS_DIR, f"{posto}_{ym}_vendas.csv")


def should_write_file(path: str, force: bool = False) -> bool:
    """Regra simples: só sobrescreve se --force; caso contrário SKIP se existir."""
    if force:
        return True
    return not os.path.exists(path)


# =========================
# Consolidação em JSON
# =========================
def build_vendas_json():
    """
    Varre dados_vendas/*_vendas.csv e gera json_vendas/vendas_mensal.json
    com agregados por mês e por posto.
    """
    ensure_dir(JSON_VENDAS_DIR)
    if not os.path.isdir(DADOS_VENDAS_DIR):
        print("[VENDAS][JSON] Pasta de dados de vendas inexistente; etapa pulada.")
        return

    pattern = re.compile(r"^(?P<posto>[A-Z])_(?P<ym>\d{4}-\d{2})_vendas\.csv$", re.I)

    agregados = {}  # ym -> {...}

    for fn in os.listdir(DADOS_VENDAS_DIR):
        m = pattern.match(fn)
        if not m:
            continue

        posto = m.group("posto").upper()
        ym    = m.group("ym")
        path  = os.path.join(DADOS_VENDAS_DIR, fn)

        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"[VENDAS][JSON] ERRO lendo {fn}: {e}")
            continue

        # Coluna de valor
        cols_lower = {c.lower(): c for c in df.columns}
        col_valor = None
        for cand in ["valor pago", "valorpago", "valor", "total"]:
            if cand.lower() in cols_lower:
                col_valor = cols_lower[cand.lower()]
                break

        if col_valor is None:
            print(f"[VENDAS][JSON] AVISO: {fn} sem coluna de valor identificável; pulando.")
            continue

        v_total = float(pd.to_numeric(df[col_valor], errors="coerce").fillna(0).sum())
        qtd     = int(len(df))

        agregados.setdefault(ym, {
            "valor_total": 0.0,
            "qtd_vendas": 0,
            "ticket_medio": None,
            "por_posto": {}
        })

        agregados[ym]["valor_total"] += v_total
        agregados[ym]["qtd_vendas"]  += qtd

        por_posto = agregados[ym]["por_posto"]
        por_posto.setdefault(posto, {
            "valor_total": 0.0,
            "qtd_vendas": 0,
            "ticket_medio": None
        })
        por_posto[posto]["valor_total"] += v_total
        por_posto[posto]["qtd_vendas"]  += qtd

    # Pós-processa tickets médios
    for ym, info in agregados.items():
        if info["qtd_vendas"] > 0:
            info["ticket_medio"] = round(info["valor_total"] / info["qtd_vendas"], 2)
        info["valor_total"] = round(info["valor_total"], 2)

        for posto, vals in info["por_posto"].items():
            if vals["qtd_vendas"] > 0:
                vals["ticket_medio"] = round(vals["valor_total"] / vals["qtd_vendas"], 2)
            vals["valor_total"] = round(vals["valor_total"], 2)

    # Ordena por mês
    out = {}
    for ym in sorted(agregados.keys()):
        out[ym] = agregados[ym]

    out["_meta"] = {
        "gerado_em": datetime.now(timezone.utc).astimezone().isoformat()
    }

    out_path = os.path.join(JSON_VENDAS_DIR, "vendas_mensal.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    _set_mtime(out_path)

    print(f"[VENDAS][JSON] Gerado -> {os.path.relpath(out_path, BASE_DIR)}")


# =========================
# CLI
# =========================
def parse_args():
    p = argparse.ArgumentParser(
        description="Export de vendas: executa fin_receita_vendas.sql por posto/mês, gera CSV e JSON.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--from", dest="from_ym", default=None,
                   help="Início YYYY-MM. Default=2024-01.")
    p.add_argument("--to", dest="to_ym", default=None,
                   help="Fim exclusivo YYYY-MM. Default=início do mês corrente.")
    p.add_argument("--only-month", dest="only_month", default=None,
                   help="Executa só um mês YYYY-MM.")
    p.add_argument("--postos", default="".join(POSTOS),
                   help="Subset de postos. Ex: ANX.")
    p.add_argument("--force", action="store_true",
                   help="Sobrescreve CSVs existentes.")
    p.add_argument("--no-validate", action="store_true",
                   help="Não valida o conteúdo do .sql.")
    p.add_argument("--dry-run", action="store_true",
                   help="Executa sem gravar CSV/JSON.")
    return p.parse_args()


# =========================
# Orquestração principal
# =========================
def run():
    args = parse_args()
    print("========== EXPORT VENDAS ==========")
    print("[ETAPA 1/3] Setup")

    ensure_dir(SQL_VENDAS_DIR)
    ensure_dir(DADOS_VENDAS_DIR)
    ensure_dir(JSON_VENDAS_DIR)

    ini_cur, fim_cur, ym_current = current_month_bounds()

    if args.only_month:
        start = ym_to_date(args.only_month)
        end_exclusive = month_bounds(start)[1]
        periodo_desc = f"{args.only_month}"
    else:
        start = ym_to_date(args.from_ym) if args.from_ym else EARLIEST_ALLOWED
        end_exclusive = ym_to_date(args.to_ym) if args.to_ym else fim_cur
        periodo_desc = f"{start} .. < {end_exclusive}"

    postos = [c for c in args.postos if c.isalpha()]
    conns = build_conns_from_env(postos)
    if not conns:
        print("ERRO: .env sem DB_HOST_*/DB_BASE_* configurados para os postos informados.")
        sys.exit(1)

    # Carrega SQL de vendas
    if not os.path.exists(SQL_VENDAS_FILE):
        print(f"ERRO: arquivo SQL de vendas não encontrado -> {SQL_VENDAS_FILE}")
        sys.exit(1)

    sql_text = load_sql_strip_go(SQL_VENDAS_FILE)
    if not sql_text:
        print(f"ERRO: SQL vazio em {SQL_VENDAS_FILE}")
        sys.exit(1)

    if not args.no_validate:
        # Validação simples: não permitir código Python acidental dentro do SQL
        proibidos = [r"\bsql_txt\s*=", r"\bpandas\b", r"\bimport\b", r"\bengine\.connect\("]
        for pat in proibidos:
            if re.search(pat, sql_text, flags=re.IGNORECASE):
                print(f"ERRO: SQL contém padrão proibido ({pat}) em {SQL_VENDAS_FILE}")
                sys.exit(1)

    print(f"- Período: {periodo_desc}")
    print(f"- Mês corrente: {ym_current}")
    print(f"- Postos: {list(conns.keys())}")
    print(f"- SQL vendas: {os.path.basename(SQL_VENDAS_FILE)}")
    print(f"- Force={args.force}  DryRun={args.dry_run}  Validate={not args.no_validate}")

    # -------------------------
    # ETAPA 2: Execução por mês/posto
    # -------------------------
    print("\n[ETAPA 2/3] Execução por mês/posto")
    for month_start in month_iter(start, end_exclusive):
        ini, fim, ym = month_bounds(month_start)
        print(f"\n-- Mês {ym} ----------------------------")
        for posto, odbc_str in conns.items():
            print(f"   [{posto}] conectando...")
            try:
                engine = make_engine(odbc_str)
            except Exception as e:
                print(f"   [{posto}] ERRO engine: {e}")
                continue

            out_path = target_csv_path(posto, ym)
            if not should_write_file(out_path, force=args.force):
                print(f"   [{posto}] SKIP {os.path.basename(out_path)} (existe)")
                continue

            print(f"   [{posto}] RUN {os.path.basename(SQL_VENDAS_FILE)} -> {os.path.basename(out_path)}")
            if args.dry_run:
                continue

            try:
                df = run_query(engine, sql_text, ini, fim)
            except Exception as e:
                print(f"   [{posto}] ERRO exec: {e}")
                continue

            try:
                df.to_csv(out_path, index=False, encoding="utf-8-sig")
                print(f"   [{posto}] OK linhas={len(df)}")
            except Exception as e:
                print(f"   [{posto}] ERRO salvar: {e}")

    if args.dry_run:
        print("\n[ETAPA 3/3] JSON vendas -> SKIP (dry-run)")
        return

    # -------------------------
    # ETAPA 3: JSON agregado
    # -------------------------
    print("\n[ETAPA 3/3] JSON vendas (agregado mensal)")
    build_vendas_json()

    print("\n✅ Finalizado export_vendas.py.")


if __name__ == "__main__":
    run()
