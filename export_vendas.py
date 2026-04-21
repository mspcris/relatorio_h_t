# export_vendas.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv

import os
import re
import sys
import json
import argparse
from datetime import date, datetime, timezone

import pandas as pd
from sqlalchemy import text

# Reuso de utilitários do export_governanca
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
from etl_meta import ETLMeta

# =============================================================================
# METADADOS
# =============================================================================
def add_metadata(payload: dict) -> dict:
    payload["_meta"] = {
        "gerado_em": datetime.now(timezone.utc).astimezone().isoformat()
    }
    return payload


# =============================================================================
# CONSTANTES
# =============================================================================
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
SQL_VENDAS_DIR    = os.path.join(BASE_DIR, "sql_vendas")
DADOS_VENDAS_DIR  = os.path.join(BASE_DIR, "dados_vendas")
JSON_VENDAS_DIR   = os.path.join(BASE_DIR, "json_vendas")

SQL_VENDAS_FILE   = os.path.join(SQL_VENDAS_DIR, "fin_receita_vendas.sql")


def _set_mtime(path: str) -> None:
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))


# =============================================================================
# SQL helpers
# =============================================================================
def _ensure_nocount(sql_text: str) -> str:
    if sql_text.lstrip().upper().startswith("SET NOCOUNT ON"):
        return sql_text
    return "SET NOCOUNT ON;\n" + sql_text


def run_query(engine, sql_txt, ini, fim):
    body = _ensure_nocount(sql_txt)
    with engine.connect() as con:
        df = pd.read_sql_query(
            text(body),
            con,
            params={"ini": ini, "fim": fim},
        )
    return df


def target_csv_path(posto: str, ym: str) -> str:
    return os.path.join(DADOS_VENDAS_DIR, f"{posto}_{ym}_vendas.csv")


# =============================================================================
# INTELIGÊNCIA DE REPROCESSAMENTO
# =============================================================================
def previous_month_slug():
    """Retorna 'YYYY-MM' do mês anterior ao mês corrente."""
    today = date.today()
    if today.month > 1:
        y = today.year
        m = today.month - 1
    else:
        y = today.year - 1
        m = 12
    return f"{y:04d}-{m:02d}"


def should_write_file_vendas(path: str, ym: str, ym_current: str, ym_prev: str, force: bool, only_month: str):
    """
    Lógica oficial:
      - mês atual  => sempre reprocessa
      - mês anterior => sempre reprocessa
      - demais => só se não existir, ou se --force, ou se --only-month foi usado
    """
    if ym == ym_current:
        return True

    if ym == ym_prev:
        return True

    if force:
        return True

    if only_month and ym == only_month:
        return True

    return not os.path.exists(path)


# =============================================================================
# GERA JSON CONSOLIDADO
# =============================================================================
def build_vendas_json():
    ensure_dir(JSON_VENDAS_DIR)

    if not os.path.isdir(DADOS_VENDAS_DIR):
        print("[VENDAS][JSON] Pasta de dados de vendas inexistente; etapa pulada.")
        return

    pattern = re.compile(r"^(?P<posto>[A-Z])_(?P<ym>\d{4}-\d{2})_vendas\.csv$", re.I)
    agregados = {}

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

        # descobrir nome da coluna de valor
        cols_lower = {c.lower(): c for c in df.columns}
        col_valor = None
        for cand in ["valor pago", "valorpago", "valor", "total"]:
            if cand.lower() in cols_lower:
                col_valor = cols_lower[cand.lower()]
                break

        if not col_valor:
            print(f"[VENDAS][JSON] AVISO: {fn} sem coluna de valor; pulando.")
            continue

        v_total = float(pd.to_numeric(df[col_valor], errors="coerce").fillna(0).sum())
        qtd     = len(df)

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
            "ticket_medio": None,
            "por_corretor": {},
            "por_corretor_subcorretor": {}
        })

        por_posto[posto]["valor_total"] += v_total
        por_posto[posto]["qtd_vendas"]  += qtd

        # Agregação por corretor / subcorretor dentro do posto
        col_corr = cols_lower.get("corretor")
        col_sub  = cols_lower.get("subcorretor")
        valores_num = pd.to_numeric(df[col_valor], errors="coerce").fillna(0)

        if col_corr:
            corr_series = df[col_corr].fillna("(sem corretor)").astype(str).str.strip().replace("", "(sem corretor)")
            por_corr = por_posto[posto]["por_corretor"]
            for nome, grupo in valores_num.groupby(corr_series):
                bucket = por_corr.setdefault(nome, {"valor_total": 0.0, "qtd_vendas": 0, "ticket_medio": None})
                bucket["valor_total"] += float(grupo.sum())
                bucket["qtd_vendas"]  += int(grupo.size)

            if col_sub:
                sub_series = df[col_sub].fillna("(sem subcorretor)").astype(str).str.strip().replace("", "(sem subcorretor)")
                chave = corr_series + "||" + sub_series
                por_pair = por_posto[posto]["por_corretor_subcorretor"]
                for k, grupo in valores_num.groupby(chave):
                    corr_nome, sub_nome = k.split("||", 1)
                    bucket = por_pair.setdefault(k, {
                        "corretor": corr_nome,
                        "subcorretor": sub_nome,
                        "valor_total": 0.0,
                        "qtd_vendas": 0,
                        "ticket_medio": None
                    })
                    bucket["valor_total"] += float(grupo.sum())
                    bucket["qtd_vendas"]  += int(grupo.size)

    # pós-processamento
    for ym, info in agregados.items():
        if info["qtd_vendas"] > 0:
            info["ticket_medio"] = round(info["valor_total"] / info["qtd_vendas"], 2)
        info["valor_total"] = round(info["valor_total"], 2)

        for posto, v in info["por_posto"].items():
            if v["qtd_vendas"] > 0:
                v["ticket_medio"] = round(v["valor_total"] / v["qtd_vendas"], 2)
            v["valor_total"] = round(v["valor_total"], 2)

            for nome, c in v.get("por_corretor", {}).items():
                if c["qtd_vendas"] > 0:
                    c["ticket_medio"] = round(c["valor_total"] / c["qtd_vendas"], 2)
                c["valor_total"] = round(c["valor_total"], 2)

            pair_dict = v.get("por_corretor_subcorretor", {}) or {}
            pair_list = []
            for p in pair_dict.values():
                if p["qtd_vendas"] > 0:
                    p["ticket_medio"] = round(p["valor_total"] / p["qtd_vendas"], 2)
                p["valor_total"] = round(p["valor_total"], 2)
                pair_list.append(p)
            pair_list.sort(key=lambda x: (-x["qtd_vendas"], -x["valor_total"]))
            v["por_corretor_subcorretor"] = pair_list

    # ordena meses
    out = {}
    for ym in sorted(agregados.keys()):
        out[ym] = agregados[ym]

    # injeta metadados
    out = add_metadata(out)

    # salvar
    out_path = os.path.join(JSON_VENDAS_DIR, "vendas_mensal.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    _set_mtime(out_path)
    print(f"[VENDAS][JSON] Gerado -> {os.path.relpath(out_path, BASE_DIR)}")


# =============================================================================
# CLI
# =============================================================================
def parse_args():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--from", dest="from_ym", default=None)
    p.add_argument("--to", dest="to_ym", default=None)
    p.add_argument("--only-month", dest="only_month", default=None)
    p.add_argument("--postos", default="".join(POSTOS))
    p.add_argument("--force", action="store_true", help="Sobrescreve CSVs.")
    p.add_argument("--no-validate", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


# =============================================================================
# MAIN
# =============================================================================
def run():
    args = parse_args()
    print("========== EXPORT VENDAS ==========")

    meta = ETLMeta('export_vendas', 'json_vendas')

    ensure_dir(SQL_VENDAS_DIR)
    ensure_dir(DADOS_VENDAS_DIR)
    ensure_dir(JSON_VENDAS_DIR)

    ini_cur, fim_cur, ym_current = current_month_bounds()
    ym_prev = previous_month_slug()

    if args.only_month:
        start = ym_to_date(args.only_month)
        end_exclusive = month_bounds(start)[1]
    else:
        start = ym_to_date(args.from_ym) if args.from_ym else EARLIEST_ALLOWED
        end_exclusive = ym_to_date(args.to_ym) if args.to_ym else fim_cur

    postos = [c for c in args.postos if c.isalpha()]
    conns = build_conns_from_env(postos)

    # valida SQL
    if not os.path.exists(SQL_VENDAS_FILE):
        print(f"ERRO: SQL vendas não encontrado -> {SQL_VENDAS_FILE}")
        sys.exit(1)

    sql_text = load_sql_strip_go(SQL_VENDAS_FILE)
    if not sql_text:
        print(f"ERRO: SQL vazio -> {SQL_VENDAS_FILE}")
        sys.exit(1)

    # Execução
    print("\n[ETAPA 2/3] Execução mês/posto")
    for month_start in month_iter(start, end_exclusive):
        ini, fim, ym = month_bounds(month_start)
        print(f"\n-- Mês {ym} ----------------")

        for posto, odbc_str in conns.items():
            print(f"   [{posto}] conectando...")
            try:
                engine = make_engine(odbc_str)
            except Exception as e:
                print(f"   [{posto}] ERRO engine: {e}")
                meta.error(posto, str(e))
                continue

            out_path = target_csv_path(posto, ym)

            if not should_write_file_vendas(out_path, ym, ym_current, ym_prev, args.force, args.only_month):
                print(f"   [{posto}] SKIP {os.path.basename(out_path)} (cache)")
                continue

            print(f"   [{posto}] EXEC -> {os.path.basename(out_path)}")

            if args.dry_run:
                continue

            try:
                df = run_query(engine, sql_text, ini, fim)
            except Exception as e:
                print(f"   [{posto}] ERRO SQL: {e}")
                meta.error(posto, str(e))
                continue

            try:
                df.to_csv(out_path, index=False, encoding="utf-8-sig")
                print(f"   [{posto}] OK linhas={len(df)}")
                meta.ok(posto)
            except Exception as e:
                print(f"   [{posto}] ERRO salvar: {e}")
                meta.error(posto, str(e))

    if args.dry_run:
        print("\n[ETAPA 3/3] JSON -> SKIP (dry-run)")
        return

    print("\n[ETAPA 3/3] JSON vendas (agregado mensal)")
    build_vendas_json()

    meta.save()
    print("\n✔ Finalizado export_vendas.py.")


if __name__ == "__main__":
    run()
