# export_consultas_mensal_json.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
#
# O que este script faz:
# - Gera JSON mensal por posto em: ./json_consultas_mensal/{POSTO}_consultas_YYYY-MM.json
# - (Opcional) Gera CSV mensal por posto em: ./dados_consultas_mensal/{POSTO}_consultas_YYYY-MM.csv
# - Mantém lógica incremental desde 2020-01:
#     * se JSON do mês NÃO existe -> roda e cria
#     * se JSON do mês JÁ existe -> não roda (skip)
# - Rotina diária: sempre força (recria) mês anterior e mês atual
#
# CONSOLIDADO (NOVO, COMPLETO para o front):
# - Salva em: ./json_consolidado/consultas_mensal_status_consolidado.json
# - Contém tudo necessário para reproduzir o que o front mostra HOJE, sem perder informação:
#   Para cada mês e posto, grava:
#     * totais por grupo (realizadas, medico_faltou, faltas, pend_recepcao, outros, total)
#     * agregação por Especialidade: para cada especialidade, soma por grupo + total
#     * agregação por Médico: para cada médico, soma por grupo + total
# - Assim o front consegue:
#     * KPIs e tabela mensal
#     * gráficos (linha total e stack por status)
#     * Top especialidades (Consultas vs Faltas)
#     * Top médicos (Consultas vs Faltas)
#
# Observação:
# - NÃO salvamos as “linhas cruas” no consolidado (isso explodiria o arquivo).
#   Em vez disso, salvamos agregados por especialidade/médico/status que preservam tudo que o front usa.

import os
import re
import json
import argparse
from datetime import date, datetime, timezone
from urllib.parse import quote_plus
import time

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# =========================
# Paths / Defaults
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SQL_PATH = os.path.join(BASE_DIR, "sql_consultas_mensal", "sql.sql")

OUT_JSON_DIR = os.path.join(BASE_DIR, "json_consultas_mensal")
OUT_CSV_DIR  = os.path.join(BASE_DIR, "dados_consultas_mensal")

# Pasta já existente
OUT_CONSOL_DIR = os.path.join(BASE_DIR, "json_consolidado")
CONSOL_FILENAME = "consultas_mensal_status_consolidado.json"
CONSOL_PATH = os.path.join(OUT_CONSOL_DIR, CONSOL_FILENAME)

os.makedirs(OUT_JSON_DIR, exist_ok=True)
os.makedirs(OUT_CSV_DIR, exist_ok=True)
os.makedirs(OUT_CONSOL_DIR, exist_ok=True)

POSTOS_DEFAULT = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

EARLIEST_ALLOWED = date(2020, 1, 1)


# =========================
# Utilitários (governança-like)
# =========================
def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _set_mtime(path: str) -> None:
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))

def load_sql_strip_go(path: str) -> str:
    txt = open(path, "r", encoding="utf-8", errors="ignore").read()
    txt = re.sub(r"(?im)^\s*go\s*$", "", txt)
    return txt.strip()

def _ensure_nocount(sql_text: str) -> str:
    return sql_text if sql_text.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_text

def build_conn_str(host, base, user, pwd, port, encrypt, trust_cert, timeout):
    server = f"tcp:{host},{port or '1433'}"
    common = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server};DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout or '5'};"
    )
    if user:
        return common + f"UID={user};PWD={pwd}"
    return common + "Trusted_Connection=yes"

def make_engine(odbc_conn_str: str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        pool_pre_ping=True,
        pool_recycle=300,
        future=True,
    )

def _sleep_backoff(attempt: int):
    time.sleep(min(0.5 * (2 ** (attempt - 1)), 5.0))

def build_conns_from_env(postos=None):
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")

    conns = {}
    base_postos = postos or POSTOS_DEFAULT

    for p in base_postos:
        host = env(f"DB_HOST_{p}")
        base = env(f"DB_BASE_{p}")
        if not host or not base:
            continue

        user = env(f"DB_USER_{p}")
        pwd  = env(f"DB_PASSWORD_{p}")
        port = env(f"DB_PORT_{p}", "1433")

        conns[p] = build_conn_str(host, base, user, pwd, port, encrypt, trust_cert, timeout)

    return conns


# =========================
# Datas / iteração mensal
# =========================
def month_bounds(dt: date):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    return ini, nxt, f"{ini.year:04d}-{ini.month:02d}"

def previous_month_bounds(dt: date):
    m = 12 if dt.month == 1 else dt.month - 1
    y = dt.year - 1 if dt.month == 1 else dt.year
    ini = date(y, m, 1)
    nxt = date(dt.year, dt.month, 1)
    return ini, nxt, f"{y:04d}-{m:02d}"

def month_iter(start: date, end_exclusive: date):
    y, m = start.year, start.month
    while True:
        ini = date(y, m, 1)
        if ini >= end_exclusive:
            break
        yield ini
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y


# =========================
# IO / naming
# =========================
def json_path(posto: str, ym: str) -> str:
    return os.path.join(OUT_JSON_DIR, f"{posto}_consultas_{ym}.json")

def csv_path(posto: str, ym: str) -> str:
    return os.path.join(OUT_CSV_DIR, f"{posto}_consultas_{ym}.csv")

def should_write(out_path: str, force: bool) -> bool:
    if force:
        return True
    return not os.path.exists(out_path)

def safe_read_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def safe_write_json(path: str, payload: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    _set_mtime(path)


# =========================
# Classificação (igual ao front)
# =========================
def strip_diacritics_basic(s: str) -> str:
    # sem libs externas: cobre o básico pro seu domínio
    return (s.replace("Á","A").replace("À","A").replace("Â","A").replace("Ã","A")
             .replace("É","E").replace("Ê","E")
             .replace("Í","I")
             .replace("Ó","O").replace("Ô","O").replace("Õ","O")
             .replace("Ú","U")
             .replace("Ç","C")
             .replace("á","a").replace("à","a").replace("â","a").replace("ã","a")
             .replace("é","e").replace("ê","e")
             .replace("í","i")
             .replace("ó","o").replace("ô","o").replace("õ","o")
             .replace("ú","u")
             .replace("ç","c"))

def norm_status(s: str) -> str:
    return strip_diacritics_basic(str(s or "").strip()).upper()

def classify_group(status: str) -> str:
    s = norm_status(status)

    # Realizadas
    if s == "ATENDIDO" or s == "AGUARDANDO":
        return "realizadas"

    # Médico faltou
    if "MEDICO" in s and "FALTOU" in s:
        return "medico_faltou"

    # Pendência recepção
    if "PENDENCIA" in s and ("RECEP" in s or "RECEPCAO" in s):
        return "pend_recepcao"

    # Faltas / não atendido / pendência guia/pagamento (exceto recepção)
    if (s == "FALTOU" or s == "AUSENTE" or "NAO ATEND" in s
        or ("PENDENCIA" in s and ("GUIA" in s or "PAGAMENTO" in s))):
        return "faltas"

    return "outros"


# =========================
# Execução DB
# =========================
def run_query(engine, sql_txt: str, ini: date, fim: date, retries: int = 3) -> pd.DataFrame:
    body = _ensure_nocount(sql_txt)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as con:
                return pd.read_sql_query(text(body), con, params={"ini": ini, "fim": fim})
        except Exception as e:
            last_err = e
            print(f"[WARN] tentativa {attempt}/{retries} falhou (query): {e}")
            if attempt < retries:
                _sleep_backoff(attempt)

    raise last_err


def try_build_engine(odbc_str: str, retries: int = 3):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            eng = make_engine(odbc_str)
            with eng.connect() as con:
                con.execute(text("SELECT 1"))
            return eng
        except Exception as e:
            last_err = e
            print(f"[WARN] tentativa {attempt}/{retries} falhou (engine/connect): {e}")
            if attempt < retries:
                _sleep_backoff(attempt)
    raise last_err


def write_outputs(posto: str, ym: str, ini: date, fim: date, df: pd.DataFrame, do_csv: bool):
    payload = {
        "posto": posto,
        "periodo": {"ym": ym, "ini": ini.isoformat(), "fim": fim.isoformat()},
        "linhas": df.to_dict(orient="records"),
    }
    out_json = json_path(posto, ym)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _set_mtime(out_json)

    if do_csv:
        out_csv = csv_path(posto, ym)
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        _set_mtime(out_csv)

    print(f"[{posto}] OK {ym} linhas={len(df)} -> {os.path.relpath(out_json, BASE_DIR)}" +
          ("" if not do_csv else f" + {os.path.relpath(csv_path(posto, ym), BASE_DIR)}"))


# =========================
# Consolidação COMPLETA pro front (sem perder info)
# =========================
GROUPS = ["realizadas", "medico_faltou", "faltas", "pend_recepcao", "outros"]

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # qtde
    if "qtde" not in out.columns:
        out["qtde"] = 0
    out["qtde"] = pd.to_numeric(out["qtde"], errors="coerce").fillna(0).astype(int)

    # status
    if "status" not in out.columns:
        out["status"] = ""

    # especialidade/médico
    if "Especialidade" not in out.columns:
        out["Especialidade"] = "Sem especialidade"
    out["Especialidade"] = out["Especialidade"].fillna("Sem especialidade").astype(str).str.strip()
    out.loc[out["Especialidade"].eq(""), "Especialidade"] = "Sem especialidade"

    if "medico" not in out.columns:
        out["medico"] = "Sem médico"
    out["medico"] = out["medico"].fillna("Sem médico").astype(str).str.strip()
    out.loc[out["medico"].eq(""), "medico"] = "Sem médico"

    return out

def _totais_por_grupo(df: pd.DataFrame) -> dict:
    total = int(df["qtde"].sum()) if len(df) else 0
    by_group = df.groupby("group")["qtde"].sum().to_dict() if len(df) else {}

    out = {g: int(by_group.get(g, 0)) for g in GROUPS}
    out["total"] = total
    # garantia: outros coerente
    out["outros"] = max(0, out["total"] - (out["realizadas"] + out["medico_faltou"] + out["faltas"] + out["pend_recepcao"]))
    return out

def _pivot_dim_by_group(df: pd.DataFrame, dim_col: str) -> dict:
    """
    Retorna dict:
      { "Nome": { "total": X, "realizadas": a, "medico_faltou": b, "faltas": c, "pend_recepcao": d, "outros": e }, ... }
    """
    if df is None or df.empty:
        return {}

    g = df.groupby([dim_col, "group"])["qtde"].sum().reset_index()

    out = {}
    for _, row in g.iterrows():
        name = str(row[dim_col] if row[dim_col] is not None else "").strip() or (f"Sem {dim_col}")
        grp = row["group"]
        val = int(row["qtde"] or 0)

        node = out.get(name)
        if node is None:
            node = {k: 0 for k in GROUPS}
            node["total"] = 0
            out[name] = node

        if grp in GROUPS:
            node[grp] += val
            node["total"] += val

    # coerência “outros” por item
    for _, node in out.items():
        node["outros"] = max(0, node["total"] - (node["realizadas"] + node["medico_faltou"] + node["faltas"] + node["pend_recepcao"]))

    return out

def build_consolidado_payload_for_month_posto(posto: str, ym: str, df: pd.DataFrame) -> dict:
    df2 = _ensure_cols(df)
    df2["group"] = df2["status"].apply(classify_group)

    totais = _totais_por_grupo(df2)
    por_especialidade = _pivot_dim_by_group(df2, "Especialidade")
    por_medico = _pivot_dim_by_group(df2, "medico")

    return {
        "posto": posto,
        "ym": ym,
        "totais": totais,
        "por_especialidade": por_especialidade,
        "por_medico": por_medico,
    }

def sum_totais_gerais(postos_dict: dict) -> dict:
    acc = {k: 0 for k in GROUPS}
    acc["total"] = 0

    for _, node in postos_dict.items():
        t = (node or {}).get("totais") or {}
        for g in GROUPS:
            acc[g] += int(t.get(g, 0) or 0)
        acc["total"] += int(t.get("total", 0) or 0)

    acc["outros"] = max(0, acc["total"] - (acc["realizadas"] + acc["medico_faltou"] + acc["faltas"] + acc["pend_recepcao"]))
    return acc


# =========================
# Orquestração
# =========================
def run_incremental_all_postos(postos=None, do_csv=False, force_months=None):
    ensure_dir(OUT_JSON_DIR)
    ensure_dir(OUT_CSV_DIR)
    ensure_dir(OUT_CONSOL_DIR)

    if not os.path.exists(SQL_PATH):
        raise FileNotFoundError(f"SQL não encontrado: {SQL_PATH}")

    sql_txt = load_sql_strip_go(SQL_PATH)
    if not sql_txt:
        raise RuntimeError(f"SQL vazio: {SQL_PATH}")

    conns = build_conns_from_env(postos=postos)
    if not conns:
        raise RuntimeError("Nenhum posto encontrado no .env. Precisa de DB_HOST_{P} e DB_BASE_{P}.")

    today = date.today()
    ini_cur, fim_cur, ym_cur = month_bounds(today)
    _, _, ym_prev = previous_month_bounds(today)

    forced = set(force_months or set())
    forced.update({ym_cur, ym_prev})

    start = EARLIEST_ALLOWED
    end_exclusive = fim_cur

    ENGINE_RETRIES = 4
    QUERY_RETRIES = 4

    # Carrega consolidado existente
    consol = safe_read_json(CONSOL_PATH)
    if not isinstance(consol, dict):
        consol = {}
    if "months" not in consol or not isinstance(consol["months"], dict):
        consol["months"] = {}

    consol.setdefault("name", CONSOL_FILENAME)

    for month_start in month_iter(start, end_exclusive):
        ini, fim, ym = month_bounds(month_start)

        month_node = consol["months"].get(ym)
        if not isinstance(month_node, dict):
            month_node = {}
        postos_node = month_node.get("postos")
        if not isinstance(postos_node, dict):
            postos_node = {}

        any_change = False

        for posto, odbc_str in conns.items():
            out_json = json_path(posto, ym)
            out_csv  = csv_path(posto, ym)

            force = (ym in forced)

            need_json = should_write(out_json, force=force)
            need_csv  = (do_csv and should_write(out_csv, force=force))

            # Se forçado ou não existe, roda DB e gera mensal + atualiza consolidado
            if need_json or need_csv:
                try:
                    engine = try_build_engine(odbc_str, retries=ENGINE_RETRIES)
                except Exception as e:
                    print(f"[{posto}] ERRO conexão {ym}: {e} (pulando)")
                    continue

                try:
                    df = run_query(engine, sql_txt, ini, fim, retries=QUERY_RETRIES)
                except Exception as e:
                    print(f"[{posto}] ERRO exec {ym}: {e} (pulando)")
                    continue

                try:
                    write_outputs(posto, ym, ini, fim, df, do_csv=do_csv)
                except Exception as e:
                    print(f"[{posto}] ERRO salvar mensal {ym}: {e} (pulando)")
                    continue

                try:
                    postos_node[posto] = build_consolidado_payload_for_month_posto(posto, ym, df)
                    any_change = True
                except Exception as e:
                    print(f"[{posto}] ERRO consolidar {ym}: {e} (pulando)")
                    continue

            else:
                # Mês “skip” (não bate no DB). Ainda assim, garantimos consolidado completo:
                # se o consolidado não tiver esse posto nesse mês, construímos a partir do JSON mensal já existente.
                if posto in postos_node:
                    continue

                if os.path.exists(out_json):
                    mj = safe_read_json(out_json)
                    linhas = (mj or {}).get("linhas")
                    if isinstance(linhas, list):
                        try:
                            df = pd.DataFrame(linhas)
                            postos_node[posto] = build_consolidado_payload_for_month_posto(posto, ym, df)
                            any_change = True
                        except Exception as e:
                            print(f"[{posto}] ERRO consolidar a partir do mensal {ym}: {e} (pulando)")

        if any_change:
            month_node["postos"] = postos_node
            month_node["totais_gerais"] = sum_totais_gerais(postos_node)
            consol["months"][ym] = month_node
            print(f"[CONSOL] atualizado {ym} total_postos_no_mes={len(postos_node)}")

    consol["generated_at"] = datetime.now(timezone.utc).astimezone().isoformat()
    safe_write_json(CONSOL_PATH, consol)

    print(f"[CONSOL] OK -> {os.path.relpath(CONSOL_PATH, BASE_DIR)}  meses={len(consol['months'])}")


def parse_args():
    p = argparse.ArgumentParser(
        description="Export consultas mensais (JSON; opcional CSV) incremental desde 2020-01, forçando mês atual e anterior, e gerando consolidado completo em json_consolidado."
    )
    p.add_argument("--postos", default="", help="Opcional: subset de postos. Ex: ANX. Se vazio, usa lista padrão.")
    p.add_argument("--csv", action="store_true", help="Também grava CSV (mesma lógica de skip/force).")
    return p.parse_args()


def main():
    args = parse_args()
    postos = [c for c in (args.postos or "") if c.isalpha()]
    run_incremental_all_postos(
        postos=postos if postos else None,
        do_csv=args.csv,
        force_months=None
    )


if __name__ == "__main__":
    main()
