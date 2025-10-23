# update.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
import os, re, sys, glob, json, argparse, shutil
from datetime import date
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# Constantes e Defaults
# =========================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SQL_DIR    = os.path.join(BASE_DIR, "sql")
DADOS_DIR  = os.path.join(BASE_DIR, "dados")
JSON_DIR   = os.path.join(BASE_DIR, "json_consolidado")
SRC_TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
TARGET_TEMPLATES_DIR = r"C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T\templates"

EARLIEST_ALLOWED = date(2024, 1, 1)      # backfill padrão
POSTOS = list("ANXYBRPCDGIMJ")           # ENV filtrará
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

KEY_PATTERNS = {
    "mensalidade": re.compile(r"(mensal|mensalid|receita|fin_?receita)", re.I),
    "medico":      re.compile(r"(medic|custo_?med|assist|sinistral)", re.I),
    "alimentacao": re.compile(r"(alimenta|refeic|cozinha|posto)", re.I),
}

# =========================
# Utilitários de data
# =========================
def ym_to_date(ym: str) -> date:
    return date(int(ym[0:4]), int(ym[5:7]), 1)

def month_bounds(dt: date):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    return ini, nxt, f"{ini.year:04d}-{ini.month:02d}"

def current_month_bounds():
    return month_bounds(date.today())

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
# Utilitários gerais
# =========================
def ensure_dir(path): os.makedirs(path, exist_ok=True)

def load_sql_strip_go(path):
    txt = open(path, "r", encoding="utf-8", errors="ignore").read()
    txt = re.sub(r"(?im)^\s*go\s*$", "", txt)  # remove linhas GO
    return txt.strip()

def infer_key_from_filename(fn):
    name = os.path.basename(fn)
    for key, pat in KEY_PATTERNS.items():
        if pat.search(name):
            return key
    return None

def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

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

def make_engine(odbc_conn_str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        pool_pre_ping=True,
        future=True,
    )

def build_conns_from_env():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "5")
    conns = {}
    for p in POSTOS:
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
# Sanitização de SQL
# =========================
def validate_sql(sql_txt, path):
    if not sql_txt.strip():
        raise ValueError(f"Arquivo SQL vazio: {os.path.basename(path)}")
    proibidos = [r"\bsql_txt\s*=", r"\brun_query\s*\(", r"\bpd\.read_sql", r"\bengine\.connect\("]
    for pat in proibidos:
        if re.search(pat, sql_txt):
            raise ValueError(f"Arquivo SQL contém código Python: {os.path.basename(path)}")

def collect_sql_files(validate=True):
    files = sorted(glob.glob(os.path.join(SQL_DIR, "*.sql")))
    items = []
    for f in files:
        key = infer_key_from_filename(f)
        txt = load_sql_strip_go(f)
        if validate:
            validate_sql(txt, f)
        if not txt:
            continue
        items.append({"path": f, "sql": txt, "key": key})
    return items

# =========================
# Execução de consultas
# =========================
def _ensure_nocount(sql_text: str) -> str:
    return sql_text if sql_text.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_text

def _has_named_params(sql_text: str) -> bool:
    return (":ini" in sql_text) and (":fim" in sql_text)

def _has_positional_q(sql_text: str) -> bool:
    return ("?" in sql_text) and not _has_named_params(sql_text)

def run_query(engine, sql_text, ini, fim):
    body = _ensure_nocount(sql_text)
    if _has_named_params(body):
        with engine.connect() as con:
            stmt = text(body)
            return pd.read_sql_query(stmt, con, params={"ini": ini, "fim": fim})
    elif _has_positional_q(body):
        conn = engine.raw_connection()  # compatível com '?'
        try:
            return pd.read_sql_query(body, conn, params=[ini, fim])
        finally:
            conn.close()
    else:
        with engine.connect() as con:
            stmt = text(body)
            return pd.read_sql_query(stmt, con)

# =========================
# Persistência e Consolidação
# =========================
def target_csv_path(posto, ym, key):
    safe_key = key or "desconhecido"
    return os.path.join(DADOS_DIR, f"{posto}_{ym}_{safe_key}.csv")

def should_write_file(path, ym_current, ym_target, force=False):
    if force:
        return True
    if ym_target == ym_current:
        return True
    return not os.path.exists(path)

def sum_numeric(df, prefer_names):
    if df.empty:
        return 0.0
    cols_lower = {c.lower(): c for c in df.columns}
    picked = None
    for p in prefer_names:
        if p.lower() in cols_lower:
            picked = cols_lower[p.lower()]
            break
    if picked is None:
        for c in df.columns:
            if pd.api.types.is_numeric_dtype(df[c]):
                picked = c
                break
    if not picked:
        return 0.0
    return float(pd.to_numeric(df[picked], errors="coerce").fillna(0).sum())

def build_monthly_json():
    ensure_dir(JSON_DIR)
    pattern = re.compile(r"^(?P<posto>[A-Z])_(?P<ym>\d{4}-\d{2})_(?P<key>[a-z_]+)\.csv$", re.I)
    prefer_by_key = {
        "mensalidade": ["ValorPago", "Mensalidades", "valor", "total"],
        "medico":      ["medicos", "valor", "total"],
        "alimentacao": ["alimentacao", "valor", "total"],
        "desconhecido":["valor", "total"]
    }
    totals = {}  # {ym: {key: total}}
    for fn in os.listdir(DADOS_DIR):
        m = pattern.match(fn)
        if not m:
            continue
        ym  = m.group("ym")
        key = m.group("key").lower()
        path = os.path.join(DADOS_DIR, fn)
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        pref = prefer_by_key.get(key, ["valor", "total"])
        val  = sum_numeric(df, pref)
        if ym not in totals:
            totals[ym] = {}
        totals[ym][key] = round(totals[ym].get(key, 0.0) + val, 2)
    out_path = os.path.join(JSON_DIR, "consolidado_mensal.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(totals, f, ensure_ascii=False, indent=2)
    print(f"[ETAPA 4/5] JSON consolidado gerado -> {os.path.relpath(out_path, BASE_DIR)}")

def copy_templates():
    src, dst = SRC_TEMPLATES_DIR, TARGET_TEMPLATES_DIR
    if not os.path.isdir(src):
        print(f"[ETAPA 5/5] SKIP cópia: origem inexistente -> {src}")
        return
    os.makedirs(dst, exist_ok=True)
    shutil.copytree(src, dst, dirs_exist_ok=True)
    print(f"[ETAPA 5/5] Templates copiados -> {dst}")

# =========================
# CLI
# =========================
def parse_args():
    p = argparse.ArgumentParser(
        description="ETL mensal: executa SQLs por posto, gera CSVs por mês, consolida JSON e copia templates.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--from", dest="from_ym", default=None,
                   help="Mês inicial YYYY-MM. Default=2024-01.")
    p.add_argument("--to", dest="to_ym", default=None,
                   help="Mês final exclusivo YYYY-MM. Default=início do mês corrente.")
    p.add_argument("--force", action="store_true",
                   help="Reprocessa todos os meses e sobrescreve arquivos existentes.")
    p.add_argument("--no-validate", action="store_true",
                   help="Não valida o conteúdo dos .sql.")
    p.add_argument("--only-month", dest="only_month", default=None,
                   help="Executa somente um mês específico YYYY-MM.")
    p.add_argument("--dry-run", action="store_true",
                   help="Simula execução sem gravar arquivos nem copiar templates.")
    return p.parse_args()

# =========================
# Orquestração
# =========================
def run():
    args = parse_args()
    print("========== PIPELINE RELATÓRIO ==========")
    print("[ETAPA 1/5] Setup e parâmetros")

    ensure_dir(DADOS_DIR)
    ensure_dir(JSON_DIR)

    ini_cur, fim_cur, ym_current = current_month_bounds()

    if args.only_month:
        start = ym_to_date(args.only_month)
        end_exclusive = month_bounds(start)[1]
        periodo_desc = f"{args.only_month}"
    else:
        start = ym_to_date(args.from_ym) if args.from_ym else EARLIEST_ALLOWED
        end_exclusive = ym_to_date(args.to_ym) if args.to_ym else fim_cur
        periodo_desc = f"{start} .. < {end_exclusive}"

    print(f"- Backfill: {periodo_desc}")
    print(f"- Mês corrente: {ym_current}")
    print(f"- Forçar reprocessamento: {args.force}")
    print(f"- Dry-run: {args.dry_run}")
    print(f"- Validar .sql: {not args.no_validate}")

    conns = build_conns_from_env()
    if not conns:
        print("ERRO: .env sem DB_HOST_*/DB_BASE_* configurados.")
        sys.exit(1)

    sqls = collect_sql_files(validate=not args.no_validate)
    if not sqls:
        print(f"ERRO: Sem .sql em {SQL_DIR}.")
        sys.exit(1)

    print(f"- Postos: {list(conns.keys())}")
    print(f"- SQLs: {[os.path.basename(s['path']) for s in sqls]}")

    print("\n[ETAPA 2/5] Execução por mês/posto/sql")
    mensalidade_por_posto = {}
    medico_por_posto      = {}
    aliment_por_posto     = {}

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

            for entry in sqls:
                key = entry["key"] or "desconhecido"
                sql_txt = entry["sql"]
                out_path = target_csv_path(posto, ym, key)

                if not should_write_file(out_path, ym_current, ym, force=args.force):
                    print(f"   [{posto}] SKIP {os.path.basename(out_path)} (já existe)")
                    continue

                print(f"   [{posto}] RUN {os.path.basename(entry['path'])} -> {os.path.basename(out_path)}")
                if args.dry_run:
                    continue

                try:
                    df = run_query(engine, sql_txt, ini, fim)
                except Exception as e:
                    print(f"   [{posto}] ERRO exec: {e}")
                    continue

                try:
                    df.to_csv(out_path, index=False, encoding="utf-8-sig")
                    action = "sobrescrito" if ym == ym_current or args.force else "criado"
                    print(f"   [{posto}] OK {action}  linhas={len(df)}")
                except Exception as e:
                    print(f"   [{posto}] ERRO salvar: {e}")

                if ym == ym_current and not df.empty:
                    if key == "mensalidade":
                        mensalidade_por_posto[posto] = mensalidade_por_posto.get(posto, 0.0) + \
                            sum_numeric(df, ["ValorPago", "Mensalidades", "valor", "total"])
                    elif key == "medico":
                        medico_por_posto[posto] = medico_por_posto.get(posto, 0.0) + \
                            sum_numeric(df, ["medicos", "valor", "total"])
                    elif key == "alimentacao":
                        aliment_por_posto[posto] = aliment_por_posto.get(posto, 0.0) + \
                            sum_numeric(df, ["alimentacao", "valor", "total"])

    if args.dry_run:
        print("\n[ETAPA 3/5] Consolidados CSV -> SKIP (dry-run)")
        print("[ETAPA 4/5] JSON consolidado -> SKIP (dry-run)")
        print("[ETAPA 5/5] Cópia de templates -> SKIP (dry-run)")
        print("\nFinalizado (dry-run).")
        return

    print("\n[ETAPA 3/5] Consolidados CSV do mês corrente")
    def safe_get(d, k): return float(d.get(k, 0.0) or 0.0)

    rows_med, rows_al = [], []
    for posto in sorted(conns.keys()):
        v_med  = safe_get(medico_por_posto, posto)
        v_mens = safe_get(mensalidade_por_posto, posto)
        v_al   = safe_get(aliment_por_posto, posto)
        perc_m = (v_med / v_mens * 100.0) if v_mens > 0 else None
        perc_a = (v_al  / v_mens * 100.0) if v_mens > 0 else None
        rows_med.append({"mes": ym_current, "posto": posto,
                         "valor_medico": round(v_med, 2),
                         "mensalidade": round(v_mens, 2),
                         "perc_medico_sobre_mensalidade": round(perc_m, 4) if perc_m is not None else None})
        rows_al.append({"mes": ym_current, "posto": posto,
                        "valor_alimentacao": round(v_al, 2),
                        "mensalidade": round(v_mens, 2),
                        "perc_alimentacao_sobre_mensalidade": round(perc_a, 4) if perc_a is not None else None})

    ensure_dir(DADOS_DIR)
    pd.DataFrame(rows_med).to_csv(os.path.join(DADOS_DIR, "consolidado_medico.csv"),
                                  index=False, encoding="utf-8-sig")
    pd.DataFrame(rows_al).to_csv(os.path.join(DADOS_DIR, "consolidado_alimentacao.csv"),
                                 index=False, encoding="utf-8-sig")
    print(f"- consolidado_medico.csv e consolidado_alimentacao.csv atualizados em {os.path.relpath(DADOS_DIR, BASE_DIR)}")

    print("\n[ETAPA 4/5] JSON consolidado mensal")
    build_monthly_json()

    print("\n[ETAPA 5/5] Cópia de templates")
    copy_templates()

    print("\n✅ Finalizado.")

# =========================
# Main
# =========================
if __name__ == "__main__":
    run()
