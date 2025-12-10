# export_receita_despesa.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
# Função: executa SQLs de RECEITA/DESPESA por posto e mês, salva CSVs em /dados_fin_full,
#         e gera um JSON de resumo (valor_total + qtd) por mês/posto/indicador em /json_consolidado.

import os, re, sys, glob, json, argparse
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# Utilitários gerais
# =========================

def _set_mtime(path: str) -> None:
    """Ajusta atime/mtime do arquivo para 'agora' com timezone local."""
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))

def sanitize_nan(obj):
    """Converte NaN/NaT/inf em None de forma recursiva para JSON."""
    if isinstance(obj, dict):
        return {k: sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_nan(v) for v in obj]
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return obj

# =========================
# Constantes e Defaults
# =========================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SQL_DIR    = os.path.join(BASE_DIR, "sql_full")          # novos SQLs
DADOS_DIR  = os.path.join(BASE_DIR, "dados_fin_full")    # nova pasta de CSVs
JSON_DIR   = os.path.join(BASE_DIR, "json_consolidado")  # reaproveita pasta de JSON

EARLIEST_ALLOWED = date(2020, 1, 1)
POSTOS = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# preferência de coluna numérica para somatório (todos os SQLs expõem 'valorpago')
PREFER_NUM_COLS = ["valorpago", "valor_pago", "valor", "total"]

SRC_TEMPLATES_DIR    = os.path.join(BASE_DIR, "templates")
TARGET_TEMPLATES_DIR = os.getenv("TARGET_TEMPLATES_DIR", os.path.join(BASE_DIR, "public"))

# =========================
# Utilitários de data
# =========================

def ym_to_date(ym: str) -> date:
    return date(int(ym[0:4]), int(ym[5:7]), 1)

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
# Funções auxiliares diversas
# =========================

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def load_sql_strip_go(path):
    txt = open(path, "r", encoding="utf-8", errors="ignore").read()
    txt = re.sub(r"(?im)^\s*go\s*$", "", txt)
    return txt.strip()

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

def build_conns_from_env(postos=None):
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")
    conns = {}
    base_postos = postos or POSTOS
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
    """
    Lê todos os .sql da pasta sql_full.
    A 'key' é o nome do arquivo sem extensão, para evitar colisão entre os 6 indicadores.
    """
    files = sorted(glob.glob(os.path.join(SQL_DIR, "*.sql")))
    items = []
    for f in files:
        txt = load_sql_strip_go(f)
        if validate:
            validate_sql(txt, f)
        if not txt:
            continue
        key = os.path.splitext(os.path.basename(f))[0].lower()
        items.append({"path": f, "sql": txt, "key": key})
    return items

# =========================
# Execução de consultas
# =========================

def _ensure_nocount(sql_text: str) -> str:
    return sql_text if sql_text.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_text

def run_query(engine, sql_txt, ini, fim):
    body = _ensure_nocount(sql_txt)
    with engine.connect() as con:
        return pd.read_sql_query(text(body), con, params={"ini": ini, "fim": fim})

# =========================
# Persistência
# =========================

def target_csv_path(posto, ym, key):
    safe_key = key or "desconhecido"
    return os.path.join(DADOS_DIR, f"{posto}_{ym}_{safe_key}.csv")

def should_write_file(path, ym_current, ym_target, force=False, forced_months=None):
    if force:
        return True
    if forced_months and ym_target in forced_months:
        return True
    return not os.path.exists(path)

def sum_numeric(df, prefer_names=None):
    """Soma genérica usando coluna numérica preferencial (valorpago/valor/etc.)."""
    if df.empty:
        return 0.0
    prefer = prefer_names or PREFER_NUM_COLS
    cols_lower = {c.lower(): c for c in df.columns}
    picked = None
    for p in prefer:
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

# =========================
# JSON de resumo
# =========================

def build_json_por_indicador():
    """
    Para cada CSV {POSTO}_{YYYY-MM}_{key}.csv em dados_fin_full,
    gera json_consolidado/{key}.json com:
      - periodo (inicio/fim/n_meses)
      - postos
      - meses
      - dados[mes][posto] = {linhas, valor_total, qtd}
    Onde key é o nome do .sql sem extensão (fin_receita_tipo, etc).
    """
    ensure_dir(JSON_DIR)
    pattern = re.compile(
        r"^(?P<posto>[A-Z])_(?P<ym>\d{4}-\d{2})_(?P<key>[^.]+)\.csv$", re.I
    )

    # dados_por_key[key][ym][posto] = {...}
    dados_por_key = {}
    meses_por_key = {}
    postos_por_key = {}

    for fn in os.listdir(DADOS_DIR):
        m = pattern.match(fn)
        if not m:
            continue

        posto = m.group("posto").upper()
        ym    = m.group("ym")
        key   = m.group("key").lower()
        path  = os.path.join(DADOS_DIR, fn)

        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"[WARN] Falha ao ler {fn}: {e}")
            continue

        linhas = df.to_dict("records")
        valor_total = sum_numeric(df)
        qtd_total   = int(len(df))

        dados_por_key.setdefault(key, {}).setdefault(ym, {})[posto] = {
            "linhas": linhas,
            "valor_total": round(valor_total, 2),
            "qtd": qtd_total,
        }

        meses_por_key.setdefault(key, set()).add(ym)
        postos_por_key.setdefault(key, set()).add(posto)

    # grava um JSON por indicador (key)
    for key, por_mes in dados_por_key.items():
        meses_sorted  = sorted(meses_por_key.get(key, []))
        postos_sorted = sorted(postos_por_key.get(key, []))

        payload = sanitize_nan({
            "indicador": key,
            "periodo": {
                "inicio": meses_sorted[0] if meses_sorted else None,
                "fim":    meses_sorted[-1] if meses_sorted else None,
                "n_meses": len(meses_sorted),
            },
            "postos": postos_sorted,
            "meses": meses_sorted,
            "dados": {ym: por_mes[ym] for ym in meses_sorted},
        })

        out_path = os.path.join(JSON_DIR, f"{key}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        _set_mtime(out_path)
        print(f"[ETAPA 3/3] JSON indicador -> {os.path.relpath(out_path, BASE_DIR)}")

# =========================
# Templates estáticos (opcional, mesma regra do export_governanca)
# =========================

def copy_templates():
    src, dst = SRC_TEMPLATES_DIR, TARGET_TEMPLATES_DIR
    if not os.path.isdir(src):
        print(f"[INFO] SKIP cópia: origem inexistente -> {src}")
        return
    os.makedirs(dst, exist_ok=True)
    from shutil import copytree
    copytree(src, dst, dirs_exist_ok=True)
    print(f"[INFO] Templates copiados -> {dst}")

# =========================
# CLI
# =========================

def parse_args():
    p = argparse.ArgumentParser(
        description="Receita/Despesa: executa SQLs por posto, gera CSV por mês e JSON de resumo.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--from", dest="from_ym", default=None, help="Início YYYY-MM. Default=2024-01.")
    p.add_argument("--to", dest="to_ym", default=None, help="Fim exclusivo YYYY-MM. Default=início do mês corrente.")
    p.add_argument("--only-month", dest="only_month", default=None, help="Executa só um mês YYYY-MM.")
    p.add_argument("--postos", default="".join(POSTOS), help="Subset de postos. Ex: ANX.")
    p.add_argument("--force", action="store_true", help="Sobrescreve arquivos existentes.")
    p.add_argument("--outubro", action="store_true", help="Regera o mês imediatamente anterior ao atual.")
    p.add_argument("--no-validate", action="store_true", help="Não valida o conteúdo dos .sql.")
    p.add_argument("--dry-run", action="store_true", help="Executa sem gravar CSV/JSON.")
    p.add_argument("--copy-templates", action="store_true", help="Copia templates/ para TARGET_TEMPLATES_DIR.")
    return p.parse_args()

# =========================
# Orquestração
# =========================

def run():
    args = parse_args()
    print("========== EXPORT RECEITA/DESPESA ==========")
    print("[ETAPA 1/3] Setup")

    ensure_dir(DADOS_DIR)
    ensure_dir(JSON_DIR)

    ini_cur, fim_cur, ym_current = current_month_bounds()
    _, _, ym_prev = previous_month_bounds(date.today())
    forced_months = {ym_current, ym_prev}

    if args.outubro:
        forced_months.add(ym_prev)
        pat_prev = re.compile(rf"^[A-Z]_{ym_prev}_[^\.]+\.csv$", re.I)
        removed = 0
        for fn in os.listdir(DADOS_DIR):
            if pat_prev.match(fn):
                try:
                    os.remove(os.path.join(DADOS_DIR, fn))
                    removed += 1
                except Exception as e:
                    print(f"[WARN] Falha ao remover {fn}: {e}")
        print(f"[INFO] --outubro: {removed} CSV(s) de {ym_prev} removidos para reprocessamento.")

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

    sqls = collect_sql_files(validate=not args.no_validate)
    if not sqls:
        print(f"ERRO: Sem .sql em {SQL_DIR}.")
        sys.exit(1)

    print(f"- Período: {periodo_desc}")
    print(f"- Mês corrente: {ym_current}")
    print(f"- Postos: {list(conns.keys())}")
    print(f"- SQLs: {[os.path.basename(s['path']) for s in sqls]}")
    print(f"- Force={args.force}  DryRun={args.dry_run}  Validate={not args.no_validate}  Outubro={args.outubro}")

    print("\n[ETAPA 2/3] Execução por mês/posto/sql")
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

                if not should_write_file(out_path, ym_current, ym, force=args.force, forced_months=forced_months):
                    print(f"   [{posto}] SKIP {os.path.basename(out_path)} (existe)")
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
                    print(f"   [{posto}] OK linhas={len(df)}")
                except Exception as e:
                    print(f"   [{posto}] ERRO salvar: {e}")

    if args.dry_run:
        print("\n[ETAPA 3/3] JSON resumo -> SKIP (dry-run)")
        return

    print("\n[ETAPA 3/3] JSONs por indicador (receita/despesa)")
    build_json_por_indicador()


    if args.copy_templates:
        print("\n[EXTRA] Cópia de templates")
        copy_templates()

    print("\n✅ Finalizado export_receita_despesa.")

if __name__ == "__main__":
    run()
