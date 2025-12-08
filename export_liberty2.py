# export_liberty2.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
# Função:
# - Executa 3 SQLs CAMIM LIBERTY (consultas, mensalidade por forma, taxa de inscrição)
#   em TODOS os postos configurados (DB_HOST_A, DB_HOST_N, ...).
# - Para cada mês/posto, concatena os resultados.
# - Gera um único JSON: json_consolidado/liberty_dashboard.json
#   com dados brutos + KPIs globais (somando todos os postos).

import os, sys, json, argparse, re
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# Paths e constantes
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_LIBERTY_DIR = os.path.join(BASE_DIR, "sql_liberty_new")
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# mesmos postos do export_governanca.py
POSTOS = list("ANXYBRPCDGIMJ")

SQL_FILES = {
    "consultas": "consultasmedicaspagas.sql",
    "taxa_inscricao": "liberty_taxainscricao.sql",
    "mensalidade_forma": "mensalidade_por_forma.sql",
}

# =========================
# Helpers básicos
# =========================

def _set_mtime(path: str) -> None:
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sanitize_nan(obj):
    """Normaliza objetos para JSON: converte NaN/NaT em None e datas em ISO."""
    # dict
    if isinstance(obj, dict):
        return {k: sanitize_nan(v) for k, v in obj.items()}

    # lista
    if isinstance(obj, list):
        return [sanitize_nan(v) for v in obj]

    # pandas / numpy datetime → string ISO
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()

    # numpy datetime64 ou outros tipos numpy genéricos
    if isinstance(obj, np.generic):
        return sanitize_nan(obj.item())

    # fallback para coisas que têm .isoformat() (ex. datetime puro)
    if hasattr(obj, "isoformat") and callable(getattr(obj, "isoformat")):
        try:
            return obj.isoformat()
        except Exception:
            pass

    # NaN / NaT / inf etc → None
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass

    return obj


# =========================
# Datas / período
# =========================

def ym_to_date(ym: str) -> date:
    return date(int(ym[0:4]), int(ym[5:7]), 1)


def month_bounds(dt: date):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    return ini, nxt, f"{ini.year:04d}-{ini.month:02d}"


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
# Conexão – MESMO modelo do export_governanca
# =========================

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
    """
    Igual ao export_governanca.py:
    lê DB_HOST_A, DB_BASE_A, DB_USER_A, DB_PASSWORD_A, DB_PORT_A etc.
    """
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")

    base_postos = postos or POSTOS
    conns = {}
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
# Carregar SQL
# =========================

def load_sql_strip_go(path: str) -> str:
    txt = open(path, "r", encoding="utf-8", errors="ignore").read()
    txt = re.sub(r"(?im)^\s*go\s*$", "", txt)
    return txt.strip()


def load_sql_named(name_key: str) -> str:
    fname = SQL_FILES.get(name_key)
    if not fname:
        raise ValueError(f"Chave SQL desconhecida: {name_key}")
    path = os.path.join(SQL_LIBERTY_DIR, fname)
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL não encontrado: {path}")
    return load_sql_strip_go(path)


def _ensure_nocount(sql_text: str) -> str:
    sql_up = sql_text.lstrip().upper()
    if sql_up.startswith("SET NOCOUNT ON"):
        return sql_text
    return "SET NOCOUNT ON;\n" + sql_text


def run_query(engine, sql_txt: str, ini: date, fim: date) -> pd.DataFrame:
    body = _ensure_nocount(sql_txt)
    with engine.connect() as con:
        df = pd.read_sql_query(text(body), con, params={"ini": ini, "fim": fim})
    return df

# =========================
# Normalização de colunas
# =========================

def normalize_consultas_df(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    cols_lower = {c.lower(): c for c in df.columns}

    def has(col_name):
        return col_name.lower() in cols_lower

    def src(col_name):
        return cols_lower[col_name.lower()]

    if has("data prestação"):
        rename[src("data prestação")] = "data_prestacao"
    if has("data prestação "):
        rename[src("data prestação ")] = "data_prestacao"
    if has("data prestaçao"):
        rename[src("data prestaçao")] = "data_prestacao"
    if has("valor pago"):
        rename[src("valor pago")] = "valor_pago"
    if has("especialidade"):
        rename[src("especialidade")] = "especialidade"
    if has("classe"):
        rename[src("classe")] = "classe"
    if has("plano"):
        rename[src("plano")] = "plano"
    if has("tipo"):
        rename[src("tipo")] = "tipo"
    if has("matricula"):
        rename[src("matricula")] = "matricula"
    if has("nome"):
        rename[src("nome")] = "nome"
    if has("dataadmissao"):
        rename[src("dataadmissao")] = "data_admissao"
    if has("data admissao"):
        rename[src("data admissao")] = "data_admissao"
    if has("datacancelamento"):
        rename[src("datacancelamento")] = "data_cancelamento"
    if has("data cancelamento"):
        rename[src("data cancelamento")] = "data_cancelamento"

    if rename:
        df = df.rename(columns=rename)

    for col in ["data_prestacao", "data_admissao", "data_cancelamento"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "valor_pago" in df.columns:
        df["valor_pago"] = pd.to_numeric(df["valor_pago"], errors="coerce")

    return df


def normalize_taxa_df(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    cols_lower = {c.lower(): c for c in df.columns}

    def has(col_name):
        return col_name.lower() in cols_lower

    def src(col_name):
        return cols_lower[col_name.lower()]

    if has("valor pago"):
        rename[src("valor pago")] = "valor_pago"
    if has("data de pagamento"):
        rename[src("data de pagamento")] = "data_pagamento"
    if has("data pagamento"):
        rename[src("data pagamento")] = "data_pagamento"

    if rename:
        df = df.rename(columns=rename)

    if "data_pagamento" in df.columns:
        df["data_pagamento"] = pd.to_datetime(df["data_pagamento"], errors="coerce")
    if "valor_pago" in df.columns:
        df["valor_pago"] = pd.to_numeric(df["valor_pago"], errors="coerce")

    return df


def normalize_mensalidade_forma_df(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    cols_lower = {c.lower(): c for c in df.columns}

    def has(col_name):
        return col_name.lower() in cols_lower

    def src(col_name):
        return cols_lower[col_name.lower()]

    if has("qtd_menslidades"):
        rename[src("qtd_menslidades")] = "qtd_mensalidades"
    elif has("qtd_mensalidades"):
        rename[src("qtd_mensalidades")] = "qtd_mensalidades"
    if has("forma"):
        rename[src("forma")] = "forma"
    if has("nome do servidor"):
        rename[src("nome do servidor")] = "nome_servidor"

    if rename:
        df = df.rename(columns=rename)

    if "qtd_mensalidades" in df.columns:
        df["qtd_mensalidades"] = (
            pd.to_numeric(df["qtd_mensalidades"], errors="coerce").fillna(0).astype(int)
        )

    return df

# =========================
# KPIs
# =========================

def kpis_consultas(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "qtd_consultas": 0,
            "valor_total_consultas": 0.0,
            "valor_medio_consulta": None,
            "especialidade_mais_usada": None,
            "especialidade_por_qtd": {},
            "tempo_medio_primeira_consulta_dias": None,
            "tempo_medio_entre_consultas_dias": None,
            "tempo_medio_adesao_cancelamento_dias": None,
        }

    kpis = {}
    kpis["qtd_consultas"] = int(len(df))

    if "valor_pago" in df.columns:
        total = float(df["valor_pago"].fillna(0).sum())
        kpis["valor_total_consultas"] = round(total, 2)
        kpis["valor_medio_consulta"] = round(
            float(df["valor_pago"].mean()), 2
        ) if len(df["valor_pago"].dropna()) > 0 else None
    else:
        kpis["valor_total_consultas"] = 0.0
        kpis["valor_medio_consulta"] = None

    if "especialidade" in df.columns:
        cont = df["especialidade"].fillna("SEM ESPECIALIDADE").value_counts()
        if not cont.empty:
            kpis["especialidade_mais_usada"] = cont.index[0]
            kpis["especialidade_por_qtd"] = {
                str(idx): int(v) for idx, v in cont.to_dict().items()
            }
        else:
            kpis["especialidade_mais_usada"] = None
            kpis["especialidade_por_qtd"] = {}
    else:
        kpis["especialidade_mais_usada"] = None
        kpis["especialidade_por_qtd"] = {}

    tempo_primeira = None
    tempo_entre = None
    tempo_adesao_cancel = None

    if "matricula" in df.columns and "data_prestacao" in df.columns:
        df_valid = df.dropna(subset=["matricula", "data_prestacao"]).copy()

        if "data_admissao" in df_valid.columns:
            grp = df_valid.groupby("matricula", as_index=False)

            def _primeira(g):
                adm = g["data_admissao"].iloc[0]
                if pd.isna(adm):
                    return np.nan
                return (g["data_prestacao"].min() - adm).days

            primeira = grp.apply(_primeira)
            primeira = primeira.replace([np.inf, -np.inf], np.nan).dropna()
            if len(primeira) > 0:
                tempo_primeira = float(primeira.mean())

        def _media_intervalos(g):
            if len(g) < 2:
                return np.nan
            datas = g.sort_values().values
            diffs = np.diff(datas).astype("timedelta64[D]").astype(int)
            return np.mean(diffs) if len(diffs) > 0 else np.nan

        grp2 = df_valid.groupby("matricula")["data_prestacao"]
        inter = grp2.apply(_media_intervalos)
        inter = inter.replace([np.inf, -np.inf], np.nan).dropna()
        if len(inter) > 0:
            tempo_entre = float(inter.mean())

    if "data_admissao" in df.columns and "data_cancelamento" in df.columns:
        df_canc = df.dropna(subset=["data_admissao", "data_cancelamento"]).copy()
        if "matricula" in df_canc.columns:
            grp = df_canc.groupby("matricula", as_index=False)

            def _adesao_cancel(g):
                return (g["data_cancelamento"].iloc[0] - g["data_admissao"].iloc[0]).days

            diffs = grp.apply(_adesao_cancel)
            diffs = diffs.replace([np.inf, -np.inf], np.nan).dropna()
            if len(diffs) > 0:
                tempo_adesao_cancel = float(diffs.mean())

    kpis["tempo_medio_primeira_consulta_dias"] = round(tempo_primeira, 2) if tempo_primeira is not None else None
    kpis["tempo_medio_entre_consultas_dias"] = round(tempo_entre, 2) if tempo_entre is not None else None
    kpis["tempo_medio_adesao_cancelamento_dias"] = (
        round(tempo_adesao_cancel, 2) if tempo_adesao_cancel is not None else None
    )

    return kpis


def kpis_taxa_inscricao(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "qtd_inscricoes": 0,
            "valor_total_inscricao": 0.0,
            "valor_medio_inscricao": None,
            "inscricoes_por_corretor": {},
        }

    out = {}
    out["qtd_inscricoes"] = int(len(df))
    if "valor_pago" in df.columns:
        total = float(df["valor_pago"].fillna(0).sum())
        out["valor_total_inscricao"] = round(total, 2)
        out["valor_medio_inscricao"] = round(
            float(df["valor_pago"].mean()), 2
        ) if len(df["valor_pago"].dropna()) > 0 else None
    else:
        out["valor_total_inscricao"] = 0.0
        out["valor_medio_inscricao"] = None

    if "corretor" in df.columns:
        cont = df["corretor"].fillna("SEM CORRETOR").value_counts()
        out["inscricoes_por_corretor"] = {
            str(k): int(v) for k, v in cont.to_dict().items()
        }
    else:
        out["inscricoes_por_corretor"] = {}

    return out


def mensalidades_mensais(df_all: pd.DataFrame) -> dict:
    if df_all.empty:
        return {"por_mes": {}, "total_por_forma": {}, "total_geral_mensalidades": 0}

    out = {}
    for (mes, forma), sub in df_all.groupby(["mes", "forma"]):
        qtd = int(sub["qtd_mensalidades"].sum())
        out.setdefault(mes, {})
        out[mes][str(forma)] = out[mes].get(str(forma), 0) + qtd

    total_por_forma = {}
    for mes, formas in out.items():
        for f, q in formas.items():
            total_por_forma[f] = total_por_forma.get(f, 0) + q

    total_geral = int(sum(total_por_forma.values() or [0]))

    return {
        "por_mes": out,
        "total_por_forma": total_por_forma,
        "total_geral_mensalidades": total_geral,
    }

# =========================
# CLI / Orquestração
# =========================

def parse_args():
    p = argparse.ArgumentParser(
        description="Exporta dados CAMIM LIBERTY (consultas, mensalidades por forma, taxa inscrição) a partir dos mesmos bancos do governança.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--from", dest="from_ym", default=None,
                   help="Mês inicial YYYY-MM (default: ano corrente em janeiro).")
    p.add_argument("--to", dest="to_ym", default=None,
                   help="Mês final exclusivo YYYY-MM (default: primeiro mês após o atual).")
    p.add_argument("--only-month", dest="only_month", default=None,
                   help="Se informado, processa apenas este mês YYYY-MM.")
    p.add_argument("--postos", default="".join(POSTOS),
                   help="Subset de postos. Ex: ANX. Default = todos.")
    p.add_argument("--dry-run", action="store_true",
                   help="Executa as queries e mostra contagens, sem gravar JSON.")
    return p.parse_args()


def run():
    args = parse_args()
    ensure_dir(JSON_DIR)

    today = date.today()

    if args.only_month:
        start = ym_to_date(args.only_month)
        end_exclusive = month_bounds(start)[1]
        periodo_desc = args.only_month
    else:
        if args.from_ym:
            start = ym_to_date(args.from_ym)
        else:
            start = date(today.year, 1, 1)
        if args.to_ym:
            end_exclusive = ym_to_date(args.to_ym)
        else:
            _, end_exclusive, _ = month_bounds(today)
        periodo_desc = f"{start} .. < {end_exclusive}"

    postos = [c for c in args.postos if c.isalpha()]
    conns = build_conns_from_env(postos)
    if not conns:
        print("ERRO: .env sem DB_HOST_*/DB_BASE_* configurados para os postos informados.")
        sys.exit(1)

    print("========== EXPORT LIBERTY 2 ==========")
    print(f"- Período de competência (meses): {periodo_desc}")
    print(f"- SQL dir: {SQL_LIBERTY_DIR}")
    print(f"- JSON dir: {JSON_DIR}")
    print(f"- Postos: {list(conns.keys())}")

    try:
        sql_consultas = load_sql_named("consultas")
        sql_taxa = load_sql_named("taxa_inscricao")
        sql_mensalidade_forma = load_sql_named("mensalidade_forma")
    except Exception as e:
        print(f"ERRO ao carregar SQLs: {e}")
        sys.exit(1)

    all_consultas = []
    all_taxa = []
    all_mensalidade_forma = []

    for dt_mes in month_iter(start, end_exclusive):
        ini, fim, ym = month_bounds(dt_mes)
        print(f"\n-- Mês {ym} --------------------")

        for posto, odbc_str in conns.items():
            print(f"   [{posto}] conectando...")
            try:
                engine = make_engine(odbc_str)
            except Exception as e:
                print(f"   [{posto}] ERRO engine: {e}")
                continue

            # CONSULTAS
            try:
                df_c = run_query(engine, sql_consultas, ini, fim)
                df_c = normalize_consultas_df(df_c)
                df_c["mes"] = ym
                df_c["posto"] = posto
                all_consultas.append(df_c)
                print(f"      Consultas: {len(df_c)} linhas")
            except Exception as e:
                print(f"      ERRO consultas ({ym}, posto {posto}): {e}")

            # TAXA INSCRIÇÃO
            try:
                df_t = run_query(engine, sql_taxa, ini, fim)
                df_t = normalize_taxa_df(df_t)
                df_t["mes"] = ym
                df_t["posto"] = posto
                all_taxa.append(df_t)
                print(f"      Taxa inscrição: {len(df_t)} linhas")
            except Exception as e:
                print(f"      ERRO taxa inscrição ({ym}, posto {posto}): {e}")

            # MENSALIDADE POR FORMA
            try:
                df_m = run_query(engine, sql_mensalidade_forma, ini, fim)
                df_m = normalize_mensalidade_forma_df(df_m)
                if not df_m.empty:
                    df_m["mes"] = ym
                    df_m["posto"] = posto
                all_mensalidade_forma.append(df_m)
                print(f"      Mensalidade/forma: {len(df_m)} linhas")
            except Exception as e:
                print(f"      ERRO mensalidade/forma ({ym}, posto {posto}): {e}")

    df_consultas = pd.concat(all_consultas, ignore_index=True) if all_consultas else pd.DataFrame()
    df_taxa = pd.concat(all_taxa, ignore_index=True) if all_taxa else pd.DataFrame()
    df_mensalidade_forma = pd.concat(all_mensalidade_forma, ignore_index=True) if all_mensalidade_forma else pd.DataFrame()

    print("\n========== RESUMO ==========")
    print(f"- Total consultas (todos os postos): {len(df_consultas)}")
    print(f"- Total taxa inscrição (todos os postos): {len(df_taxa)}")
    print(f"- Total registros mensalidade/forma: {len(df_mensalidade_forma)}")

    k_consultas = kpis_consultas(df_consultas)
    k_taxa = kpis_taxa_inscricao(df_taxa)
    k_mensalidade = mensalidades_mensais(df_mensalidade_forma)

    print("\nKPIs-chave (consultas):")
    print(f"  - qtd_consultas: {k_consultas['qtd_consultas']}")
    print(f"  - valor_total_consultas: {k_consultas['valor_total_consultas']}")
    print(f"  - valor_medio_consulta: {k_consultas['valor_medio_consulta']}")
    print(f"  - especialidade_mais_usada: {k_consultas['especialidade_mais_usada']}")

    print("\nKPIs-chave (taxa inscrição):")
    print(f"  - qtd_inscricoes: {k_taxa['qtd_inscricoes']}")
    print(f"  - valor_total_inscricao: {k_taxa['valor_total_inscricao']}")

    print("\nKPIs-chave (mensalidade/forma):")
    print(f"  - total_geral_mensalidades: {k_mensalidade['total_geral_mensalidades']}")

    if args.dry_run:
        print("\n[DRY-RUN] Nenhum JSON será gravado.")
        return

    payload = {
        "periodo": {
            "inicio_competencia": start.isoformat(),
            "fim_competencia_exclusivo": end_exclusive.isoformat(),
        },
        "postos": list(conns.keys()),
        "consultas": {
            "kpis": k_consultas,
            "rows": df_consultas.to_dict("records"),
        },
        "taxa_inscricao": {
            "kpis": k_taxa,
            "rows": df_taxa.to_dict("records"),
        },
        "mensalidades_por_forma": {
            "kpis": k_mensalidade,
            "rows": df_mensalidade_forma.to_dict("records"),
        },
    }

    payload = sanitize_nan(payload)

    out_path = os.path.join(JSON_DIR, "liberty_dashboard.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _set_mtime(out_path)

    print(f"\n✅ JSON gerado -> {os.path.relpath(out_path, BASE_DIR)}")


if __name__ == "__main__":
    run()
