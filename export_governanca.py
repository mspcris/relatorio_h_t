# export_governanca.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
# Função: executa SQL por posto e mês, salva CSVs em /dados,
#         gera JSON consolidado global e por posto em /json_consolidado.
#         Adiciona mensalidade_qtd, ticket_medio e performance aos JSONs.
#         NOVO: KPIs mensais e de período (rodapé) com médias geométricas, mix e estatísticas.

import os, re, sys, glob, json, argparse, shutil, math
from datetime import date
from urllib.parse import quote_plus

import numpy as pd
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# Constantes e Defaults
# =========================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SQL_DIR    = os.path.join(BASE_DIR, "SQL")
DADOS_DIR  = os.path.join(BASE_DIR, "dados")
JSON_DIR   = os.path.join(BASE_DIR, "json_consolidado")
SRC_TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")  # opcional: front estático
TARGET_TEMPLATES_DIR = os.getenv("TARGET_TEMPLATES_DIR", os.path.join(BASE_DIR, "public"))

EARLIEST_ALLOWED = date(2020, 1, 1)
POSTOS = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

KEY_PATTERNS = {
    "mensalidade": re.compile(r"(mensal|mensalid|receita|fin_?receita)", re.I),
    "medico":      re.compile(r"(medic|custo_?med|assist|sinistral)", re.I),
    "alimentacao": re.compile(r"(alimenta|refeic|cozinha|posto)", re.I),
}

PREFER_NOMES = {
    "mensalidade": ["ValorPago", "Mensalidades", "valor", "total"],
    "medico":      ["medicos", "valor", "total"],
    "alimentacao": ["alimentacao", "valor", "total"],
    "desconhecido":["valor", "total"]
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
    txt = re.sub(r"(?im)^\s*go\s*$", "", txt)
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
        f"Connection Timeout={timeout or '20'};"
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
        conn = engine.raw_connection()
        try:
            return pd.read_sql_query(body, conn, params=[ini, fim])
        finally:
            conn.close()
    else:
        with engine.connect() as con:
            stmt = text(body)
            return pd.read_sql_query(stmt, con)

# =========================
# Funções de cálculo (KPIs)
# =========================
def safe_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def geometric_mean(values):
    vals = [safe_float(v) for v in values if v is not None and safe_float(v) > 0]
    if not vals:
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))

def gm_rate_from_percents(perc_list):
    """MG de fatores 1+p/100 -> retorna taxa média em %."""
    facs = []
    for p in perc_list:
        try:
            facs.append(1.0 + float(p)/100.0)
        except Exception:
            pass
    facs = [f for f in facs if f > 0]
    if not facs:
        return None
    gm = math.exp(sum(math.log(f) for f in facs) / len(facs))
    return (gm - 1.0) * 100.0

def accumulated_from_percents(perc_list):
    fac = 1.0
    for p in perc_list:
        try:
            fac *= (1.0 + float(p)/100.0)
        except Exception:
            continue
    return (fac - 1.0) * 100.0

def pct_change(curr, prev):
    if prev is None or prev == 0:
        return None
    return (safe_float(curr)/safe_float(prev) - 1.0) * 100.0

def log_return(curr, prev):
    if prev is None or prev <= 0 or curr is None or curr <= 0:
        return None
    return math.log(curr/prev)

def mix_decomposition(rt, rt_1, qt, qt_1, tk, tk_1):
    """ΔR ≈ ΔQ*Tk_avg + ΔTk*Q_avg + interação. Usa médias simples entre t e t-1."""
    if None in (rt, rt_1, qt, qt_1, tk, tk_1):
        return None, None, None
    dQ = qt - qt_1
    dT = tk - tk_1
    Qavg = (qt + qt_1)/2.0
    Tavg = (tk + tk_1)/2.0
    delta_preco = dT * Qavg
    delta_volume = dQ * Tavg
    interacao = (rt - rt_1) - (delta_preco + delta_volume)
    return delta_preco, delta_volume, interacao

def volatility(series):
    vals = [v for v in series if v is not None]
    if len(vals) < 2:
        return None
    s = pd.Series(vals, dtype="float64")
    return float(s.std(ddof=1))

def hit_rate_growth(series):
    cnt = 0
    tot = 0
    prev = None
    for v in series:
        if prev is not None and v is not None:
            tot += 1
            if v > prev:
                cnt += 1
        prev = v
    if tot == 0:
        return None
    return 100.0 * cnt / tot

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

def count_rows(df) -> int:
    return int(len(df))

def _sorted_ym(keys):
    return sorted(keys)

def build_monthly_json():
    """Agregado global por mês. Adiciona campos derivados, deltas, mix e KPIs de período (_kpis_periodo)."""
    ensure_dir(JSON_DIR)
    pattern = re.compile(r"^(?P<posto>[A-Z])_(?P<ym>\d{4}-\d{2})_(?P<key>[a-z_]+)\.csv$", re.I)

    totals = {}           # {ym: {key: total}}
    mensalidade_qtd = {}  # {ym: int}

    # varre CSVs mensais por posto e soma para o global
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

        val = sum_numeric(df, PREFER_NOMES.get(key, ["valor", "total"]))
        totals.setdefault(ym, {})
        totals[ym][key] = round(totals[ym].get(key, 0.0) + val, 2)

        if key == "mensalidade":
            mensalidade_qtd[ym] = mensalidade_qtd.get(ym, 0) + count_rows(df)

    # injeta quantidade
    for ym, qtd in mensalidade_qtd.items():
        totals.setdefault(ym, {})
        totals[ym]["mensalidade_qtd"] = int(qtd)

    months = _sorted_ym(totals.keys())

    # campos mensais derivados + deltas e mix
    for i, ym in enumerate(months):
        mens = float(totals[ym].get("mensalidade", 0) or 0)
        qtd  = int(totals[ym].get("mensalidade_qtd", 0) or 0)
        ticket = (mens / qtd) if qtd > 0 else None
        totals[ym]["ticket_medio"] = round(ticket, 2) if ticket is not None else None

        # performance de QTD mês a mês (%)
        if i > 0:
            prev_ym = months[i-1]
            prev_qtd = int(totals[prev_ym].get("mensalidade_qtd", 0) or 0)
            perf = ((qtd - prev_qtd) / prev_qtd * 100.0) if prev_qtd > 0 else None
        else:
            perf = None
        totals[ym]["performance"] = round(perf, 2) if perf is not None else None

        # deltas MoM e logret da receita
        if i > 0:
            prev_mens   = float(totals[prev_ym].get("mensalidade", 0) or 0)
            prev_ticket = totals[prev_ym].get("ticket_medio", None)
            prev_qtd_v  = int(totals[prev_ym].get("mensalidade_qtd", 0) or 0)

            totals[ym]["receita_mom_pct"] = round(pct_change(mens, prev_mens), 4) if prev_mens else None
            totals[ym]["qtd_mom_pct"]     = round(pct_change(qtd,  prev_qtd_v), 4) if prev_qtd_v else None
            totals[ym]["ticket_mom_pct"]  = round(pct_change(ticket, prev_ticket), 4) if prev_ticket else None

            lr = log_return(mens, prev_mens)
            totals[ym]["receita_logret"]  = round(lr, 6) if lr is not None else None

            dp, dv, inter = mix_decomposition(mens, prev_mens, qtd, prev_qtd_v, ticket, prev_ticket)
            totals[ym]["mix_delta_receita_preco"]      = round(dp, 2) if dp is not None else None
            totals[ym]["mix_delta_receita_volume"]     = round(dv, 2) if dv is not None else None
            totals[ym]["mix_delta_receita_interacao"]  = round(inter, 2) if inter is not None else None
        else:
            totals[ym]["receita_mom_pct"] = None
            totals[ym]["qtd_mom_pct"]     = None
            totals[ym]["ticket_mom_pct"]  = None
            totals[ym]["receita_logret"]  = None
            totals[ym]["mix_delta_receita_preco"] = None
            totals[ym]["mix_delta_receita_volume"] = None
            totals[ym]["mix_delta_receita_interacao"] = None

        # YoY quando existir base (t e t-12) — no agregado global
        idx_yoy = i - 12
        if idx_yoy >= 0:
            base_ym   = months[idx_yoy]
            base_vals = totals.get(base_ym, {}) or {}

            base_mens = float(base_vals.get("mensalidade", 0) or 0)
            base_qtd  = int(base_vals.get("mensalidade_qtd", 0) or 0)
            base_tk   = base_vals.get("ticket_medio", None)

            totals[ym]["receita_yoy_pct"] = round(pct_change(mens, base_mens), 4) if base_mens else None
            totals[ym]["qtd_yoy_pct"]     = round(pct_change(qtd,  base_qtd),  4) if base_qtd  else None
            totals[ym]["ticket_yoy_pct"]  = round(pct_change(ticket, base_tk), 4) if base_tk   else None
        else:
            totals[ym]["receita_yoy_pct"] = None
            totals[ym]["qtd_yoy_pct"]     = None
            totals[ym]["ticket_yoy_pct"]  = None

    # KPIs de período (_kpis_periodo)
    receita_series = [safe_float(totals[m].get("mensalidade", 0)) for m in months]
    qtd_series     = [float(totals[m].get("mensalidade_qtd", 0) or 0) for m in months]
    tk_series      = [totals[m].get("ticket_medio") for m in months]
    perf_series    = [totals[m].get("performance") for m in months if totals[m].get("performance") is not None]
    logrets        = [totals[m].get("receita_logret") for m in months if totals[m].get("receita_logret") is not None]

    total_receita  = sum(receita_series)
    total_qtd      = sum(qtd_series)
    tk_ponderado   = (total_receita/total_qtd) if total_qtd > 0 else None

    gm_ticket = geometric_mean([t for t in tk_series if t is not None])
    gm_qtd    = geometric_mean([q for q in qtd_series if q > 0])
    gm_perf_mensal = gm_rate_from_percents(perf_series)       # %
    perf_acumulada = accumulated_from_percents(perf_series)   # %

    # CAGR receita entre primeiro e último mês
    cagr = None
    if receita_series and receita_series[0] > 0 and receita_series[-1] > 0:
        n = max(1, len(receita_series)-1)
        cagr = (receita_series[-1]/receita_series[0])**(1/n) - 1.0

    run_rate_12m = receita_series[-1] * 12.0 if receita_series else None
    hit_rate     = hit_rate_growth(receita_series)
    vol_logret   = volatility(logrets)

    # outliers de receita por z-score simples
    if len(receita_series) >= 3:
        s = pd.Series(receita_series, dtype="float64")
        sd = float(s.std(ddof=1))
        mu = float(s.mean())
        outliers = []
        if sd != 0.0:
            for i, m in enumerate(months):
                z = (receita_series[i]-mu)/sd
                if abs(z) >= 2.0:
                    outliers.append({"mes": m, "zscore": round(z, 3), "mensalidade": round(receita_series[i], 2)})
    else:
        outliers = []

    # sazonalidade simples: média por mês-calendário / média geral - 1
    sazonalidade = {}
    if months:
        df_aux = pd.DataFrame({"ym": months, "receita": receita_series})
        df_aux["mm"] = df_aux["ym"].str[5:7]
        mean_all = df_aux["receita"].mean() if len(df_aux) else None
        if mean_all and mean_all != 0:
            idx = (df_aux.groupby("mm")["receita"].mean() / mean_all) - 1.0
            sazonalidade = {k: round(float(v), 6) for k, v in idx.to_dict().items()}

    kpis_periodo = {
        "periodo": {"inicio": months[0] if months else None, "fim": months[-1] if months else None, "n_meses": len(months)},
        "totais": {
            "receita_total": round(total_receita, 2),
            "qtd_total": int(total_qtd),
            "ticket_ponderado": round(tk_ponderado, 2) if tk_ponderado is not None else None,
        },
        "medias_geometricas": {
            "ticket_medio_gm": round(gm_ticket, 2) if gm_ticket is not None else None,
            "qtd_gm": round(gm_qtd, 0) if gm_qtd is not None else None,
            "performance_mensal_gm_pct": round(gm_perf_mensal, 2) if gm_perf_mensal is not None else None,
            "performance_acumulada_pct": round(perf_acumulada, 2) if perf_acumulada is not None else None,
        },
        "crescimento": {
            "cagr_receita": round(cagr, 6) if cagr is not None else None,
            "run_rate_12m": round(run_rate_12m, 2) if run_rate_12m is not None else None,
        },
        "qualidade": {
            "hit_rate_crescimento_receita_pct": round(hit_rate, 2) if hit_rate is not None else None,
            "volatilidade_logret_receita": round(vol_logret, 6) if vol_logret is not None else None,
            "outliers_receita_zscore_ge_2": outliers,
        },
        "sazonalidade_idx": sazonalidade
    }

    totals["_kpis_periodo"] = kpis_periodo

    out_path = os.path.join(JSON_DIR, "consolidado_mensal.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(totals, f, ensure_ascii=False, indent=2)
    print(f"[ETAPA 4/6] JSON consolidado -> {os.path.relpath(out_path, BASE_DIR)}")


def build_monthly_json_by_posto():
    """Detalhe por mês e por posto. Adiciona mensalidade_qtd, ticket_medio, performance, deltas MoM, logret, mix e YoY."""
    ensure_dir(JSON_DIR)
    pattern = re.compile(r"^(?P<posto>[A-Z])_(?P<ym>\d{4}-\d{2})_(?P<key>[a-z_]+)\.csv$", re.I)

    out = {}              # {ym: {posto: {key: total}}}
    mensalidade_qtd = {}  # {ym: {posto: int}}

    # ingestão
    for fn in os.listdir(DADOS_DIR):
        m = pattern.match(fn)
        if not m:
            continue
        posto = m.group("posto")
        ym = m.group("ym")
        key = m.group("key").lower()
        path = os.path.join(DADOS_DIR, fn)
        try:
            df = pd.read_csv(path)
        except Exception:
            continue

        val = sum_numeric(df, PREFER_NOMES.get(key, ["valor", "total"]))
        out.setdefault(ym, {}).setdefault(posto, {})
        out[ym][posto][key] = round(out[ym][posto].get(key, 0.0) + val, 2)

        if key == "mensalidade":
            mensalidade_qtd.setdefault(ym, {}).setdefault(posto, 0)
            mensalidade_qtd[ym][posto] += count_rows(df)

    # injeta quantidade
    for ym, postos in mensalidade_qtd.items():
        for p, qtd in postos.items():
            out.setdefault(ym, {}).setdefault(p, {})
            out[ym][p]["mensalidade_qtd"] = int(qtd)

    months = _sorted_ym(out.keys())

    # derivados por posto
    for i, ym in enumerate(months):
        prev_ym = months[i - 1] if i > 0 else None
        yoy_idx = i - 12
        base_ym = months[yoy_idx] if yoy_idx >= 0 else None

        for posto, vals in out.get(ym, {}).items():
            mens = float(vals.get("mensalidade", 0) or 0)
            qtd = int(vals.get("mensalidade_qtd", 0) or 0)
            tk = (mens / qtd) if qtd > 0 else None
            out[ym][posto]["ticket_medio"] = round(tk, 2) if tk is not None else None

            # MoM + logret + mix
            if prev_ym:
                prev_vals = out.get(prev_ym, {}).get(posto, {}) or {}
                prev_mens = float(prev_vals.get("mensalidade", 0) or 0)
                prev_qtd = int(prev_vals.get("mensalidade_qtd", 0) or 0)
                prev_tk = prev_vals.get("ticket_medio", None)

                perf = ((qtd - prev_qtd) / prev_qtd * 100.0) if prev_qtd > 0 else None
                out[ym][posto]["performance"] = round(perf, 2) if perf is not None else None
                out[ym][posto]["receita_mom_pct"] = round(pct_change(mens, prev_mens),4) if prev_mens else None
                out[ym][posto]["qtd_mom_pct"] = round(pct_change(qtd, prev_qtd), 4) if prev_qtd else None
                out[ym][posto]["ticket_mom_pct"] = round(pct_change(tk, prev_tk), 4) if prev_tk else None

                lr = log_return(mens, prev_mens)
                out[ym][posto]["receita_logret"] = round(lr, 6) if lr is not None else None

                dp, dv, inter = mix_decomposition(mens, prev_mens, qtd, prev_qtd, tk, prev_tk)
                out[ym][posto]["mix_delta_receita_preco"] = round(dp, 2) if dp is not None else None
                out[ym][posto]["mix_delta_receita_volume"] = round(dv, 2) if dv is not None else None
                out[ym][posto]["mix_delta_receita_interacao"] = round(inter, 2) if inter is not None else None
            else:
                out[ym][posto]["performance"] = None
                out[ym][posto]["receita_mom_pct"] = None
                out[ym][posto]["qtd_mom_pct"] = None
                out[ym][posto]["ticket_mom_pct"] = None
                out[ym][posto]["receita_logret"] = None
                out[ym][posto]["mix_delta_receita_preco"] = None
                out[ym][posto]["mix_delta_receita_volume"] = None
                out[ym][posto]["mix_delta_receita_interacao"] = None

            # YoY por posto
            if base_ym:
                base_vals = out.get(base_ym, {}).get(posto, {}) or {}
                base_mens = float(base_vals.get("mensalidade", 0) or 0)
                base_qtd = int(base_vals.get("mensalidade_qtd", 0) or 0)
                base_tk = base_vals.get("ticket_medio", None)

                out[ym][posto]["receita_yoy_pct"] = round(pct_change(mens, base_mens),4) if base_mens else None
                out[ym][posto]["qtd_yoy_pct"] = round(pct_change(qtd, base_qtd), 4) if base_qtd else None
                out[ym][posto]["ticket_yoy_pct"] = round(pct_change(tk, base_tk), 4) if base_tk else None
            else:
                out[ym][posto]["receita_yoy_pct"] = None
                out[ym][posto]["qtd_yoy_pct"] = None
                out[ym][posto]["ticket_yoy_pct"] = None

    # KPI por posto no mês corrente
    cur_ym = month_bounds(date.today())[2]
    kpis_por_posto = {}
    if cur_ym in out:
        for p, vals in out[cur_ym].items():
            if not isinstance(vals, dict):
                continue
            mens = float(vals.get("mensalidade", 0) or 0)
            qtd = int(vals.get("mensalidade_qtd", 0) or 0)
            tk = vals.get("ticket_medio", None)
            kpis_por_posto[p] = {
                "receita": mens,
                "qtd": qtd,
                "ticket_medio": tk,
                "participacao_receita_pct": None,
            }
        total_cur = sum(v["receita"] for v in kpis_por_posto.values())
        if total_cur > 0:
            for p in kpis_por_posto:
                kpis_por_posto[p]["participacao_receita_pct"] = round(
                    100.0 * kpis_por_posto[p]["receita"] / total_cur, 4
                )

    out["_kpis_periodo_por_posto"] = {"mes": cur_ym, "sumario": kpis_por_posto}

    out_path = os.path.join(JSON_DIR, "consolidado_mensal_por_posto.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[ETAPA 5/6] JSON por posto -> {os.path.relpath(out_path, BASE_DIR)}")

def build_percent_by_posto():
    """Percentuais por posto no mês corrente: medico/mensalidade e alimentacao/mensalidade."""
    ensure_dir(JSON_DIR)
    cur_ym = month_bounds(date.today())[2]
    by_posto_path = os.path.join(JSON_DIR, "consolidado_mensal_por_posto.json")
    if not os.path.exists(by_posto_path):
        print("[ETAPA 6/6] JSON percentuais -> base por posto ausente; etapa pulada")
        return
    with open(by_posto_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cur = data.get(cur_ym, {})
    out_rows = []
    for posto, vals in sorted(cur.items()):
        if not isinstance(vals, dict):
            continue
        v_mens = float(vals.get("mensalidade", 0) or 0)
        v_med  = float(vals.get("medico", 0) or 0)
        v_al   = float(vals.get("alimentacao", 0) or 0)
        perc_m = (v_med / v_mens * 100.0) if v_mens > 0 else None
        perc_a = (v_al  / v_mens * 100.0) if v_mens > 0 else None
        out_rows.append({
            "mes": cur_ym,
            "posto": posto,
            "valor_medico": round(v_med, 2),
            "valor_alimentacao": round(v_al, 2),
            "mensalidade": round(v_mens, 2),
            "perc_medico_sobre_mensalidade": round(perc_m, 4) if perc_m is not None else None,
            "perc_alimentacao_sobre_mensalidade": round(perc_a, 4) if perc_a is not None else None,
        })

    out_path = os.path.join(JSON_DIR, "percentuais_mensais_por_posto.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_rows, f, ensure_ascii=False, indent=2)
    print(f"[ETAPA 6/6] JSON percentuais -> {os.path.relpath(out_path, BASE_DIR)}")


def copy_templates():
    """Copia templates/ para a pasta pública do site estático, se configurado."""
    src, dst = SRC_TEMPLATES_DIR, TARGET_TEMPLATES_DIR
    if not os.path.isdir(src):
        print(f"[INFO] SKIP cópia: origem inexistente -> {src}")
        return
    os.makedirs(dst, exist_ok=True)
    shutil.copytree(src, dst, dirs_exist_ok=True)
    print(f"[INFO] Templates copiados -> {dst}")

# =========================
# CLI
# =========================
def parse_args():
    p = argparse.ArgumentParser(
        description="Governança: executa SQLs por posto, gera CSV por mês, consolida JSON.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--from", dest="from_ym", default=None, help="Início YYYY-MM. Default=2024-01.")
    p.add_argument("--to", dest="to_ym", default=None, help="Fim exclusivo YYYY-MM. Default=início do mês corrente.")
    p.add_argument("--only-month", dest="only_month", default=None, help="Executa só um mês YYYY-MM.")
    p.add_argument("--postos", default="".join(POSTOS), help="Subset de postos. Ex: ANX.")
    p.add_argument("--force", action="store_true", help="Sobrescreve arquivos existentes.")
    p.add_argument("--no-validate", action="store_true", help="Não valida o conteúdo dos .sql.")
    p.add_argument("--dry-run", action="store_true", help="Executa sem gravar CSV/JSON.")
    p.add_argument("--copy-templates", action="store_true", help="Copia templates/ para TARGET_TEMPLATES_DIR.")
    return p.parse_args()

# =========================
# Orquestração
# =========================
def run():
    args = parse_args()
    print("========== EXPORT GOVERNANÇA ==========")
    print("[ETAPA 1/6] Setup")

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
    print(f"- Force={args.force}  DryRun={args.dry_run}  Validate={not args.no_validate}")

    print("\n[ETAPA 2/6] Execução por mês/posto/sql")
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
                    action = "sobrescrito" if ym == ym_current or args.force else "criado"
                    print(f"   [{posto}] OK {action}  linhas={len(df)}")
                except Exception as e:
                    print(f"   [{posto}] ERRO salvar: {e}")

    if args.dry_run:
        print("\n[ETAPA 3/6] Consolidados -> SKIP (dry-run)")
        print("[ETAPA 4/6] JSON consolidado -> SKIP (dry-run)")
        print("[ETAPA 5/6] JSON por posto -> SKIP (dry-run)")
        print("[ETAPA 6/6] Percentuais -> SKIP (dry-run)")
        return

    print("\n[ETAPA 3/6] Conferência rápida")
    csv_count = len([f for f in os.listdir(DADOS_DIR) if f.lower().endswith(".csv")])
    print(f"- CSVs gerados: {csv_count}")

    print("\n[ETAPA 4/6] JSON consolidado mensal (global)")
    build_monthly_json()

    print("\n[ETAPA 5/6] JSON consolidado por posto")
    build_monthly_json_by_posto()

    print("\n[ETAPA 6/6] JSON de percentuais do mês corrente")
    build_percent_by_posto()

    if args.copy_templates:
        print("\n[EXTRA] Cópia de templates")
        copy_templates()

    print("\n✅ Finalizado.")

# =========================
# Main
# =========================
if __name__ == "__main__":
    run()
