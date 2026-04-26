#!/usr/bin/env python3
# export_preagendamento.py
# ETL noturno — Dashboard Pré-Agendamento
#
# Gera: json_consolidado/preagendamento.json
# Cron recomendado: 0 2 * * * (1x/dia, ~02h)
#
# Cobertura: últimos 12 meses + 90 dias futuros (consultas já agendadas).
# Regenera o JSON inteiro a cada execução (não é incremental).
#
# Estratégia: 13 postos em paralelo, READ UNCOMMITTED na view pesada.
# Uma única query por posto, depois consolida tudo num JSON.

import os, sys, json, time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from etl_meta import ETLMeta

# =========================
# Constantes
# =========================

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
JSON_DIR    = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR     = os.path.join(BASE_DIR, "logs")
LOG_FILE    = os.path.join(LOG_DIR, f"export_preagendamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
OUT_FILE    = os.path.join(JSON_DIR, "preagendamento.json")
SQL_FILE    = os.path.join(BASE_DIR, "sql", "preagendamento.sql")

POSTOS         = list("ABCDGIJMNPRXY")
ODBC_DRIVER    = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
MAX_WORKERS    = 6

# Janela de coleta
MESES_PASSADO  = 12
DIAS_FUTURO    = 90


# =========================
# Logger
# =========================

class Logger:
    def __init__(self, log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.fh = open(log_file, 'w', encoding='utf-8')

    def write(self, msg):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(msg, flush=True)
        self.fh.write(f"[{ts}] {msg}\n")
        self.fh.flush()

    def close(self):
        self.fh.close()


logger = Logger(LOG_FILE)


def fmt_time(elapsed):
    if elapsed < 1:    return f"{elapsed*1000:.0f}ms"
    if elapsed < 60:   return f"{elapsed:.1f}s"
    return f"{int(elapsed//60)}m {elapsed%60:.0f}s"


# =========================
# Conexão
# =========================

def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def build_conn_str(posto):
    p = posto.strip().upper()
    host = _env(f"DB_HOST_{p}")
    base = _env(f"DB_BASE_{p}")
    if not host or not base:
        return None
    user       = _env(f"DB_USER_{p}")
    pwd        = _env(f"DB_PASSWORD_{p}")
    port       = _env(f"DB_PORT_{p}", "1433") or "1433"
    encrypt    = _env("DB_ENCRYPT", "yes")
    trust_cert = _env("DB_TRUST_CERT", "yes")
    timeout    = _env("DB_TIMEOUT", "30")
    server     = f"tcp:{host},{port}"
    common = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server};DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
    )
    return common + (f"UID={user};PWD={pwd}" if user else "Trusted_Connection=yes")


def make_engine(odbc_str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}",
        pool_pre_ping=True,
        future=True,
    )


def connect_posto(posto):
    conn_str = build_conn_str(posto)
    if not conn_str:
        return posto, None, "sem config no .env"
    try:
        eng = make_engine(conn_str)
        with eng.connect() as con:
            con.execute(text("SELECT 1"))
        return posto, eng, None
    except Exception as e:
        return posto, None, str(e)


def connect_all_postos():
    engines = {}
    logger.write("")
    logger.write("FASE 1 — Conectando em todos os postos (paralelo)...")
    logger.write("-" * 70)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(connect_posto, p): p for p in POSTOS}
        for future in as_completed(futures):
            posto, eng, err = future.result()
            if eng:
                engines[posto] = eng
                logger.write(f"  [{posto}] ✓ conectado")
            else:
                logger.write(f"  [{posto}] ✗ {err}")
    logger.write(f"  → {len(engines)}/{len(POSTOS)} postos online | {fmt_time(time.time()-t0)}")
    return engines


# =========================
# Query
# =========================

def load_sql():
    with open(SQL_FILE, encoding="utf-8") as f:
        return f.read()


def fetch_posto(eng, posto, sql, dt_ini, dt_fim):
    """Executa a query no posto. Datas em DD/MM/YYYY (regra views CAMIM)."""
    params = {
        "dt_ini": dt_ini.strftime("%d/%m/%Y"),
        "dt_fim": dt_fim.strftime("%d/%m/%Y"),
    }
    t0 = time.time()
    try:
        with eng.connect() as con:
            rows = con.execute(text(sql), params).mappings().all()
        elapsed = time.time() - t0
        logger.write(f"  [{posto}] ✓ {len(rows):>6} linhas | {fmt_time(elapsed)}")
        return posto, rows, None
    except Exception as e:
        elapsed = time.time() - t0
        logger.write(f"  [{posto}] ✗ {fmt_time(elapsed)} | {e}")
        return posto, [], str(e)


def fetch_all(engines, dt_ini, dt_fim):
    sql = load_sql()
    logger.write("")
    logger.write(f"FASE 2 — Coletando consultas de {dt_ini.isoformat()} a {dt_fim.isoformat()} (paralelo)...")
    logger.write("-" * 70)
    t0 = time.time()
    resultado = {}
    erros = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_posto, eng, posto, sql, dt_ini, dt_fim): posto
            for posto, eng in engines.items()
        }
        for future in as_completed(futures):
            posto, rows, err = future.result()
            if err:
                erros[posto] = err
            resultado[posto] = rows
    total = sum(len(r) for r in resultado.values())
    logger.write(f"  → {total} linhas em {len(resultado)} postos | {fmt_time(time.time()-t0)}")
    return resultado, erros


# =========================
# Transformação
# =========================

def _to_float(v):
    if v is None:        return 0.0
    if isinstance(v, Decimal): return float(v)
    try:    return float(v)
    except: return 0.0


def _str(v):
    if v is None: return None
    s = str(v).strip()
    return s if s else None


def _bool_int(v):
    """0/1 inteiro a partir de campos que podem vir como bit/int/None."""
    if v in (None, 0, "0", False): return 0
    return 1


def derivar_canal(row):
    if _bool_int(row.get("via_web")):  return "WEB"
    if _bool_int(row.get("via_asu")):  return "ASU"
    return "F6"  # CtrlF6=1 OU todos 0/null


def transformar(rows, posto):
    out = []
    for r in rows:
        out.append({
            "p":   posto,
            "il":  r.get("id_lancamento"),
            "m":   _str(r.get("matricula")),
            "pac": _str(r.get("paciente")),
            "med": _str(r.get("medico")),
            "esp": _str(r.get("especialidade")),
            "dl":  _str(r.get("data_lancamento")),       # YYYY-MM-DD
            "dc":  _str(r.get("data_consulta")),         # YYYY-MM-DD
            "hc":  _str(r.get("hora_consulta")),         # HH:MM
            "dp":  _str(r.get("data_push")),             # YYYY-MM-DD HH:MM:SS
            "da":  _str(r.get("data_conf_agend")),
            "df":  _str(r.get("data_conf_chegada")),
            "dif": int(r.get("dif_dias") or 0),
            "at":  _str(r.get("atendido")),
            "des": _bool_int(r.get("desistencia")),
            "can": derivar_canal(r),
            "vp":  _to_float(r.get("valor_pago")),
            "tal": _str(r.get("talao")),
            "ic":  r.get("id_cliente"),
        })
    return out


# =========================
# Main
# =========================

def run():
    meta = ETLMeta('export_preagendamento', 'json_consolidado')
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    os.makedirs(JSON_DIR, exist_ok=True)

    t0_total = time.time()
    hoje    = date.today()
    dt_ini  = (hoje.replace(day=1) - timedelta(days=1)).replace(day=1)
    # Volta MESES_PASSADO meses do mês atual
    y, m = hoje.year, hoje.month
    m_ini = m - MESES_PASSADO
    y_ini = y
    while m_ini <= 0:
        m_ini += 12
        y_ini -= 1
    dt_ini  = date(y_ini, m_ini, 1)
    dt_fim  = hoje + timedelta(days=DIAS_FUTURO + 1)  # exclusivo

    logger.write("=" * 70)
    logger.write("EXPORT PRÉ-AGENDAMENTO")
    logger.write("=" * 70)
    logger.write(f"  Janela:   {dt_ini.isoformat()}  →  {dt_fim.isoformat()}  (exclusivo)")
    logger.write(f"  Postos:   {', '.join(POSTOS)}")
    logger.write(f"  Workers:  {MAX_WORKERS}")
    logger.write(f"  Início:   {datetime.now().strftime('%H:%M:%S')}")
    logger.write("=" * 70)

    # FASE 1
    engines = connect_all_postos()
    if not engines:
        logger.write("ERRO FATAL: nenhum posto online")
        meta.error('geral', 'nenhum posto online')
        meta.save()
        sys.exit(1)

    # FASE 2 — coletar
    bruto, erros = fetch_all(engines, dt_ini, dt_fim)

    # FASE 3 — transformar
    logger.write("")
    logger.write("FASE 3 — Transformando registros...")
    logger.write("-" * 70)
    t0 = time.time()
    todas = []
    por_posto_count = {}
    for posto, rows in bruto.items():
        transformadas = transformar(rows, posto)
        todas.extend(transformadas)
        por_posto_count[posto] = len(transformadas)
    logger.write(f"  → {len(todas)} linhas transformadas | {fmt_time(time.time()-t0)}")

    # FASE 4 — salvar
    payload = {
        "meta": {
            "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "janela": {"inicio": dt_ini.isoformat(), "fim": dt_fim.isoformat()},
            "postos": POSTOS,
            "postos_online": sorted(engines.keys()),
            "postos_erro": erros,
            "por_posto_count": por_posto_count,
            "total": len(todas),
            "fases": {
                "regra_ativa":   "2026-01-01",
                "cancelamento":  "2026-05-01",
            },
            "janela_confirmacao_dias": {"min": 2, "max": 5},
            "origem": "export_preagendamento.py",
        },
        "consultas": todas,
    }

    logger.write("")
    logger.write("FASE 4 — Salvando JSON...")
    logger.write("-" * 70)
    t0 = time.time()
    tmp = OUT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, OUT_FILE)
    size_mb = os.path.getsize(OUT_FILE) / (1024 * 1024)
    logger.write(f"  → {OUT_FILE}  ({size_mb:.1f} MB)  | {fmt_time(time.time()-t0)}")

    # Meta status
    for posto in POSTOS:
        if posto in erros:
            meta.error(posto, erros[posto])
        elif posto in engines:
            meta.ok(posto)
        else:
            meta.error(posto, 'conexao falhou')
    meta.save()

    logger.write("")
    logger.write("=" * 70)
    logger.write(f"FINALIZADO em {fmt_time(time.time()-t0_total)} | {len(todas)} linhas | {len(engines)}/{len(POSTOS)} postos")
    logger.write("=" * 70)


if __name__ == "__main__":
    try:
        run()
    finally:
        logger.close()
