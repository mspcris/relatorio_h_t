# export_growth.py
# Gera json_consolidado/growth_dashboard.json com MRR e CAC por posto/mes.
# Usa o mesmo padrao de conexao do export_governanca.py.

import os, re, json, sys, time
from datetime import date, datetime, timezone
from urllib.parse import quote_plus
from etl_meta import ETLMeta
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(BASE_DIR, "sql_growth")
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, f"export_growth_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

POSTOS = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# ========================= Logging & Timing =========================

class Logger:
    """Escreve simultaneamente em console e arquivo."""
    def __init__(self, log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.file_handle = open(log_file, 'w', encoding='utf-8')

    def write(self, msg: str):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{timestamp}] {msg}"
        print(msg, flush=True)
        self.file_handle.write(line + '\n')
        self.file_handle.flush()

    def close(self):
        if self.file_handle:
            self.file_handle.close()

logger = Logger(LOG_FILE)

def fmt_time(elapsed):
    if elapsed < 1:
        return f"{elapsed*1000:.0f}ms"
    elif elapsed < 60:
        return f"{elapsed:.1f}s"
    else:
        mins = int(elapsed // 60)
        secs = elapsed % 60
        return f"{mins}m {secs:.0f}s"

class Stats:
    def __init__(self):
        self.queries = 0
        self.queries_ok = 0
        self.queries_err = 0
        self.rows = 0
        self.db_time = 0
        self.call_times = []  # (posto, mes, query, elapsed)
        self.errors = []      # (posto, mes, query, error)

    def add(self, posto, mes, query, elapsed, ok, rows=0, error=None):
        self.queries += 1
        self.db_time += elapsed
        self.call_times.append((posto, mes, query, elapsed))
        if ok:
            self.queries_ok += 1
            self.rows += rows
        else:
            self.queries_err += 1
            self.errors.append((posto, mes, query, str(error)))

    def report(self):
        logger.write("")
        logger.write("=" * 70)
        logger.write("ESTATISTICAS FINAIS")
        logger.write("=" * 70)
        logger.write(f"  Queries executadas: {self.queries} ({self.queries_ok} ok, {self.queries_err} erro)")
        logger.write(f"  Linhas retornadas:  {self.rows}")
        logger.write(f"  Tempo total de DB:  {fmt_time(self.db_time)}")
        logger.write("=" * 70)

        # Resumo por posto
        posto_times = {}
        for posto, mes, query, elapsed in self.call_times:
            posto_times.setdefault(posto, []).append(elapsed)
        logger.write("")
        logger.write("RESUMO POR POSTO:")
        logger.write("-" * 70)
        for p in sorted(posto_times):
            times = posto_times[p]
            total = sum(times)
            avg = total / len(times)
            logger.write(f"  [{p}] {len(times)} queries | Total: {fmt_time(total)} | Media: {fmt_time(avg)}")

        # Top 10 mais lentas
        logger.write("")
        logger.write("TOP 10 QUERIES MAIS LENTAS:")
        logger.write("-" * 70)
        top = sorted(self.call_times, key=lambda x: x[3], reverse=True)[:10]
        for i, (posto, mes, query, elapsed) in enumerate(top, 1):
            logger.write(f"  {i:2d}. [{posto}] {mes} {query} -> {fmt_time(elapsed)}")

        # Erros
        if self.errors:
            logger.write("")
            logger.write(f"ERROS ({len(self.errors)}):")
            logger.write("-" * 70)
            for posto, mes, query, err in self.errors:
                logger.write(f"  [{posto}] {mes} {query}: {err}")

stats = Stats()

# ========================= Helpers =========================

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

def build_conns():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")
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

def build_mysql_conn():
    """Conexao MySQL para banco de leads."""
    host = env("LEADS_DB_HOST")
    port = int(env("LEADS_DB_PORT", "3306"))
    user = env("LEADS_DB_USER")
    pwd  = env("LEADS_DB_PASSWORD")
    db   = env("LEADS_DB_NAME")
    if not host or not db:
        return None
    return {"host": host, "port": port, "user": user, "password": pwd, "database": db}

def query_leads(mysql_cfg, ini, fim):
    """Retorna dict {filialCode: {leads_total, leads_convertidos}} para o mes."""
    conn = pymysql.connect(**mysql_cfg)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT filialCode,
                   COUNT(*) as leads_total,
                   SUM(finish_lead_signup) as leads_convertidos
            FROM leads
            WHERE created_at >= %s AND created_at < %s
              AND deleted_at IS NULL
            GROUP BY filialCode
        """, (str(ini), str(fim)))
        result = {}
        for row in cur.fetchall():
            code = row[0]
            if code and len(code) == 1 and code in POSTOS:
                result[code] = {
                    "leads_total": int(row[1] or 0),
                    "leads_convertidos": int(row[2] or 0)
                }
        return result
    finally:
        conn.close()

def month_bounds(dt):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    return ini, nxt, f"{ini.year:04d}-{ini.month:02d}"

def month_iter(start, end_exclusive):
    y, m = start.year, start.month
    while True:
        ini = date(y, m, 1)
        if ini >= end_exclusive:
            break
        yield ini
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y

def run_query(engine, sql_txt, ini, fim):
    body = sql_txt if sql_txt.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_txt
    with engine.connect() as con:
        return pd.read_sql_query(text(body), con, params={"ini": ini.strftime("%d/%m/%Y"), "fim": fim.strftime("%d/%m/%Y")})

def load_sql(name):
    path = os.path.join(SQL_DIR, name)
    return open(path, "r", encoding="utf-8").read().strip()

# ========================= Main =========================

def main():
    t_total = time.time()
    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    logger.write("=" * 70)
    logger.write("EXPORT GROWTH DASHBOARD")
    logger.write(f"  Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.write(f"  Log:    {LOG_FILE}")
    logger.write("=" * 70)

    conns = build_conns()
    if not conns:
        logger.write("ERRO: Nenhuma conexao de posto configurada (.env)")
        sys.exit(1)

    logger.write(f"\nPostos configurados ({len(conns)}): {', '.join(sorted(conns.keys()))}")

    sql_mrr   = load_sql("mrr.sql")
    sql_cac   = load_sql("cac_despesas_vendas.sql")
    sql_churn = load_sql("churn.sql")

    mysql_cfg = build_mysql_conn()
    if mysql_cfg:
        logger.write(f"MySQL leads: {mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}")
    else:
        logger.write("WARN: MySQL leads nao configurado (LEADS_DB_* no .env)")
    logger.write(f"SQL carregados: mrr.sql, cac_despesas_vendas.sql, churn.sql")

    # Periodo
    today = date.today()
    start = date(2024, 1, 1)
    _, end_exc, current_ym = month_bounds(today)

    # Carregar JSON existente (cache de meses fechados)
    out_path = os.path.join(JSON_DIR, "growth_dashboard.json")
    cached = {}
    if os.path.exists(out_path):
        try:
            with open(out_path, "r", encoding="utf-8") as f:
                prev = json.load(f)
            cached = prev.get("dados", {})
            logger.write(f"JSON existente carregado: {len(cached)} meses em cache")
        except Exception as e:
            logger.write(f"WARN: falha ao ler JSON existente: {e}")

    meses_all = list(month_iter(start, end_exc))

    # Filtrar: so roda meses sem cache + mes atual (pode mudar ao longo do mes)
    meses_to_run = []
    meses_skipped = []
    for dt in meses_all:
        _, _, ym = month_bounds(dt)
        if ym in cached and ym != current_ym:
            meses_skipped.append(ym)
        else:
            meses_to_run.append(dt)

    # Ordem: mes atual primeiro, depois antigos cronologicamente
    meses_to_run.sort(key=lambda d: (0 if month_bounds(d)[2] == current_ym else 1, d))

    if meses_skipped:
        logger.write(f"Meses em cache (pulando): {', '.join(meses_skipped)}")
    logger.write(f"Meses a processar: {len(meses_to_run)} de {len(meses_all)} total")
    if meses_to_run:
        ordem = [month_bounds(d)[2] for d in meses_to_run]
        logger.write(f"Ordem: {', '.join(ordem)}")

    meta = ETLMeta('export_growth', 'json_consolidado')

    total_steps = len(meses_to_run) * len(conns) * 3  # 3 queries por posto/mes (MRR, CAC, Churn)
    logger.write(f"Periodo: {start} -> {end_exc} ({len(meses_all)} meses, {len(meses_to_run)} a rodar)")
    logger.write(f"Total de queries previstas: {total_steps} ({len(meses_to_run)} meses x {len(conns)} postos x 3 queries)")
    logger.write("")

    # Iniciar com dados do cache
    dados = dict(cached)
    all_postos = set(p for m in cached.values() for p in m)
    all_meses = set(cached.keys())
    step = 0

    def save_json():
        """Grava JSON incremental a cada mes processado."""
        out = {
            "meta": {
                "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
                "arquivo": "growth_dashboard.json",
                "origem": "export_growth.py"
            },
            "meses": sorted(all_meses),
            "postos": sorted(all_postos),
            "dados": dados
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

    for idx_mes, dt in enumerate(meses_to_run, 1):
        ini, fim, ym = month_bounds(dt)
        all_meses.add(ym)
        t_mes = time.time()

        # Limpar cache do mes atual para regravar
        dados.pop(ym, None)

        logger.write(f"[{ym}] Mes {idx_mes}/{len(meses_to_run)} ----------------------------------------")

        for posto, odbc_str in conns.items():
            all_postos.add(posto)
            engine = make_engine(odbc_str)
            rec = {}
            posto_ok = True

            # MRR
            step += 1
            elapsed_pct = step / total_steps * 100
            t0 = time.time()
            try:
                df = run_query(engine, sql_mrr, ini, fim)
                elapsed = time.time() - t0
                rows = len(df)
                if not df.empty:
                    rec["mrr_count"] = int(df.iloc[0].get("mrr_count", 0) or 0)
                    rec["mrr_valor"] = round(float(df.iloc[0].get("mrr_valor", 0) or 0), 2)
                stats.add(posto, ym, "MRR", elapsed, True, rows)
                logger.write(f"  [{posto}] MRR  -> {rec.get('mrr_count', 0):>6d} contratos | R$ {rec.get('mrr_valor', 0):>12,.2f} | {fmt_time(elapsed):>6s} | {elapsed_pct:5.1f}%")
            except Exception as e:
                elapsed = time.time() - t0
                stats.add(posto, ym, "MRR", elapsed, False, error=e)
                logger.write(f"  [{posto}] MRR  -> ERRO: {e} | {fmt_time(elapsed):>6s} | {elapsed_pct:5.1f}%")
                meta.error(posto, str(e))
                posto_ok = False

            # CAC
            step += 1
            elapsed_pct = step / total_steps * 100
            t0 = time.time()
            try:
                df = run_query(engine, sql_cac, ini, fim)
                elapsed = time.time() - t0
                rows = len(df)
                if not df.empty:
                    rec["cac_despesas_vendas"] = round(float(df.iloc[0].get("cac_despesas_vendas", 0) or 0), 2)
                stats.add(posto, ym, "CAC", elapsed, True, rows)
                logger.write(f"  [{posto}] CAC  -> R$ {rec.get('cac_despesas_vendas', 0):>12,.2f} | {fmt_time(elapsed):>6s} | {elapsed_pct:5.1f}%")
            except Exception as e:
                elapsed = time.time() - t0
                stats.add(posto, ym, "CAC", elapsed, False, error=e)
                logger.write(f"  [{posto}] CAC  -> ERRO: {e} | {fmt_time(elapsed):>6s} | {elapsed_pct:5.1f}%")
                meta.error(posto, str(e))
                posto_ok = False

            # CHURN (cancelamentos no mes)
            step += 1
            elapsed_pct = step / total_steps * 100
            t0 = time.time()
            try:
                df = run_query(engine, sql_churn, ini, fim)
                elapsed = time.time() - t0
                rows = len(df)
                if not df.empty:
                    rec["cancelamentos"] = int(df.iloc[0].get("cancelamentos", 0) or 0)
                stats.add(posto, ym, "CHURN", elapsed, True, rows)
                logger.write(f"  [{posto}] CHURN-> {rec.get('cancelamentos', 0):>6d} cancelamentos | {fmt_time(elapsed):>6s} | {elapsed_pct:5.1f}%")
            except Exception as e:
                elapsed = time.time() - t0
                stats.add(posto, ym, "CHURN", elapsed, False, error=e)
                logger.write(f"  [{posto}] CHURN-> ERRO: {e} | {fmt_time(elapsed):>6s} | {elapsed_pct:5.1f}%")
                meta.error(posto, str(e))
                posto_ok = False

            if posto_ok:
                meta.ok(posto)

            if rec:
                dados.setdefault(ym, {})[posto] = rec

        # Leads (MySQL) - uma query para todos os postos do mes
        if mysql_cfg:
            t0 = time.time()
            try:
                leads_data = query_leads(mysql_cfg, ini, fim)
                elapsed = time.time() - t0
                total_leads = sum(v["leads_total"] for v in leads_data.values())
                total_conv = sum(v["leads_convertidos"] for v in leads_data.values())
                for p, ld in leads_data.items():
                    dados.setdefault(ym, {}).setdefault(p, {}).update(ld)
                logger.write(f"  [*] LEADS -> {total_leads:>6d} leads | {total_conv:>6d} convertidos | {fmt_time(elapsed):>6s}")
            except Exception as e:
                elapsed = time.time() - t0
                logger.write(f"  [*] LEADS -> ERRO: {e} | {fmt_time(elapsed):>6s}")

        elapsed_mes = time.time() - t_mes
        # ETA
        elapsed_total = time.time() - t_total
        avg_per_mes = elapsed_total / idx_mes
        remaining = (len(meses_to_run) - idx_mes) * avg_per_mes
        logger.write(f"  [{ym}] concluido em {fmt_time(elapsed_mes)} | ETA restante: {fmt_time(remaining)}")

        # Gravar JSON a cada mes (incremental)
        save_json()
        logger.write(f"  JSON salvo ({len(dados)} meses)")
        logger.write("")

    out = {
        "meta": {
            "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
            "arquivo": "growth_dashboard.json",
            "origem": "export_growth.py"
        },
        "meses": sorted(all_meses),
        "postos": sorted(all_postos),
        "dados": dados
    }

    t_json = time.time()
    out_path = os.path.join(JSON_DIR, "growth_dashboard.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    elapsed_json = time.time() - t_json

    elapsed_total = time.time() - t_total

    logger.write(f"JSON gravado: {out_path} ({fmt_time(elapsed_json)})")
    logger.write(f"Tempo total: {fmt_time(elapsed_total)}")

    meta.save()

    stats.report()
    logger.write(f"\nLog salvo em: {LOG_FILE}")
    logger.close()

if __name__ == "__main__":
    main()
