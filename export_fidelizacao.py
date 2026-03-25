#!/usr/bin/env python3
# export_fidelizacao.py
# Exporta dados de Fidelização de Clientes (PC_Fin_Fidelizacao)
# Salva em SQLite local para histórico + JSON para frontend

import os, re, sys, json, argparse, sqlite3, time
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# Constantes
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
DB_PATH = os.path.join(BASE_DIR, "fidelizacao.db")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, f"export_fidelizacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

EARLIEST_ALLOWED = date(2020, 1, 1)
POSTOS = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# =========================
# Timing & Logging
# =========================

class Logger:
    """Escreve simultâneamente em console e arquivo."""
    def __init__(self, log_file):
        self.log_file = log_file
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.file_handle = open(log_file, 'w', encoding='utf-8')

    def write(self, msg: str):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{timestamp}] {msg}"
        print(msg)  # console
        self.file_handle.write(line + '\n')
        self.file_handle.flush()

    def close(self):
        if self.file_handle:
            self.file_handle.close()

logger = Logger(LOG_FILE)

class Timer:
    def __init__(self, name: str):
        self.name = name
        self.start = None
        self.elapsed = 0

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = time.time() - self.start

    def format(self):
        if self.elapsed < 1:
            return f"{self.elapsed*1000:.0f}ms"
        elif self.elapsed < 60:
            return f"{self.elapsed:.1f}s"
        else:
            mins = self.elapsed // 60
            secs = self.elapsed % 60
            return f"{int(mins)}m {secs:.0f}s"

    def log(self):
        print(f"  [{self.name}] {self.format()}")

class Statistics:
    def __init__(self):
        self.db_connections = 0
        self.procedure_calls = 0
        self.rows_imported = 0
        self.db_time = 0
        self.json_time = 0
        self.total_time = 0
        self.posto_times = {}  # {posto: [tempos]}
        self.call_times = []   # lista de (posto, mes, tempo) para análise

    def add_call_time(self, posto, mes, elapsed):
        """Registra tempo de cada chamada."""
        if posto not in self.posto_times:
            self.posto_times[posto] = []
        self.posto_times[posto].append(elapsed)
        self.call_times.append((posto, mes, elapsed))

    def report(self):
        logger.write("\n" + "="*70)
        logger.write("📊 ESTATÍSTICAS FINAIS")
        logger.write("="*70)
        logger.write(f"  Conexões ao banco: {self.db_connections}")
        logger.write(f"  Chamadas à procedure: {self.procedure_calls}")
        logger.write(f"  Linhas importadas: {self.rows_imported}")
        logger.write(f"  Tempo de banco: {self._format_time(self.db_time)}")
        logger.write(f"  Tempo de JSON: {self._format_time(self.json_time)}")
        logger.write(f"  Tempo total: {self._format_time(self.total_time)}")
        logger.write("="*70)

        # Resumo por posto
        logger.write("\n📊 RESUMO POR POSTO:")
        logger.write("-"*70)
        for posto in sorted(self.posto_times.keys()):
            times = self.posto_times[posto]
            total = sum(times)
            avg = total / len(times) if times else 0
            logger.write(f"  [{posto}] {len(times)} chamadas | Total: {self._format_time(total)} | Média: {self._format_time(avg)}")

        # Top 10 chamadas mais lentas
        logger.write("\n🐢 TOP 10 CHAMADAS MAIS LENTAS:")
        logger.write("-"*70)
        sorted_calls = sorted(self.call_times, key=lambda x: x[2], reverse=True)[:10]
        for posto, mes, elapsed in sorted_calls:
            logger.write(f"  [{posto}] {mes}: {self._format_time(elapsed)}")

    @staticmethod
    def _format_time(seconds):
        if seconds < 60:
            return f"{seconds:.1f}s"
        else:
            mins = int(seconds // 60)
            secs = seconds % 60
            return f"{mins}m {secs:.0f}s"

stats = Statistics()

def _set_mtime(path: str) -> None:
    """Ajusta atime/mtime do arquivo para 'agora'."""
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))

def sanitize_nan(obj):
    """Converte NaN/NaT/inf em None de forma recursiva."""
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

def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

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
# SQLite Database
# =========================

def init_db():
    """Cria a tabela de fidelização se não existir."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS fidelizacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            posto TEXT NOT NULL,
            ano_admissao INTEGER NOT NULL,
            mes_admissao INTEGER NOT NULL,
            qtd_admissao INTEGER NOT NULL,
            ano_referencia INTEGER NOT NULL,
            mes_referencia INTEGER NOT NULL,
            qtd_recebida INTEGER NOT NULL,
            percentual_fidelizacao REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def upsert_fidelizacao_batch(posto: str, df: pd.DataFrame):
    """Insere/atualiza lote de dados de fidelização."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for _, row in df.iterrows():
        c.execute('''
            REPLACE INTO fidelizacao (
                posto, ano_admissao, mes_admissao, qtd_admissao,
                ano_referencia, mes_referencia, qtd_recebida, percentual_fidelizacao
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            posto,
            int(row.get('AnoAdmissao', 0)),
            int(row.get('MesAdmissao', 0)),
            int(row.get('QuantidadeAdmissao', 0)),
            int(row.get('AnoReferencia', 0)),
            int(row.get('MesReferencia', 0)),
            int(row.get('QuantidadeRecebida', 0)),
            float(row.get('PercentualFidelizacao', 0.0))
        ))

    conn.commit()
    conn.close()

def read_fidelizacao_from_db():
    """Lê todos os dados de fidelização do SQLite."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('SELECT * FROM fidelizacao', conn)
    conn.close()
    return df

# =========================
# Exportação para JSON
# =========================

def build_json_fidelizacao():
    """Constrói JSON agregado por mês de admissão e posto."""
    ensure_dir(JSON_DIR)

    df = read_fidelizacao_from_db()
    if df.empty:
        print("[WARN] Nenhum dado em SQLite para exportar JSON")
        return

    # Agregar por mês de admissão (ano_admissao-mes_admissao) e posto
    # Mostrar taxa de retenção em 1, 3, 6, 12 meses

    dados = {}  # {ym: {posto: {...}}}
    postos_set = set()

    for _, row in df.iterrows():
        posto = row['posto']
        ym_adm = f"{int(row['ano_admissao']):04d}-{int(row['mes_admissao']):02d}"

        if ym_adm not in dados:
            dados[ym_adm] = {}

        if posto not in dados[ym_adm]:
            dados[ym_adm][posto] = {
                'admissoes': int(row['qtd_admissao']),
                'retencao_por_mes': {}
            }

        meses_diferenca = (row['ano_referencia'] - row['ano_admissao']) * 12 + \
                         (row['mes_referencia'] - row['mes_admissao'])

        dados[ym_adm][posto]['retencao_por_mes'][meses_diferenca] = {
            'qtd': int(row['qtd_recebida']),
            'percentual': round(float(row['percentual_fidelizacao']), 2)
        }

        postos_set.add(posto)

    # Salvar JSON
    payload = sanitize_nan({
        'indicador': 'fidelizacao_cliente',
        'periodo': {
            'inicio': min(dados.keys()),
            'fim': max(dados.keys()),
            'n_meses': len(dados)
        },
        'postos': sorted(list(postos_set)),
        'meses': sorted(dados.keys()),
        'dados': dados
    })

    out_path = os.path.join(JSON_DIR, "fidelizacao_cliente.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _set_mtime(out_path)
    print(f"[JSON] Fidelização -> {os.path.relpath(out_path, BASE_DIR)}")

# =========================
# Execução
# =========================

def month_iter(start: date, end_exclusive: date):
    """Itera por meses entre start e end_exclusive."""
    y, m = start.year, start.month
    while True:
        ini = date(y, m, 1)
        if ini >= end_exclusive:
            break
        yield ini
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y

def run_extraction_filtered(start_date: date, end_date: date, postos_str: str):
    """Extrai dados de postos filtrados para período especificado."""
    import pyodbc

    global stats
    total_start = time.time()

    logger.write("\n" + "="*70)
    logger.write("🔄 EXPORT FIDELIZAÇÃO DE CLIENTES")
    logger.write("="*70)

    # Setup
    with Timer("Setup") as t_setup:
        ensure_dir(JSON_DIR)
        init_db()

        # Filtrar apenas os postos solicitados
        postos_filtrados = [c.upper() for c in postos_str if c.isalpha()]
        if not postos_filtrados:
            postos_filtrados = POSTOS

        conns = build_conns_from_env(postos_filtrados)
        if not conns:
            logger.write("ERRO: .env sem DB_HOST_*/DB_BASE_* configurados para os postos informados.")
            sys.exit(1)

    logger.write(f"  [Setup] {t_setup.format()}")

    # Info
    postos_list = list(conns.keys())
    meses_list = list(month_iter(start_date, end_date))
    total_calls = len(postos_list) * len(meses_list)

    logger.write(f"\n📋 Plano de Execução:")
    logger.write(f"  Postos: {', '.join(postos_list)} ({len(postos_list)} postos)")
    logger.write(f"  Período: {start_date} ... {end_date - pd.DateOffset(days=1)}")
    logger.write(f"  Meses: {len(meses_list)}")
    logger.write(f"  Total de chamadas: {total_calls} (postos × meses)")

    # Execução
    logger.write(f"\n⏱️  [ETAPA 1/3] Extração por mês/posto")
    logger.write("-" * 70)

    total_linhas = 0
    meses_com_dados = set()
    db_start = time.time()
    calls_completed = 0
    calls_failed = 0

    for i, month_date in enumerate(meses_list, 1):
        ym = f"{month_date.year:04d}-{month_date.month:02d}"
        logger.write(f"\n[{i}/{len(meses_list)}] {ym}")

        for j, (posto, odbc_str) in enumerate(conns.items(), 1):
            call_start = time.time()
            prefix = f"  [{j}/{len(postos_list)}] [{posto}]"

            try:
                # Conexão
                with Timer(f"conexão") as t_conn:
                    cnx = pyodbc.connect(odbc_str, timeout=20)
                    cursor = cnx.cursor()
                stats.db_connections += 1

                # Procedure
                with Timer(f"procedure") as t_proc:
                    cursor.execute('EXEC PC_Fin_Fidelizacao ?', (month_date,))
                    stats.procedure_calls += 1

                # Fetch
                with Timer(f"fetch") as t_fetch:
                    all_rows = []
                    columns = None

                    while True:
                        if cursor.description:
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()
                            all_rows.extend(rows)

                        if not cursor.nextset():
                            break

                # Store
                with Timer(f"store") as t_store:
                    if all_rows and columns:
                        df = pd.DataFrame([dict(zip(columns, row)) for row in all_rows])
                        upsert_fidelizacao_batch(posto, df)
                        total_linhas += len(df)
                        meses_com_dados.add(ym)
                        status = f"✓ {len(df):4d} linhas"
                    else:
                        status = "✓ (vazio)"

                call_elapsed = time.time() - call_start
                call_time = f"{call_elapsed*1000:.0f}ms" if call_elapsed < 1 else f"{call_elapsed:.1f}s"

                # Registrar tempo e calcular previsão
                stats.add_call_time(posto, ym, call_elapsed)
                calls_completed += 1
                calls_remaining = total_calls - calls_completed

                if calls_completed > 0 and calls_completed % 10 == 0:
                    avg_time = sum(t[2] for t in stats.call_times) / len(stats.call_times)
                    eta_seconds = calls_remaining * avg_time
                    eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
                    logger.write(f"{prefix} {status:20s} | conn:{t_conn.format():>6s} | proc:{t_proc.format():>6s} | fetch:{t_fetch.format():>6s} | ETA: {eta_str:>8s} | [{call_time}]")
                else:
                    logger.write(f"{prefix} {status:20s} | conn:{t_conn.format():>6s} | proc:{t_proc.format():>6s} | fetch:{t_fetch.format():>6s} | [{call_time}]")

            except Exception as e:
                call_elapsed = time.time() - call_start
                call_time = f"{call_elapsed*1000:.0f}ms" if call_elapsed < 1 else f"{call_elapsed:.1f}s"
                logger.write(f"{prefix} ✗ ERRO: {str(e)[:40]:40s} [{call_time}]")
                calls_failed += 1
            finally:
                try:
                    cursor.close()
                    cnx.close()
                except:
                    pass

    db_elapsed = time.time() - db_start
    stats.db_time = db_elapsed

    # JSON
    logger.write(f"\n⏱️  [ETAPA 2/3] Gerando JSON")
    logger.write("-" * 70)

    json_start = time.time()
    with Timer("build_json") as t_json:
        build_json_fidelizacao()
    logger.write(f"  [build_json] {t_json.format()}")
    stats.json_time = time.time() - json_start

    # Resumo
    stats.rows_imported = total_linhas
    stats.total_time = time.time() - total_start

    logger.write(f"\n⏱️  [ETAPA 3/3] Resumo")
    logger.write("-" * 70)
    logger.write(f"  ✓ Linhas importadas: {total_linhas}")
    logger.write(f"  ✓ Meses com dados: {len(meses_com_dados)}")
    logger.write(f"  ✓ Chamadas completadas: {calls_completed}/{total_calls}")
    logger.write(f"  ✓ Chamadas falhadas: {calls_failed}")
    logger.write(f"  ✓ DB: {stats._format_time(stats.db_time)} | JSON: {stats._format_time(stats.json_time)} | Total: {stats._format_time(stats.total_time)}")

    stats.report()
    logger.write("\n✅ Finalizado export_fidelizacao.")
    logger.write(f"📄 Log salvo em: {LOG_FILE}\n")
    logger.close()

def parse_args():
    p = argparse.ArgumentParser(
        description="Fidelização: extrai dados de PC_Fin_Fidelizacao por posto.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--postos", default="".join(POSTOS), help="Subset de postos. Ex: ANX ou Y.")
    p.add_argument("--force", action="store_true", help="Limpa SQLite antes de executar.")
    p.add_argument("--dry-run", action="store_true", help="Executa sem gravar SQLite/JSON.")
    p.add_argument("--from", dest="from_ym", default=None, help="Início YYYY-MM. Default=2020-01.")
    p.add_argument("--to", dest="to_ym", default=None, help="Fim exclusivo YYYY-MM. Default=mês corrente.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()

    if args.force:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print(f"[INFO] Base {DB_PATH} removida para reprocessamento.")

    # Determinar período
    def ym_to_date(ym: str) -> date:
        return date(int(ym[0:4]), int(ym[5:7]), 1)

    start_date = ym_to_date(args.from_ym) if args.from_ym else EARLIEST_ALLOWED
    end_date = ym_to_date(args.to_ym) if args.to_ym else date.today()

    if not args.dry_run:
        run_extraction_filtered(start_date, end_date, args.postos)
    else:
        print("[DRY-RUN] Nenhuma alteração realizada.")
