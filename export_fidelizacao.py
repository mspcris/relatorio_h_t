#!/usr/bin/env python3
# export_fidelizacao.py
# Exporta dados de Fidelização de Clientes (PC_Fin_Fidelizacao)
# Salva em SQLite local para histórico + JSON para frontend

import os, re, sys, json, argparse, sqlite3
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

EARLIEST_ALLOWED = date(2020, 1, 1)
POSTOS = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# =========================
# Utilitários
# =========================

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

def run_extraction():
    """Extrai dados de todos os postos para todos os meses."""
    print("========== EXPORT FIDELIZAÇÃO ==========")
    print("[ETAPA 1/3] Setup")

    ensure_dir(JSON_DIR)
    init_db()

    conns = build_conns_from_env()
    if not conns:
        print("ERRO: .env sem DB_HOST_*/DB_BASE_* configurados.")
        sys.exit(1)

    print(f"- Postos: {list(conns.keys())}")
    print(f"- DB local: {DB_PATH}")

    print("\n[ETAPA 2/3] Execução por posto")

    for posto, odbc_str in conns.items():
        print(f"\n[{posto}] conectando...")
        try:
            engine = make_engine(odbc_str)
        except Exception as e:
            print(f"[{posto}] ERRO engine: {e}")
            continue

        # A procedure precisa de uma data para executar
        # Iteraremos mês a mês desde 2020 até hoje
        print(f"[{posto}] extraindo fidelização...")
        try:
            with engine.connect() as con:
                # Executar procedure para retornar dados consolidados
                df = pd.read_sql_query(
                    text('EXEC PC_Fin_Fidelizacao @database = :ini'),
                    con,
                    params={'ini': date(2020, 1, 1)}
                )

            if not df.empty:
                upsert_fidelizacao_batch(posto, df)
                print(f"[{posto}] OK linhas={len(df)}")
            else:
                print(f"[{posto}] VAZIO (sem dados)")

        except Exception as e:
            print(f"[{posto}] ERRO exec: {e}")
            continue

    print("\n[ETAPA 3/3] Gerando JSON")
    build_json_fidelizacao()

    print("\n✅ Finalizado export_fidelizacao.")

def parse_args():
    p = argparse.ArgumentParser(
        description="Fidelização: extrai dados de PC_Fin_Fidelizacao por posto.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--postos", default="".join(POSTOS), help="Subset de postos. Ex: ANX.")
    p.add_argument("--force", action="store_true", help="Limpa SQLite antes de executar.")
    p.add_argument("--dry-run", action="store_true", help="Executa sem gravar SQLite/JSON.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()

    if args.force:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print(f"[INFO] Base {DB_PATH} removida para reprocessamento.")

    if not args.dry_run:
        run_extraction()
    else:
        print("[DRY-RUN] Nenhuma alteração realizada.")
