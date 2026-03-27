#!/usr/bin/env python3
"""
sync_gerentes.py
Sincroniza dados de gerente (EmailGestor, TelefoneWhatsApp) de cada posto
a partir do SQL Server, tabela sis_empresa.

Cron (11:30 seg-sex):
  30 11 * * 1-5 /opt/relatorio_h_t/.venv/bin/python /opt/relatorio_h_t/sync_gerentes.py \
    >> /opt/relatorio_h_t/logs/sync_gerentes.log 2>&1
"""

import os
import sys
import logging

sys.path.insert(0, '/opt/camim-auth')
sys.path.insert(0, '/opt/relatorio_h_t')

from dotenv import load_dotenv
load_dotenv('/opt/relatorio_h_t/.env')

import alarmes_db as adb

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

POSTOS_ALL  = list('ANXYBRPCDGIMJ')
ODBC_DRIVER = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')


def _env(key, default=''):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def _build_conn_str(posto):
    p = posto.strip()
    host = _env(f'DB_HOST_{p}') or _env(f'DB_HOST_{p.lower()}')
    base = _env(f'DB_BASE_{p}') or _env(f'DB_BASE_{p.lower()}')
    if not host or not base:
        return None
    user = _env(f'DB_USER_{p}') or _env(f'DB_USER_{p.lower()}')
    pwd  = _env(f'DB_PASSWORD_{p}') or _env(f'DB_PASSWORD_{p.lower()}')
    port = _env(f'DB_PORT_{p}', '1433') or '1433'
    encrypt = _env('DB_ENCRYPT', 'yes')
    trust   = _env('DB_TRUST_CERT', 'yes')
    timeout = _env('DB_TIMEOUT', '20')
    cs = (
        f'DRIVER={{{ODBC_DRIVER}}};SERVER=tcp:{host},{port};DATABASE={base};'
        f'Encrypt={encrypt};TrustServerCertificate={trust};Connection Timeout={timeout};'
    )
    if user:
        cs += f'UID={user};PWD={pwd}'
    else:
        cs += 'Trusted_Connection=yes'
    return cs


def sync_posto(posto):
    conn_str = _build_conn_str(posto)
    if not conn_str:
        log.warning('Posto %s: sem configuração de banco — pulando', posto)
        return False
    try:
        import pyodbc
        conn = pyodbc.connect(conn_str, timeout=20)
        row = conn.execute(
            'SELECT TOP 1 e.EmailGestor, e.TelefoneWhatsApp FROM sis_empresa e'
        ).fetchone()
        conn.close()
        if row:
            email    = str(row[0] or '').strip() or None
            telefone = str(row[1] or '').strip() or None
            adb.upsert_gerente(posto, email, telefone)
            log.info('Posto %s: email=%r telefone=%r atualizado', posto, email, telefone)
            return True
        else:
            log.warning('Posto %s: nenhum registro em sis_empresa', posto)
            return False
    except Exception as e:
        log.error('Posto %s: erro: %s', posto, e)
        return False


def main():
    adb.init_db()
    ok_count = 0
    for posto in POSTOS_ALL:
        if sync_posto(posto):
            ok_count += 1
    log.info('Sync gerentes concluído: %d/%d postos atualizados', ok_count, len(POSTOS_ALL))


if __name__ == '__main__':
    main()
