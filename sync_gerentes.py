#!/usr/bin/env python3
"""
sync_gerentes.py
Sincroniza a tabela gerente_posto (alarmes.db) a partir do CADASTRO DE
GESTORES DO CRM (Postgres do /opt/crm, tabelas gestores + postos).

Decisão 2026-08-10 (Cristiano): o CRM é a FONTE ÚNICA dos gestores — lá tem
nome e celular pessoal reais, com tela de edição própria
(https://crm.camim.com.br/admin). A fonte antiga (sis_empresa.EmailGestor /
TelefoneWhatsApp do SQL Server de cada posto) estava suja: links wa.me do
número da própria clínica, telefone fixo, campos vazios. Não editar
gerente aqui — editar no CRM; este sync espelha.

Regras:
  - postos.codigo (letra) ↔ gestores.id_posto (id_endereco);
  - posto com MAIS DE UM gestor ativo: ganha o de MENOR id (mais antigo).
    Para trocar o titular do alerta, desative o outro no CRM;
  - falha de conexão com o CRM NÃO apaga nada — mantém o espelho anterior.

Cron (diário, cron/relatorio_ht):
  0 23 * * * root ... /opt/relatorio_h_t/sync_gerentes.py
"""

import sys
import logging

sys.path.insert(0, '/opt/camim-auth')
sys.path.insert(0, '/opt/relatorio_h_t')

from dotenv import dotenv_values

import alarmes_db as adb

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

CRM_ENV_PATH = '/opt/crm/.env'


def _conn_crm():
    """Conexão ao Postgres do CRM usando o PRÓPRIO .env do CRM (isolado —
    dotenv_values não polui o ambiente deste processo)."""
    import psycopg2
    cfg = dotenv_values(CRM_ENV_PATH)
    host = (cfg.get('DB_HOST') or '').strip()
    name = (cfg.get('DB_NAME') or '').strip()
    if not host or not name:
        raise RuntimeError(f'DB_HOST/DB_NAME ausentes em {CRM_ENV_PATH}')
    return psycopg2.connect(
        host=host,
        port=int(cfg.get('DB_PORT') or 5432),
        dbname=name,
        user=cfg.get('DB_USER'),
        password=cfg.get('DB_PASSWORD'),
        connect_timeout=15,
    )


def buscar_gestores_crm() -> dict:
    """{letra_posto: {nome, email, telefone}} — menor id ativo ganha."""
    conn = _conn_crm()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.codigo, g.id, g.nome, g.email, g.telefone
            FROM gestores g
            JOIN postos p ON p.id_endereco = g.id_posto
            WHERE g.ativo = 1
            ORDER BY p.codigo, g.id
        """)
        por_posto: dict = {}
        for codigo, gid, nome, email, telefone in cur.fetchall():
            letra = str(codigo or '').strip().upper()
            if not letra or letra in por_posto:
                continue  # menor id já ganhou
            por_posto[letra] = {
                'nome': (nome or '').strip(),
                'email': (email or '').strip() or None,
                'telefone': (telefone or '').strip() or None,
            }
        return por_posto
    finally:
        conn.close()


def main():
    adb.init_db()
    try:
        gestores = buscar_gestores_crm()
    except Exception as e:
        log.error('CRM inacessível (%s) — espelho anterior mantido, nada alterado', e)
        return 1

    if not gestores:
        log.error('CRM devolveu 0 gestores — espelho anterior mantido por segurança')
        return 1

    for letra, g in sorted(gestores.items()):
        adb.upsert_gerente(letra, g['email'], g['telefone'])
        log.info('Posto %s: %s <%s> %s', letra, g['nome'], g['email'], g['telefone'])

    log.info('Sync gerentes (fonte CRM) concluído: %d postos', len(gestores))
    return 0


if __name__ == '__main__':
    sys.exit(main())
