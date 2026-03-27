"""
alarmes_db.py
Schema SQLite e helpers CRUD para o sistema de Alarmes CAMIM.

DB: /opt/camim-auth/alarmes.db (env ALARMES_DB_PATH)
"""

import os
import sqlite3
import json
from datetime import datetime, timedelta

DB_PATH = os.getenv("ALARMES_DB_PATH", "/opt/camim-auth/alarmes.db")

POSTOS_ALL  = list("ANXYBRPCDGIMJ")
POSTOS_NOMES = {
    'A': 'Anchieta', 'N': 'Nilópolis', 'I': 'Nova Iguaçu', 'X': 'CGX',
    'G': 'Campo Grande', 'Y': 'CGY', 'B': 'Bangu', 'R': 'Realengo',
    'M': 'Madureira', 'C': 'Campinho', 'D': 'Del Castilho', 'J': 'JPA',
    'P': 'Rio das Pedras',
}
SERVICOS = {
    'push':  'Push Cobrança',
    'email': 'Boleto (Email)',
    'tef':   'TEF Recorrente',
    'wpp':   'WhatsApp Cobrança',
}
STATUS_ORDER = {'otimo': 6, 'bom': 5, 'ok': 4, 'ruim': 3, 'pessimo': 2, 'horrivel': 1}
STATUS_LABELS = {
    'otimo': 'Ótimo', 'bom': 'Bom', 'ok': 'Ok',
    'ruim': 'Ruim', 'pessimo': 'Péssimo', 'horrivel': 'Horrível',
}
DIAS_SEMANA_NOMES = {
    '0': 'Seg', '1': 'Ter', '2': 'Qua', '3': 'Qui', '4': 'Sex', '5': 'Sáb', '6': 'Dom',
}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS gerente_posto (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                posto        TEXT NOT NULL UNIQUE,
                email        TEXT,
                telefone     TEXT,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS alarme (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                nome           TEXT NOT NULL,
                posto          TEXT NOT NULL,
                servico        TEXT NOT NULL,
                status_gatilho TEXT NOT NULL,
                mensagem       TEXT NOT NULL,
                via_whatsapp   INTEGER NOT NULL DEFAULT 1,
                via_email      INTEGER NOT NULL DEFAULT 1,
                hora_disparo   TEXT NOT NULL DEFAULT '08:00',
                dias_semana    TEXT NOT NULL DEFAULT '0,1,2,3,4',
                ativo          INTEGER NOT NULL DEFAULT 1,
                criado_em      DATETIME DEFAULT CURRENT_TIMESTAMP,
                criado_por     TEXT
            );

            CREATE TABLE IF NOT EXISTS gerente_extra (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                alarme_id    INTEGER NOT NULL REFERENCES alarme(id) ON DELETE CASCADE,
                nome         TEXT,
                email        TEXT,
                telefone     TEXT,
                via_whatsapp INTEGER DEFAULT 1,
                via_email    INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS auditor (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                nome          TEXT NOT NULL,
                email         TEXT,
                telefone      TEXT,
                recebe_1_wpp  INTEGER DEFAULT 0,
                recebe_1_email INTEGER DEFAULT 0,
                ativo         INTEGER DEFAULT 1,
                criado_em     DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS diretor (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                nome          TEXT NOT NULL,
                email         TEXT,
                telefone      TEXT,
                recebe_1_wpp  INTEGER DEFAULT 0,
                recebe_1_email INTEGER DEFAULT 0,
                ativo         INTEGER DEFAULT 1,
                criado_em     DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS alarme_auditor (
                alarme_id  INTEGER NOT NULL REFERENCES alarme(id) ON DELETE CASCADE,
                auditor_id INTEGER NOT NULL REFERENCES auditor(id) ON DELETE CASCADE,
                PRIMARY KEY (alarme_id, auditor_id)
            );

            CREATE TABLE IF NOT EXISTS alarme_diretor (
                alarme_id  INTEGER NOT NULL REFERENCES alarme(id) ON DELETE CASCADE,
                diretor_id INTEGER NOT NULL REFERENCES diretor(id) ON DELETE CASCADE,
                PRIMARY KEY (alarme_id, diretor_id)
            );

            CREATE TABLE IF NOT EXISTS disparo (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                alarme_id            INTEGER NOT NULL REFERENCES alarme(id),
                disparado_em         DATETIME DEFAULT CURRENT_TIMESTAMP,
                numero_ciclo         INTEGER DEFAULT 1,
                status_registrado    TEXT,
                enviado_wpp_gerente  INTEGER DEFAULT 0,
                enviado_email_gerente INTEGER DEFAULT 0,
                detalhes             TEXT
            );

            CREATE TABLE IF NOT EXISTS silenciamento (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                alarme_id     INTEGER NOT NULL REFERENCES alarme(id),
                usuario_email TEXT NOT NULL,
                silenciado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                silenciado_ate DATETIME NOT NULL,
                motivo        TEXT,
                ativo         INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS auditoria_alarme (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_email TEXT,
                acao          TEXT NOT NULL,
                entidade      TEXT,
                entidade_id   INTEGER,
                detalhe       TEXT,
                criado_em     DATETIME DEFAULT CURRENT_TIMESTAMP,
                ip            TEXT
            );
        """)
        conn.commit()
    finally:
        conn.close()


# ── Gerente Posto ─────────────────────────────────────────────────────────────

def upsert_gerente(posto, email, telefone):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO gerente_posto (posto, email, telefone, atualizado_em)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(posto) DO UPDATE SET
                email = excluded.email,
                telefone = excluded.telefone,
                atualizado_em = CURRENT_TIMESTAMP
        """, (posto.upper(), email, telefone))


def listar_gerentes():
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM gerente_posto ORDER BY posto"
        ).fetchall()]


def get_gerente(posto):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM gerente_posto WHERE posto = ?", (posto.upper(),)
        ).fetchone()
        return dict(row) if row else None


# ── Alarme CRUD ───────────────────────────────────────────────────────────────

def criar_alarme(dados, criado_por=None):
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO alarme (nome, posto, servico, status_gatilho, mensagem,
                via_whatsapp, via_email, hora_disparo, dias_semana, ativo, criado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """, (
            dados['nome'], dados['posto'].upper(), dados['servico'],
            dados['status_gatilho'], dados['mensagem'],
            int(bool(dados.get('via_whatsapp', True))),
            int(bool(dados.get('via_email', True))),
            dados.get('hora_disparo', '08:00'),
            dados.get('dias_semana', '0,1,2,3,4'),
            criado_por,
        ))
        alarme_id = cur.lastrowid
        for aid in (dados.get('auditores') or []):
            try:
                conn.execute("INSERT OR IGNORE INTO alarme_auditor VALUES (?, ?)", (alarme_id, int(aid)))
            except Exception:
                pass
        for did in (dados.get('diretores') or []):
            try:
                conn.execute("INSERT OR IGNORE INTO alarme_diretor VALUES (?, ?)", (alarme_id, int(did)))
            except Exception:
                pass
        return alarme_id


def get_alarme(alarme_id):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM alarme WHERE id=?", (alarme_id,)).fetchone()
        if not row:
            return None
        a = dict(row)
        a['auditores'] = [r[0] for r in conn.execute(
            "SELECT auditor_id FROM alarme_auditor WHERE alarme_id=?", (alarme_id,)
        ).fetchall()]
        a['diretores'] = [r[0] for r in conn.execute(
            "SELECT diretor_id FROM alarme_diretor WHERE alarme_id=?", (alarme_id,)
        ).fetchall()]
        a['extras'] = [dict(r) for r in conn.execute(
            "SELECT * FROM gerente_extra WHERE alarme_id=? ORDER BY id", (alarme_id,)
        ).fetchall()]
        return a


def listar_alarmes(posto=None, ativo=None):
    with get_conn() as conn:
        cond, params = [], []
        if posto:
            cond.append("posto=?"); params.append(posto.upper())
        if ativo is not None:
            cond.append("ativo=?"); params.append(int(ativo))
        where = f"WHERE {' AND '.join(cond)}" if cond else ""
        rows = conn.execute(
            f"SELECT * FROM alarme {where} ORDER BY posto, servico, hora_disparo", params
        ).fetchall()
        return [dict(r) for r in rows]


def atualizar_alarme(alarme_id, dados):
    with get_conn() as conn:
        conn.execute("""
            UPDATE alarme SET nome=?, posto=?, servico=?, status_gatilho=?,
                mensagem=?, via_whatsapp=?, via_email=?, hora_disparo=?, dias_semana=?
            WHERE id=?
        """, (
            dados['nome'], dados['posto'].upper(), dados['servico'],
            dados['status_gatilho'], dados['mensagem'],
            int(bool(dados.get('via_whatsapp', True))),
            int(bool(dados.get('via_email', True))),
            dados.get('hora_disparo', '08:00'),
            dados.get('dias_semana', '0,1,2,3,4'),
            alarme_id,
        ))
        conn.execute("DELETE FROM alarme_auditor WHERE alarme_id=?", (alarme_id,))
        for aid in (dados.get('auditores') or []):
            try:
                conn.execute("INSERT OR IGNORE INTO alarme_auditor VALUES (?, ?)", (alarme_id, int(aid)))
            except Exception:
                pass
        conn.execute("DELETE FROM alarme_diretor WHERE alarme_id=?", (alarme_id,))
        for did in (dados.get('diretores') or []):
            try:
                conn.execute("INSERT OR IGNORE INTO alarme_diretor VALUES (?, ?)", (alarme_id, int(did)))
            except Exception:
                pass


def excluir_alarme(alarme_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM alarme WHERE id=?", (alarme_id,))


def toggle_alarme(alarme_id):
    with get_conn() as conn:
        conn.execute("UPDATE alarme SET ativo = 1 - ativo WHERE id=?", (alarme_id,))
        row = conn.execute("SELECT ativo FROM alarme WHERE id=?", (alarme_id,)).fetchone()
        return bool(row['ativo']) if row else False


# ── Gerente Extra ─────────────────────────────────────────────────────────────

def add_gerente_extra(alarme_id, nome, email, telefone, via_wpp, via_email):
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO gerente_extra (alarme_id, nome, email, telefone, via_whatsapp, via_email)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (alarme_id, nome or None, email or None, telefone or None, int(via_wpp), int(via_email)))
        return cur.lastrowid


def del_gerente_extra(extra_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM gerente_extra WHERE id=?", (extra_id,))


# ── Auditor CRUD ──────────────────────────────────────────────────────────────

def criar_auditor(dados):
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO auditor (nome, email, telefone, recebe_1_wpp, recebe_1_email)
            VALUES (?, ?, ?, ?, ?)
        """, (
            dados['nome'], dados.get('email') or None, dados.get('telefone') or None,
            int(bool(dados.get('recebe_1_wpp'))),
            int(bool(dados.get('recebe_1_email'))),
        ))
        return cur.lastrowid


def listar_auditores(ativo=True):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM auditor WHERE ativo=? ORDER BY nome", (int(ativo),)
        ).fetchall()
        return [dict(r) for r in rows]


def get_auditor(aid):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM auditor WHERE id=?", (aid,)).fetchone()
        return dict(row) if row else None


def atualizar_auditor(aid, dados):
    with get_conn() as conn:
        conn.execute("""
            UPDATE auditor SET nome=?, email=?, telefone=?, recebe_1_wpp=?, recebe_1_email=?
            WHERE id=?
        """, (
            dados['nome'], dados.get('email') or None, dados.get('telefone') or None,
            int(bool(dados.get('recebe_1_wpp'))),
            int(bool(dados.get('recebe_1_email'))),
            aid,
        ))


def desativar_auditor(aid):
    with get_conn() as conn:
        conn.execute("UPDATE auditor SET ativo=0 WHERE id=?", (aid,))


# ── Diretor CRUD ──────────────────────────────────────────────────────────────

def criar_diretor(dados):
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO diretor (nome, email, telefone, recebe_1_wpp, recebe_1_email)
            VALUES (?, ?, ?, ?, ?)
        """, (
            dados['nome'], dados.get('email') or None, dados.get('telefone') or None,
            int(bool(dados.get('recebe_1_wpp'))),
            int(bool(dados.get('recebe_1_email'))),
        ))
        return cur.lastrowid


def listar_diretores(ativo=True):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM diretor WHERE ativo=? ORDER BY nome", (int(ativo),)
        ).fetchall()
        return [dict(r) for r in rows]


def get_diretor(did):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM diretor WHERE id=?", (did,)).fetchone()
        return dict(row) if row else None


def atualizar_diretor(did, dados):
    with get_conn() as conn:
        conn.execute("""
            UPDATE diretor SET nome=?, email=?, telefone=?, recebe_1_wpp=?, recebe_1_email=?
            WHERE id=?
        """, (
            dados['nome'], dados.get('email') or None, dados.get('telefone') or None,
            int(bool(dados.get('recebe_1_wpp'))),
            int(bool(dados.get('recebe_1_email'))),
            did,
        ))


def desativar_diretor(did):
    with get_conn() as conn:
        conn.execute("UPDATE diretor SET ativo=0 WHERE id=?", (did,))


# ── Silenciamento ─────────────────────────────────────────────────────────────

def criar_silenciamento(alarme_id, usuario_email, dias, motivo=None):
    ate = datetime.now() + timedelta(days=int(dias))
    with get_conn() as conn:
        conn.execute(
            "UPDATE silenciamento SET ativo=0 WHERE alarme_id=? AND ativo=1",
            (alarme_id,)
        )
        cur = conn.execute("""
            INSERT INTO silenciamento (alarme_id, usuario_email, silenciado_ate, motivo, ativo)
            VALUES (?, ?, ?, ?, 1)
        """, (alarme_id, usuario_email, ate.isoformat(), motivo or None))
        return cur.lastrowid


def esta_silenciado(alarme_id):
    with get_conn() as conn:
        row = conn.execute("""
            SELECT id FROM silenciamento
            WHERE alarme_id=? AND ativo=1
              AND datetime(silenciado_ate) > datetime('now', 'localtime')
        """, (alarme_id,)).fetchone()
        return row is not None


def listar_silenciamentos(alarme_id=None, apenas_ativos=True):
    with get_conn() as conn:
        cond, params = [], []
        if alarme_id:
            cond.append("s.alarme_id=?"); params.append(alarme_id)
        if apenas_ativos:
            cond.append("s.ativo=1")
            cond.append("datetime(s.silenciado_ate) > datetime('now', 'localtime')")
        where = f"WHERE {' AND '.join(cond)}" if cond else ""
        rows = conn.execute(f"""
            SELECT s.*, a.nome as alarme_nome, a.posto, a.servico
            FROM silenciamento s
            JOIN alarme a ON a.id = s.alarme_id
            {where}
            ORDER BY s.silenciado_em DESC
        """, params).fetchall()
        return [dict(r) for r in rows]


# ── Disparo ───────────────────────────────────────────────────────────────────

def registrar_disparo(alarme_id, numero_ciclo, status_registrado, wpp_ok, email_ok, detalhes):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO disparo (alarme_id, numero_ciclo, status_registrado,
                enviado_wpp_gerente, enviado_email_gerente, detalhes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (alarme_id, numero_ciclo, status_registrado, int(wpp_ok), int(email_ok),
              json.dumps(detalhes, ensure_ascii=False) if detalhes else None))


def get_numero_ciclo(alarme_id):
    """Conta disparos após o último silenciamento (ou desde sempre)."""
    with get_conn() as conn:
        sil_row = conn.execute(
            "SELECT MAX(silenciado_em) as ultimo FROM silenciamento WHERE alarme_id=?",
            (alarme_id,)
        ).fetchone()
        ultimo_sil = sil_row['ultimo'] if sil_row and sil_row['ultimo'] else None
        if ultimo_sil:
            count = conn.execute("""
                SELECT COUNT(*) FROM disparo WHERE alarme_id=? AND disparado_em > ?
            """, (alarme_id, ultimo_sil)).fetchone()[0]
        else:
            count = conn.execute(
                "SELECT COUNT(*) FROM disparo WHERE alarme_id=?", (alarme_id,)
            ).fetchone()[0]
        return count + 1


def listar_disparos(alarme_id=None, limit=100):
    with get_conn() as conn:
        cond = ""
        params = []
        if alarme_id:
            cond = "WHERE d.alarme_id=?"
            params.append(alarme_id)
        rows = conn.execute(f"""
            SELECT d.*, a.nome as alarme_nome, a.posto, a.servico
            FROM disparo d
            JOIN alarme a ON a.id = d.alarme_id
            {cond}
            ORDER BY d.disparado_em DESC LIMIT ?
        """, params + [limit]).fetchall()
        return [dict(r) for r in rows]


# ── Auditoria ─────────────────────────────────────────────────────────────────

def registrar_auditoria(usuario_email, acao, entidade, entidade_id, detalhe, ip=None):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO auditoria_alarme (usuario_email, acao, entidade, entidade_id, detalhe, ip)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            usuario_email, acao, entidade, entidade_id,
            json.dumps(detalhe, ensure_ascii=False, default=str) if detalhe else None,
            ip,
        ))


def listar_auditoria(limit=300):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM auditoria_alarme ORDER BY criado_em DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# ── Helpers de status ─────────────────────────────────────────────────────────

def dias_para_status(dias):
    if dias is None or dias >= 999: return 'horrivel'
    if dias == 0: return 'otimo'
    if dias == 1: return 'bom'
    if dias == 2: return 'ok'
    if dias == 3: return 'ruim'
    if dias == 4: return 'pessimo'
    return 'horrivel'


def status_igual_ou_pior(status_atual, status_gatilho):
    """True se status_atual é igual ou PIOR (menor order) que status_gatilho."""
    return STATUS_ORDER.get(status_atual, 1) <= STATUS_ORDER.get(status_gatilho, 1)


# ── API resumo para página de monitoramento ───────────────────────────────────

def alarmes_ativos_por_posto_servico(postos=None):
    """Retorna dict {posto: {servico: {alarme_id, nome, silenciado, silenciado_ate}}}"""
    with get_conn() as conn:
        cond = "WHERE a.ativo=1"
        params = []
        if postos:
            ph = ','.join('?' * len(postos))
            cond += f" AND a.posto IN ({ph})"
            params = list(postos)
        rows = conn.execute(f"""
            SELECT a.id, a.nome, a.posto, a.servico
            FROM alarme a {cond}
            ORDER BY a.posto, a.servico
        """, params).fetchall()

        result = {}
        for r in rows:
            eid = esta_silenciado(r['id'])
            sil_ate = None
            if eid:
                sil_row = conn.execute("""
                    SELECT silenciado_ate FROM silenciamento
                    WHERE alarme_id=? AND ativo=1
                      AND datetime(silenciado_ate) > datetime('now', 'localtime')
                    ORDER BY silenciado_ate DESC LIMIT 1
                """, (r['id'],)).fetchone()
                sil_ate = sil_row['silenciado_ate'] if sil_row else None

            posto = r['posto']
            serv  = r['servico']
            if posto not in result:
                result[posto] = {}
            if serv not in result[posto]:
                result[posto][serv] = []
            result[posto][serv].append({
                'id': r['id'],
                'nome': r['nome'],
                'silenciado': eid,
                'silenciado_ate': sil_ate,
            })
        return result
