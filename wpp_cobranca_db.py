"""
wpp_cobranca_db.py
Schema SQLite e helpers CRUD para o sistema de Cobrança WhatsApp.

Tabelas:
  campanhas     — definição de cada campanha (filtros, janela, intervalo)
  envios        — mensagens enviadas com sucesso
  nao_enviados  — registros pulados com motivo

Usado por:
  wpp_cobranca_routes.py  (Flask, em /opt/camim-auth/)
  send_whatsapp_cobranca.py (engine cron, em /opt/relatorio_h_t/)
"""

import os
import sqlite3
import json
from datetime import datetime, timezone

DB_PATH = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")

# Motivos "normais" de controle de cadência (não são erro operacional).
# Não entram nos indicadores de "Não enviados".
_MOTIVOS_NAO_CONTABILIZAR = (
    "bloqueado_rodada_global",
    "bloqueado_intervalo_global",
    "bloqueado_intervalo",
    "dentro_intervalo",
    "fora_intervalo",
    "intervalo",
    "ja_enviado_recente",
    "multi_fatura",
    "mesmo_tel_nesta_rodada",
)


def _motivo_contabilizavel(motivo: str | None) -> bool:
    m = (motivo or "").strip().lower()
    if not m:
        return True
    return not any(m.startswith(prefixo) for prefixo in _MOTIVOS_NAO_CONTABILIZAR)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS campanhas (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            nome                TEXT    NOT NULL,
            template            TEXT    NOT NULL DEFAULT 'notificacao_de_fatura',
            modo_envio          TEXT    NOT NULL DEFAULT 'atraso', -- atraso | pre_vencimento
            postos              TEXT    NOT NULL DEFAULT '[]',  -- JSON array ex: ["A","X"]

            -- Filtros de atraso
            dias_atraso_min     INTEGER NOT NULL DEFAULT 1,
            dias_atraso_max     INTEGER,          -- NULL = sem limite superior
            dias_ref_min        INTEGER NOT NULL DEFAULT 4, -- dias a frente por datareferencia
            dias_ref_max        INTEGER,

            -- Filtros de cliente
            incluir_cancelados  INTEGER NOT NULL DEFAULT 0,   -- 0=não, 1=sim
            sem_email           INTEGER NOT NULL DEFAULT 0,   -- 1=apenas sem email
            sexo                TEXT,             -- NULL=ambos | 'M' | 'F'
            idade_min           INTEGER,
            idade_max           INTEGER,
            nao_recorrente      INTEGER NOT NULL DEFAULT 0,   -- 1=apenas não recorrentes
            operadora           TEXT,             -- NULL=todas
            cobrador            TEXT,             -- NULL=todos (LIKE)
            corretor            TEXT,             -- NULL=todos (LIKE)

            -- Filtros de localização
            bairro              TEXT,             -- NULL=todos (LIKE)
            rua                 TEXT,             -- NULL=todas (LIKE, campo endereco)

            -- Janela de envio
            hora_inicio         TEXT    NOT NULL DEFAULT '08:00',
            hora_fim            TEXT    NOT NULL DEFAULT '20:00',
            dias_semana         TEXT    NOT NULL DEFAULT '0,1,2,3,4',  -- 0=seg..6=dom

            -- Controle de reenvio
            intervalo_dias      INTEGER NOT NULL DEFAULT 7,

            -- Status
            ativa               INTEGER NOT NULL DEFAULT 1,
            created_at          TEXT    NOT NULL,
            updated_at          TEXT    NOT NULL,

            -- Fila WhatsApp por campanha (NULL = usa WAPP_QUEUE_ID do .env)
            queue_id            TEXT,

            -- Filtros de admissão (modo clientes_admissao)
            adm_data_ini        TEXT,
            adm_data_fim        TEXT,
            tipo_cliente        TEXT,
            titular_dependente  TEXT,
            situacao_cliente    TEXT,
            tipo_fj             TEXT,
            clube_beneficio     INTEGER NOT NULL DEFAULT 0,
            clube_beneficio_joy INTEGER NOT NULL DEFAULT 0,
            plano_premium       INTEGER NOT NULL DEFAULT 0,
            origem              TEXT,
            pagador_atrasado    INTEGER NOT NULL DEFAULT 0,
            from_user_id        TEXT    NOT NULL DEFAULT 'cmg8cum8g0519jbbm6r9l93f7'
        );

        CREATE TABLE IF NOT EXISTS envios (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            campanha_id  INTEGER NOT NULL REFERENCES campanhas(id),
            posto        TEXT,
            telefone     TEXT    NOT NULL,
            idreceita    TEXT,
            matricula    TEXT,
            nome         TEXT,
            ref          TEXT,
            valor        TEXT,
            venc         TEXT,
            dias_atraso  INTEGER,
            template     TEXT,
            status       TEXT    NOT NULL,   -- accepted | dry_run
            wamid        TEXT,
            enviado_em   TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_envios_tel      ON envios(telefone);
        CREATE INDEX IF NOT EXISTS idx_envios_camp     ON envios(campanha_id);
        CREATE INDEX IF NOT EXISTS idx_envios_data     ON envios(enviado_em);

        CREATE TABLE IF NOT EXISTS nao_enviados (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            campanha_id  INTEGER NOT NULL REFERENCES campanhas(id),
            rodada_em    TEXT    NOT NULL,
            posto        TEXT,
            idreceita    TEXT,
            matricula    TEXT,
            nome         TEXT,
            dias_atraso  INTEGER,
            telefone_raw TEXT,
            telefone_ok  TEXT,
            motivo       TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_naoenviados_camp   ON nao_enviados(campanha_id);
        CREATE INDEX IF NOT EXISTS idx_naoenviados_rodada ON nao_enviados(rodada_em);

        -- Auditoria de todas as ações no módulo WhatsApp Cobrança
        CREATE TABLE IF NOT EXISTS auditoria (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario      TEXT    NOT NULL,   -- email do usuário logado
            acao         TEXT    NOT NULL,   -- CRIAR | EDITAR | EXCLUIR | ATIVAR | DESATIVAR
            campanha_id  INTEGER,
            campanha_nome TEXT,
            detalhe      TEXT,              -- JSON com dados antes/depois
            ocorrido_em  TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_auditoria_data  ON auditoria(ocorrido_em);
        CREATE INDEX IF NOT EXISTS idx_auditoria_user  ON auditoria(usuario);
        CREATE INDEX IF NOT EXISTS idx_auditoria_camp  ON auditoria(campanha_id);

        -- Cache local de clientes (populado pelo ETL wpp_cache_clientes.py)
        CREATE TABLE IF NOT EXISTS cache_clientes (
            idcliente           INTEGER NOT NULL,
            row_id              INTEGER NOT NULL,
            posto               TEXT    NOT NULL,
            id_endereco         INTEGER,
            sexo                TEXT,
            matricula           INTEGER,
            nomecadastro        TEXT,
            titular_dependente  TEXT,
            plano               TEXT,
            idade               INTEGER,
            dataadmissao        TEXT,
            nascimento          TEXT,
            canceladoans        INTEGER NOT NULL DEFAULT 0,
            tipo_fj             TEXT,
            tipo_cliente        TEXT,
            situacao_efetiva    TEXT,
            situacaoclube       TEXT,
            situacao            TEXT,
            clubebeneficio      INTEGER NOT NULL DEFAULT 0,
            clubebeneficiojoy   INTEGER NOT NULL DEFAULT 0,
            planopremium        INTEGER NOT NULL DEFAULT 0,
            cobradornome        TEXT,
            corretor            TEXT,
            bairro              TEXT,
            origem              TEXT,
            diacobranca         INTEGER,
            responsavel         TEXT,
            responsavel_tel_wpp TEXT,
            telefone_whatsapp   TEXT,
            telefone_efetivo    TEXT,
            pagador_atrasado    INTEGER NOT NULL DEFAULT 0,
            carregado_em        TEXT,
            PRIMARY KEY (idcliente, row_id, posto)
        );
        CREATE INDEX IF NOT EXISTS idx_cc_posto             ON cache_clientes(posto);
        CREATE INDEX IF NOT EXISTS idx_cc_idcliente         ON cache_clientes(idcliente, posto);
        CREATE INDEX IF NOT EXISTS idx_cc_dataadmissao      ON cache_clientes(dataadmissao);
        CREATE INDEX IF NOT EXISTS idx_cc_tipo_cliente      ON cache_clientes(tipo_cliente);
        CREATE INDEX IF NOT EXISTS idx_cc_situacao          ON cache_clientes(situacao_efetiva);
        CREATE INDEX IF NOT EXISTS idx_cc_tel               ON cache_clientes(telefone_efetivo);
        CREATE INDEX IF NOT EXISTS idx_cc_pagador_atrasado  ON cache_clientes(pagador_atrasado);
        """)
        # Migração leve para bases já existentes.
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(campanhas)").fetchall()}
        if "modo_envio" not in cols:
            conn.execute(
                "ALTER TABLE campanhas ADD COLUMN modo_envio TEXT NOT NULL DEFAULT 'atraso'"
            )
        if "dias_ref_min" not in cols:
            conn.execute(
                "ALTER TABLE campanhas ADD COLUMN dias_ref_min INTEGER NOT NULL DEFAULT 4"
            )
        if "dias_ref_max" not in cols:
            conn.execute("ALTER TABLE campanhas ADD COLUMN dias_ref_max INTEGER")
        _novos = {
            "queue_id":            "ALTER TABLE campanhas ADD COLUMN queue_id TEXT",
            "adm_data_ini":        "ALTER TABLE campanhas ADD COLUMN adm_data_ini TEXT",
            "adm_data_fim":        "ALTER TABLE campanhas ADD COLUMN adm_data_fim TEXT",
            "tipo_cliente":        "ALTER TABLE campanhas ADD COLUMN tipo_cliente TEXT",
            "titular_dependente":  "ALTER TABLE campanhas ADD COLUMN titular_dependente TEXT",
            "situacao_cliente":    "ALTER TABLE campanhas ADD COLUMN situacao_cliente TEXT",
            "tipo_fj":             "ALTER TABLE campanhas ADD COLUMN tipo_fj TEXT",
            "clube_beneficio":     "ALTER TABLE campanhas ADD COLUMN clube_beneficio INTEGER NOT NULL DEFAULT 0",
            "clube_beneficio_joy": "ALTER TABLE campanhas ADD COLUMN clube_beneficio_joy INTEGER NOT NULL DEFAULT 0",
            "plano_premium":       "ALTER TABLE campanhas ADD COLUMN plano_premium INTEGER NOT NULL DEFAULT 0",
            "origem":              "ALTER TABLE campanhas ADD COLUMN origem TEXT",
            "pagador_atrasado":    "ALTER TABLE campanhas ADD COLUMN pagador_atrasado INTEGER NOT NULL DEFAULT 0",
            "from_user_id":        "ALTER TABLE campanhas ADD COLUMN from_user_id TEXT NOT NULL DEFAULT 'cmg8cum8g0519jbbm6r9l93f7'",
            "enviar_chat":         "ALTER TABLE campanhas ADD COLUMN enviar_chat INTEGER NOT NULL DEFAULT 1",
            "enviar_meta":         "ALTER TABLE campanhas ADD COLUMN enviar_meta INTEGER NOT NULL DEFAULT 0",
        }
        for _col, _ddl in _novos.items():
            if _col not in cols:
                conn.execute(_ddl)

        # Migração: adiciona coluna pagador_atrasado no cache_clientes se ainda não existe
        cc_cols = {r["name"] for r in conn.execute("PRAGMA table_info(cache_clientes)").fetchall()}
        if "pagador_atrasado" not in cc_cols:
            conn.execute(
                "ALTER TABLE cache_clientes ADD COLUMN pagador_atrasado INTEGER NOT NULL DEFAULT 0"
            )


# ---------------------------------------------------------------------------
# Helpers de tempo
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _row_to_dict(row) -> dict:
    if row is None:
        return None
    d = dict(row)
    # Deserializa postos de JSON para lista
    try:
        d["postos"] = json.loads(d.get("postos") or "[]")
    except Exception:
        d["postos"] = []
    return d


# ---------------------------------------------------------------------------
# CAMPANHAS — CRUD
# ---------------------------------------------------------------------------

def listar_campanhas() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM campanhas ORDER BY ativa DESC, updated_at DESC"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_campanha(campanha_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM campanhas WHERE id = ?", (campanha_id,)
        ).fetchone()
    return _row_to_dict(row)


def criar_campanha(dados: dict) -> int:
    now = _now_iso()
    postos_json = json.dumps(dados.get("postos") or [])
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO campanhas (
                nome, template, modo_envio, postos,
                dias_atraso_min, dias_atraso_max, dias_ref_min, dias_ref_max,
                incluir_cancelados, sem_email, sexo,
                idade_min, idade_max, nao_recorrente,
                operadora, cobrador, corretor,
                bairro, rua,
                hora_inicio, hora_fim, dias_semana,
                intervalo_dias, ativa,
                queue_id,
                adm_data_ini, adm_data_fim,
                tipo_cliente, titular_dependente, situacao_cliente, tipo_fj,
                clube_beneficio, clube_beneficio_joy, plano_premium,
                origem, pagador_atrasado, from_user_id,
                enviar_chat, enviar_meta,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                dados["nome"], dados.get("template", "notificacao_de_fatura"),
                dados.get("modo_envio", "atraso"), postos_json,
                dados.get("dias_atraso_min", 1), dados.get("dias_atraso_max") or None,
                dados.get("dias_ref_min", 4), dados.get("dias_ref_max") or None,
                1 if dados.get("incluir_cancelados") else 0,
                1 if dados.get("sem_email") else 0,
                dados.get("sexo") or None,
                dados.get("idade_min") or None, dados.get("idade_max") or None,
                1 if dados.get("nao_recorrente") else 0,
                dados.get("operadora") or None,
                dados.get("cobrador") or None,
                dados.get("corretor") or None,
                dados.get("bairro") or None,
                dados.get("rua") or None,
                dados.get("hora_inicio", "08:00"),
                dados.get("hora_fim", "20:00"),
                dados.get("dias_semana", "0,1,2,3,4"),
                dados.get("intervalo_dias", 7),
                1 if dados.get("ativa", True) else 0,
                dados.get("queue_id") or None,
                dados.get("adm_data_ini") or None,
                dados.get("adm_data_fim") or None,
                dados.get("tipo_cliente") or None,
                dados.get("titular_dependente") or None,
                dados.get("situacao_cliente") or None,
                dados.get("tipo_fj") or None,
                1 if dados.get("clube_beneficio") else 0,
                1 if dados.get("clube_beneficio_joy") else 0,
                1 if dados.get("plano_premium") else 0,
                dados.get("origem") or None,
                1 if dados.get("pagador_atrasado") else 0,
                dados.get("from_user_id") or "cmg8cum8g0519jbbm6r9l93f7",
                1 if dados.get("enviar_chat", True) else 0,
                1 if dados.get("enviar_meta") else 0,
                now, now,
            )
        )
        return cur.lastrowid


def atualizar_campanha(campanha_id: int, dados: dict) -> None:
    postos_json = json.dumps(dados.get("postos") or [])
    now = _now_iso()
    with get_conn() as conn:
        conn.execute(
            """UPDATE campanhas SET
                nome=?, template=?, modo_envio=?, postos=?,
                dias_atraso_min=?, dias_atraso_max=?, dias_ref_min=?, dias_ref_max=?,
                incluir_cancelados=?, sem_email=?, sexo=?,
                idade_min=?, idade_max=?, nao_recorrente=?,
                operadora=?, cobrador=?, corretor=?,
                bairro=?, rua=?,
                hora_inicio=?, hora_fim=?, dias_semana=?,
                intervalo_dias=?, ativa=?,
                queue_id=?,
                adm_data_ini=?, adm_data_fim=?,
                tipo_cliente=?, titular_dependente=?, situacao_cliente=?, tipo_fj=?,
                clube_beneficio=?, clube_beneficio_joy=?, plano_premium=?,
                origem=?, pagador_atrasado=?, from_user_id=?,
                enviar_chat=?, enviar_meta=?,
                updated_at=?
            WHERE id=?""",
            (
                dados["nome"], dados.get("template", "notificacao_de_fatura"),
                dados.get("modo_envio", "atraso"), postos_json,
                dados.get("dias_atraso_min", 1), dados.get("dias_atraso_max") or None,
                dados.get("dias_ref_min", 4), dados.get("dias_ref_max") or None,
                1 if dados.get("incluir_cancelados") else 0,
                1 if dados.get("sem_email") else 0,
                dados.get("sexo") or None,
                dados.get("idade_min") or None, dados.get("idade_max") or None,
                1 if dados.get("nao_recorrente") else 0,
                dados.get("operadora") or None,
                dados.get("cobrador") or None,
                dados.get("corretor") or None,
                dados.get("bairro") or None,
                dados.get("rua") or None,
                dados.get("hora_inicio", "08:00"),
                dados.get("hora_fim", "20:00"),
                dados.get("dias_semana", "0,1,2,3,4"),
                dados.get("intervalo_dias", 7),
                1 if dados.get("ativa", True) else 0,
                dados.get("queue_id") or None,
                dados.get("adm_data_ini") or None,
                dados.get("adm_data_fim") or None,
                dados.get("tipo_cliente") or None,
                dados.get("titular_dependente") or None,
                dados.get("situacao_cliente") or None,
                dados.get("tipo_fj") or None,
                1 if dados.get("clube_beneficio") else 0,
                1 if dados.get("clube_beneficio_joy") else 0,
                1 if dados.get("plano_premium") else 0,
                dados.get("origem") or None,
                1 if dados.get("pagador_atrasado") else 0,
                dados.get("from_user_id") or "cmg8cum8g0519jbbm6r9l93f7",
                1 if dados.get("enviar_chat", True) else 0,
                1 if dados.get("enviar_meta") else 0,
                now,
                campanha_id,
            )
        )


def toggle_campanha(campanha_id: int) -> bool:
    """Alterna ativa/inativa. Retorna novo estado (True=ativa)."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT ativa FROM campanhas WHERE id=?", (campanha_id,)
        ).fetchone()
        if not row:
            return False
        novo = 0 if row["ativa"] else 1
        conn.execute(
            "UPDATE campanhas SET ativa=?, updated_at=? WHERE id=?",
            (novo, _now_iso(), campanha_id)
        )
    return bool(novo)


def excluir_campanha(campanha_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM nao_enviados WHERE campanha_id=?", (campanha_id,))
        conn.execute("DELETE FROM envios WHERE campanha_id=?", (campanha_id,))
        conn.execute("DELETE FROM campanhas WHERE id=?", (campanha_id,))


# ---------------------------------------------------------------------------
# ENVIOS — leitura
# ---------------------------------------------------------------------------

def listar_envios(campanha_id: int, limit: int = 200, offset: int = 0) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM envios
               WHERE campanha_id=?
               ORDER BY enviado_em DESC
               LIMIT ? OFFSET ?""",
            (campanha_id, limit, offset)
        ).fetchall()
    return [dict(r) for r in rows]


def get_envio(envio_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT e.*, c.nome as campanha_nome, c.* FROM envios e "
            "JOIN campanhas c ON c.id = e.campanha_id "
            "WHERE e.id=?", (envio_id,)
        ).fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["postos"] = json.loads(d.get("postos") or "[]")
    except Exception:
        d["postos"] = []
    return d


def resumo_campanha(campanha_id: int) -> dict:
    """Contadores rápidos para exibir na listagem."""
    with get_conn() as conn:
        env = conn.execute(
            "SELECT COUNT(*) as total, SUM(CASE WHEN status='accepted' THEN 1 ELSE 0 END) as ok "
            "FROM envios WHERE campanha_id=?", (campanha_id,)
        ).fetchone()
        nenv = conn.execute(
            "SELECT COUNT(*) as total FROM nao_enviados "
            "WHERE campanha_id=? "
            "AND LOWER(motivo) NOT LIKE '%intervalo%' "
            "AND LOWER(motivo) NOT LIKE 'bloqueado_rodada_global%' "
            "AND LOWER(motivo) NOT LIKE 'ja_enviado_recente%' "
            "AND LOWER(motivo) NOT LIKE '%multi_fatura%' "
            "AND LOWER(motivo) NOT LIKE '%mesmo_tel_nesta_rodada%'",
            (campanha_id,)
        ).fetchone()
    return {
        "enviados": env["ok"] or 0,
        "total_tentativas": env["total"] or 0,
        "nao_enviados": nenv["total"] or 0,
    }


def listar_nao_enviados(campanha_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM nao_enviados
               WHERE campanha_id=?
                 AND LOWER(motivo) NOT LIKE '%intervalo%'
                 AND LOWER(motivo) NOT LIKE 'bloqueado_rodada_global%'
                 AND LOWER(motivo) NOT LIKE 'ja_enviado_recente%'
                 AND LOWER(motivo) NOT LIKE '%multi_fatura%'
                 AND LOWER(motivo) NOT LIKE '%mesmo_tel_nesta_rodada%'
               ORDER BY rodada_em DESC, motivo""",
            (campanha_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def buscar_envios_global(q: str, limit: int = 200) -> list[dict]:
    """Busca envios por telefone ou nome em todas as campanhas."""
    digits = "".join(c for c in q if c.isdigit())
    with get_conn() as conn:
        if digits and len(digits) >= 4:
            rows = conn.execute(
                """SELECT e.*, c.nome as campanha_nome
                   FROM envios e JOIN campanhas c ON c.id = e.campanha_id
                   WHERE e.telefone LIKE ?
                   ORDER BY e.enviado_em DESC LIMIT ?""",
                (f"%{digits}%", limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT e.*, c.nome as campanha_nome
                   FROM envios e JOIN campanhas c ON c.id = e.campanha_id
                   WHERE e.nome LIKE ?
                   ORDER BY e.enviado_em DESC LIMIT ?""",
                (f"%{q.upper()}%", limit)
            ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# ENVIOS — escrita (usada pelo engine)
# ---------------------------------------------------------------------------

def registrar_envio(campanha_id: int, posto: str, fatura: dict,
                    telefone: str, template: str,
                    status: str, wamid: str | None) -> None:
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO envios
               (campanha_id, posto, telefone, idreceita, matricula, nome,
                ref, valor, venc, dias_atraso, template, status, wamid, enviado_em)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                campanha_id, posto, telefone,
                str(fatura.get("idreceita") or ""),
                str(fatura.get("matricula") or ""),
                str(fatura.get("nome") or ""),
                str(fatura.get("ref") or ""),
                str(fatura.get("_valor_fmt") or ""),
                str(fatura.get("_venc_fmt") or ""),
                fatura.get("diasdebito") or 0,
                template, status, wamid,
                _now_iso(),
            )
        )


def registrar_nao_enviado(campanha_id: int, posto: str, fatura: dict,
                           rodada_em: str, telefone_ok: str | None,
                           motivo: str) -> None:
    if not _motivo_contabilizavel(motivo):
        return
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO nao_enviados
               (campanha_id, rodada_em, posto, idreceita, matricula, nome,
                dias_atraso, telefone_raw, telefone_ok, motivo)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                campanha_id, rodada_em, posto,
                str(fatura.get("idreceita") or ""),
                str(fatura.get("matricula") or ""),
                str(fatura.get("nome") or ""),
                fatura.get("diasdebito") or 0,
                str(fatura.get("telefonewhatsapp") or ""),
                telefone_ok or "",
                motivo,
            )
        )


# ---------------------------------------------------------------------------
# AUDITORIA
# ---------------------------------------------------------------------------

def registrar_auditoria(usuario: str, acao: str, campanha_id: int | None,
                         campanha_nome: str | None, detalhe: dict | None) -> None:
    import json as _json
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO auditoria (usuario, acao, campanha_id, campanha_nome, detalhe, ocorrido_em)
               VALUES (?,?,?,?,?,?)""",
            (
                usuario, acao, campanha_id, campanha_nome,
                _json.dumps(detalhe, ensure_ascii=False) if detalhe else None,
                _now_iso(),
            )
        )


def listar_auditoria(limit: int = 500) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM auditoria ORDER BY ocorrido_em DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# CONTADOR HOJE
# ---------------------------------------------------------------------------

def enviados_hoje(campanha_id: int) -> int:
    """Quantidade de envios accepted hoje para esta campanha."""
    from datetime import date
    hoje = date.today().isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as n FROM envios "
            "WHERE campanha_id=? AND status='accepted' AND date(enviado_em)=?",
            (campanha_id, hoje)
        ).fetchone()
    return row["n"] if row else 0


# ---------------------------------------------------------------------------
# CONTROLE DE INTERVALO (por telefone, global entre campanhas)
# ---------------------------------------------------------------------------

def ultimo_envio_aceito(telefone: str) -> str | None:
    """Retorna ISO string do último envio accepted para este telefone, ou None."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(enviado_em) as dt FROM envios WHERE telefone=? AND status='accepted'",
            (telefone,)
        ).fetchone()
    return row["dt"] if row and row["dt"] else None


# ---------------------------------------------------------------------------
# INDICADORES (painel operacional)
# ---------------------------------------------------------------------------

def indicadores_wpp() -> list[dict]:
    """Para cada campanha ativa, retorna o último envio accepted por posto.
    Retorna lista de {id, nome, postos: {posto: {dias, ultimo_envio}}}."""
    from datetime import date as _date, datetime as _datetime

    hoje = _date.today()
    with get_conn() as conn:
        campanhas = conn.execute(
            "SELECT id, nome, postos FROM campanhas WHERE ativa=1"
        ).fetchall()

        result = []
        for c in campanhas:
            try:
                postos_lista = json.loads(c["postos"] or "[]")
            except Exception:
                postos_lista = []

            postos_dados = {}
            for posto in postos_lista:
                row = conn.execute(
                    "SELECT MAX(enviado_em) as ultimo FROM envios "
                    "WHERE campanha_id=? AND posto=? AND status='accepted'",
                    (c["id"], posto)
                ).fetchone()

                ultimo = row["ultimo"] if row and row["ultimo"] else None
                if ultimo:
                    try:
                        dt = _datetime.fromisoformat(ultimo).date()
                        dias = (hoje - dt).days
                    except Exception:
                        dias = 999
                else:
                    dias = 999

                postos_dados[posto] = {"dias": dias, "ultimo_envio": ultimo}

            result.append({
                "id": c["id"],
                "nome": c["nome"],
                "postos": postos_dados,
            })

    return result


# ---------------------------------------------------------------------------
# Inicialização automática ao importar
# ---------------------------------------------------------------------------
init_db()
