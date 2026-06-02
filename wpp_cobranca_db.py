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
            -- Quando 1, ESTA campanha ignora o intervalo global cross-campanha
            -- (ultimo_envio_aceito) e passa a usar a trava "enviar só 1x por
            -- contato NESTA campanha". Para campanhas one-shot (ex.: indique e
            -- ganhe) que devem chegar ao contato mesmo que ele já tenha recebido
            -- uma cobrança. Default 0 = comportamento atual (respeita 7d global).
            ignorar_intervalo   INTEGER NOT NULL DEFAULT 0,

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
            from_user_id        TEXT    NOT NULL DEFAULT 'cmg8cum8g0519jbbm6r9l93f7',

            -- Número de saída do WhatsApp (default '2455-9600' = número atual da conta Meta)
            -- '3529-6666' inclui from: "552135296666" no payload Meta (Centro Médico do Couto)
            numero_saida        TEXT    NOT NULL DEFAULT '2455-9600'
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

        -- Configuração dos templates Meta: quais aparecem no dropdown do form
        -- de campanha (visivel=1) e qual é o padrão de cada modo_envio (1 por
        -- modo, em modo_template_default). Evita confusão quando a Meta tem
        -- 30+ templates parecidos — operador marca só os que usa.
        CREATE TABLE IF NOT EXISTS templates_config (
            template_name   TEXT    PRIMARY KEY,
            visivel         INTEGER NOT NULL DEFAULT 1,
            atualizado_em   TEXT    NOT NULL,
            atualizado_por  TEXT
        );

        -- Default por modo_envio. Não obrigatório — quando o operador escolhe o
        -- modo no form, o template é pré-selecionado mas pode ser trocado.
        CREATE TABLE IF NOT EXISTS modo_template_default (
            modo_envio      TEXT    PRIMARY KEY,
            template_name   TEXT    NOT NULL,
            atualizado_em   TEXT    NOT NULL,
            atualizado_por  TEXT
        );

        -- Pedidos de desculpa enviados manualmente (por operador via chat externo)
        -- após um envio errado de campanha. Usada pra tela /wpp/<id>/respondentes
        -- não mostrar de novo o envio que já foi atendido. UNIQUE por envio garante
        -- idempotência: 1 desculpa por envio errado.
        CREATE TABLE IF NOT EXISTS desculpas_enviadas (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            campanha_id  INTEGER NOT NULL,
            envio_id     INTEGER NOT NULL UNIQUE,
            ticket_id    TEXT,
            marcado_em   TEXT    NOT NULL,
            marcado_por  TEXT    NOT NULL,
            obs          TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_desculpas_camp ON desculpas_enviadas(campanha_id);

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
            "header_image_url":    "ALTER TABLE campanhas ADD COLUMN header_image_url TEXT",
            # Default '2455-9600' garante que campanhas pré-existentes continuem
            # saindo pelo número atual (default da Meta) sem mudança de comportamento.
            "numero_saida":        "ALTER TABLE campanhas ADD COLUMN numero_saida TEXT NOT NULL DEFAULT '2455-9600'",
            # Quando 1, o payload pra api-chat inclui must_close_ticket=true em
            # messages[0] — o chat fecha o ticket automaticamente após registrar.
            # Útil pra campanhas de aviso unidirecional (cliente não precisa
            # responder); evita a fila do chat ficar entupida.
            "must_close_ticket":   "ALTER TABLE campanhas ADD COLUMN must_close_ticket INTEGER NOT NULL DEFAULT 0",
            # Bit "ignorar intervalo": ver comentário no CREATE TABLE. Default 0
            # preserva o comportamento das campanhas já existentes.
            "ignorar_intervalo":   "ALTER TABLE campanhas ADD COLUMN ignorar_intervalo INTEGER NOT NULL DEFAULT 0",
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

        # Migração: adiciona chat_ticket_id em envios pra lookup direto da
        # tela 'Ver conversa aqui' (sem depender do wamid bater no externalId
        # do chat — o chat NÃO grava wamid em outgoing, só em incoming).
        env_cols = {r["name"] for r in conn.execute("PRAGMA table_info(envios)").fetchall()}
        if "chat_ticket_id" not in env_cols:
            conn.execute("ALTER TABLE envios ADD COLUMN chat_ticket_id TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_envios_chat_ticket ON envios(chat_ticket_id)")

        # Migração: colunas específicas pra modo falta_medico. Cobrança não usa
        # (ficam NULL). Renderizadas em envios.html quando campanha.modo_envio
        # = 'falta_medico' no lugar de ref/valor/venc/dias_atraso.
        _falta_cols = {
            "medico":         "ALTER TABLE envios ADD COLUMN medico TEXT",
            "especialidade":  "ALTER TABLE envios ADD COLUMN especialidade TEXT",
            "data_falta":     "ALTER TABLE envios ADD COLUMN data_falta TEXT",
            "hora_falta":     "ALTER TABLE envios ADD COLUMN hora_falta TEXT",
            "motivo_falta":   "ALTER TABLE envios ADD COLUMN motivo_falta TEXT",
        }
        for _col, _ddl in _falta_cols.items():
            if _col not in env_cols:
                conn.execute(_ddl)


# ---------------------------------------------------------------------------
# Helpers de tempo
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Roteamento Meta por número de saída
# ---------------------------------------------------------------------------
# Mapa numero_saida → from_phone do payload Meta.
# '2455-9600' = default da conta (omite from); '3529-6666' = Couto (envia from).
NUMEROS_SAIDA_VALIDOS = ("2455-9600", "3529-6666")
NUMERO_SAIDA_DEFAULT = "2455-9600"

_FROM_PHONE_POR_NUMERO = {
    "3529-6666": "552135296666",
}

# IDs internos da Meta (phone_number_id da WhatsApp Business Account).
# Diferente do `from` do payload Meta — este vai no metadata do payload do
# /webhooks/chat para o chat conseguir taggar o "Contato" certo (2455 vs
# 3529) sem depender do display_phone_number (que é string e tava sendo
# ignorada pelo chat — bug em curso de correção pelo dev sênior do chat).
_PHONE_NUMBER_ID_POR_NUMERO = {
    "2455-9600": "1028772396984767",
    "3529-6666": "1101062063090022",
}


def _normalizar_numero_saida(valor: str | None) -> str:
    v = (valor or "").strip()
    return v if v in NUMEROS_SAIDA_VALIDOS else NUMERO_SAIDA_DEFAULT


def from_phone_por_numero_saida(numero_saida: str | None) -> str | None:
    """Resolve o `from` do payload Meta a partir do numero_saida da campanha.
    Retorna None quando deve omitir (default 2455 da conta)."""
    return _FROM_PHONE_POR_NUMERO.get(_normalizar_numero_saida(numero_saida))


def phone_number_id_por_numero_saida(numero_saida: str | None) -> str | None:
    """Resolve o phone_number_id (ID interno Meta) do numero_saida da campanha.
    Vai no metadata.phone_number_id do payload de /webhooks/chat."""
    return _PHONE_NUMBER_ID_POR_NUMERO.get(_normalizar_numero_saida(numero_saida))


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
            "SELECT * FROM campanhas "
            "ORDER BY COALESCE(numero_saida, '2455-9600') ASC, id ASC"
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
                enviar_chat, enviar_meta, header_image_url,
                numero_saida, must_close_ticket, ignorar_intervalo,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                (dados.get("header_image_url") or None),
                _normalizar_numero_saida(dados.get("numero_saida")),
                1 if dados.get("must_close_ticket") else 0,
                1 if dados.get("ignorar_intervalo") else 0,
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
                enviar_chat=?, enviar_meta=?, header_image_url=?,
                numero_saida=?, must_close_ticket=?, ignorar_intervalo=?,
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
                (dados.get("header_image_url") or None),
                _normalizar_numero_saida(dados.get("numero_saida")),
                1 if dados.get("must_close_ticket") else 0,
                1 if dados.get("ignorar_intervalo") else 0,
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
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN status LIKE 'accepted%' THEN 1 ELSE 0 END) as ok "
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
# DESCULPAS — registro de qual envio errado já recebeu pedido de desculpa
# ---------------------------------------------------------------------------

def marcar_desculpa_enviada(campanha_id: int, envio_id: int,
                             ticket_id: str | None, usuario: str,
                             obs: str | None = None) -> bool:
    """Marca que esse envio errado já recebeu pedido de desculpa (enviado
    manualmente via chat externo). Idempotente: INSERT OR IGNORE. Retorna
    True se inseriu, False se já existia."""
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO desculpas_enviadas
               (campanha_id, envio_id, ticket_id, marcado_em, marcado_por, obs)
               VALUES (?,?,?,?,?,?)""",
            (campanha_id, envio_id, ticket_id, _now_iso(), usuario, obs),
        )
        return cur.rowcount > 0


def desmarcar_desculpa_enviada(envio_id: int) -> bool:
    """Desfaz a marcação (caso o operador tenha marcado errado)."""
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM desculpas_enviadas WHERE envio_id=?", (envio_id,)
        )
        return cur.rowcount > 0


def desculpas_por_campanha(campanha_id: int) -> dict[int, dict]:
    """Retorna {envio_id: {marcado_em, marcado_por, obs}} pros envios da
    campanha que já foram atendidos. Usado na tela /respondentes pra cruzar
    com a lista de envios."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT envio_id, marcado_em, marcado_por, obs "
            "FROM desculpas_enviadas WHERE campanha_id=?",
            (campanha_id,),
        ).fetchall()
    return {r["envio_id"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# TEMPLATES — config de visibilidade no dropdown + padrão por modo_envio
# ---------------------------------------------------------------------------

def listar_templates_config() -> dict[str, dict]:
    """Retorna {template_name: {visivel: bool, atualizado_em, atualizado_por}}.
    Só inclui templates explicitamente configurados — quando ainda não houver
    config nenhuma, devolve dict vazio e o front trata como 'tudo visível'."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT template_name, visivel, atualizado_em, atualizado_por "
            "FROM templates_config"
        ).fetchall()
    return {r["template_name"]: dict(r) for r in rows}


def templates_visiveis() -> set[str]:
    """Conjunto de templates marcados como visiveis. Vazio = sem config →
    front mostra tudo (fallback compatível com instalações antigas)."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT template_name FROM templates_config WHERE visivel=1"
        ).fetchall()
    return {r["template_name"] for r in rows}


def modo_template_defaults() -> dict[str, str]:
    """Retorna {modo_envio: template_padrao} pra cada modo configurado."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT modo_envio, template_name FROM modo_template_default"
        ).fetchall()
    return {r["modo_envio"]: r["template_name"] for r in rows}


def salvar_templates_config(visiveis: list[str], todos_conhecidos: list[str],
                             modo_defaults: dict[str, str], usuario: str) -> None:
    """Salva a configuração:
    - Cada template em `todos_conhecidos` (vindos da Meta) tem seu registro
      em templates_config com visivel=1 se aparece em `visiveis`, senão 0.
      Isso garante idempotência: marcar/desmarcar grava sempre.
    - modo_template_default é reescrito por completo (REPLACE INTO)."""
    visiveis_set = set(visiveis)
    now = _now_iso()
    with get_conn() as conn:
        for nome in todos_conhecidos:
            conn.execute(
                """INSERT INTO templates_config (template_name, visivel, atualizado_em, atualizado_por)
                   VALUES (?,?,?,?)
                   ON CONFLICT(template_name) DO UPDATE SET
                     visivel=excluded.visivel,
                     atualizado_em=excluded.atualizado_em,
                     atualizado_por=excluded.atualizado_por""",
                (nome, 1 if nome in visiveis_set else 0, now, usuario),
            )
        # Defaults: limpa os ausentes e regrava os presentes
        conn.execute("DELETE FROM modo_template_default")
        for modo, tpl in modo_defaults.items():
            if tpl:
                conn.execute(
                    """INSERT INTO modo_template_default
                       (modo_envio, template_name, atualizado_em, atualizado_por)
                       VALUES (?,?,?,?)""",
                    (modo, tpl, now, usuario),
                )


def contar_desculpas_por_campanha() -> dict[int, int]:
    """Retorna {campanha_id: qtd_desculpas_enviadas} pra exibir badge na
    tela /wpp/desculpas. Só campanhas que têm ao menos 1 registro."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT campanha_id, COUNT(*) c FROM desculpas_enviadas "
            "GROUP BY campanha_id"
        ).fetchall()
    return {r["campanha_id"]: r["c"] for r in rows}


def envios_da_campanha(campanha_id: int) -> list[dict]:
    """Todos os envios accepted (status='accepted') de uma campanha, ordenados
    por enviado_em DESC. Inclui chat_ticket_id pra cruzar com chat MySQL."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, posto, telefone, idreceita, matricula, nome,
                      ref, valor, venc, dias_atraso, template,
                      status, wamid, chat_ticket_id, enviado_em
                 FROM envios
                WHERE campanha_id=? AND status='accepted'
                ORDER BY enviado_em DESC""",
            (campanha_id,),
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
            "WHERE campanha_id=? AND status LIKE 'accepted%' AND date(enviado_em)=?",
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
            "SELECT MAX(enviado_em) as dt FROM envios WHERE telefone=? AND status LIKE 'accepted%'",
            (telefone,)
        ).fetchone()
    return row["dt"] if row and row["dt"] else None


def ja_enviado_na_campanha(campanha_id: int, telefone: str) -> bool:
    """True se este telefone já recebeu um envio accepted DESTA campanha (em
    qualquer data). Trava de "enviar só 1x por contato" usada pelas campanhas
    com ignorar_intervalo=1 — substitui o intervalo global cross-campanha.
    Sem ela, ignorar o intervalo faria a campanha reenviar a cada rodada."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM envios WHERE campanha_id=? AND telefone=? "
            "AND status LIKE 'accepted%' LIMIT 1",
            (campanha_id, telefone)
        ).fetchone()
    return row is not None


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
                    "WHERE campanha_id=? AND posto=? AND status LIKE 'accepted%'",
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
