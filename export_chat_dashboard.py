#!/usr/bin/env python3
"""
export_chat_dashboard.py — Dashboard do chat camim_chat_production.

Janela: últimos 90 dias (env CHAT_DASH_DIAS).
Cron:   a cada 120 min.
Saída:  json_consolidado/chat_dashboard.json

═══════════════════════════════════════════════════════════════════════════════
BUCKETS (categorias de ticket)
═══════════════════════════════════════════════════════════════════════════════
  inbound_resolvido_camila        — cliente iniciou, sem transfer (Camila resolveu
                                    sozinha ou cliente abandonou no fluxo IA)
  inbound_atendido_humano         — cliente iniciou, transferido p/ fila, humano
                                    respondeu  (FUNIL COMPLETO)
  inbound_transferido_sem_humano  — cliente iniciou, transferido, ninguém pegou
  outbound_sem_reply              — CAMIM iniciou (template/cobrança/lembrete) e
                                    o cliente NUNCA respondeu  ← categoria à parte
  outbound_com_reply              — CAMIM iniciou e o cliente respondeu

═══════════════════════════════════════════════════════════════════════════════
TEMPOS (apenas inbound_atendido_humano para o funil completo)
═══════════════════════════════════════════════════════════════════════════════
  delta_camila_responde   = first_camila   - first_customer
  delta_camila_classifica = first_transfer - first_customer
  delta_espera_humano     = first_human    - first_transfer
  delta_humano_resolve    = closedAt       - first_human
  delta_total             = closedAt       - first_customer

OBS: NÃO usamos t.firstContactAt (vem com valores negativos no DB).
     Tudo é derivado de Message.createdAt.

═══════════════════════════════════════════════════════════════════════════════
FECHAMENTO (v2 — 2026-04-11)
═══════════════════════════════════════════════════════════════════════════════
Derivado da ÚLTIMA mensagem do ticket. A versão anterior (só humano / inatividade /
normal / aberto) enganava: ~70% dos "camila_normal" eram fechamentos automáticos
por timeout de 1h ou 3h que não batiam o regex antigo — mascarando a realidade de
que a Camila quase nunca "resolve sozinha", a maioria das conversas expira.

  humano               — última msg antes de closedAt foi de humano da equipe
                         (algum membro real da operação encerrou ou respondeu por
                         último, independente de ter havido transferência)

  auto_inatividade     — bot mandou um dos 2 templates de inatividade:
                         • "Notamos que seu atendimento ainda está em andamento"
                           (Camila pergunta se ainda precisa; roda 23:58/23:59)
                         • "Estamos encerrando automaticamente este atendimento
                           devido ao tempo de inatividade"

  auto_timeout         — bot mandou template de timeout absoluto:
                         • "atendimento atingiu nosso tempo máximo de 3 horas"
                         • "atendimento atingiu o tempo máximo de 1 hora"
                         (fechamento por duração total, não por ociosidade)

  cliente_sem_retorno  — última mensagem foi do próprio cliente e ninguém
                         respondeu até o closedAt. Cliente mandou e sumiu antes
                         de qualquer reação; depois algum processo fechou.

  bot_sem_conclusao    — residual: última msg foi do bot, SEM bater nenhum regex
                         acima. Inclui: saudações iniciais que o cliente ignorou,
                         "não consigo ouvir áudios", aviso de transferência, e
                         raríssimas despedidas genuínas do bot. NÃO é sinônimo
                         de "Camila resolveu o problema" — é só "o bot falou por
                         último e o ticket fechou sem sinal claro".

  aberto               — sem closedAt

═══════════════════════════════════════════════════════════════════════════════
CRUZAMENTO bucket × fechamento
═══════════════════════════════════════════════════════════════════════════════
O JSON expõe `kpis.matriz_bf` e `matriz_bf_ids` para permitir drill-down de
perguntas como "quantos tickets foram transferidos a humanos mas finalizados
pela Camila por ociosidade" = matriz_bf[inbound_atendido_humano][auto_timeout]
                             + matriz_bf[inbound_atendido_humano][auto_inatividade]
                             + matriz_bf[inbound_transferido_sem_humano][auto_*]
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import pymysql
from dotenv import load_dotenv

# ─── Config ─────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
OUT_PATH = os.path.join(JSON_DIR, "chat_dashboard.json")
JANELA_DIAS = int(os.getenv("CHAT_DASH_DIAS", "90"))

# userId da Camila.ai — concentra ~97% das mensagens de bot
CAMILA_USER_ID = "cmg8cum8g0519jbbm6r9l93f7"

# Regex para detectar fechamento automático por INATIVIDADE (dois templates conhecidos)
RE_AUTO_INATIVIDADE = re.compile(
    r"Notamos que seu atendimento ainda est[áa] em andamento"
    r"|Estamos encerrando automaticamente este atendimento devido ao tempo de inatividade",
    re.IGNORECASE,
)

# Regex para detectar fechamento automático por TIMEOUT de duração (1h / 3h em aberto)
RE_AUTO_TIMEOUT = re.compile(
    r"atendimento atingiu (nosso tempo m[áa]ximo|o tempo m[áa]ximo) de (1 hora|3 horas)",
    re.IGNORECASE,
)


# ─── DB helpers ─────────────────────────────────────────────────────────────
def conn():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    return pymysql.connect(
        host=os.environ["CHAT_MYSQL_HOST"],
        port=int(os.environ.get("CHAT_MYSQL_PORT", 3306)),
        user=os.environ["CHAT_MYSQL_USER"],
        password=os.environ["CHAT_MYSQL_PASSWORD"],
        database=os.environ.get("CHAT_MYSQL_DATABASE", "camim_chat_production"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def fetch(c, sql, params=None):
    with c.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def atomic_write_json(path, payload, compact=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        if compact:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), default=str)
        else:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# ─── SQL ────────────────────────────────────────────────────────────────────
SQL_TICKETS = """
SELECT t.id, t.ticketNumber, t.isActive, t.closedAt, t.customerId,
       t.userProfileId, t.createdAt, t.queueId, t.source, t.score,
       q.name AS queue_nome,
       c.name AS customer_nome
FROM Ticket t
LEFT JOIN Queue q ON q.id = t.queueId
LEFT JOIN Customer c ON c.id = t.customerId
WHERE t.deletedAt IS NULL
  AND t.createdAt >= %s
"""

SQL_MSG_AGG = """
SELECT m.ticketId,
       MIN(m.createdAt) AS first_msg,
       MAX(m.createdAt) AS last_msg,
       COUNT(*)         AS n_msgs,
       SUM(CASE WHEN m.customerId    IS NOT NULL THEN 1 ELSE 0 END) AS n_customer,
       SUM(CASE WHEN m.userProfileId IS NOT NULL THEN 1 ELSE 0 END) AS n_user,
       MIN(CASE WHEN m.customerId    IS NOT NULL THEN m.createdAt END) AS first_customer_at,
       MIN(CASE WHEN m.userProfileId IS NOT NULL THEN m.createdAt END) AS first_user_at,
       MIN(CASE WHEN u.id = %s THEN m.createdAt END) AS first_camila_at,
       MIN(CASE WHEN m.userProfileId IS NOT NULL
                 AND (u.id IS NULL OR (u.id != %s AND (u.sector IS NULL OR u.sector != 'IA')))
                THEN m.createdAt END) AS first_human_at,
       MAX(CASE WHEN m.userProfileId IS NOT NULL
                 AND (u.id IS NULL OR (u.id != %s AND (u.sector IS NULL OR u.sector != 'IA')))
                THEN m.createdAt END) AS last_human_at
FROM Message m
JOIN Ticket t ON t.id = m.ticketId
LEFT JOIN UserProfile up ON up.id = m.userProfileId
LEFT JOIN User u ON u.id = up.userId
WHERE t.deletedAt IS NULL AND m.deletedAt IS NULL
  AND t.createdAt >= %s
GROUP BY m.ticketId
"""

SQL_TRANSFERS = """
SELECT tt.ticketId, tt.queueId, tt.userProfileId, tt.createdAt,
       q.name AS queue_nome
FROM Tickettransfer tt
JOIN Ticket t ON t.id = tt.ticketId
LEFT JOIN Queue q ON q.id = tt.queueId
WHERE tt.deletedAt IS NULL AND t.deletedAt IS NULL
  AND t.createdAt >= %s
ORDER BY tt.createdAt
"""

SQL_EVAL = """
SELECT te.ticketId, te.score, te.obs, te.createdAt
FROM TicketEvaluation te
JOIN Ticket t ON t.id = te.ticketId
WHERE te.deletedAt IS NULL AND t.deletedAt IS NULL
  AND t.createdAt >= %s
"""

# Primeira mensagem do humano em cada ticket — define o "dono"
SQL_HUMAN_FIRST = """
SELECT x.ticketId, x.userId, x.name, x.sector, x.createdAt
FROM (
  SELECT m.ticketId, u.id AS userId, u.name, u.sector, m.createdAt,
         ROW_NUMBER() OVER (PARTITION BY m.ticketId ORDER BY m.createdAt) AS rn
  FROM Message m
  JOIN Ticket t        ON t.id = m.ticketId
  JOIN UserProfile up  ON up.id = m.userProfileId
  JOIN User u          ON u.id = up.userId
  WHERE t.deletedAt IS NULL AND m.deletedAt IS NULL
    AND t.createdAt >= %s
    AND u.id != %s
    AND (u.sector IS NULL OR u.sector != 'IA')
) x WHERE x.rn = 1
"""

# Última mensagem de cada ticket — usada para detectar fechamento por inatividade
SQL_LAST_MSG = """
SELECT x.ticketId, x.body, x.createdAt, x.userProfileId, x.customerId, x.sector, x.userId
FROM (
  SELECT m.ticketId, m.body, m.createdAt, m.userProfileId, m.customerId,
         u.sector, u.id AS userId,
         ROW_NUMBER() OVER (PARTITION BY m.ticketId ORDER BY m.createdAt DESC) AS rn
  FROM Message m
  JOIN Ticket t        ON t.id = m.ticketId
  LEFT JOIN UserProfile up ON up.id = m.userProfileId
  LEFT JOIN User u         ON u.id = up.userId
  WHERE t.deletedAt IS NULL AND m.deletedAt IS NULL
    AND t.createdAt >= %s
) x WHERE x.rn = 1
"""

# Última mensagem de HUMANO REAL (não Camila, não sector IA). Usada para saber
# se o dono do ticket foi o mesmo que encerrou, ou se passou pra frente.
SQL_LAST_HUMAN = """
SELECT x.ticketId, x.userId, x.name, x.createdAt
FROM (
  SELECT m.ticketId, u.id AS userId, u.name, m.createdAt,
         ROW_NUMBER() OVER (PARTITION BY m.ticketId ORDER BY m.createdAt DESC) AS rn
  FROM Message m
  JOIN Ticket t        ON t.id = m.ticketId
  JOIN UserProfile up  ON up.id = m.userProfileId
  JOIN User u          ON u.id = up.userId
  WHERE t.deletedAt IS NULL AND m.deletedAt IS NULL
    AND t.createdAt >= %s
    AND u.id != %s
    AND (u.sector IS NULL OR u.sector != 'IA')
) x WHERE x.rn = 1
"""


# ─── ETL ────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"=== Chat Dashboard ETL — {datetime.now().isoformat(timespec='seconds')} ===")
    print(f"  Janela:    {JANELA_DIAS} dias")
    print(f"  Saída:     {OUT_PATH}")

    corte = (datetime.now() - timedelta(days=JANELA_DIAS)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  Corte:     {corte}")

    c = conn()
    try:
        print("  → tickets...")
        tickets = pd.DataFrame(fetch(c, SQL_TICKETS, (corte,)))
        print(f"    {len(tickets):,}")

        if tickets.empty:
            atomic_write_json(OUT_PATH, {
                "meta": {"gerado_em": datetime.now().isoformat(timespec="seconds"),
                         "vazio": True, "janela_dias": JANELA_DIAS}
            })
            return

        print("  → msg_agg...")
        msgs = pd.DataFrame(fetch(c, SQL_MSG_AGG,
            (CAMILA_USER_ID, CAMILA_USER_ID, CAMILA_USER_ID, corte)))
        print(f"    {len(msgs):,}")

        print("  → transfers...")
        transf = pd.DataFrame(fetch(c, SQL_TRANSFERS, (corte,)))
        print(f"    {len(transf):,}")

        print("  → evaluations...")
        evals = pd.DataFrame(fetch(c, SQL_EVAL, (corte,)))
        print(f"    {len(evals):,}")

        print("  → first human msg...")
        humans = pd.DataFrame(fetch(c, SQL_HUMAN_FIRST, (corte, CAMILA_USER_ID)))
        print(f"    {len(humans):,}")

        print("  → last msg...")
        last = pd.DataFrame(fetch(c, SQL_LAST_MSG, (corte,)))
        print(f"    {len(last):,}")

        print("  → last human msg...")
        last_human = pd.DataFrame(fetch(c, SQL_LAST_HUMAN, (corte, CAMILA_USER_ID)))
        print(f"    {len(last_human):,}")
    finally:
        c.close()

    print("  → build payload...")
    payload = build_payload(tickets, msgs, transf, evals, humans, last, last_human)
    # JSON compacto: tickets_index sozinho pesa ~10 MB com indent=2
    atomic_write_json(OUT_PATH, payload, compact=True)
    print(f"  ✔ {OUT_PATH}")
    print(f"  ⏱  {time.time() - t0:.1f}s")


# ─── Pandas pipeline ────────────────────────────────────────────────────────
def build_payload(tickets, msgs, transf, evals, humans, last, last_human):
    # ── Merge base ticket + msg agg ──
    df = tickets.merge(msgs, left_on="id", right_on="ticketId", how="left")
    df = df.drop(columns=["ticketId"], errors="ignore")

    dt_cols = ["createdAt", "closedAt", "first_msg", "last_msg",
               "first_customer_at", "first_user_at", "first_camila_at",
               "first_human_at", "last_human_at"]
    for col in dt_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["n_msgs", "n_customer", "n_user"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0).astype(int)

    # ── Transfers ──
    if not transf.empty:
        transf["createdAt"] = pd.to_datetime(transf["createdAt"], errors="coerce")
        first_tr = (
            transf.sort_values("createdAt")
                  .groupby("ticketId")
                  .agg(first_transfer_at=("createdAt", "min"),
                       last_transfer_queue=("queue_nome", "last"),
                       n_transfers=("createdAt", "count"))
                  .reset_index()
        )
        df = df.merge(first_tr, left_on="id", right_on="ticketId", how="left")
        df = df.drop(columns=["ticketId"], errors="ignore")
    else:
        df["first_transfer_at"] = pd.NaT
        df["last_transfer_queue"] = None
        df["n_transfers"] = 0
    df["n_transfers"] = df["n_transfers"].fillna(0).astype(int)

    # ── Primeiro humano (dono) ──
    if not humans.empty:
        humans = humans.drop_duplicates("ticketId")
        df = df.merge(
            humans[["ticketId", "userId", "name", "sector"]].rename(
                columns={"userId": "human_userId", "name": "human_name", "sector": "human_sector"}
            ),
            left_on="id", right_on="ticketId", how="left"
        )
        df = df.drop(columns=["ticketId"], errors="ignore")
    else:
        df["human_userId"] = None
        df["human_name"] = None
        df["human_sector"] = None

    # ── Última mensagem ──
    if not last.empty:
        last = last.drop_duplicates("ticketId")
        df = df.merge(
            last[["ticketId", "body", "userProfileId", "customerId", "sector", "userId"]].rename(
                columns={"body": "last_body", "userProfileId": "last_userProfileId",
                         "customerId": "last_customerId", "sector": "last_sector",
                         "userId": "last_userId"}
            ),
            left_on="id", right_on="ticketId", how="left"
        )
        df = df.drop(columns=["ticketId"], errors="ignore")
    else:
        df["last_body"] = None
        df["last_userProfileId"] = None
        df["last_customerId"] = None
        df["last_sector"] = None
        df["last_userId"] = None

    # ── Último humano real a falar (p/ saber se dono == quem fechou) ──
    if not last_human.empty:
        last_human = last_human.drop_duplicates("ticketId")
        df = df.merge(
            last_human[["ticketId", "userId", "name"]].rename(
                columns={"userId": "last_human_userId", "name": "last_human_name"}
            ),
            left_on="id", right_on="ticketId", how="left"
        )
        df = df.drop(columns=["ticketId"], errors="ignore")
    else:
        df["last_human_userId"] = None
        df["last_human_name"] = None

    # ── Avaliações (última por ticket) ──
    if not evals.empty:
        evals["createdAt"] = pd.to_datetime(evals["createdAt"], errors="coerce")
        evals_u = evals.sort_values("createdAt").drop_duplicates("ticketId", keep="last")
        df = df.merge(
            evals_u[["ticketId", "score", "obs"]].rename(
                columns={"score": "eval_score", "obs": "eval_obs"}
            ),
            left_on="id", right_on="ticketId", how="left"
        )
        df = df.drop(columns=["ticketId"], errors="ignore")
    else:
        df["eval_score"] = None
        df["eval_obs"] = None

    # ── BUCKET ──
    def classify(r):
        nc = int(r["n_customer"])
        nu = int(r["n_user"])
        first_cust = r["first_customer_at"]
        first_user = r["first_user_at"]
        n_tr = int(r["n_transfers"])
        has_human = pd.notna(r["first_human_at"])

        # Sem nenhuma msg → ticket vazio (raro, conta como aberto)
        if nc == 0 and nu == 0:
            return "vazio"

        # Caso outbound: não há msg do cliente → CAMIM disparou e ninguém respondeu
        if nc == 0 and nu > 0:
            return "outbound_sem_reply"

        # Caso outbound com reply: primeira msg foi do user-side e ANTES da msg do cliente
        if pd.notna(first_user) and pd.notna(first_cust) and first_user < first_cust:
            return "outbound_com_reply"

        # Inbound — cliente iniciou
        if n_tr == 0:
            return "inbound_resolvido_camila"
        if has_human:
            return "inbound_atendido_humano"
        return "inbound_transferido_sem_humano"

    df["bucket"] = df.apply(classify, axis=1)

    # ── DELTAS (segundos) ──
    def delta(a, b):
        if pd.isna(a) or pd.isna(b):
            return None
        d = (a - b).total_seconds()
        if d < 0 or d > 60 * 60 * 24 * 30:  # > 30d ignora (outlier)
            return None
        return float(d)

    df["delta_camila_responde"]   = df.apply(lambda r: delta(r["first_camila_at"],   r["first_customer_at"]), axis=1)
    df["delta_camila_classifica"] = df.apply(lambda r: delta(r["first_transfer_at"], r["first_customer_at"]), axis=1)
    df["delta_espera_humano"]     = df.apply(lambda r: delta(r["first_human_at"],    r["first_transfer_at"]), axis=1)
    df["delta_humano_resolve"]    = df.apply(lambda r: delta(r["closedAt"],          r["first_human_at"]),    axis=1)
    df["delta_total"]             = df.apply(lambda r: delta(r["closedAt"],          r["first_customer_at"]), axis=1)

    # ── FECHAMENTO (v2, derivado da ÚLTIMA mensagem; ver docstring) ──
    def fechamento(r):
        if pd.isna(r["closedAt"]):
            return "aberto"

        last_uid = r.get("last_userId")
        last_sector = r.get("last_sector") or ""
        last_uprofile = r.get("last_userProfileId")
        last_customer = r.get("last_customerId")
        body = r.get("last_body") or ""
        if not isinstance(body, str):
            body = ""

        # Humano foi o último a falar — independe de tudo
        is_bot_last = (
            pd.notna(last_uprofile)
            and (last_uid == CAMILA_USER_ID or last_sector == "IA")
        )
        is_human_last = pd.notna(last_uprofile) and not is_bot_last
        if is_human_last:
            return "humano"

        # Cliente foi o último a falar
        if pd.notna(last_customer) and not pd.notna(last_uprofile):
            return "cliente_sem_retorno"

        # Última msg veio do bot → classifica por conteúdo
        if is_bot_last:
            if RE_AUTO_INATIVIDADE.search(body):
                return "auto_inatividade"
            if RE_AUTO_TIMEOUT.search(body):
                return "auto_timeout"
            return "bot_sem_conclusao"

        # Sem msg nenhuma (raro) — ticket fechou sem histórico
        return "bot_sem_conclusao"

    df["fechamento"] = df.apply(fechamento, axis=1)

    # ── Fila efetiva (último transfer ou queue atual) ──
    df["fila_efetiva"] = df["last_transfer_queue"].fillna(df["queue_nome"])

    # ============================================================================
    # AGREGAÇÕES
    # ============================================================================
    gerado_em = datetime.now()

    def stats(series):
        s = pd.to_numeric(series, errors="coerce").dropna()
        if s.empty:
            return {"n": 0, "media": None, "p50": None, "p95": None}
        return {
            "n":     int(len(s)),
            "media": round(float(s.mean()), 1),
            "p50":   round(float(s.median()), 1),
            "p95":   round(float(s.quantile(0.95)), 1),
        }

    total = int(len(df))
    bk = {k: int(v) for k, v in df["bucket"].value_counts().to_dict().items()}
    fc = {k: int(v) for k, v in df["fechamento"].value_counts().to_dict().items()}
    avaliados = int(df["eval_score"].notna().sum())
    nota_media = float(pd.to_numeric(df["eval_score"], errors="coerce").dropna().mean()) if avaliados else None

    inbound_human = df[df["bucket"] == "inbound_atendido_humano"]
    funil = {
        "camila_responde":   stats(df["delta_camila_responde"]),
        "camila_classifica": stats(df["delta_camila_classifica"]),
        "espera_humano":     stats(inbound_human["delta_espera_humano"]),
        "humano_resolve":    stats(inbound_human["delta_humano_resolve"]),
        "total":             stats(inbound_human["delta_total"]),
    }

    # ============================================================================
    # DRILL-DOWN: tickets_index + ids por agregação
    # ============================================================================
    def ids_of(d):
        return [str(x) for x in d["id"].tolist()]

    # ── Índice compacto de tickets (keys curtas pra reduzir tamanho do JSON) ──
    def _s(v, maxlen=None):
        """Converte pra string; NaN/None → ''."""
        if v is None or (isinstance(v, float) and pd.isna(v)) or (hasattr(pd, "isna") and pd.isna(v) is True):
            return ""
        try:
            if pd.isna(v):
                return ""
        except (TypeError, ValueError):
            pass
        s = str(v).strip()
        if s.lower() == "nan":
            return ""
        return s[:maxlen] if maxlen else s

    tickets_index = {}
    for _, r in df.iterrows():
        cr = r.get("createdAt")
        cl = r.get("closedAt")
        tickets_index[str(r["id"])] = {
            "n": int(r["ticketNumber"]) if pd.notna(r.get("ticketNumber")) else None,
            "c": _s(r.get("customer_nome"), 60),
            "f": _s(r.get("fila_efetiva")),
            "b": _s(r.get("bucket")),
            "z": _s(r.get("fechamento")),
            "d": cr.strftime("%d/%m/%Y %H:%M") if pd.notna(cr) else "",
            "e": cl.strftime("%d/%m/%Y %H:%M") if pd.notna(cl) else "",
            "h": _s(r.get("human_name")),
            "s": int(r["eval_score"]) if pd.notna(r.get("eval_score")) else None,
            "o": _s(r.get("eval_obs"), 200),
        }

    # IDs agrupados por bucket e por tipo de fechamento
    bucket_ids = {
        str(k): ids_of(df[df["bucket"] == k])
        for k in df["bucket"].dropna().unique()
    }
    fechamento_ids = {
        str(k): ids_of(df[df["fechamento"] == k])
        for k in df["fechamento"].dropna().unique()
    }

    # ── Matriz de cruzamento bucket × fechamento ──
    # Responde perguntas do tipo: "quantos tickets transferidos foram fechados
    # pela Camila por ociosidade?" sem precisar fazer interseção de sets no front.
    matriz_bf = {}
    matriz_bf_ids = {}
    for _bucket_key in df["bucket"].dropna().unique():
        sub = df[df["bucket"] == _bucket_key]
        matriz_bf[str(_bucket_key)] = {
            str(fk): int(v)
            for fk, v in sub["fechamento"].value_counts().to_dict().items()
        }
        matriz_bf_ids[str(_bucket_key)] = {
            str(fk): ids_of(sub[sub["fechamento"] == fk])
            for fk in sub["fechamento"].dropna().unique()
        }

    # ── Por fila ──
    por_fila = []
    for fila, g in df[df["fila_efetiva"].notna()].groupby("fila_efetiva"):
        gh = g[g["bucket"] == "inbound_atendido_humano"]
        eval_scores = pd.to_numeric(g["eval_score"], errors="coerce").dropna()
        por_fila.append({
            "fila": fila,
            "tickets": int(len(g)),
            "atendidos_humano": int(len(gh)),
            "espera_humano_med":   round(float(gh["delta_espera_humano"].dropna().mean()), 1) if gh["delta_espera_humano"].notna().any() else None,
            "humano_resolve_med": round(float(gh["delta_humano_resolve"].dropna().mean()), 1) if gh["delta_humano_resolve"].notna().any() else None,
            "resolucao_med":       round(float(gh["delta_total"].dropna().mean()), 1) if gh["delta_total"].notna().any() else None,
            "fechado_auto":       int(g["fechamento"].isin(["auto_timeout", "auto_inatividade"]).sum()),
            "fechado_bot_outro":  int((g["fechamento"] == "bot_sem_conclusao").sum()),
            "fechado_cli_abandono": int((g["fechamento"] == "cliente_sem_retorno").sum()),
            "fechado_humano":     int((g["fechamento"] == "humano").sum()),
            "abertos":            int((g["fechamento"] == "aberto").sum()),
            "nota_media":         round(float(eval_scores.mean()), 2) if not eval_scores.empty else None,
            "n_avaliacoes":       int(len(eval_scores)),
            "ids":                ids_of(g),
        })
    por_fila.sort(key=lambda x: x["tickets"], reverse=True)

    # ── Por usuário humano ──
    # Cada linha mostra *o que o atendente realmente entregou* — não basta ver
    # quantos tickets ele pegou, é preciso saber:
    #   • quantos ele FECHOU ativamente (última msg foi dele mesmo)
    #   • quantos a Camila fechou por ele (auto_timeout / auto_inatividade)
    #   • quantos o cliente abandonou (não necessariamente culpa dele)
    #   • quantos ele passou pra frente (outro humano entrou e fechou)
    #   • tempo de resolução SEPARADO por tipo de fechamento
    def med(series):
        s = pd.to_numeric(series, errors="coerce").dropna()
        return round(float(s.median()), 1) if not s.empty else None

    por_user = []
    for uid, g in df[df["human_userId"].notna()].groupby("human_userId"):
        n_total = int(len(g))
        gh = g[g["bucket"] == "inbound_atendido_humano"]
        eval_scores = pd.to_numeric(g["eval_score"], errors="coerce").dropna()

        # Splits por tipo de fechamento (todos considerando tickets em que esse
        # usuário foi o *dono* do ticket = primeiro humano a mandar msg)
        fech_humano_mask = (g["fechamento"] == "humano")
        same_closer      = fech_humano_mask & (g["last_human_userId"] == uid)
        other_closer     = fech_humano_mask & g["last_human_userId"].notna() & (g["last_human_userId"] != uid)
        fechou_sozinho      = g[same_closer]     # entregou sozinho
        passou_pra_outro    = g[other_closer]    # pegou e empurrou pra colega
        camila_encerrou     = g[g["fechamento"].isin(["auto_timeout", "auto_inatividade"])]
        cli_abandonou       = g[g["fechamento"] == "cliente_sem_retorno"]
        bot_residual        = g[g["fechamento"] == "bot_sem_conclusao"]
        abertos_df          = g[g["fechamento"] == "aberto"]

        n_fechou   = int(len(fechou_sozinho))
        n_passou   = int(len(passou_pra_outro))
        n_camila   = int(len(camila_encerrou))
        n_cliaban  = int(len(cli_abandonou))
        n_bot      = int(len(bot_residual))
        n_abertos  = int(len(abertos_df))

        efetividade = round(n_fechou / n_total * 100, 1) if n_total else None

        por_user.append({
            "userId": uid,
            "nome":   g["human_name"].iloc[0],
            "sector": g["human_sector"].iloc[0],
            "tickets": n_total,
            # métricas de produtividade real
            "fechou_sozinho":    n_fechou,
            "efetividade_pct":   efetividade,
            "passou_pra_outro":  n_passou,
            "camila_encerrou":   n_camila,
            "cli_abandonou":     n_cliaban,
            "bot_residual":      n_bot,
            "abertos":           n_abertos,
            # tempos separados
            "primeira_resposta_med": round(float(gh["delta_espera_humano"].dropna().mean()), 1) if gh["delta_espera_humano"].notna().any() else None,
            "tempo_resolucao_humano_med": med(fechou_sozinho["delta_total"]),
            "tempo_ate_camila_fechar_med": med(camila_encerrou["delta_total"]),
            # avaliação
            "nota_media":    round(float(eval_scores.mean()), 2) if not eval_scores.empty else None,
            "n_avaliacoes":  int(len(eval_scores)),
            # ids para drill-down por categoria
            "ids":                 ids_of(g),
            "ids_fechou_sozinho":  ids_of(fechou_sozinho),
            "ids_passou_pra_outro": ids_of(passou_pra_outro),
            "ids_camila_encerrou": ids_of(camila_encerrou),
            "ids_cli_abandonou":   ids_of(cli_abandonou),
            "ids_bot_residual":    ids_of(bot_residual),
            "ids_abertos":         ids_of(abertos_df),
        })
    por_user.sort(key=lambda x: x["tickets"], reverse=True)

    # ── Heatmap hora x dow (0=seg) ──
    df["hora"] = df["createdAt"].dt.hour
    df["dow"] = df["createdAt"].dt.weekday
    heatmap = []
    for (h, d), g in df.dropna(subset=["hora", "dow"]).groupby(["hora", "dow"]):
        heatmap.append({
            "hora": int(h), "dow": int(d),
            "tickets": int(len(g)),
            "ids": ids_of(g),
        })

    # ── Série diária últimos 30d ──
    df["data"] = df["createdAt"].dt.strftime("%Y-%m-%d")
    df["data_br"] = df["createdAt"].dt.strftime("%d/%m/%Y")
    cutoff_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    df30 = df[df["data"] >= cutoff_30]
    serie = []
    for d, g in df30.groupby("data"):
        gh = g[g["bucket"] == "inbound_atendido_humano"]
        data_br = g["data_br"].iloc[0] if not g.empty else d
        serie.append({
            "data": d,
            "data_br": data_br,
            "total": int(len(g)),
            "inbound_camila":     int((g["bucket"] == "inbound_resolvido_camila").sum()),
            "inbound_humano":     int((g["bucket"] == "inbound_atendido_humano").sum()),
            "inbound_pendente":   int((g["bucket"] == "inbound_transferido_sem_humano").sum()),
            "outbound_sem_reply": int((g["bucket"] == "outbound_sem_reply").sum()),
            "outbound_com_reply": int((g["bucket"] == "outbound_com_reply").sum()),
            "tempo_total_med":    round(float(gh["delta_total"].dropna().mean()), 1) if gh["delta_total"].notna().any() else None,
            "ids":                ids_of(g),
        })
    serie.sort(key=lambda x: x["data"])

    # ── Por source ──
    por_source = []
    for src, g in df.groupby("source", dropna=False):
        por_source.append({
            "source": src if pd.notna(src) else "(null)",
            "tickets": int(len(g)),
            "buckets": {k: int(v) for k, v in g["bucket"].value_counts().to_dict().items()},
            "ids": ids_of(g),
        })
    por_source.sort(key=lambda x: x["tickets"], reverse=True)

    # ── Distribuição de notas ──
    notas_dist = []
    if avaliados:
        scores_num = pd.to_numeric(df["eval_score"], errors="coerce")
        for n, cnt in scores_num.dropna().astype(int).value_counts().sort_index().items():
            notas_dist.append({
                "nota": int(n),
                "n": int(cnt),
                "ids": ids_of(df[scores_num == n]),
            })

    # ── Comentários recentes (até 20) ──
    coms_df = df[df["eval_obs"].notna()].copy()
    coms_df["obs_str"] = coms_df["eval_obs"].astype(str).str.strip()
    coms_df = coms_df[coms_df["obs_str"].str.len() > 0]
    coms_df = coms_df.sort_values("createdAt", ascending=False).head(20)
    coms = [{
        "id":      str(r["id"]),
        "ticket":  int(r["ticketNumber"]) if pd.notna(r["ticketNumber"]) else None,
        "nota":    int(r["eval_score"]) if pd.notna(r["eval_score"]) else None,
        "obs":     r["obs_str"][:300],
        "fila":    r["fila_efetiva"],
        "data":    r["createdAt"].strftime("%d/%m/%Y %H:%M") if pd.notna(r["createdAt"]) else None,
    } for _, r in coms_df.iterrows()]

    # ── Outbound — top filas/templates ──
    outbound = df[df["bucket"].isin(["outbound_sem_reply", "outbound_com_reply"])]
    outbound_filas = []
    for fila, g in outbound[outbound["fila_efetiva"].notna()].groupby("fila_efetiva"):
        sr = int((g["bucket"] == "outbound_sem_reply").sum())
        cr = int((g["bucket"] == "outbound_com_reply").sum())
        outbound_filas.append({
            "fila": fila,
            "total": int(len(g)),
            "sem_reply": sr,
            "com_reply": cr,
            "taxa_reply": round(cr / len(g) * 100, 1) if len(g) else 0.0,
            "ids":       ids_of(g),
            "ids_sem_reply": ids_of(g[g["bucket"] == "outbound_sem_reply"]),
            "ids_com_reply": ids_of(g[g["bucket"] == "outbound_com_reply"]),
        })
    outbound_filas.sort(key=lambda x: x["total"], reverse=True)

    return {
        "meta": {
            "gerado_em":    gerado_em.isoformat(timespec="seconds"),
            "gerado_em_br": gerado_em.strftime("%d/%m/%Y %H:%M"),
            "janela_dias":  JANELA_DIAS,
            "fonte":        "camim_chat_production (MySQL)",
        },
        "kpis": {
            "total_tickets": total,
            "buckets":       bk,
            "fechamento":    fc,
            "matriz_bf":     matriz_bf,
            "avaliados":     avaliados,
            "nota_media":    round(nota_media, 2) if nota_media is not None else None,
        },
        "funil_segundos":      funil,
        "por_fila":            por_fila,
        "por_usuario":         por_user,
        "por_source":          por_source,
        "heatmap":             heatmap,
        "serie_30d":           serie,
        "notas_dist":          notas_dist,
        "comentarios_recentes": coms,
        "outbound": {
            "total":         int(len(outbound)),
            "sem_reply":     int((outbound["bucket"] == "outbound_sem_reply").sum()),
            "com_reply":     int((outbound["bucket"] == "outbound_com_reply").sum()),
            "ids_sem_reply": ids_of(outbound[outbound["bucket"] == "outbound_sem_reply"]),
            "ids_com_reply": ids_of(outbound[outbound["bucket"] == "outbound_com_reply"]),
            "por_fila":      outbound_filas,
        },
        "tickets_index":  tickets_index,
        "bucket_ids":     bucket_ids,
        "fechamento_ids": fechamento_ids,
        "matriz_bf_ids":  matriz_bf_ids,
    }


if __name__ == "__main__":
    main()
