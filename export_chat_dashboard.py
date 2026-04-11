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
FECHAMENTO
═══════════════════════════════════════════════════════════════════════════════
  humano               — última mensagem antes de closedAt foi de humano
  camila_inatividade   — última msg do bot bate o regex do script de inatividade
                         (Camila roda 23:58/23:59 todo dia fechando inativos)
  camila_normal        — fechado pelo lado bot por outro motivo
  aberto               — sem closedAt
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

# Padrão do script de fechamento por inatividade da Camila
CAMILA_INATIVIDADE_RE = re.compile(
    r"Notamos que seu atendimento ainda est[áa] em andamento", re.IGNORECASE
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


def atomic_write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
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
    finally:
        c.close()

    print("  → build payload...")
    payload = build_payload(tickets, msgs, transf, evals, humans, last)
    atomic_write_json(OUT_PATH, payload)
    print(f"  ✔ {OUT_PATH}")
    print(f"  ⏱  {time.time() - t0:.1f}s")


# ─── Pandas pipeline ────────────────────────────────────────────────────────
def build_payload(tickets, msgs, transf, evals, humans, last):
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

    # ── FECHAMENTO ──
    def fechamento(r):
        if pd.isna(r["closedAt"]):
            return "aberto"
        body = r.get("last_body") or ""
        if isinstance(body, str) and CAMILA_INATIVIDADE_RE.search(body):
            return "camila_inatividade"
        last_uid = r.get("last_userId")
        last_sector = r.get("last_sector") or ""
        if last_uid and last_uid != CAMILA_USER_ID and last_sector != "IA":
            return "humano"
        return "camila_normal"
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
            "fechado_camila":     int(g["fechamento"].isin(["camila_normal", "camila_inatividade"]).sum()),
            "fechado_humano":     int((g["fechamento"] == "humano").sum()),
            "abertos":            int((g["fechamento"] == "aberto").sum()),
            "nota_media":         round(float(eval_scores.mean()), 2) if not eval_scores.empty else None,
            "n_avaliacoes":       int(len(eval_scores)),
        })
    por_fila.sort(key=lambda x: x["tickets"], reverse=True)

    # ── Por usuário humano ──
    por_user = []
    for uid, g in df[df["human_userId"].notna()].groupby("human_userId"):
        gh = g[g["bucket"] == "inbound_atendido_humano"]
        eval_scores = pd.to_numeric(g["eval_score"], errors="coerce").dropna()
        por_user.append({
            "userId": uid,
            "nome": g["human_name"].iloc[0],
            "sector": g["human_sector"].iloc[0],
            "tickets": int(len(g)),
            "primeira_resposta_med": round(float(gh["delta_espera_humano"].dropna().mean()), 1) if gh["delta_espera_humano"].notna().any() else None,
            "resolucao_med":         round(float(gh["delta_total"].dropna().mean()), 1) if gh["delta_total"].notna().any() else None,
            "nota_media":            round(float(eval_scores.mean()), 2) if not eval_scores.empty else None,
            "n_avaliacoes":          int(len(eval_scores)),
        })
    por_user.sort(key=lambda x: x["tickets"], reverse=True)

    # ── Heatmap hora x dow (0=seg) ──
    df["hora"] = df["createdAt"].dt.hour
    df["dow"] = df["createdAt"].dt.weekday
    heatmap = []
    for (h, d), g in df.dropna(subset=["hora", "dow"]).groupby(["hora", "dow"]):
        heatmap.append({"hora": int(h), "dow": int(d), "tickets": int(len(g))})

    # ── Série diária últimos 30d ──
    df["data"] = df["createdAt"].dt.strftime("%Y-%m-%d")
    cutoff_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    df30 = df[df["data"] >= cutoff_30]
    serie = []
    for d, g in df30.groupby("data"):
        gh = g[g["bucket"] == "inbound_atendido_humano"]
        serie.append({
            "data": d,
            "total": int(len(g)),
            "inbound_camila":     int((g["bucket"] == "inbound_resolvido_camila").sum()),
            "inbound_humano":     int((g["bucket"] == "inbound_atendido_humano").sum()),
            "inbound_pendente":   int((g["bucket"] == "inbound_transferido_sem_humano").sum()),
            "outbound_sem_reply": int((g["bucket"] == "outbound_sem_reply").sum()),
            "outbound_com_reply": int((g["bucket"] == "outbound_com_reply").sum()),
            "tempo_total_med":    round(float(gh["delta_total"].dropna().mean()), 1) if gh["delta_total"].notna().any() else None,
        })
    serie.sort(key=lambda x: x["data"])

    # ── Por source ──
    por_source = []
    for src, g in df.groupby("source", dropna=False):
        por_source.append({
            "source": src if pd.notna(src) else "(null)",
            "tickets": int(len(g)),
            "buckets": {k: int(v) for k, v in g["bucket"].value_counts().to_dict().items()},
        })
    por_source.sort(key=lambda x: x["tickets"], reverse=True)

    # ── Distribuição de notas ──
    notas_dist = []
    if avaliados:
        for n, cnt in pd.to_numeric(df["eval_score"], errors="coerce").dropna().astype(int).value_counts().sort_index().items():
            notas_dist.append({"nota": int(n), "n": int(cnt)})

    # ── Comentários recentes (até 20) ──
    coms_df = df[df["eval_obs"].notna()].copy()
    coms_df["obs_str"] = coms_df["eval_obs"].astype(str).str.strip()
    coms_df = coms_df[coms_df["obs_str"].str.len() > 0]
    coms_df = coms_df.sort_values("createdAt", ascending=False).head(20)
    coms = [{
        "ticket":  int(r["ticketNumber"]) if pd.notna(r["ticketNumber"]) else None,
        "nota":    int(r["eval_score"]) if pd.notna(r["eval_score"]) else None,
        "obs":     r["obs_str"][:300],
        "fila":    r["fila_efetiva"],
        "data":    r["createdAt"].strftime("%Y-%m-%d %H:%M") if pd.notna(r["createdAt"]) else None,
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
            "por_fila":      outbound_filas,
        },
    }


if __name__ == "__main__":
    main()
