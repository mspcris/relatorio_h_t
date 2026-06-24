#!/usr/bin/env python3
"""
export_chat_abertos.py — ETL LEVE só dos tickets EM ABERTO do chat.

Motivo: o export_chat_dashboard.py completo leva ~3,5 min (reconstrói 90 dias
inteiros: varre 1,2M mensagens 4x + window functions sem índice + pandas
row-apply). Rodar isso a cada 15 min sobrecarrega o camim_chat_production.

A aba "Tickets em aberto" / "Abertos +24h" precisa só dos tickets abertos
(~100-200), e esses sim devem estar frescos a cada 15 min. Este script faz
APENAS isso: 3 queries restritas a `closedAt IS NULL` (poucas centenas de
tickets → segundos), sem window functions.

Janela: últimos 90 dias (env CHAT_DASH_DIAS), igual ao ETL completo.
Cron:   a cada 15 min (/etc/cron.d/relatorio_ht).
Saída:  json_consolidado/chat_abertos.json  (pequeno, ~dezenas de KB)

Campos por ticket (mesmas keys curtas do tickets_index do ETL completo, para
o front reaproveitar a lógica): id(chave), n, c, f, b, d
  b (bucket) classificado igual ao build_payload do ETL completo, mas só com
  msg_agg + transfers (sem last_msg/last_human, que aqui não são necessários).
"""

import json
import os
import time
from datetime import datetime, timedelta

import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
OUT_PATH = os.path.join(JSON_DIR, "chat_abertos.json")
JANELA_DIAS = int(os.getenv("CHAT_DASH_DIAS", "90"))

# userId da Camila.ai (bot) — mesmo do ETL completo
CAMILA_USER_ID = "cmg8cum8g0519jbbm6r9l93f7"

# O banco do chat (camim_chat_production) grava timestamps em UTC. A página e o
# "Atualizado em" usam horário de Brasília. Brasil = UTC-3 fixo (sem horário de
# verão desde 2019), então convertemos UTC→BRT subtraindo 3h ao formatar.
BRT_OFFSET = timedelta(hours=-3)


def to_brt(dt):
    """datetime UTC (naive, vindo do MySQL) → naive em horário de Brasília."""
    return (dt + BRT_OFFSET) if hasattr(dt, "strftime") else None


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


# Tickets ABERTOS (closedAt IS NULL) — restringe tudo a poucas centenas de linhas
SQL_OPEN = """
SELECT t.id, t.ticketNumber, t.createdAt, q.name AS queue_nome, c.name AS customer_nome
FROM Ticket t
LEFT JOIN Queue q    ON q.id = t.queueId
LEFT JOIN Customer c ON c.id = t.customerId
WHERE t.deletedAt IS NULL AND t.closedAt IS NULL AND t.createdAt >= %s
"""

# Agregado de mensagens — só dos tickets abertos (JOIN filtra closedAt IS NULL)
SQL_OPEN_MSG = """
SELECT m.ticketId,
       SUM(CASE WHEN m.customerId    IS NOT NULL THEN 1 ELSE 0 END) AS n_customer,
       SUM(CASE WHEN m.userProfileId IS NOT NULL THEN 1 ELSE 0 END) AS n_user,
       MIN(CASE WHEN m.customerId    IS NOT NULL THEN m.createdAt END) AS first_customer_at,
       MIN(CASE WHEN m.userProfileId IS NOT NULL THEN m.createdAt END) AS first_user_at,
       MIN(CASE WHEN m.userProfileId IS NOT NULL
                 AND (u.id IS NULL OR (u.id != %s AND (u.sector IS NULL OR u.sector != 'IA')))
                THEN m.createdAt END) AS first_human_at
FROM Message m
JOIN Ticket t ON t.id = m.ticketId
LEFT JOIN UserProfile up ON up.id = m.userProfileId
LEFT JOIN User u         ON u.id = up.userId
WHERE m.deletedAt IS NULL AND t.deletedAt IS NULL AND t.closedAt IS NULL
  AND t.createdAt >= %s
GROUP BY m.ticketId
"""

# Transfers — só dos tickets abertos. Usa a última fila como "fila efetiva".
SQL_OPEN_TR = """
SELECT tt.ticketId, tt.createdAt, q.name AS queue_nome
FROM Tickettransfer tt
JOIN Ticket t        ON t.id = tt.ticketId
LEFT JOIN Queue q    ON q.id = tt.queueId
WHERE tt.deletedAt IS NULL AND t.deletedAt IS NULL AND t.closedAt IS NULL
  AND t.createdAt >= %s
ORDER BY tt.createdAt
"""


def classify(nc, nu, first_cust, first_user, n_tr, has_human):
    """Mesma lógica do build_payload do ETL completo."""
    if nc == 0 and nu == 0:
        return "vazio"
    if nc == 0 and nu > 0:
        return "outbound_sem_reply"
    if first_user is not None and first_cust is not None and first_user < first_cust:
        return "outbound_com_reply"
    if n_tr == 0:
        return "inbound_resolvido_camila"
    if has_human:
        return "inbound_atendido_humano"
    return "inbound_transferido_sem_humano"


def main():
    t0 = time.time()
    corte = (datetime.now() - timedelta(days=JANELA_DIAS)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"=== Chat ABERTOS ETL — {datetime.now().isoformat(timespec='seconds')} === corte={corte}")

    c = conn()
    try:
        tickets = fetch(c, SQL_OPEN, (corte,))
        msgs    = fetch(c, SQL_OPEN_MSG, (CAMILA_USER_ID, corte))
        transf  = fetch(c, SQL_OPEN_TR, (corte,))
    finally:
        c.close()

    msg_by = {m["ticketId"]: m for m in msgs}
    # transfers: conta por ticket + última fila (lista já vem ORDER BY createdAt)
    tr_n, tr_last = {}, {}
    for r in transf:
        tid = r["ticketId"]
        tr_n[tid] = tr_n.get(tid, 0) + 1
        if r.get("queue_nome"):
            tr_last[tid] = r["queue_nome"]

    abertos = []
    for t in tickets:
        tid = t["id"]
        m = msg_by.get(tid, {})
        nc = int(m.get("n_customer") or 0)
        nu = int(m.get("n_user") or 0)
        n_tr = tr_n.get(tid, 0)
        has_human = m.get("first_human_at") is not None
        bucket = classify(nc, nu, m.get("first_customer_at"), m.get("first_user_at"), n_tr, has_human)
        fila = tr_last.get(tid) or t.get("queue_nome") or ""
        cr = to_brt(t.get("createdAt"))   # UTC → BRT
        abertos.append({
            "id": str(tid),
            "n": int(t["ticketNumber"]) if t.get("ticketNumber") is not None else None,
            "c": (t.get("customer_nome") or "")[:60],
            "f": fila,
            "b": bucket,
            "d": cr.strftime("%d/%m/%Y %H:%M") if cr else "",
        })

    # mais antigos primeiro
    abertos.sort(key=lambda x: x["d"] and datetime.strptime(x["d"], "%d/%m/%Y %H:%M") or datetime.max)

    gerado = datetime.now()
    payload = {
        "meta": {
            "gerado_em": gerado.isoformat(timespec="seconds"),
            "gerado_em_br": gerado.strftime("%d/%m/%Y %H:%M:%S"),
            "janela_dias": JANELA_DIAS,
            "total_abertos": len(abertos),
            "fonte": "camim_chat_production (MySQL) — ETL leve",
        },
        "abertos": abertos,
    }

    os.makedirs(JSON_DIR, exist_ok=True)
    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, OUT_PATH)

    # Publica DIRETO em /var/www (onde o nginx serve) sem esperar o sync_www
    # (que roda a cada 5 min). Como este ETL roda a cada 1 min, isso é o que
    # mantém a aba de abertos em "quase tempo real". Best-effort: se o destino
    # não existir (ambiente de dev) ou faltar permissão, só ignora.
    publish_www()

    print(f"  ✔ {OUT_PATH}  ({len(abertos)} abertos)  ⏱ {time.time() - t0:.1f}s")


def publish_www():
    """Copia o chat_abertos.json para /var/www/json_consolidado/ atomicamente."""
    www_dir = os.environ.get("WWW_JSON_DIR", "/var/www/json_consolidado")
    if not os.path.isdir(www_dir):
        return
    import shutil
    dst = os.path.join(www_dir, "chat_abertos.json")
    tmp = dst + ".tmp"
    try:
        shutil.copy2(OUT_PATH, tmp)
        os.replace(tmp, dst)
        try:
            os.chmod(dst, 0o644)
        except OSError:
            pass
        print(f"  ↳ publicado em {dst}")
    except OSError as e:
        print(f"  ! falha ao publicar em www: {e}")
        try:
            os.remove(tmp)
        except OSError:
            pass


if __name__ == "__main__":
    main()
