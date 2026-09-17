#!/usr/bin/env python3
"""
export_indicadores_painel.py

Pré-agrega os 4 indicadores do painel monitorarrobos.html em um único JSON
estático, eliminando a agregação ao vivo no Flask (que estava estourando o
timeout do gunicorn por causa da fila MAX/COUNT em ind_email — 73s para
268k linhas — e arrastando todos os outros endpoints).

Saída:
    /opt/relatorio_h_t/json_consolidado/indicadores_painel.json
    /opt/relatorio_h_t/json_consolidado/_etl_meta_indicadores_painel.json

Cron: */5 * * * *  (definido em cron/relatorio_ht)

Estrutura do JSON (`ultimo_envio` é string vinda do banco; o endpoint Flask
calcula `dias` em runtime para refletir o momento da request, não o do ETL):

    {
      "generated_at": "2026-04-26T19:35:00",
      "indicadores": {
        "push":  {"A": {"ultimo_envio": "...", "total_dia": 12}, ...},
        "email": {
          "data": {"A|Boleto": {"posto":"A","categoria":"Boleto",
                                 "ultimo_envio":"...","total":5}, ...},
          "sync": {"synced_at":"...","total_records":N,
                    "status":"ok","mensagem":""}
        },
        "tef":   {"data": {"A": {...}, ...}, "sync": {...}},
        "wpp":   [{"id":1,"nome":"Cobrança",
                    "postos":{"A":{"ultimo_envio":"..."}, ...}}, ...]
      },
      "erros": {}
    }
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import traceback
from datetime import datetime

OUT_DIR   = "/opt/relatorio_h_t/json_consolidado"
OUT_FILE  = os.path.join(OUT_DIR, "indicadores_painel.json")
META_FILE = os.path.join(OUT_DIR, "_etl_meta_indicadores_painel.json")

KPI_DB  = os.environ.get("KPI_DB_PATH",  "/opt/relatorio_h_t/camim_kpi.db")
PUSH_DB = os.environ.get("PUSH_LOG_DB",  "/opt/push_clientes/push_log.db")
WPP_DB  = os.environ.get("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")


def _connect_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _coletar_email() -> dict:
    conn = _connect_ro(KPI_DB)
    # Boleto é um robô; o resto (exame, prescrição, cancelamento, boas-vindas…)
    # vira UMA linha "Outros e-mails" — muitos programas, nenhum com hora certa
    rows = conn.execute("""
        SELECT posto,
               CASE WHEN titulo_categoria = 'Boleto' THEN 'Boleto' ELSE 'Outros e-mails' END,
               MAX(datahora)                                                    AS ultimo_envio,
               COUNT(CASE WHEN DATE(datahora)=DATE('now','localtime') THEN 1 END) AS total
        FROM ind_email
        GROUP BY 1, 2
    """).fetchall()
    sync = conn.execute("""
        SELECT synced_at, total_records, status, mensagem
        FROM   ind_sync_log
        WHERE  indicador='email'
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    hoje = _hoje_email(conn)
    conn.close()

    data = {}
    for posto, cat, ultimo, total in rows:
        posto = (posto or "").strip().upper()
        if not posto:
            continue
        data[f"{posto}|{cat}"] = {
            "posto":        posto,
            "categoria":    cat,
            "ultimo_envio": ultimo,
            "total":        total,
            "hoje":         hoje.get((posto, cat)),
        }
    return {
        "data": data,
        "sync": {
            "synced_at":     sync[0] if sync else None,
            "total_records": sync[1] if sync else 0,
            "status":        sync[2] if sync else None,
            "mensagem":      sync[3] if sync else None,
        },
    }


def _hoje_email(conn: sqlite3.Connection) -> dict:
    """E-mails de HOJE por (posto, Boleto | Outros e-mails).

    Falhou = ind_email.falhou, gravado pelo sync_email com a regra de
    email_resultado (coluna Erro da vw_cad_email). Só o "Pré agendamento
    Cancelado" grava o resultado; os outros programas deixam Erro vazio, e aí
    não há falha para contar — `sem_registro` diz quantos são.
    Repetido (só boleto): título igual no mesmo dia = mesma cobrança (em
    14/09/2026 o Y mandou 75 e-mails para 17 boletos).
    """
    colunas = {r[1] for r in conn.execute("PRAGMA table_info(ind_email)")}
    falhou = "falhou" if "falhou" in colunas else "0"
    registra = "(erro <> '')" if "erro" in colunas else "0"
    rows = conn.execute(f"""
        SELECT posto,
               CASE WHEN titulo_categoria = 'Boleto' THEN 'Boleto' ELSE 'Outros e-mails' END AS grupo,
               titulo_categoria, COUNT(*), COUNT(DISTINCT titulo_original),
               SUM({falhou}), SUM(CASE WHEN {registra} THEN 0 ELSE 1 END)
        FROM   ind_email
        WHERE  date(datahora) = date('now','localtime')
        GROUP BY 1, 2, 3
    """).fetchall()
    motivos = {}
    if "erro" in colunas:
        from email_resultado import explicar_erro_email
        for posto, grupo, erro, n in conn.execute(f"""
                SELECT posto, CASE WHEN titulo_categoria = 'Boleto' THEN 'Boleto' ELSE 'Outros e-mails' END,
                       erro, COUNT(*)
                FROM ind_email
                WHERE date(datahora) = date('now','localtime') AND {falhou} = 1
                GROUP BY 1, 2, 3"""):
            m = explicar_erro_email(erro).removeprefix("falhou: ").split(" — ")[0]
            chave = ((posto or "").strip().upper(), grupo)
            motivos.setdefault(chave, {})
            motivos[chave][m] = motivos[chave].get(m, 0) + n
    saida = {}
    for posto, grupo, cat, n, distintos, falhas, sem_registro in rows:
        chave = ((posto or "").strip().upper(), grupo)
        h = saida.setdefault(chave, {"emails": 0, "falharam": 0, "repetidos": 0, "sem_registro": 0, "tipos": {}})
        h["emails"] += n
        h["falharam"] += falhas or 0
        h["sem_registro"] += sem_registro or 0
        if grupo == "Boleto":
            h["repetidos"] += n - distintos
        else:
            h["tipos"][cat] = h["tipos"].get(cat, 0) + n
    for chave, h in saida.items():
        h["enviados"] = h["emails"] - h["falharam"]
        h["cobrancas"] = h["emails"] - h["repetidos"]          # compat: boleto
        h["tipos"] = sorted(h["tipos"].items(), key=lambda kv: -kv[1])
        h["motivos"] = sorted((motivos.get(chave) or {}).items(), key=lambda kv: -kv[1])
    return saida


# Recusa do TEF em texto de gente. O que não está aqui aparece como veio.
_MOTIVO_TEF = {
    "": "sem resposta da Cielo",
    "could not get credit card": "cartão não encontrado",
    "autorizacao negada": "negada",
}


def _motivo_tef(erro: str, resposta: str) -> str:
    erro = (erro or "").strip().rstrip(".")
    if erro:
        return erro
    return _MOTIVO_TEF.get((resposta or "").strip().lower(), resposta or "sem resposta da Cielo")


def _hoje_tef(conn: sqlite3.Connection) -> dict:
    """TEF Recorrente HOJE, por posto, contado por CLIENTE (matrícula).

    O robô tenta de novo quem teve saldo insuficiente: contar linhas inflava a
    tentativa. Pagou = alguma tentativa do dia aprovada (sync_tef.is_aprovado).
    O motivo é o da ÚLTIMA tentativa de quem não pagou.
    """
    rows = conn.execute("""
        SELECT posto, matricula, datahora, aprovado, valor, erro, resposta_cielo
        FROM   ind_tef
        WHERE  date(datahora) = date('now','localtime')
        ORDER BY datahora
    """).fetchall()
    por_cliente = {}
    for posto, mat, _dh, aprovado, valor, erro, resp in rows:
        posto = (posto or "").strip().upper()
        c = por_cliente.setdefault((posto, mat), {"pago": False, "valor": 0.0, "motivo": ""})
        c["valor"] = max(c["valor"], valor or 0)
        c["tentativas"] = c.get("tentativas", 0) + 1
        if aprovado:
            c["pago"] = True
        else:
            c["motivo"] = _motivo_tef(erro, resp)
    saida = {}
    for (posto, _mat), c in por_cliente.items():
        h = saida.setdefault(posto, {"cobrados": 0, "pagaram": 0, "nao_pagaram": 0, "tentativas": 0,
                                     "valor_cobrado": 0.0, "valor_pago": 0.0, "motivos": {}})
        h["cobrados"] += 1
        h["tentativas"] += c["tentativas"]
        h["valor_cobrado"] += c["valor"]
        if c["pago"]:
            h["pagaram"] += 1
            h["valor_pago"] += c["valor"]
        else:
            h["nao_pagaram"] += 1
            h["motivos"][c["motivo"]] = h["motivos"].get(c["motivo"], 0) + 1
    for h in saida.values():
        h["taxa"] = round(100 * h["pagaram"] / h["cobrados"], 1) if h["cobrados"] else None
        h["valor_cobrado"] = round(h["valor_cobrado"], 2)
        h["valor_pago"] = round(h["valor_pago"], 2)
        h["motivos"] = sorted(h["motivos"].items(), key=lambda kv: -kv[1])
    return saida


def _coletar_tef() -> dict:
    conn = _connect_ro(KPI_DB)
    rows = conn.execute("""
        SELECT posto, MAX(datahora) AS ultimo, COUNT(*) AS total
        FROM   ind_tef
        GROUP BY posto
    """).fetchall()
    sync = conn.execute("""
        SELECT synced_at, total_records, status, mensagem
        FROM   ind_sync_log
        WHERE  indicador='tef'
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    hoje = _hoje_tef(conn)
    conn.close()

    data = {}
    for posto, ultimo, total in rows:
        posto = (posto or "").strip().upper()
        if not posto:
            continue
        data[posto] = {
            "posto":      posto,
            "ultimo_tef": ultimo,
            "total":      total,
            "hoje":       hoje.get(posto),
        }
    return {
        "data": data,
        "sync": {
            "synced_at":     sync[0] if sync else None,
            "total_records": sync[1] if sync else 0,
            "status":        sync[2] if sync else None,
            "mensagem":      sync[3] if sync else None,
        },
    }


def _coletar_push() -> dict:
    """Push de cobrança por posto: último envio + números de HOJE.

    Lê o push_log.db do push_clientes (mesma VM). Mesma conta de
    push_clientes/push_api.resumo_envios() — mudar nos dois:
      tentativas    = linhas de produção do dia
      receberam     = status 'success' (a API devolveu id da mensagem)
      nao_receberam = status 'error'
      sem_app       = motivo sem_token/nao_inscrito (cliente sem o app novo)
      falhas_tec    = api_erro/http_erro/conexao (problema nosso ou da API)
      rodada        = concluida | em_andamento | interrompida | nao_rodou,
                      pela auditoria (cron_inicio/cron_fim) — "0 tentativas"
                      de posto sem cliente e de robô que morreu no meio têm
                      a mesma cara no log de envios.
    `sent_at` é hora local desde 17/09/2026 (antes era UTC).
    """
    conn = _connect_ro(PUSH_DB)
    hoje = datetime.now().date().isoformat()
    agora = datetime.now()

    ultimos = dict(conn.execute("""
        SELECT posto, MAX(sent_at) FROM push_log
        WHERE modo = 'producao' GROUP BY posto""").fetchall())

    # Falha de produção recuperada pelo reenvio do dia (modo='reenvio',
    # send_push.py --reenviar-falhas) conta como "recebeu", sem tentativa nova.
    recuperado = """(status = 'error' AND EXISTS (
        SELECT 1 FROM push_log r
        WHERE r.modo = 'reenvio' AND r.status = 'success'
          AND date(r.sent_at) = date(push_log.sent_at)
          AND r.posto = push_log.posto AND r.id_cliente = push_log.id_cliente
          AND ifnull(r.id_receita, 0) = ifnull(push_log.id_receita, 0)
          AND r.tipo_envio = push_log.tipo_envio))"""
    hoje_rows = conn.execute(f"""
        SELECT posto, COUNT(*),
               SUM(status = 'success' OR {recuperado}),
               SUM(status = 'error' AND NOT {recuperado}),
               SUM(motivo IN ('sem_token', 'nao_inscrito') AND NOT {recuperado}),
               SUM(status = 'error' AND ifnull(motivo, 'api_erro')
                   IN ('api_erro', 'http_erro', 'conexao') AND NOT {recuperado})
        FROM push_log
        WHERE modo = 'producao' AND date(sent_at) = ?
        GROUP BY posto""", (hoje,)).fetchall()
    numeros = {r[0]: r[1:] for r in hoje_rows}

    ini, fim = {}, {}
    try:
        for posto, acao, ts in conn.execute("""
                SELECT entidade_id, acao, ts FROM auditoria
                WHERE acao IN ('cron_inicio', 'cron_fim') AND date(ts) = ?
                ORDER BY ts""", (hoje,)):
            (ini if acao == 'cron_inicio' else fim)[posto] = ts
    except sqlite3.OperationalError:
        pass  # push_clientes antigo, sem auditoria
    conn.close()

    data = {}
    for posto_raw in set(ultimos) | set(ini):
        posto = str(posto_raw).strip().upper() if posto_raw else None
        if not posto:
            continue
        tent, rec, nrec, sem_app, tec = (numeros.get(posto_raw) or (0, 0, 0, 0, 0))
        i, f = ini.get(posto_raw), fim.get(posto_raw)
        ultimo = ultimos.get(posto_raw)
        if not i:
            rodada = "nao_rodou"
        elif f and f >= i:
            rodada = "concluida"
        else:
            atividade = max(i, ultimo or "")
            try:
                parado = agora - datetime.fromisoformat(atividade[:19])
            except ValueError:
                parado = None
            rodada = ("em_andamento" if parado is not None
                      and parado.total_seconds() < 600 else "interrompida")
        data[posto] = {
            "ultimo_envio": ultimo,
            "total_dia": tent or 0,          # compat: agora é mesmo o do dia
            "tentativas_hoje": tent or 0,
            "receberam_hoje": rec or 0,
            "nao_receberam_hoje": nrec or 0,
            "sem_app_hoje": sem_app or 0,
            "falhas_tecnicas_hoje": tec or 0,
            "taxa_hoje": round(100 * rec / tent, 1) if tent else None,
            "rodada": rodada,
        }
    return data


# Por que o cliente não recebeu o WhatsApp. FALHA é o que devia ter saído e não
# saiu; PULADO é escolha do robô (já recebeu nesta campanha, várias faturas no
# mesmo telefone viram uma mensagem só, nome de teste) — não entra na taxa.
_WPP_PULADO = {"ja_enviado_campanha", "multi_fatura_mesmo_tel_nesta_rodada", "nome_de_teste"}
_WPP_MOTIVO = {
    "sem_telefone_valido": "sem telefone válido",
    "janela_fechou_entre_fases": "janela de envio fechou",
    "ja_enviado_campanha": "já recebeu nesta campanha",
    "multi_fatura_mesmo_tel_nesta_rodada": "outra fatura no mesmo telefone",
    "nome_de_teste": "nome de teste",
}


def motivo_wpp(motivo: str) -> str:
    m = (motivo or "").strip()
    if m.startswith("erro_api") or m.startswith("erro:"):
        return "erro no envio"
    return _WPP_MOTIVO.get(m, m)


def _hoje_wpp(conn: sqlite3.Connection) -> dict:
    """WhatsApp HOJE por (campanha, posto), contado por cobrança (idreceita;
    sem idreceita, o telefone). A rodada roda várias vezes no dia e regrava os
    não enviados a cada volta — daí o DISTINCT."""
    saida = {}

    def caixa(cid, posto):
        return saida.setdefault((cid, (posto or "").strip().upper()),
                                {"enviados": 0, "falharam": 0, "pulados": 0, "motivos": {}})

    for cid, posto, n in conn.execute("""
            SELECT campanha_id, posto, COUNT(DISTINCT COALESCE(idreceita, telefone))
            FROM envios
            WHERE date(enviado_em) = date('now','localtime') AND status LIKE 'accepted%'
            GROUP BY 1, 2"""):
        caixa(cid, posto)["enviados"] += n
    falhas = conn.execute("""
            SELECT campanha_id, posto, 'erro:' || status, COUNT(DISTINCT COALESCE(idreceita, telefone))
            FROM envios
            WHERE date(enviado_em) = date('now','localtime') AND status LIKE 'erro%'
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT campanha_id, posto, motivo, COUNT(DISTINCT COALESCE(idreceita, matricula, telefone_raw))
            FROM nao_enviados
            WHERE date(rodada_em) = date('now','localtime')
            GROUP BY 1, 2, 3""").fetchall()
    for cid, posto, motivo, n in falhas:
        c = caixa(cid, posto)
        if motivo in _WPP_PULADO:
            c["pulados"] += n
        else:
            c["falharam"] += n
            m = motivo_wpp(motivo)
            c["motivos"][m] = c["motivos"].get(m, 0) + n
    for c in saida.values():
        tentou = c["enviados"] + c["falharam"]
        c["taxa"] = round(100 * c["enviados"] / tentou, 1) if tentou else None
        c["motivos"] = sorted(c["motivos"].items(), key=lambda kv: -kv[1])
    return saida


def _coletar_wpp() -> list:
    """Pra cada campanha ativa, coleta:
    - postos: {posto: {ultimo_envio}}  — granular pra detalhe
    - agregado: ultimo_envio mais recente em QUALQUER posto da campanha

    Status considerados como 'enviado de fato' incluem 'accepted',
    'accepted_meta' e 'accepted_chat' (depois da refatoração de batch
    em 26/05/2026 o cron registra com sufixo).

    Campanhas com modo='falta_medico' são one-shot via API — não fazem
    sentido como 'robô atrasado'; excluídas.
    """
    conn = _connect_ro(WPP_DB)
    conn.row_factory = sqlite3.Row
    campanhas = conn.execute(
        "SELECT id, nome, postos FROM campanhas "
        "WHERE ativa=1 AND COALESCE(modo_envio,'atraso') <> 'falta_medico'"
    ).fetchall()

    hoje = _hoje_wpp(conn)
    result = []
    for c in campanhas:
        try:
            postos_lista = json.loads(c["postos"] or "[]")
        except Exception:
            postos_lista = []

        # Último envio AGREGADO da campanha (qualquer posto)
        row_agg = conn.execute(
            "SELECT MAX(enviado_em) AS ultimo FROM envios "
            "WHERE campanha_id=? AND status LIKE 'accepted%'",
            (c["id"],),
        ).fetchone()
        ultimo_agg = row_agg["ultimo"] if row_agg and row_agg["ultimo"] else None

        # Detalhe por posto pra mostrar cobertura
        postos_dados = {}
        for posto in postos_lista:
            row = conn.execute(
                "SELECT MAX(enviado_em) AS ultimo FROM envios "
                "WHERE campanha_id=? AND posto=? AND status LIKE 'accepted%'",
                (c["id"], posto),
            ).fetchone()
            postos_dados[posto] = {
                "ultimo_envio": row["ultimo"] if row and row["ultimo"] else None,
                "hoje": hoje.get((c["id"], str(posto).strip().upper())),
            }
        result.append({
            "id": c["id"], "nome": c["nome"],
            "postos": postos_dados,
            "ultimo_envio_agregado": ultimo_agg,
        })
    conn.close()
    return result


def _atomic_write(path: str, payload) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main() -> int:
    started_at = datetime.now()
    os.makedirs(OUT_DIR, exist_ok=True)

    indicadores = {}
    erros = {}
    for nome, fn in (
        ("push",  _coletar_push),
        ("email", _coletar_email),
        ("tef",   _coletar_tef),
        ("wpp",   _coletar_wpp),
    ):
        try:
            indicadores[nome] = fn()
        except Exception as exc:
            erros[nome] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc(file=sys.stderr)

    _atomic_write(OUT_FILE, {
        "generated_at": started_at.isoformat(timespec="seconds"),
        "indicadores":  indicadores,
        "erros":        erros,
    })

    finished_at = datetime.now()
    _atomic_write(META_FILE, {
        "script":            "export_indicadores_painel",
        "started_at":        started_at.isoformat(timespec="seconds"),
        "finished_at":       finished_at.isoformat(timespec="seconds"),
        "duracao_segundos":  round((finished_at - started_at).total_seconds(), 2),
        "indicadores_ok":    [k for k in indicadores if k not in erros],
        "erros":             erros,
    })

    print(f"[OK] {OUT_FILE} "
          f"({(finished_at - started_at).total_seconds():.2f}s, "
          f"ok={[k for k in indicadores if k not in erros]}, "
          f"erros={list(erros)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
