"""backfill_envios_falta_medico.py

Backfill das colunas medico/especialidade/data_falta/hora_falta/motivo_falta
em envios cujo campanha.modo_envio = 'falta_medico'.

Lógica:

  1) Envios LEGÍTIMOS (idreceita = '' e ref começa com 'falta '):
     - Parseia `ref` no formato "falta NNN<posto>" → id_falta, posto
     - Conecta ao SQL Server do posto via _conn_for_posto
     - Busca em Cad_MedicoFalta (JOIN cad_medico + vw_Cad_MedicoFaltaMotivo)
       os 5 campos e UPDATE no envios.

  2) Envios do INCIDENTE 2026-05-06 (campanha falta_medico + idreceita != ''):
     - Foram disparados PELO CRON DE COBRANÇA usando o template
       notificacao_de_falta_do_medico com dados de fatura. Não correspondem
       a faltas reais — não tem como popular medico/data/etc.
     - Só marcamos motivo_falta = 'INCIDENTE 2026-05-06 — envio em massa errado'
       pra deixar visível na tela.

Uso:
    python3 backfill_envios_falta_medico.py            # dry-run (default)
    python3 backfill_envios_falta_medico.py --apply    # grava de verdade
"""

import argparse
import logging
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime

# Permite rodar de /opt/relatorio_h_t sem PYTHONPATH explícito
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from medico_novo_routes import _conn_for_posto  # noqa: E402

LOG_FMT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("backfill")

WAPP_DB = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")

INCIDENTE_MARKER = "INCIDENTE 2026-05-06 — envio em massa errado"

REF_RE = re.compile(r"^falta\s+(\d+)\s*([A-Z])$", re.IGNORECASE)


def _format_hora_falta(dh_ini, dh_fim) -> str | None:
    """Retorna 'HH:MM-HH:MM' ou None. Falta integral (00:00-23:59) → None."""
    if not dh_ini or not dh_fim:
        return None
    try:
        h_ini = dh_ini.strftime("%H:%M")
        h_fim = dh_fim.strftime("%H:%M")
    except Exception:
        return None
    if (h_ini, h_fim) == ("00:00", "23:59"):
        return None
    return f"{h_ini}-{h_fim}"


def _identificar_envios(conn: sqlite3.Connection):
    """Retorna duas listas:
      (legitimos, incidente)

    `legitimos`: list[dict] com id, ref, posto, id_falta (já parseado)
    `incidente`: list[int] de envio_id pra marcar com motivo_falta.
    """
    # Campanhas de modo falta_medico
    rows = conn.execute(
        "SELECT id, nome FROM campanhas WHERE modo_envio = 'falta_medico'"
    ).fetchall()
    if not rows:
        log.warning("Nenhuma campanha modo_envio='falta_medico' encontrada.")
        return [], []
    camp_ids = [r["id"] for r in rows]
    log.info("Campanhas falta_medico: %s", [(r["id"], r["nome"]) for r in rows])

    placeholders = ",".join("?" * len(camp_ids))
    envios = conn.execute(
        f"""SELECT id, ref, idreceita, valor, venc, dias_atraso,
                   medico, especialidade, data_falta, hora_falta, motivo_falta
              FROM envios
             WHERE campanha_id IN ({placeholders})""",
        camp_ids,
    ).fetchall()

    legitimos = []
    incidente = []

    for e in envios:
        # Já preenchido? pula (idempotência).
        if e["medico"] or e["especialidade"] or e["data_falta"]:
            continue
        # Incidente: cobrança disfarçada → tem idreceita preenchido
        if (e["idreceita"] or "").strip():
            if (e["motivo_falta"] or "") != INCIDENTE_MARKER:
                incidente.append(e["id"])
            continue
        # Legítimo: ref começa com "falta NNN<posto>"
        m = REF_RE.match((e["ref"] or "").strip())
        if not m:
            log.debug("envio id=%s sem ref parseável: %r", e["id"], e["ref"])
            continue
        legitimos.append({
            "id": e["id"],
            "id_falta": int(m.group(1)),
            "posto": m.group(2).upper(),
        })

    return legitimos, incidente


def _buscar_faltas_no_posto(posto: str, ids_falta: list[int]) -> dict:
    """Consulta Cad_MedicoFalta de um posto e devolve dict { id_falta: dados }.

    Cada valor é dict com chaves: medico, especialidade, data_falta, hora_falta, motivo.
    """
    if not ids_falta:
        return {}
    # SQL Server limita IN a 2100 itens. Como cada posto deve ter no máximo
    # algumas centenas de faltas históricas, vamos fatiar conservadoramente.
    out: dict[int, dict] = {}
    CHUNK = 500
    with _conn_for_posto(posto) as con:
        cur = con.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        for i in range(0, len(ids_falta), CHUNK):
            chunk = ids_falta[i:i + CHUNK]
            ph = ",".join("?" * len(chunk))
            cur.execute(
                f"""SELECT mf.idFalta,
                           m.Nome           AS NomeMedico,
                           mf.Especialidade AS Especialidade,
                           mf.DataFalta     AS DataFalta,
                           mf.DataHora      AS DataHoraIni,
                           mf.DatahoraFim   AS DataHoraFim,
                           mfm.Motivo       AS Motivo
                      FROM Cad_MedicoFalta mf WITH (NOLOCK)
                      LEFT JOIN cad_medico m WITH (NOLOCK)
                        ON m.idmedico = mf.idMedico
                      LEFT JOIN vw_Cad_MedicoFaltaMotivo mfm
                        ON mfm.idMedicoFaltaMotivo = mf.idMedicoFaltaMotivo
                     WHERE mf.idFalta IN ({ph})""",
                *chunk,
            )
            for r in cur.fetchall():
                idf = int(r[0])
                data_str = r[3].strftime("%d/%m/%Y") if r[3] else None
                hora_str = _format_hora_falta(r[4], r[5])
                motivo = (r[6] or "").strip() or None
                out[idf] = {
                    "medico": (r[1] or "").strip() or None,
                    "especialidade": (r[2] or "").strip() or None,
                    "data_falta": data_str,
                    "hora_falta": hora_str,
                    "motivo_falta": motivo,
                }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Grava de verdade (sem isso, é dry-run)")
    args = ap.parse_args()
    dry = not args.apply

    log.info("DB: %s | modo=%s", WAPP_DB, "DRY-RUN" if dry else "APPLY")

    conn = sqlite3.connect(WAPP_DB)
    conn.row_factory = sqlite3.Row

    legitimos, incidente = _identificar_envios(conn)
    log.info("Encontrados: %d legítimos pra backfill, %d do incidente",
             len(legitimos), len(incidente))

    # Agrupa legítimos por posto pra abrir 1 conexão por posto
    por_posto: dict[str, list] = defaultdict(list)
    for L in legitimos:
        por_posto[L["posto"]].append(L)

    total_atualizados = 0
    total_sem_match = 0

    for posto, envios in por_posto.items():
        ids = sorted({L["id_falta"] for L in envios})
        log.info("Posto %s: %d envios, %d idFalta únicos — consultando SQL Server",
                 posto, len(envios), len(ids))
        try:
            faltas = _buscar_faltas_no_posto(posto, ids)
        except Exception as e:
            log.error("Falha ao consultar posto %s: %s", posto, e)
            continue
        log.info("Posto %s: %d idFalta encontrados no banco", posto, len(faltas))

        for L in envios:
            f = faltas.get(L["id_falta"])
            if not f:
                total_sem_match += 1
                continue
            if dry:
                log.info("  [dry] envio_id=%s id_falta=%s → medico=%r esp=%r data=%s hora=%s motivo=%r",
                         L["id"], L["id_falta"], f["medico"],
                         f["especialidade"], f["data_falta"],
                         f["hora_falta"], f["motivo_falta"])
            else:
                conn.execute(
                    """UPDATE envios
                          SET medico=?, especialidade=?, data_falta=?,
                              hora_falta=?, motivo_falta=?
                        WHERE id=?""",
                    (f["medico"], f["especialidade"], f["data_falta"],
                     f["hora_falta"], f["motivo_falta"], L["id"]),
                )
            total_atualizados += 1

    log.info("Marcando %d envios do incidente 2026-05-06", len(incidente))
    if not dry and incidente:
        conn.executemany(
            "UPDATE envios SET motivo_falta=? WHERE id=?",
            [(INCIDENTE_MARKER, eid) for eid in incidente],
        )

    if not dry:
        conn.commit()
    conn.close()

    log.info("FIM | atualizados=%d | sem_match=%d | incidente=%d | dry_run=%s",
             total_atualizados, total_sem_match, len(incidente), dry)


if __name__ == "__main__":
    main()
