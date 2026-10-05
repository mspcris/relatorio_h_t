"""
reanotar_falta_wpp.py — Refaz no F3 a anotação "Foi enviada mensagem pelo
whatsapp … no ticket-chat: #N" dos avisos de falta do médico que SAÍRAM mas
ficaram sem o registro no prontuário (Cad_LancamentoServico.MotivoDesistencia).

Chamado MUU-GWB-HNM3 (2026-10-05): a recepção de Anchieta viu 6 de 15 pacientes
sem a anotação e concluiu que não tinham recebido. Tinham — a API do prontuário
recusou a gravação com 503 (conexão dela pendurada no link do posto). O envio
agora grava direto no posto quando a API recusa; este script fecha o que ficou
para trás e serve para quando as duas rotas falharem juntas.

Não envia WhatsApp, não cria CRM, não cria ticket. Só completa a anotação, com
auditoria em Sis_Historico no nome de quem autorizou (--login).

Sem --run é dry-run: lê e lista, não grava nada. Uso:
    python reanotar_falta_wpp.py --desde 05/10/2026
    python reanotar_falta_wpp.py --falta 175574A
    python reanotar_falta_wpp.py --falta 175574A --login fulano.a --run
"""

import argparse
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv("/opt/relatorio_h_t/.env")
except Exception:
    pass

import medico_falta_routes as mf  # noqa: E402
from medico_novo_routes import _conn_for_posto, _resolver_idusuario_no_posto  # noqa: E402

# Mesmo caminho do wpp_cobranca_db, sem importar o módulo: o import dele roda
# init_db() e este script abre o banco de envios só para leitura.
DB_PATH = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")

RE_REF = re.compile(r"^falta (\d+)([A-Z])$")


def _dmy(s):
    try:
        return datetime.strptime((s or "").strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


def envios_aceitos(desde: date | None, falta: str | None) -> dict:
    """{(posto, id_falta): [envio, ...]} — só o que a Meta aceitou."""
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=15)
    try:
        rows = con.execute(
            """SELECT e.ref, e.nome, e.chat_ticket_id, e.data_falta, c.numero_saida
                 FROM envios e LEFT JOIN campanhas c ON c.id = e.campanha_id
                WHERE e.ref LIKE 'falta %' AND e.status LIKE 'accepted%'
                ORDER BY e.id""").fetchall()
    finally:
        con.close()
    out = defaultdict(list)
    for ref, nome, ticket_id, data_falta, numero_saida in rows:
        m = RE_REF.match(ref or "")
        if not m:
            continue
        id_falta, posto = int(m.group(1)), m.group(2)
        if falta:
            if f"{id_falta}{posto}" != falta:
                continue
        else:
            dia = _dmy(data_falta)
            if not dia or dia < desde:
                continue
        out[(posto, id_falta)].append({
            "nome": (nome or "").strip(), "ticket_id": ticket_id,
            "numero_saida": (numero_saida or "2455-9600").strip(),
        })
    return out


def numeros_dos_tickets(ticket_ids: list[str]) -> dict:
    """{cuid: ticketNumber}. SÓ LEITURA no MySQL do chat."""
    ids = sorted({t for t in ticket_ids if t})
    out = {}
    if not ids:
        return out
    con = mf._chat_conn()
    if con is None:
        return out
    try:
        with con.cursor() as cur:
            for i in range(0, len(ids), 200):
                lote = ids[i:i + 200]
                cur.execute("SELECT id, ticketNumber FROM Ticket WHERE id IN ("
                            + ",".join(["%s"] * len(lote)) + ")", lote)
                out.update({i_: n for i_, n in cur.fetchall() if n is not None})
    finally:
        con.close()
    return out


def linhas_da_falta(con, id_falta: int):
    """(médico, dia, [linha do F3]) — mesmo universo que o envio lê, sem o
    filtro de desistência: depois do aviso o paciente cancela ou remarca, e é
    nessa linha que a anotação precisa estar."""
    cur = con.cursor()
    cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cur.execute(
        """SELECT mf.idMedico, m.Nome, mf.DataFalta, mf.DataHora
             FROM Cad_MedicoFalta mf
             LEFT JOIN cad_medico m ON m.idmedico = mf.idMedico
            WHERE mf.idFalta = ?""", id_falta)
    f = cur.fetchone()
    if not f:
        return None, None, []
    dia = f[2] or f[3]
    dia = dia.date() if isinstance(dia, datetime) else dia
    # View com SET DATEFORMAT dmy: data SEMPRE em DD/MM/YYYY.
    cur.execute(
        """SELECT idLancamentoServico, idCliente, idDependente, Paciente
             FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
            WHERE idMedico = ? AND DataConsulta >= ? AND DataConsulta < ?""",
        f[0], dia.strftime("%d/%m/%Y"), (dia + timedelta(days=1)).strftime("%d/%m/%Y"))
    linhas, vistos = [], set()
    for id_ls, idcli, iddep, paciente_view in cur.fetchall():
        # A view repete a mesma linha às vezes (mesmo idLancamentoServico duas
        # vezes) — é uma linha só no F3.
        if id_ls in vistos:
            continue
        vistos.add(id_ls)
        # Mesma escolha de nome do envio (api_enviar_wpp): é com ela que o
        # nome foi gravado em `envios`.
        cur.execute("SELECT TOP 1 Nome, NomeSocial FROM cad_cliente WHERE idCliente = ?", int(idcli))
        t = cur.fetchone()
        nome = ((t[1] or t[0] or "") if t else "").strip() or (paciente_view or "").strip()
        if iddep and int(iddep) > 0:
            cur.execute("SELECT TOP 1 NomeSocial, Nome FROM cad_clientedependente "
                        "WHERE idCliente = ? AND idDependente = ?", int(idcli), int(iddep))
            d = cur.fetchone()
            if d:
                nome = (d[0] or d[1] or paciente_view or "").strip()
        cur.execute("SELECT CAST(MotivoDesistencia AS VARCHAR(MAX)) FROM Cad_LancamentoServico "
                    "WITH (NOLOCK) WHERE idLancamentoServico = ?", int(id_ls))
        md = cur.fetchone()
        linhas.append({"id_ls": int(id_ls), "nome": nome, "motivo": (md[0] or "") if md else ""})
    return (f[1] or "").strip(), dia, linhas


def planejar(envios: list, linhas: list, numeros: dict):
    """Casa cada envio com a linha do F3 do paciente.

    Devolve (a_anotar, ja_anotados, sem_par). `sem_par` nunca é chutado: fica
    listado para conferência à mão.
    """
    for e in envios:
        e["numero"] = numeros.get(e["ticket_id"])
    marca = lambda n: f"ticket-chat: #{n} -"  # noqa: E731
    ja, pend, repetidos = [], [], set()
    for e in envios:
        if e["numero"] and any(marca(e["numero"]) in l["motivo"] for l in linhas):
            ja.append(e)
            continue
        # Paciente que recebeu a mesma mensagem duas vezes cai no mesmo ticket:
        # é uma anotação só.
        chave = (e["nome"].casefold(), e["numero"])
        if e["numero"] and chave in repetidos:
            continue
        repetidos.add(chave)
        pend.append(e)
    # Linha livre = a que não carrega anotação de NENHUM envio desta falta.
    todas = [marca(e["numero"]) for e in envios if e["numero"]]
    livres = defaultdict(list)
    for l in linhas:
        if not any(m in l["motivo"] for m in todas):
            livres[l["nome"].casefold()].append(l)
    por_nome = defaultdict(list)
    for e in pend:
        por_nome[e["nome"].casefold()].append(e)
    a_anotar, sem_par = [], []
    for chave, es in por_nome.items():
        ls = livres.get(chave, [])
        sem_ticket = [e for e in es if not e["numero"]]
        if sem_ticket or len(ls) != len(es):
            motivo = ("sem número de ticket no chat" if sem_ticket else
                      "paciente sem linha no F3 deste médico/dia" if not ls else
                      f"{len(es)} envio(s) para {len(ls)} linha(s) do mesmo paciente")
            sem_par += [(e, motivo) for e in es]
            continue
        a_anotar += list(zip(es, ls))
    return a_anotar, ja, sem_par


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--desde", help="DD/MM/AAAA — faltas com data da consulta a partir daqui (padrão: hoje)")
    ap.add_argument("--falta", help="uma falta só: idFalta + letra do posto, ex. 175574A")
    ap.add_argument("--login", help="login do ERP (login_campinho) de quem autoriza — vai na auditoria")
    ap.add_argument("--run", action="store_true", help="grava. Sem isto, só lista")
    a = ap.parse_args()

    desde = _dmy(a.desde) if a.desde else date.today()
    if a.desde and not desde:
        print("--desde inválido: use DD/MM/AAAA")
        return 2
    falta = (a.falta or "").strip().upper() or None
    if a.run and not a.login:
        print("--run exige --login: escrita no CAMIM sem auditoria em Sis_Historico não existe.")
        return 2

    grupos = envios_aceitos(desde, falta)
    print(("GRAVANDO" if a.run else "DRY-RUN (nada é gravado)") + " — "
          + (f"falta {falta}" if falta else f"faltas com consulta a partir de {desde:%d/%m/%Y}")
          + f" — {len(grupos)} falta(s), {sum(len(v) for v in grupos.values())} mensagem(ns) aceita(s)")
    if not grupos:
        return 0
    numeros = numeros_dos_tickets([e["ticket_id"] for v in grupos.values() for e in v])
    if not numeros:
        print("SEM LEITURA do chat: não deu para resolver o número dos tickets. Nada a fazer agora.")
        return 1

    tot = {"ja": 0, "anotar": 0, "gravadas": 0, "falhas": 0, "sem_par": 0}
    sem_leitura = []
    por_posto = defaultdict(list)
    for (posto, id_falta), envios in sorted(grupos.items()):
        por_posto[posto].append((id_falta, envios))

    for posto, faltas in por_posto.items():
        try:
            con = _conn_for_posto(posto)
        except Exception as e:  # noqa: BLE001
            sem_leitura.append((posto, str(e)[:120]))
            continue
        try:
            id_usuario = None
            if a.run:
                id_usuario = _resolver_idusuario_no_posto(con, a.login)
                if not id_usuario:
                    sem_leitura.append((posto, f"login {a.login} não existe ativo neste posto — nada gravado"))
                    continue
            for id_falta, envios in faltas:
                try:
                    medico, dia, linhas = linhas_da_falta(con, id_falta)
                except Exception as e:  # noqa: BLE001
                    sem_leitura.append((f"{id_falta}{posto}", str(e)[:120]))
                    continue
                if dia is None:
                    sem_leitura.append((f"{id_falta}{posto}", "falta não existe mais no posto"))
                    continue
                a_anotar, ja, sem_par = planejar(envios, linhas, numeros)
                tot["ja"] += len(ja)
                tot["anotar"] += len(a_anotar)
                tot["sem_par"] += len(sem_par)
                if not a_anotar and not sem_par:
                    continue
                print(f"\nfalta {id_falta}{posto} · {medico} · consulta {dia:%d/%m/%Y} · "
                      f"{len(envios)} enviadas: {len(ja)} já anotadas, {len(a_anotar)} a anotar"
                      + (f", {len(sem_par)} sem par" if sem_par else ""))
                for e, l in a_anotar:
                    texto = mf._texto_anotacao(e["numero_saida"], f"#{e['numero']}")
                    linha = f"  idLs {l['id_ls']:>9}  {e['nome'][:38]:38}  ticket #{e['numero']}"
                    if not a.run:
                        print(linha)
                        continue
                    r = mf._append_observacao_direto(con, l["id_ls"], texto, id_usuario)
                    if r.get("ok"):
                        tot["gravadas"] += 1
                        print(linha + ("  já estava" if r.get("ja_anotado") else "  GRAVADA"))
                    else:
                        tot["falhas"] += 1
                        print(linha + f"  FALHOU: {r.get('error')}")
                for e, motivo in sem_par:
                    print(f"  SEM PAR         {e['nome'][:38]:38}  ticket #{e['numero'] or '?'}  ({motivo})")
        finally:
            con.close()

    print(f"\nresumo: {tot['ja']} já anotadas · {tot['anotar']} a anotar"
          + (f" · {tot['gravadas']} gravadas · {tot['falhas']} falhas" if a.run else "")
          + f" · {tot['sem_par']} sem par (conferir à mão)")
    for onde, erro in sem_leitura:
        print(f"SEM LEITURA {onde}: {erro}")
    return 1 if (sem_leitura or tot["falhas"]) else 0


if __name__ == "__main__":
    sys.exit(main())
