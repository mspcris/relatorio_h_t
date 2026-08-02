"""
medico_custo_hist.py — grava o histórico diário e reconstrói um dia passado.

Duas funções, e as duas em cima das mesmas tabelas mc_* (medico_custo_hist_db):

    registrar(...)     chamada pelo ETL depois de montar as linhas do dia.
                       Compara com o que está vigente e grava só a diferença.
    linhas_em(data)    devolve as linhas do cadastro como estavam naquele dia.

Sobre a comparação: a chave é `posto|id_medico|id_especialidade|dia_semana`
(medida única em 776 linhas) e o "mudou?" é o hash dos campos de CADASTRO — nada
de derivado, senão mexer numa constante do ETL marcaria as 776 agendas como
alteradas no mesmo dia.
"""
from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import func, or_, select

import medico_custo_hist_db as db
from medico_custo_hist_db import (
    CAMPOS_CADASTRO, McAgendaVersao, McExecucao, McMudanca, McSession,
    chave_agenda, hash_cadastro,
)

log = logging.getLogger(__name__)


def _fmt(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "sim" if v else "não"
    return str(v)


def _diff_campos(antigo: dict, novo: dict) -> list[tuple[str, str | None, str | None]]:
    """Campos de cadastro que mudaram entre duas versões da MESMA agenda."""
    fora = []
    for c in CAMPOS_CADASTRO:
        a, b = antigo.get(c), novo.get(c)
        if a != b:
            fora.append((c, _fmt(a), _fmt(b)))
    return fora


def registrar(linhas: list[dict], status: dict, parametros: dict,
              resumo: dict, quando: date | None = None,
              dry_run: bool = False) -> dict:
    """Registra a foto do dia. Devolve o resumo do que mudou.

    `status` é o dict do ETL por posto ({posto: {"erro": ...}}). Ele decide
    quais postos podem ser processados:

    ►► SÓ postos que voltaram OK. ◄◄

    Posto que falhou não aparece em `linhas`, e fechar as agendas dele por
    ausência gravaria "45 agendas removidas, −R$ 180 mil/mês". Economia falsa é
    pior que dado faltando — tem a cara exata do resultado que se procura.
    O posto com erro fica congelado: suas versões vigentes continuam vigentes.
    """
    hoje = quando or db.hoje_brt()
    ok = {p for p, s in status.items() if not s.get("erro")}
    falhos = sorted(p for p, s in status.items() if s.get("erro"))

    atual = {}
    for l in linhas:
        if l["posto"] not in ok:
            continue                       # não deveria acontecer; defesa
        k = chave_agenda(l)
        if k in atual:
            # Unicidade da chave medida em 02/08/2026. Se cair, é para APARECER,
            # não para a última linha sobrescrever a irmã em silêncio — é
            # literalmente o bug do idLancamentoServico do CLAUDE.md.
            log.warning("chave de agenda repetida (%s) — histórico pode ficar torto", k)
        atual[k] = l

    ses = McSession()
    try:
        vig = {v.chave: v for v in ses.execute(
            select(McAgendaVersao).where(McAgendaVersao.valido_ate.is_(None))
        ).scalars()}

        novas, alteradas, removidas, mudancas = [], [], [], []

        for k, l in atual.items():
            h = hash_cadastro(l)
            v = vig.get(k)
            if v is None:
                novas.append((k, l, h))
            elif v.hash != h:
                alteradas.append((k, l, h, v))

        # Removidas: sumiu do cadastro. SÓ olha postos que voltaram OK.
        for k, v in vig.items():
            if v.posto in ok and k not in atual:
                removidas.append((k, v))

        res = {
            "data": hoje.isoformat(),
            "novas": len(novas), "alteradas": len(alteradas),
            "removidas": len(removidas),
            "postos_ok": len(ok), "postos_erro": len(falhos),
            "postos_falhos": falhos,
            "primeira_carga": not vig,
            "detalhe": [],
        }

        for k, l, h in novas:
            res["detalhe"].append(("nova", k, l.get("medico"), None, None,
                                   l.get("custo_mensal") or 0))
        for k, l, h, v in alteradas:
            for campo, de, para in _diff_campos(v.dados, l):
                delta = ((l.get("custo_mensal") or 0) - float(v.custo_mensal or 0)
                         if campo == "valor_plantao" else 0)
                res["detalhe"].append(("alterada", k, l.get("medico"), campo, (de, para), delta))
        for k, v in removidas:
            res["detalhe"].append(("removida", k, v.medico, None, None,
                                   -float(v.custo_mensal or 0)))

        if dry_run:
            return res

        # ── escrita ─────────────────────────────────────────────────────────
        for k, l, h in novas:
            ses.add(_versao(k, l, h, hoje))
            mudancas.append(McMudanca(
                data=hoje, chave=k, tipo="nova", posto=l["posto"],
                medico=l.get("medico"), especialidade=l.get("especialidade"),
                dia_semana=l.get("dia_semana"),
                delta_mensal=round(l.get("custo_mensal") or 0, 2)))

        for k, l, h, v in alteradas:
            v.valido_ate = hoje              # fecha a versão anterior
            ses.add(_versao(k, l, h, hoje))
            for campo, de, para in _diff_campos(v.dados, l):
                delta = ((l.get("custo_mensal") or 0) - float(v.custo_mensal or 0)
                         if campo == "valor_plantao" else 0)
                mudancas.append(McMudanca(
                    data=hoje, chave=k, tipo="alterada", posto=l["posto"],
                    medico=l.get("medico"), especialidade=l.get("especialidade"),
                    dia_semana=l.get("dia_semana"), campo=campo,
                    valor_de=de, valor_para=para, delta_mensal=round(delta, 2)))

        for k, v in removidas:
            v.valido_ate = hoje
            mudancas.append(McMudanca(
                data=hoje, chave=k, tipo="removida", posto=v.posto,
                medico=v.medico, especialidade=v.especialidade,
                dia_semana=v.dia_semana,
                delta_mensal=-round(float(v.custo_mensal or 0), 2)))

        for m in mudancas:
            ses.add(m)

        # Execução: uma por dia. Reexecutar no mesmo dia ATUALIZA a linha em vez
        # de duplicar (o cron pode ser rodado à mão depois de uma falha).
        ex = ses.execute(select(McExecucao).where(McExecucao.data == hoje)).scalar_one_or_none()
        if ex is None:
            ex = McExecucao(data=hoje)
            ses.add(ex)
        ex.gerado_em = db.now_brt()
        ex.postos_ok, ex.postos_erro = len(ok), len(falhos)
        ex.postos_falhos = ",".join(falhos)
        ex.completa = not falhos
        ex.linhas = resumo.get("linhas", 0)
        ex.linhas_plantao = resumo.get("linhas_plantao", 0)
        ex.medicos = resumo.get("medicos", 0)
        ex.custo_semanal = resumo.get("custo_semanal", 0)
        ex.custo_mensal = resumo.get("custo_mensal", 0)
        ex.parametros = parametros
        ex.novas, ex.alteradas = len(novas), len(alteradas)
        ex.removidas = len(removidas)

        ses.commit()
        return res
    except Exception:
        ses.rollback()
        raise
    finally:
        ses.close()


def _versao(chave: str, l: dict, h: str, quando: date) -> McAgendaVersao:
    return McAgendaVersao(
        chave=chave, posto=l["posto"], id_medico=l.get("id_medico"),
        id_especialidade=l.get("id_especialidade"), medico=l.get("medico"),
        especialidade=l.get("especialidade"), dia_semana=l.get("dia_semana"),
        hash=h, dados=l, valido_de=quando, valido_ate=None,
        custo_mensal=round(l.get("custo_mensal") or 0, 2))


# ── leitura ─────────────────────────────────────────────────────────────────
def datas_disponiveis() -> list[dict]:
    """Dias com execução registrada, do mais antigo para o mais novo.

    A tela só deixa escolher a partir da primeira — pedir 12/2025 quando a
    coleta começou em 08/2026 devolveria uma página vazia sem explicação.
    """
    ses = McSession()
    try:
        return [
            {"data": e.data.isoformat(), "completa": bool(e.completa),
             "postos_erro": e.postos_erro, "postos_falhos": e.postos_falhos or "",
             "custo_mensal": float(e.custo_mensal or 0),
             "novas": e.novas, "alteradas": e.alteradas, "removidas": e.removidas}
            for e in ses.execute(
                select(McExecucao).order_by(McExecucao.data)).scalars()
        ]
    finally:
        ses.close()


def linhas_em(dia: date) -> tuple[list[dict], dict | None]:
    """As linhas do cadastro como estavam em `dia`, e a execução daquele dia.

    Versão vigente naquela data: valido_de <= dia < valido_ate (ou ainda aberta).
    Se `dia` cair num buraco (ETL não rodou), devolve a execução ANTERIOR mais
    próxima — e quem chama diz na tela qual data está realmente sendo mostrada.
    """
    ses = McSession()
    try:
        ex = ses.execute(
            select(McExecucao).where(McExecucao.data <= dia)
            .order_by(McExecucao.data.desc()).limit(1)).scalar_one_or_none()
        if ex is None:
            return [], None
        alvo = ex.data
        vs = ses.execute(
            select(McAgendaVersao).where(
                McAgendaVersao.valido_de <= alvo,
                or_(McAgendaVersao.valido_ate.is_(None),
                    McAgendaVersao.valido_ate > alvo))
        ).scalars()
        meta = {
            "data": alvo.isoformat(), "gerado_em": ex.gerado_em.isoformat(),
            "completa": bool(ex.completa), "postos_erro": ex.postos_erro,
            "postos_falhos": ex.postos_falhos or "", "parametros": ex.parametros or {},
            "novas": ex.novas, "alteradas": ex.alteradas, "removidas": ex.removidas,
        }
        return [dict(v.dados) for v in vs], meta
    finally:
        ses.close()


def contagem() -> dict:
    ses = McSession()
    try:
        return {
            "execucoes": ses.execute(select(func.count(McExecucao.id))).scalar(),
            "versoes": ses.execute(select(func.count(McAgendaVersao.id))).scalar(),
            "vigentes": ses.execute(select(func.count(McAgendaVersao.id))
                                    .where(McAgendaVersao.valido_ate.is_(None))).scalar(),
            "mudancas": ses.execute(select(func.count(McMudanca.id))).scalar(),
        }
    finally:
        ses.close()
