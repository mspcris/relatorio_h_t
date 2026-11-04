"""Regra de "dia com vaga válida" da Qualidade da Agenda — sem dependências.

Caso que motivou (Leonardo, 04/11/2026): Pediatria em Campo Grande atendia 18
pacientes no dia e tinha 1 vaga livre (5,56 %). O cad_cbos pede no mínimo 20 %
de vagas livres, logo esse dia NÃO é "próxima vaga". A tela mostrava 6d porque
o SQL casava o cad_cbos por nome exato (a agenda diz PEDIATRIA, o cad_cbos diz
Pediatra), não achava e usava mínimo 0.

Um dia é descrito por ``[offset_em_dias, vagas_livres, capacidade]`` — soma do
dia inteiro da especialidade no posto (todas as agendas/médicos), que é como o
gestor lê ("a pediatria do dia atende 18"). O percentual é sobre essa soma.

Usado por export_qualidade_agenda.py; qualidade_agenda.html repete a mesma
conta em JS (``diaPassa``) para refletir na hora um % alterado em Configurar
Prazos. Mudou aqui, muda lá.
"""


def pct_livre(vagas, capacidade):
    """% de vagas livres do dia, 2 casas; None quando não há capacidade."""
    try:
        cap = float(capacidade or 0)
        if cap <= 0:
            return None
        return round(float(vagas or 0) * 100.0 / cap, 2)
    except (TypeError, ValueError):
        return None


def dia_passa(item, minimo):
    """True se o dia tem pelo menos 1 vaga livre E % livre >= mínimo do cad_cbos.

    `minimo` None/0 = sem exigência de percentual (continua exigindo 1 vaga —
    dia lotado nunca é próxima vaga, desvio consciente da view, 2026-09-13).
    """
    try:
        vagas = int(item[1] or 0)
        cap   = int(item[2] or 0)
    except (TypeError, ValueError, IndexError):
        return False
    if vagas <= 0:
        return False
    try:
        m = float(minimo or 0)
    except (TypeError, ValueError):
        m = 0.0
    if m <= 0:
        return True
    pct = pct_livre(vagas, cap)
    return pct is not None and pct >= m


def proxima_vaga(dias_livres, minimo, a_partir_de=0):
    """Primeiro dia (offset >= a_partir_de) que passa na régua, ou None."""
    if not dias_livres:
        return None
    for item in dias_livres:
        try:
            off = int(item[0])
        except (TypeError, ValueError, IndexError):
            continue
        if off < 0 or off < (a_partir_de or 0):
            continue
        if dia_passa(item, minimo):
            return item
    return None
