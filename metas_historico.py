"""Como cada posto COSTUMA estar no dia X do mês — a régua honesta da meta.

Regra de três engana. A meta de mensalidades é do mês inteiro, mas o dinheiro
não entra em fatia igual todo dia: medindo os 26 meses de série diária do
`export_metas.py`, o posto A tem 18% do mês no dia 5, 46% no dia 10 e 58% no
dia 13 — a régua linear diria 16%, 32% e 42%. Quem compara com a linear pinta
de verde, no dia 13, um posto que na verdade está no piso do normal.

Vendas é outro bicho: a curva é quase linear (45% no dia 13 contra 42% da
linear), mas o mês fecha entre 47% e 138% da meta. Ali um ponto único mente —
por isso aqui sai uma FAIXA (p25–p75), não um número só.

Testado em 13/09/2026: casar os meses pelo mesmo número de DIAS ÚTEIS
decorridos, em vez do dia do mês, não apertou a faixa (4,1 pontos contra 2,9
no posto A) — mensalidade é presa ao dia do calendário (vencimento), não ao
expediente. Fica o dia do mês, que é mais simples e mais fiel.

Só lê o que o `export_metas.py` já gerou: mês fechado, com meta cadastrada.
Mês corrente nunca entra na própria régua.
"""

from __future__ import annotations

import json
import os
import statistics as st
from datetime import date

# Meses fechados que entram na conta. Três anos cobre a sazonalidade (o mesmo
# mês em anos anteriores) sem arrastar um posto que mudou de tamanho.
JANELA_MESES = 36
# Abaixo disso não há régua: seis meses é o mínimo para uma mediana significar
# alguma coisa. Com menos, quem pergunta cai na régua linear e sabe disso.
MINIMO_MESES = 6
RECENTES = 6  # "nos últimos meses estava em Y"

_cache: dict = {}


def _ler(metas_dir: str, posto: str, ym: str) -> dict | None:
    """JSON do mês, memorizado por (caminho, mtime): mês fechado nunca muda."""
    caminho = os.path.join(metas_dir, f"{posto}_metas_{ym}.json")
    try:
        stat = os.stat(caminho)
    except OSError:
        return None
    chave = (caminho, stat.st_mtime, stat.st_size)
    if chave not in _cache:
        try:
            with open(caminho, encoding="utf-8") as f:
                _cache[chave] = json.load(f)
        except (OSError, ValueError):
            _cache[chave] = None
    return _cache[chave]


def _meses_antes(ym: str, quantos: int) -> list[str]:
    """Os `quantos` meses fechados imediatamente anteriores a `ym`, do mais
    antigo para o mais novo."""
    ano, mes = int(ym[:4]), int(ym[5:7])
    saida = []
    for _ in range(quantos):
        mes -= 1
        if mes == 0:
            ano, mes = ano - 1, 12
        saida.append(f"{ano:04d}-{mes:02d}")
    return list(reversed(saida))


def dias_no_mes(ym: str) -> int:
    ano, mes = int(ym[:4]), int(ym[5:7])
    return (date(ano + (mes == 12), (mes % 12) + 1, 1) - date(ano, mes, 1)).days


def _acumulado(dias: list, campo: str, ate: int) -> float:
    return sum(float(d.get(campo) or 0) for d in dias or [] if int(d.get("dia") or 0) <= ate)


def _fechado(dias: list) -> bool:
    """Mês inteiro no arquivo. O corrente vem pela metade e não pode virar régua."""
    return len({int(d.get("dia") or 0) for d in dias or []}) >= 20


def _corte(dia: int, dias_alvo: int, dias_daquele: int) -> int:
    """O dia equivalente naquele mês.

    Dia 13 é dia 13 em qualquer mês — mensalidade segue o calendário (dia 5,
    dia 10), não a fração do mês. A exceção é a BORDA: 28 de fevereiro é o mês
    inteiro, e comparar isso com o dia 28 de um mês de 31 diria que o posto
    está muito à frente quando ele só chegou ao fim. Fechou aqui, compara com
    o fechamento de lá."""
    if dia >= dias_alvo:
        return 31
    return min(dia, dias_daquele)


def _serie(metas_dir: str, posto: str, ym: str, dia: int, dias_alvo: int, lista: str,
           campo: str, chave_meta: str) -> list[tuple[str, float, float]]:
    """(mês, % da meta no dia equivalente, % da meta no fechamento) por mês fechado."""
    saida = []
    for anterior in _meses_antes(ym, JANELA_MESES):
        d = _ler(metas_dir, posto, anterior)
        if not d:
            continue
        meta = float((d.get("meta") or {}).get(chave_meta) or 0)
        dias = d.get(lista) or []
        if meta <= 0 or not _fechado(dias):
            continue
        ate = _corte(dia, dias_alvo, dias_no_mes(anterior))
        saida.append((anterior,
                      100.0 * _acumulado(dias, campo, ate) / meta,
                      100.0 * _acumulado(dias, campo, 31) / meta))
    return saida


def _resumir(serie: list[tuple[str, float, float]], mes_do_ano: int) -> dict | None:
    if len(serie) < MINIMO_MESES:
        return dict(meses=len(serie), suficiente=False)
    valores = [pct for _, pct, _ in serie]
    q = st.quantiles(valores, n=4)
    return dict(
        suficiente=True, meses=len(serie),
        mediana=round(st.median(valores), 1),
        p25=round(q[0], 1), p75=round(q[2], 1),
        minimo=round(min(valores), 1), maximo=round(max(valores), 1),
        # Onde o mês costuma TERMINAR: mensalidade fecha em ~96% da meta, e
        # sem isso "56% no dia 13" parece longe de 100% quando não está.
        fecha_mediana=round(st.median([f for _, _, f in serie]), 1),
        # As duas referências que o gestor pede em voz alta: "em setembro do
        # ano passado estava em X" e "nos últimos meses, em Y".
        mesmo_mes=[dict(ym=ym, pct=round(pct, 1))
                   for ym, pct, _ in serie if int(ym[5:7]) == mes_do_ano],
        recentes=[dict(ym=ym, pct=round(pct, 1)) for ym, pct, _ in serie[-RECENTES:]],
    )


# ── Chance de bater a meta ──────────────────────────────────────────────────
# Previsão feita SÓ com o passado do próprio posto — nada de fórmula mágica.
#
# Dois modelos ingênuos, cada um certo por um motivo diferente, e a média dos
# dois (medido em 14/09/2026 sobre 884 previsões de 13 postos):
#   • multiplicativo: quem está atrás continua proporcionalmente atrás.
#     Ganha em vendas (erro médio 12,9 contra 13,0 do aditivo).
#   • aditivo: o que falta entra em bloco, independente do que já entrou.
#     Ganha em mensalidades (3,8 contra 4,1) — mensalidade atrasada é paga.
#
# A CHANCE não sai do ponto: sai da distribuição dos ERROS que este mesmo
# modelo cometeu no passado do posto (leave-one-out). Sem isso ele fica
# convencido demais: a versão que só espalhava os cenários históricos dizia
# "80–99%" e acontecia 75% das vezes. Com os erros, a calibração fica quase
# na diagonal — disse 60–79%, aconteceu 65,6%; disse 80–99%, aconteceu 85,4%.
#
# Antes do dia 10 não se prevê: o erro médio mais que dobra (13,7 pontos no
# dia 5 contra 5,4 no dia 10). Número ruim é pior do que "ainda é cedo".
DIA_MINIMO = 10
FATIA_MULTIPLICATIVA = 0.5


def _ponto(serie_acc: list[tuple[str, dict]], dia: int, hoje: float) -> float | None:
    """Fechamento central previsto, em % da meta: média dos dois ingênuos."""
    mult, add = [], []
    for ym, acc in serie_acc:
        d = min(dia, dias_no_mes(ym))
        if acc.get(d, 0) > 0:
            mult.append(acc[31] / acc[d])
            add.append(acc[31] - acc[d])
    if not mult:
        return None
    return (FATIA_MULTIPLICATIVA * (hoje * st.median(mult))
            + (1 - FATIA_MULTIPLICATIVA) * (hoje + st.median(add)))


def _acumulado_por_mes(metas_dir: str, posto: str, ym: str, lista: str, campo: str,
                       chave_meta: str) -> list[tuple[str, dict]]:
    """[(mês, {dia: % da meta acumulado})] dos meses fechados com meta."""
    saida = []
    for anterior in _meses_antes(ym, JANELA_MESES):
        d = _ler(metas_dir, posto, anterior)
        if not d:
            continue
        meta = float((d.get("meta") or {}).get(chave_meta) or 0)
        dias = d.get(lista) or []
        if meta <= 0 or not _fechado(dias):
            continue
        acc, total = {}, 0.0
        por_dia: dict = {}
        for r in dias:
            por_dia[int(r.get("dia") or 0)] = por_dia.get(int(r.get("dia") or 0), 0.0) + float(r.get(campo) or 0)
        for dia in range(1, 32):
            total += por_dia.get(dia, 0.0)
            acc[dia] = 100.0 * total / meta
        saida.append((anterior, acc))
    return saida


def _prever(serie_acc, dia: int, hoje: float) -> dict | None:
    central = _ponto(serie_acc, dia, hoje)
    if central is None:
        return None
    # Quanto este modelo ERROU, mês a mês, prevendo neste mesmo dia. É essa
    # nuvem de erros que vira a incerteza de hoje — e é ela que o calibra.
    erros = []
    for alvo, acc in serie_acc:
        d = min(dia, dias_no_mes(alvo))
        if acc.get(d, 0) <= 0:
            continue
        estimado = _ponto([(y, a) for y, a in serie_acc if y != alvo], dia, acc[d])
        if estimado is not None:
            erros.append(acc[31] - estimado)
    if len(erros) < MINIMO_MESES:
        return dict(suficiente=False, meses=len(erros))
    cenarios = sorted(central + e for e in erros)
    batem = sum(1 for c in cenarios if c >= 100)
    mult = [acc[31] / acc[min(dia, dias_no_mes(y))]
            for y, acc in serie_acc if acc.get(min(dia, dias_no_mes(y)), 0) > 0]
    add = [acc[31] - acc[min(dia, dias_no_mes(y))]
           for y, acc in serie_acc if acc.get(min(dia, dias_no_mes(y)), 0) > 0]
    return dict(
        suficiente=True, meses=len(erros), hoje=round(hoje, 1),
        chance=round(100 * batem / len(cenarios)),
        cenarios_que_batem=batem, cenarios=len(cenarios),
        projecao=round(central, 1),
        piso=round(cenarios[0], 1), teto=round(cenarios[-1], 1),
        # O quanto esta previsão costuma errar — sem isso, o número parece
        # mais exato do que é.
        erro_medio=round(st.mean(abs(e) for e in erros), 1),
        multiplicador=round(st.median(mult), 3),
        acrescimo=round(st.median(add), 1),
    )


def previsao_do_posto(metas_dir: str, posto: str, ym: str, dia: int,
                      mens_hoje: float | None, vendas_hoje: float | None) -> dict:
    """Chance de fechar a meta deste mês, em % da meta, para as duas metas."""
    saida = {"dia": dia, "dias_no_mes": dias_no_mes(ym), "dia_minimo": DIA_MINIMO}
    cedo = dia < DIA_MINIMO
    for chave, lista, campo, chave_meta, hoje in (
        ("mensalidades", "mensalidades_por_dia", "mens_dia", "meta_mens", mens_hoje),
        ("vendas", "vendas_por_dia", "vendas_dia", "meta_venda", vendas_hoje),
    ):
        if cedo:
            saida[chave] = dict(suficiente=False, cedo=True, meses=0)
        elif hoje is None:
            saida[chave] = dict(suficiente=False, sem_meta=True, meses=0)
        else:
            serie = _acumulado_por_mes(metas_dir, posto, ym, lista, campo, chave_meta)
            saida[chave] = _prever(serie, dia, hoje) or dict(suficiente=False, meses=0)
    return saida


def do_posto(metas_dir: str, posto: str, ym: str, dia: int) -> dict:
    """Régua histórica deste posto para o dia `dia` do mês `ym`, em % da meta."""
    mes_do_ano, dias_alvo = int(ym[5:7]), dias_no_mes(ym)
    return {
        "dia": dia, "dias_no_mes": dias_alvo,
        "mensalidades": _resumir(
            _serie(metas_dir, posto, ym, dia, dias_alvo,
                   "mensalidades_por_dia", "mens_dia", "meta_mens"),
            mes_do_ano),
        "vendas": _resumir(
            _serie(metas_dir, posto, ym, dia, dias_alvo,
                   "vendas_por_dia", "vendas_dia", "meta_venda"),
            mes_do_ano),
    }


# ── Ritmo do dia: quanto costuma ter entrado até hoje, sem meta ─────────────
# Pedido do Cristiano (16/09/2026): "em média eu, no dia X, recebo 400
# mensalidades, mas recebi 390". É a régua em QUANTIDADE, não em % da meta —
# vale para posto sem meta cadastrada (vendas, quase todos) e se soma entre
# postos sem ponderação nenhuma: a média do agrupamento é a soma das médias.
#
# O corte é o último dia FECHADO: o `export_metas.py` roda de 6 em 6 horas e
# o dia de hoje chega pela metade (A tinha 154 mensalidades às 18h do dia 16,
# num dia que costuma dar 300). Comparar com ele pintaria toda manhã de
# vermelho. A régua olha os 12 meses fechados anteriores.

MESES_RITMO = 12
MINIMO_RITMO = 3   # com menos de três meses, "média" é só um mês bom ou ruim


def ritmo_do_posto(metas_dir: str, posto: str, ym: str, dia: int) -> dict:
    """Acumulado de mensalidades e vendas até o dia `dia` no mês `ym`, e a
    média do mesmo acumulado nos 12 meses fechados anteriores.

    `dia` = 0 (primeiro dia do mês, nada fechou ainda) devolve tudo zerado
    com `suficiente=False` — não há o que comparar."""
    dias_alvo = dias_no_mes(ym)
    atual = _ler(metas_dir, posto, ym) or {}
    saida = {"dia": dia, "meses_janela": MESES_RITMO}
    for metrica, lista, campo in (("mensalidades", "mensalidades_por_dia", "mens_dia"),
                                  ("vendas", "vendas_por_dia", "vendas_dia")):
        valores = []
        if dia > 0:
            for anterior in _meses_antes(ym, MESES_RITMO):
                d = _ler(metas_dir, posto, anterior)
                dias = (d or {}).get(lista) or []
                if not d or not _fechado(dias):
                    continue
                ate = _corte(dia, dias_alvo, dias_no_mes(anterior))
                valores.append(_acumulado(dias, campo, ate))
        realizado = _acumulado(atual.get(lista), campo, dia) if dia > 0 else 0.0
        suficiente = len(valores) >= MINIMO_RITMO
        saida[metrica] = dict(
            realizado=round(realizado), meses=len(valores), suficiente=suficiente,
            media=round(st.mean(valores), 1) if suficiente else None,
            minimo=round(min(valores)) if suficiente else None,
            maximo=round(max(valores)) if suficiente else None,
        )
    return saida
