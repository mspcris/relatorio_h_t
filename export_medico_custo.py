#!/usr/bin/env python3
"""
export_medico_custo.py — ETL do "Médico · Custo Efetivo Nominal".

Roda sql/medico_custo_efetivo.sql em TODOS os postos e gera um JSON achatado
que a página consome inteiro e agrupa no navegador (são ~1.300 linhas, cabe).

DEDICADO de propósito: não toca no export_custo_medico_ctrlq.py, que alimenta
o botão "Custo Médico no Ctrl-Q" da página de KPI e tem outro contrato de saída.

Só SELECT nos SQL Servers dos postos. Não escreve em lugar nenhum além do JSON.

O que o ETL calcula em cima do SQL (e por quê):

  horas_liquidas   (bruto − almoço) / 60. É a base honesta do valor/hora: um
                   plantão das 8h às 18h com 1h de almoço são 9h, não 10.
  valor_hora       valor_plantao / horas_liquidas.
  custo_por_vaga   valor_plantao / vagas. É o custo TETO por consulta — quanto
                   cada atendimento custa se a agenda encher. O número que
                   realmente diz se um plantão se paga.
  minutos_por_vaga tempo de agenda por consulta; cruza com o custo por vaga.
  custo_mensal     valor semanal × 4,345 (média de semanas no mês). Quinzenal
                   entra pela METADE — é o erro clássico de projeção.

Saída: json_consolidado/medico_custo.json
"""
from __future__ import annotations

import decimal
import json
import math
import os
import re
import sys
import traceback
from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_PATH = os.path.join(BASE_DIR, "sql", "medico_custo_efetivo.sql")
OUT_PATH = os.path.join(BASE_DIR, "json_consolidado", "medico_custo.json")

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS = list("ANXYBRPCDGIMJ")
_BRT = timezone(timedelta(hours=-3))

# Média de semanas num mês (365,25 / 7 / 12). Usar 4 subestima ~8%.
SEMANAS_NO_MES = 4.345

POSTOS_NOMES = {
    "A": "Anchieta", "N": "Nova Iguaçu", "X": "Caxias", "Y": "Campo Grande",
    "B": "Bangu", "R": "Realengo", "P": "Padre Miguel", "C": "Centro",
    "D": "Duque de Caxias", "G": "Guadalupe", "I": "Irajá", "M": "Madureira",
    "J": "Jacarepaguá",
}


def _env(k: str, d: str = "") -> str:
    v = os.getenv(k, d)
    return v.strip() if isinstance(v, str) else v


def _num(v):
    """Decimal/NaN/None → float|None. NaN mata o JSON.parse do browser."""
    if v is None:
        return None
    if isinstance(v, decimal.Decimal):
        v = float(v)
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v):
    n = _num(v)
    return int(n) if n is not None else None


def _str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _hora(v):
    """time/str → 'HH:MM' ou None."""
    if v is None:
        return None
    if isinstance(v, time):
        return v.strftime("%H:%M")
    if isinstance(v, timedelta):          # alguns drivers devolvem timedelta
        m = int(v.total_seconds() // 60)
        return f"{m // 60:02d}:{m % 60:02d}"
    s = str(v).strip()
    return s[:5] if s else None


def _data(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return None


def _conn(posto: str):
    host, base = _env(f"DB_HOST_{posto}"), _env(f"DB_BASE_{posto}")
    if not host or not base:
        return None
    cs = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{_env(f'DB_PORT_{posto}', '1433')};DATABASE={base};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        f"UID={_env(f'DB_USER_{posto}')};PWD={_env(f'DB_PASSWORD_{posto}')}"
    )
    return create_engine("mssql+pyodbc:///?odbc_connect=" + quote_plus(cs),
                         pool_pre_ping=True)


# Regra do almoço (definida pelo Cristiano em 2026-08-02):
#
#   Por PADRÃO a CAMIM não paga a hora de almoço. Então o valor/hora sai sobre
#   as horas TRABALHADAS, não sobre a janela da agenda:
#       R$ 1.100 numa janela de 12h com 1h de almoço → 11h → R$ 100/hora.
#
#   Mas a realidade é misturada, e ele foi explícito sobre isso:
#     - médicos ANTIGOS: a hora de almoço era paga, faz parte do pagamento;
#     - médicos NOVOS: paga-se só a hora trabalhada, exceto plantão;
#     - quem faz menos de 10h nem tem almoço na jornada.
#
#   Por isso o ETL calcula as DUAS visões e a página deixa alternar. O padrão é
#   o líquido (não paga almoço); o bruto fica ao lado para medir quanto a hora
#   de almoço custaria se fosse paga — que é a pergunta que resolve o imbróglio.
LIMITE_JORNADA_LONGA_H = 10.0


def _derivados(r: dict) -> dict:
    """Métricas calculadas — a parte que transforma cadastro em análise."""
    brutos = r.get("minutos_brutos")
    almoco = r.get("minutos_almoco") or 0
    liquidos = (brutos - almoco) if brutos is not None else None
    if liquidos is not None and liquidos <= 0:
        liquidos = None

    valor = r.get("valor_plantao") or 0.0
    # QuantidadeMaxima é o teto da agenda; quando não cadastrado, cai na
    # Quantidade normal. SEM NENHUM DOS DOIS o médico atende por ORDEM DE
    # CHEGADA (confirmado pelo Cristiano em 2026-08-02) — não é dado faltando.
    # Nesse caso não existe custo por consulta e a página não deve inventar um:
    # marca o selo "ordem de chegada" e deixa a métrica vazia. Em Anchieta isso
    # é ~44% das linhas, então tratar como "faltando" distorceria tudo.
    vagas = r.get("vagas_maxima") or r.get("vagas") or None

    # Modalidade real vem dos BITS, não da ausência de vagas (Cristiano):
    #   OC = ordem de chegada / livre demanda · WWW = internet · TEL = central.
    # Os três convivem: "10 números agendados e o resto por ordem de chegada".
    oc = bool(r.get("oc"))
    agendado = bool(r.get("www")) or bool(r.get("tel"))
    if oc and agendado:
        modalidade = "misto"
    elif oc:
        modalidade = "ordem_chegada"
    elif agendado:
        modalidade = "agendado"
    else:
        # nenhum bit ligado: sem vaga cadastrada é livre demanda na prática
        modalidade = "ordem_chegada" if vagas is None else "agendado"

    # Custo por consulta só existe onde há vaga contada. Em OC puro a demanda é
    # aberta e não há denominador — a página mostra o selo e deixa vazio, em vez
    # de inventar número. No misto, as vagas cobrem SÓ a parte agendada.
    conta_vaga = vagas is not None and modalidade != "ordem_chegada"
    ordem_chegada = modalidade == "ordem_chegada"

    horas_liq = round(liquidos / 60.0, 3) if liquidos else None
    horas_bru = round(brutos / 60.0, 3) if brutos else None
    quinzenal = bool(r.get("agenda_quinzenal"))

    vh_liq = round(valor / horas_liq, 2) if horas_liq else None
    vh_bru = round(valor / horas_bru, 2) if horas_bru else None

    return {
        "horas_liquidas": horas_liq,
        "horas_brutas": horas_bru,
        "horas_almoco": round(almoco / 60.0, 3) if almoco else 0.0,
        "vagas_efetivas": vagas,
        "ordem_chegada": ordem_chegada,
        "modalidade": modalidade,
        "oc": oc,
        "agendado_www": bool(r.get("www")),
        "agendado_tel": bool(r.get("tel")),
        # padrão da casa: não paga almoço → hora sobre a jornada trabalhada
        "valor_hora": vh_liq,
        # e a mesma hora se o almoço fosse pago (janela cheia)
        "valor_hora_bruto": vh_bru,
        # quanto a hora "barateia" por não pagar o almoço — o custo escondido
        "delta_almoco_hora": round(vh_liq - vh_bru, 2)
                             if (vh_liq and vh_bru) else None,
        # jornada longa é onde o almoço realmente pesa; abaixo de 10h não há
        "jornada_longa": bool(horas_bru and horas_bru >= LIMITE_JORNADA_LONGA_H),
        "custo_por_vaga": round(valor / vagas, 2) if conta_vaga else None,
        "minutos_por_vaga": round(liquidos / vagas, 1)
                            if (conta_vaga and liquidos) else None,
        # quinzenal ocorre metade das vezes: projetar cheio infla o mês
        "custo_mensal": round(valor * SEMANAS_NO_MES / (2 if quinzenal else 1), 2),
        "vagas_mensais": round(vagas * SEMANAS_NO_MES / (2 if quinzenal else 1))
                         if conta_vaga else None,
    }


_OPC = re.compile(r"\{\{OPC:(\w+)\.(\w+):([^}]+)\}\}")


def resolver_opcionais(con, sql: str) -> str:
    """Troca {{OPC:alias.coluna:tipo}} pela conversão real ou por NULL.

    Os 13 postos NÃO têm o mesmo schema — Nova Iguaçu, por exemplo, não tem
    cad_especialidade.valorconsultaclube, e a query inteira falhava com
    "Nome de coluna inválido" naquele posto só. Em vez de remover a coluna de
    todo mundo (perdendo o dado nos 12 que a têm), o placeholder vira NULL
    apenas onde ela não existe.
    """
    TABELAS = {"e": "cad_especialidade", "m": "cad_medico"}
    def _troca(mo):
        alias, coluna, tipo = mo.group(1), mo.group(2), mo.group(3)
        tab = TABELAS.get(alias)
        existe = con.execute(
            text("SELECT COL_LENGTH(:t, :c)"), {"t": tab, "c": coluna}
        ).scalar() is not None if tab else False
        return (f"TRY_CONVERT({tipo}, {alias}.{coluna})" if existe
                else f"TRY_CONVERT({tipo}, NULL)")
    return _OPC.sub(_troca, sql)


def coletar(posto: str, sql: str) -> tuple[list, str | None]:
    eng = _conn(posto)
    if eng is None:
        return [], "sem credenciais no .env"
    try:
        with eng.connect() as con:
            res = con.execute(text(resolver_opcionais(con, sql)))
            cols = list(res.keys())
            linhas = []
            for tupla in res.fetchall():
                r = dict(zip(cols, tupla))
                linha = {
                    "posto": posto,
                    "posto_nome": POSTOS_NOMES.get(posto, posto),
                    "id_medico": _int(r.get("idMedico")),
                    "medico": _str(r.get("medico")),
                    "crm": _str(r.get("crm")) or _str(r.get("conselho_numero")),
                    "conselho": _str(r.get("conselho")),
                    "conselho_uf": _str(r.get("conselho_uf")),
                    # CPF, telefone e e-mail NÃO entram no JSON (decisão do
                    # Cristiano, 2026-08-02): a página é de custo e o JSON vai
                    # inteiro para o navegador. Ele pretende dar contato dos
                    # médicos a gestores mais adiante — quando for, reabilitar
                    # aqui e revisar quem tem acesso à página ANTES.
                    "especializacao": _str(r.get("especializacao")),
                    "sexo": _str(r.get("sexo")),
                    "medico_desde": _data(r.get("medico_desde")),
                    "pessoa_juridica": bool(r.get("pessoa_juridica")),
                    "id_especialidade": _int(r.get("idEspecialidade")),
                    "especialidade": _str(r.get("especialidade")) or "— sem especialidade —",
                    "descricao": _str(r.get("descricao")),
                    "cbos": _str(r.get("cbos")),
                    "sala": _str(r.get("sala")),
                    "dia_ordem": _int(r.get("dia_ordem")),
                    "dia_semana": _str(r.get("dia_semana")),
                    "hora_inicio": _hora(r.get("hora_inicio")),
                    "hora_fim": _hora(r.get("hora_fim")),
                    "almoco_inicio": _hora(r.get("almoco_inicio")),
                    "almoco_fim": _hora(r.get("almoco_fim")),
                    "minutos_brutos": _int(r.get("minutos_brutos")),
                    "minutos_almoco": _int(r.get("minutos_almoco")) or 0,
                    "valor_plantao": _num(r.get("valor_plantao")),
                    "vagas": _int(r.get("vagas")),
                    "vagas_maxima": _int(r.get("vagas_maxima")),
                    "qtd_custo": _int(r.get("qtd_custo")),
                    "oc": bool(r.get("oc")),
                    "www": bool(r.get("www")),
                    "tel": bool(r.get("tel")),
                    "agenda_quinzenal": bool(r.get("agenda_quinzenal")),
                    "temporario": bool(r.get("temporario")),
                    "recebe_por_comissao": bool(r.get("recebe_por_comissao")),
                    "atendimento_online": bool(r.get("atendimento_online")),
                    "acolhimento": bool(r.get("acolhimento")),
                    "exibe_no_f3": bool(r.get("exibe_no_f3")),
                    "data_plantao": _data(r.get("data_plantao")),
                    "exibe_de": _data(r.get("exibe_de")),
                    "exibe_ate": _data(r.get("exibe_ate")),
                    "valor_consulta_clube": _num(r.get("valor_consulta_clube")),
                    "total_semanal_medico": _num(r.get("total_semanal_medico")),
                    "total_semanal_especialidade": _num(r.get("total_semanal_especialidade")),
                    "dias_na_semana": _int(r.get("dias_na_semana")),
                }
                linha.update(_derivados(linha))
                linhas.append(linha)
        return linhas, None
    except Exception as e:  # noqa: BLE001
        return [], f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        try:
            eng.dispose()
        except Exception:
            pass


# Um médico é "fora da curva" quando a hora dele passa deste múltiplo da
# MEDIANA da própria especialidade na rede (confirmado pelo Cristiano: 30%).
# Mediana e não média: um único plantão caríssimo puxaria a média e esconderia
# justamente o que se quer achar.
FATOR_FORA_DA_CURVA = 1.30
# "caro com pouca vaga": custo por consulta acima do dobro da mediana da
# especialidade. Só faz sentido onde há vaga contada (agendado puro).
FATOR_CUSTO_VAGA_ALTO = 2.0
# ── Piso do plantão remunerado ──────────────────────────────────────────────
# Abaixo deste valor a linha NÃO é jornada de médico: é agenda de exame ou de
# equipamento, com o valor servindo só de marcador para a agenda existir.
# Medido em 2026-08-02: 183 das 776 linhas caem aqui — MAPA (com o "médico"
# chamado MAPA, das 10h às 10h01), Raio-X, Ultrassonografia, Ecocardiograma,
# Densitometria, Mamografia, Holter, Fisioterapia. Somam quase nada em reais
# mas envenenavam tudo: entravam na contagem de médicos e criavam spread de
# 2.400% entre postos.
#
# Elas continuam no JSON, num grupo próprio (tipo_agenda), e ficam fora das
# estatísticas de médico — decisão do Cristiano.
#
# Piso definido pelo Cristiano em 2026-08-02: "menos de R$ 200 é estranho
# demais; 200 já é muito estranho, mas passa". Então o corte é >= 200,00.
# Ele derruba junto as agendas por sessão (fono, psicologia, psicopedagogia,
# terapia ABA — que apareciam com R$ 60 a R$ 80 por jornada de 9h, ou seja
# R$ 6,67/hora), sem precisar de regra por especialidade.
PISO_PLANTAO_REMUNERADO = 200.00

# Especialidades que a CAMIM remunera SÓ por comissão/sessão — nunca por
# plantão (Cristiano, 2026-08-02). Elas caem abaixo do piso naturalmente, mas
# a lista existe para a tela dizer o MOTIVO certo em vez de "abaixo do piso",
# que sugeriria erro de cadastro.
ESPECIALIDADES_POR_COMISSAO = {
    "FONOAUDIOLOGIA", "FONOAUDIOLOGIA EXAME", "PSICOLOGIA", "NEUROPSICOLOGIA",
    "PSICOPEDAGOGIA", "NEUROPSICOPEDAGOGIA", "PSICOMOTRICIDADE CLÍNICA",
    "TERAPIA ABA", "TERAPIA ALIMENTAR", "FISIOTERAPIA", "BIOLOGIA",
}
# Agendas de exame/equipamento: o "médico" é o aparelho ou a sala.
ESPECIALIDADES_EXAME = {
    "MAPA", "HOLTER 24 HRS", "RAIO X", "RADIOLOGIA", "MAMOGRAFIA",
    "DENSITOMETRIA", "ULTRASSONOGRAFIA", "ECOCARDIOGRAMA", "ENDOSCOPIA",
    "COLONOSCOPIA", "ELETROENCEFALOGRAMA", "ELETRONEUROMIOGRAFIA",
    "ESPIROMETRIA", "ECO DOPPLER VASCULAR", "RESSONANCIA MAGNETICA",
    "TOMOGRAFIA COM CONTRASTE", "AGENDAMENTO DE EXAMES",
}


def _mediana(vals: list[float]) -> float | None:
    v = sorted(x for x in vals if x is not None)
    if not v:
        return None
    meio = len(v) // 2
    return v[meio] if len(v) % 2 else (v[meio - 1] + v[meio]) / 2


def analisar(linhas: list[dict]) -> dict:
    """Marca cada linha com os alertas e devolve as referências usadas.

    Roda depois de juntar TODOS os postos: a comparação é contra a rede, não
    contra o posto — é assim que dá para ver diferença de preço entre postos.
    """
    por_esp: dict[str, list] = {}
    for l in linhas:
        por_esp.setdefault(l["especialidade"], []).append(l)

    def _vale(l) -> bool:
        """Linha que entra na estatística de PLANTÃO MÉDICO.

        Fora: agenda de exame/equipamento (abaixo do piso) e médico por
        comissão — no segundo caso o valor fixo não é o custo real, então a
        hora dele não é comparável com a dos outros.
        """
        return (l["tipo_agenda"] == "plantao"
                and not l["recebe_por_comissao"])

    referencias = {}
    for esp, todas in por_esp.items():
        # SEM fallback para `todas`: quando NENHUMA linha da especialidade é
        # plantão remunerado (fono, psicologia, eletroencefalograma — todas por
        # sessão), a especialidade simplesmente não tem referência. O `or todas`
        # que estava aqui anulava o filtro justamente nesses casos e devolvia
        # mediana de R$ 0,30 com spread de 16.000%.
        ls = [l for l in todas if _vale(l)]
        if not ls:
            referencias[esp] = {
                "mediana_hora": None, "mediana_custo_vaga": None,
                "medicos": len({(l["posto"], l["id_medico"]) for l in todas}),
                "linhas_ignoradas": len(todas), "postos": [],
                "hora_por_posto": {}, "spread_postos": None, "spread_pct": None,
                "sem_plantao_remunerado": True,
            }
            continue
        med_hora = _mediana([l["valor_hora"] for l in ls])
        med_vaga = _mediana([l["custo_por_vaga"] for l in ls])
        # diferença de preço da MESMA especialidade entre postos
        por_posto = {}
        for l in ls:
            if l["valor_hora"]:
                por_posto.setdefault(l["posto"], []).append(l["valor_hora"])
        medianas_posto = {p: _mediana(v) for p, v in por_posto.items()}
        validas = [v for v in medianas_posto.values() if v]
        referencias[esp] = {
            "mediana_hora": round(med_hora, 2) if med_hora else None,
            "mediana_custo_vaga": round(med_vaga, 2) if med_vaga else None,
            "medicos": len({(l["posto"], l["id_medico"]) for l in todas}),
            "linhas_ignoradas": len(todas) - len(ls),
            "postos": sorted(por_posto),
            "hora_por_posto": {p: round(v, 2) for p, v in medianas_posto.items() if v},
            "spread_postos": round(max(validas) - min(validas), 2) if len(validas) > 1 else None,
            "spread_pct": round((max(validas) / min(validas) - 1) * 100, 1)
                          if len(validas) > 1 and min(validas) else None,
            "sem_plantao_remunerado": False,
        }

    hoje = datetime.now(_BRT).date().isoformat()
    for l in linhas:
        ref = referencias[l["especialidade"]]
        alertas = []
        # linha fora do plantão remunerado não é comparável com ninguém
        if l["tipo_agenda"] != "plantao":
            l["alertas"] = []
            l["motivos_suspeita"] = [{
                "comissao": f"{l['especialidade'].title()} é remunerada por "
                            "comissão, não por plantão — o valor cadastrado "
                            f"(R$ {l['valor_plantao']:.2f}) não é o custo real",
                "exame": f"agenda de exame/equipamento (R$ {l['valor_plantao']:.2f} "
                         "é marcador, não preço)",
            }.get(l["motivo_fora"],
                  f"plantão de R$ {l['valor_plantao']:.2f}, abaixo do piso de "
                  f"R$ {PISO_PLANTAO_REMUNERADO:.0f} — conferir cadastro")]
            continue

        mh = ref["mediana_hora"]
        if mh and l["valor_hora"] and l["valor_hora"] > mh * FATOR_FORA_DA_CURVA:
            alertas.append("fora_da_curva")
            l["pct_acima_mediana"] = round((l["valor_hora"] / mh - 1) * 100, 1)

        mv = ref["mediana_custo_vaga"]
        if mv and l["custo_por_vaga"] and l["custo_por_vaga"] > mv * FATOR_CUSTO_VAGA_ALTO:
            alertas.append("caro_por_vaga")

        # cadastro suspeito: cada motivo é um item, para a tela explicar
        motivos = []
        if l["minutos_brutos"] is None:
            motivos.append("sem horário cadastrado")
        elif l["horas_brutas"] and l["horas_brutas"] > 16:
            motivos.append(f"jornada de {l['horas_brutas']:.0f}h num dia só")
        if l["exibe_ate"] and l["exibe_ate"] < hoje:
            motivos.append(f"agenda vencida em {l['exibe_ate']}")
        if l["recebe_por_comissao"]:
            motivos.append("recebe por comissão — o valor fixo não é o custo real")
        if l["valor_hora"] and l["valor_hora"] > 2000:
            motivos.append("valor/hora fora de qualquer padrão")
        if motivos:
            alertas.append("cadastro_suspeito")
        l["motivos_suspeita"] = motivos
        l["alertas"] = alertas

    return referencias


def _gravar(caminho: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    tmp = caminho + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        # allow_nan=False: NaN passa pelo json.dump do Python e o JSON.parse do
        # browser rejeita — a página cairia num 404 enganoso.
        json.dump(payload, f, ensure_ascii=False, allow_nan=False, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, caminho)


def main() -> int:
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    load_dotenv("/opt/relatorio_h_t/.env")
    print("=== Médico · Custo Efetivo Nominal ===")

    sql = open(SQL_PATH, encoding="utf-8").read()
    linhas, status = [], {}
    for p in POSTOS:
        try:
            achadas, erro = coletar(p, sql)
        except Exception:  # noqa: BLE001
            achadas, erro = [], traceback.format_exc(limit=1).strip()[:200]
        status[p] = {"posto": p, "nome": POSTOS_NOMES.get(p, p),
                     "linhas": len(achadas), "erro": erro}
        linhas.extend(achadas)
        print(f"  {p} {POSTOS_NOMES.get(p, p):<16} {len(achadas):>5} linhas"
              + (f"  ERRO: {erro}" if erro else ""))

    for l in linhas:
        plantao = (l["valor_plantao"] or 0) >= PISO_PLANTAO_REMUNERADO
        l["tipo_agenda"] = "plantao" if plantao else "sem_custo_plantao"
        l["conta_como_medico"] = plantao
        # POR QUE ficou de fora — o piso é só o detector; o motivo real muda a
        # leitura. Fono e psico, por exemplo, recebem exclusivamente por
        # comissão (Cristiano, 2026-08-02); não é agenda mal cadastrada.
        l["motivo_fora"] = None if plantao else (
            "comissao" if (l["recebe_por_comissao"]
                           or l["especialidade"] in ESPECIALIDADES_POR_COMISSAO)
            else "exame" if l["especialidade"] in ESPECIALIDADES_EXAME
            else "abaixo_do_piso")
    referencias = analisar(linhas)
    plantoes = [l for l in linhas if l["tipo_agenda"] == "plantao"]
    outras = [l for l in linhas if l["tipo_agenda"] != "plantao"]
    medicos = {(l["posto"], l["id_medico"]) for l in plantoes}
    semanal = sum(l["valor_plantao"] or 0 for l in plantoes)
    mensal = sum(l["custo_mensal"] or 0 for l in plantoes)
    payload = {
        "gerado_em": datetime.now(_BRT).replace(microsecond=0).isoformat(),
        "semanas_no_mes": SEMANAS_NO_MES,
        "postos": list(status.values()),
        "referencias": referencias,
        "parametros": {
            "fator_fora_da_curva": FATOR_FORA_DA_CURVA,
            "fator_custo_vaga_alto": FATOR_CUSTO_VAGA_ALTO,
            "piso_plantao_remunerado": PISO_PLANTAO_REMUNERADO,
            "piso_provisorio": False,
            "limite_jornada_longa_h": LIMITE_JORNADA_LONGA_H,
        },
        "resumo": {
            "linhas": len(linhas),
            "linhas_plantao": len(plantoes),
            "linhas_sem_custo_plantao": len(outras),
            "medicos": len(medicos),
            "por_comissao": len({(l["posto"], l["id_medico"]) for l in plantoes
                                 if l["recebe_por_comissao"]}),
            "fora_por_motivo": {
                m: sum(1 for l in outras if l["motivo_fora"] == m)
                for m in ("comissao", "exame", "abaixo_do_piso")},
            "especialidades": len({l["especialidade"] for l in plantoes}),
            "custo_semanal": round(semanal, 2),
            "custo_mensal": round(mensal, 2),
            "postos_ok": sum(1 for s in status.values() if not s["erro"]),
            "postos_erro": sum(1 for s in status.values() if s["erro"]),
        },
        "linhas": linhas,
    }
    _gravar(OUT_PATH, payload)
    r = payload["resumo"]
    print(f"\n{r['linhas']} linhas · {r['medicos']} médicos · "
          f"{r['especialidades']} especialidades")
    print(f"custo semanal R$ {r['custo_semanal']:,.2f} · "
          f"mensal projetado R$ {r['custo_mensal']:,.2f}")
    print(f"postos ok {r['postos_ok']} / erro {r['postos_erro']}")
    print(f"→ {OUT_PATH} ({os.path.getsize(OUT_PATH) / 1024:.0f} KB)")
    return 0 if r["postos_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
