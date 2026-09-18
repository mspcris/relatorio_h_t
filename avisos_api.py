"""avisos_api.py — o que o avisos_gerenciais lê do KPI para a home dos gestores.

Porta de máquina: `Authorization: Bearer $AVISOS_API_TOKEN`. Só LEITURA, e
sempre por posto — cada gestor enxerga o posto dele, e é o avisos quem decide
quais postos pedir (o ACL é de lá). O nginx tem um `location ^~ /api/avisos/`
sem `auth_request` justamente porque máquina não tem cookie; quem autentica
aqui é o token.

  GET /api/avisos/notas?postos=A,C[&ym=2026-09]   NF emitidas × meta do mês
  GET /api/avisos/metas?postos=A,C[&ym=2026-09]   mensalidades e vendas × meta (+ ritmo sem meta)
  GET /api/avisos/robos?postos=A,C                robôs do posto e há quanto tempo pararam
  GET /api/avisos/leads?postos=A,C                leads criados hoje, ontem e a média de 30d
  GET /api/avisos/agenda?postos=A,N,I             prazo da próxima vaga por especialidade
  GET /api/avisos/prescricoes?postos=A,G          prescrições: hoje, ontem, semanas, mês, tipos e médicos

Nenhuma delas recalcula regra de negócio; todas leem o que o KPI já produziu:
as notas saem do `relatorio_nf.carregar_dados` (o mesmo agregado do relatório
"NF emitidas × meta" que vai por zap, modo Clínicas, % sobre as
CONTABILIZADAS); as metas somam os dias do JSON do `export_metas.py` — e a régua de "quanto
já deveria ter entrado a esta altura do mês" sai da série diária dos meses
fechados desse mesmo posto (`metas_historico`), não de regra de três; os robôs
saem do `indicadores_painel.json` com a mesma régua de dias da página
monitorarrobos.html; os leads, do `monitor_leads.json` do robô que roda de hora
em hora. Se a régua mudar lá, muda aqui junto — não pode existir uma segunda
verdade sobre o mesmo número.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import re
from datetime import date, datetime, timedelta

from flask import Blueprint, jsonify, request

import metas_historico

logger = logging.getLogger(__name__)

avisos_bp = Blueprint("avisos_bp", __name__)

_METAS_DIR_PADRAO = "/opt/relatorio_h_t/json_metas"
METAS_DIR = os.getenv("METAS_JSON_DIR") or (
    _METAS_DIR_PADRAO if os.path.isdir(_METAS_DIR_PADRAO)
    else os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_metas")
)


# ── Porta de máquina ────────────────────────────────────────────────────────

def token_de_maquina() -> bool:
    """`Authorization: Bearer $AVISOS_API_TOKEN`. Sem token no .env, ninguém
    entra por essa porta — é o único guarda destas rotas."""
    esperado = (os.getenv("AVISOS_API_TOKEN") or "").strip()
    if not esperado:
        return False
    enviado = (request.headers.get("Authorization") or "").strip()
    if enviado.lower().startswith("bearer "):
        enviado = enviado[7:].strip()
    return bool(enviado) and hmac.compare_digest(enviado, esperado)


def _postos_pedidos() -> list:
    return sorted({p.strip().upper() for p in (request.args.get("postos") or "").split(",")
                   if len(p.strip()) == 1})


def _ym() -> str:
    ym = (request.args.get("ym") or "").strip()
    if len(ym) == 7 and ym[4] == "-" and ym[:4].isdigit() and ym[5:].isdigit():
        return ym
    hoje = date.today()
    return f"{hoje.year:04d}-{hoje.month:02d}"


def _num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


# ── NF emitidas × meta ──────────────────────────────────────────────────────

@avisos_bp.get("/api/avisos/notas")
def api_notas():
    """Por posto: quanto foi contabilizado no mês, a meta e o % — mais a linha
    de cada empresa do posto, que é como o gerente cobra (um posto pode ter
    mais de um CNPJ, e a meta é por CNPJ)."""
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401
    from relatorio_nf import carregar_dados  # import tardio: puxa PIL

    postos, ym = _postos_pedidos(), _ym()
    try:
        dados = carregar_dados(ym)
    except Exception as e:  # JSON do mês ainda não gerado, disco fora, etc.
        logger.warning("notas do avisos falharam (%s): %s", ym, e)
        return jsonify({"error": f"não consegui ler as notas de {ym}: {str(e)[:120]}"}), 503

    saida = {}
    for linha in dados["linhas"]:
        if postos and linha["posto"] not in postos:
            continue
        posto = saida.setdefault(linha["posto"], dict(
            contabilizado=0.0, emitidas=0.0, meta=0.0, qtd=0, canceladas=0, empresas=[],
        ))
        posto["contabilizado"] += _num(linha["cont_val"])
        posto["emitidas"] += _num(linha["emitidas"])
        posto["meta"] += _num(linha["objetivo"])
        posto["qtd"] += int(_num(linha["qtd"]))
        posto["canceladas"] += int(_num(linha["canc_qtd"]))
        posto["empresas"].append(dict(
            empresa=linha["empresa"], origem=linha["origem"], qtd=int(_num(linha["qtd"])),
            emitidas=_num(linha["emitidas"]), canceladas=int(_num(linha["canc_qtd"])),
            contabilizado=_num(linha["cont_val"]), meta=_num(linha["objetivo"]),
            pct=linha["pct"], faixa=linha["faixa"],
        ))
    for posto in saida.values():
        posto["pct"] = (posto["contabilizado"] / posto["meta"] * 100.0) if posto["meta"] else None
        posto["empresas"].sort(key=lambda e: -e["emitidas"])

    return jsonify({
        "ym": ym, "mes_nome": dados["mes_nome"], "postos": saida,
        "gerado_em": dados["gerado_em"].isoformat() if dados["gerado_em"] else None,
    })


# ── Mensalidades e vendas × meta ────────────────────────────────────────────

def _ler_metas(posto: str, ym: str) -> dict | None:
    caminho = os.path.join(METAS_DIR, f"{posto}_metas_{ym}.json")
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _somar(dias: list, campo: str) -> float:
    return sum(_num(d.get(campo)) for d in dias or [])


def _ritmo(posto: str, ym: str, d: dict, mes_corrente: bool, dias_no_mes: int) -> dict:
    """Ritmo cortado na HORA DA FOTO do posto: o JSON gerado às 10h05 tem os
    pagamentos até as 10h, e é contra 10h dos outros meses que se compara.
    Mês fechado compara o mês inteiro."""
    if not mes_corrente:
        return metas_historico.ritmo_do_posto(METAS_DIR, posto, ym, dias_no_mes)
    try:
        foto = datetime.fromisoformat(d.get("gerado_em") or "")
    except ValueError:
        foto = None
    if not foto or foto.strftime("%Y-%m") != ym:
        # Sem carimbo (ou foto do mês passado): até o último dia inteiro
        return metas_historico.ritmo_do_posto(METAS_DIR, posto, ym, date.today().day - 1)
    return metas_historico.ritmo_do_posto(METAS_DIR, posto, ym, foto.day, foto.hour)


@avisos_bp.get("/api/avisos/metas")
def api_metas():
    """Mensalidades e vendas do mês contra a meta cadastrada, por posto.

    Devolve também o dia de hoje dentro do mês E a régua histórica do posto
    (`metas_historico`): metade do mês NÃO é metade da meta. A mensalidade é
    fortemente adiantada — no posto A, 58% da meta já entrou no dia 13, contra
    os 42% da regra de três —, então quem compara com a linear dá verde a quem
    está atrás. Quem julga é o `historico`; a régua linear fica só de reserva,
    para posto sem histórico suficiente."""
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401

    postos, ym = _postos_pedidos(), _ym()
    hoje = date.today()
    ano, mes = int(ym[:4]), int(ym[5:])
    dias_no_mes = (date(ano + (mes == 12), (mes % 12) + 1, 1) - date(ano, mes, 1)).days
    # Mês passado conta o mês inteiro; o corrente conta até hoje
    dia_corrente = hoje.day if (hoje.year, hoje.month) == (ano, mes) else dias_no_mes
    mes_corrente = (hoje.year, hoje.month) == (ano, mes)

    saida, erros = {}, {}
    for posto in postos:
        d = _ler_metas(posto, ym)
        if not d:
            erros[posto] = f"sem dados de metas para {ym}"
            continue
        meta = d.get("meta") or {}
        mens = _somar(d.get("mensalidades_por_dia"), "mens_dia")
        vendas = _somar(d.get("vendas_por_dia"), "vendas_dia")
        meta_mens, meta_venda = _num(meta.get("meta_mens")), _num(meta.get("meta_venda"))
        saida[posto] = dict(
            mensalidades=mens, meta_mensalidades=meta_mens,
            pct_mensalidades=(mens / meta_mens * 100.0) if meta_mens else None,
            vendas=vendas, meta_vendas=meta_venda,
            pct_vendas=(vendas / meta_venda * 100.0) if meta_venda else None,
            gerado_em=d.get("gerado_em"),
            historico=metas_historico.do_posto(METAS_DIR, posto, ym, dia_corrente),
            # Chance de fechar a meta, prevista SÓ com o passado deste posto —
            # e junto a conta que levou até ela, porque número de previsão sem
            # a conta é adivinhação com cara de ciência.
            # Quantidade até o último dia fechado contra a média dos 12 meses
            # anteriores — sem meta, e somável entre postos.
            ritmo=_ritmo(posto, ym, d, mes_corrente, dias_no_mes),
            previsao=metas_historico.previsao_do_posto(
                METAS_DIR, posto, ym, dia_corrente,
                (mens / meta_mens * 100.0) if meta_mens else None,
                (vendas / meta_venda * 100.0) if meta_venda else None,
            ),
        )

    return jsonify({
        "ym": ym, "dia": dia_corrente, "dias_no_mes": dias_no_mes,
        "postos": saida, "erros": erros,
    })


# ── Monitor de robôs ────────────────────────────────────────────────────────
# Mesma leitura da página monitorarrobos.html: o JSON pré-agregado
# (indicadores_painel.json, cron de 5 em 5 min) e a régua de dias sem rodar.
# Régua copiada do diasParaStatus() da página — mudar nos DOIS.

PAINEL_JSON = os.getenv("INDICADORES_PAINEL_JSON",
                        "/opt/relatorio_h_t/json_consolidado/indicadores_painel.json")
STATUS_POR_DIA = {0: "otimo", 1: "bom", 2: "ok", 3: "ruim", 4: "pessimo"}
# Ordem do pior para o melhor: é assim que o gestor quer ler a lista
ORDEM_STATUS = ("horrivel", "pessimo", "ruim", "ok", "bom", "otimo")
ROTULO_STATUS = {"otimo": "Ótimo", "bom": "Bom", "ok": "Ok", "ruim": "Ruim",
                 "pessimo": "Péssimo", "horrivel": "Horrível"}


def _dias_desde(texto: str | None) -> int:
    """Dias inteiros desde o último envio. Sem data = 999 (nunca rodou)."""
    if not texto:
        return 999
    try:
        quando = datetime.fromisoformat(str(texto).replace(" ", "T")[:19])
    except ValueError:
        return 999
    return max((date.today() - quando.date()).days, 0)


def _status(dias: int) -> str:
    return STATUS_POR_DIA.get(dias, "horrivel")


def envio_push(item: dict) -> dict | None:
    """Números de hoje do Push Cobrança, no formato que as telas mostram.

    None quando o JSON ainda é do export antigo (sem os campos do dia).
    """
    if "tentativas_hoje" not in (item or {}):
        return None
    return dict(tentativas=item.get("tentativas_hoje") or 0,
                receberam=item.get("receberam_hoje") or 0,
                nao_receberam=item.get("nao_receberam_hoje") or 0,
                sem_app=item.get("sem_app_hoje") or 0,
                falhas_tecnicas=item.get("falhas_tecnicas_hoje") or 0,
                taxa=item.get("taxa_hoje"),
                rodada=item.get("rodada"))


def status_push(dias: int, envio: dict | None) -> str:
    """Régua de dias, rebaixada quando o envio de HOJE foi ruim.

    "Rodou hoje" não basta: em 17/09/2026 o robô rodou, parou no meio em dois
    postos, e o monitor dizia Ótimo. Mesma regra em monitorarrobos.html
    (statusPush) — mudar nos DOIS.
    """
    st = _status(dias)
    if dias != 0 or not envio:
        return st
    rebaixa = None
    if envio["tentativas"] and not envio["receberam"]:
        rebaixa = "pessimo"          # tentou e ninguém recebeu
    elif envio["rodada"] == "interrompida":
        rebaixa = "ruim"             # parte dos clientes nem foi tentada
    elif envio["tentativas"] and envio["falhas_tecnicas"] > 0.2 * envio["tentativas"]:
        rebaixa = "ruim"             # falha da API/rede, não falta de app
    if rebaixa and ORDEM_STATUS.index(rebaixa) < ORDEM_STATUS.index(st):
        return rebaixa
    return st


# ── Números de HOJE de cada robô, num formato só ────────────────────────────
# "Rodou hoje" não diz se deu certo (Cristiano, 17/09/2026: "precisa me
# mostrar a realidade, não só ok ou não ok"). Cada família conta do seu jeito
# no export_indicadores_painel.py; aqui vira o MESMO desenho para as telas:
#   taxa + rotulo_taxa  → o número grande ("21%" "pagaram")
#   partes              → [{n, rotulo, tom: neutro|ok|falha}] na linha miúda
#   obs                 → leituras curtas (motivo principal, valor, rodada)
# None = o robô não trabalhou hoje (ou o JSON é do export antigo).

def _milhar(n) -> str:
    return f"{int(n or 0):,}".replace(",", ".")


def _reais(v) -> str:
    return "R$ " + f"{float(v or 0):,.0f}".replace(",", ".")


def _numeros(taxa, rotulo_taxa, partes, obs=()) -> dict:
    return dict(taxa=taxa, rotulo_taxa=rotulo_taxa,
                partes=[dict(n=n, rotulo=r, tom=t) for n, r, t in partes],
                obs=[o for o in obs if o])


def _motivos(motivos, quantos=2) -> str:
    return " · ".join(f"{_milhar(n)} {m[:1].lower()}{m[1:]}" for m, n in (motivos or [])[:quantos])


def numeros_push(envio: dict | None) -> dict | None:
    if not envio or not envio["tentativas"]:
        return None
    return _numeros(
        envio["taxa"], "entregue",
        [(envio["tentativas"], "tentados", "neutro"),
         (envio["receberam"], "enviados", "ok"),
         (envio["nao_receberam"], "falharam", "falha")],
        [f"{_milhar(envio['sem_app'])} sem o app" if envio["sem_app"] else "",
         "robô parou no meio" if envio["rodada"] == "interrompida" else
         "em andamento" if envio["rodada"] == "em_andamento" else ""])


def numeros_tef(hoje: dict | None) -> dict | None:
    if not hoje or not hoje.get("cobrados"):
        return None
    return _numeros(
        hoje["taxa"], "pagaram",
        [(hoje["cobrados"], "cobrados", "neutro"),
         (hoje["pagaram"], "pagaram", "ok"),
         (hoje["nao_pagaram"], "não pagaram", "falha")],
        [f"{_reais(hoje['valor_pago'])} de {_reais(hoje['valor_cobrado'])}",
         _motivos(hoje.get("motivos"))])


def numeros_email(hoje: dict | None) -> dict | None:
    """Enviados de quantos deviam sair: "20 de 35 enviados".

    Enviado = está na Cad_Email sem erro (em branco ou ID da Amazon SES);
    falhou = "EMAIL EXCECAO…" (email_resultado); na fila = quem a fila do robô
    ainda aponta e não recebeu (boas-vindas, exame, aviso de 3 dias). A taxa é
    enviados ÷ (enviados + falharam + na fila). Boleto: + repetidos."""
    if not hoje or not (hoje.get("emails") or hoje.get("na_fila")):
        return None
    enviados, falharam, fila = hoje.get("enviados", 0), hoje.get("falharam", 0), hoje.get("na_fila", 0)
    devidos = enviados + falharam + fila
    partes = [(enviados, f"de {_milhar(devidos)} enviados" if fila or falharam else "enviados", "ok"),
              (falharam, "falharam", "falha")]
    if fila:
        partes.append((fila, "na fila, não enviados", "falha"))
    if hoje.get("repetidos"):
        partes.append((hoje["repetidos"], "repetidos", "falha"))
    obs = [_motivos(hoje.get("motivos")),
           " · ".join(f"{_milhar(n)} {rot} na fila" + (f" ({_milhar(sem)} sem e-mail)" if sem else "")
                      for rot, n, sem in hoje.get("filas") or [] if n)]
    if hoje.get("tipos"):
        obs.append("enviados: " + " · ".join(
            f"{_milhar(n)} {re.sub(r'^camim\s*[-–]\s*', '', t, flags=re.I).lower()}"
            for t, n in hoje["tipos"][:3]))
    taxa = round(100 * enviados / devidos, 1) if devidos else None
    return _numeros(taxa, "enviados", partes, obs)


def numeros_wpp(hoje: dict | None) -> dict | None:
    if not hoje or not (hoje.get("enviados") or hoje.get("falharam")):
        return None
    return _numeros(
        hoje["taxa"], "enviados",
        [(hoje["enviados"], "enviados", "ok"),
         (hoje["falharam"], "falharam", "falha")],
        [_motivos(hoje.get("motivos")),
         f"{_milhar(hoje['pulados'])} pulados (já receberam ou mesmo telefone)" if hoje.get("pulados") else ""])


def _robos_do_painel(painel: dict) -> list[dict]:
    """Normaliza as quatro famílias (push, e-mail, TEF, WhatsApp) numa lista só."""
    ind = painel.get("indicadores") or {}
    robos = []

    for posto, item in (ind.get("push") or {}).items():
        envio = envio_push(item)
        robos.append(dict(familia="Push", nome="Push Cobrança", posto=posto,
                          ultimo=item.get("ultimo_envio"),
                          envio=envio, numeros=numeros_push(envio), robo="push"))

    for item in ((ind.get("email") or {}).get("data") or {}).values():
        outros = item.get("categoria") == "Outros e-mails"
        robos.append(dict(familia="E-mail", nome=item.get("categoria") or "E-mail",
                          posto=item.get("posto"), ultimo=item.get("ultimo_envio"),
                          numeros=numeros_email(item.get("hoje")),
                          robo="email_outros" if outros else "email",
                          # Muitos programas, sem hora certa: dia sem e-mail
                          # não é robô parado — fica fora da conta de parados
                          vigia=not outros))

    for item in ((ind.get("tef") or {}).get("data") or {}).values():
        robos.append(dict(familia="TEF", nome="TEF Recorrente",
                          posto=item.get("posto"), ultimo=item.get("ultimo_tef"),
                          numeros=numeros_tef(item.get("hoje")), robo="tef"))

    for campanha in (ind.get("wpp") or []):
        for posto, item in (campanha.get("postos") or {}).items():
            robos.append(dict(familia="WhatsApp", nome=campanha.get("nome") or "WhatsApp",
                              posto=posto, ultimo=item.get("ultimo_envio"),
                              numeros=numeros_wpp(item.get("hoje")),
                              robo=f"wpp:{campanha.get('id')}"))

    for r in robos:
        r["dias"] = _dias_desde(r["ultimo"])
        r["status"] = (status_push(r["dias"], r.get("envio"))
                       if r["familia"] == "Push" else _status(r["dias"]))
    return robos


@avisos_bp.get("/api/avisos/robos")
def api_robos():
    """Robôs do posto: quantos rodaram hoje e quais estão parados, com há
    quantos dias. Parado é o que importa — por isso a lista vem do pior."""
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401
    postos = _postos_pedidos()
    try:
        with open(PAINEL_JSON, encoding="utf-8") as f:
            painel = json.load(f)
    except (OSError, ValueError) as e:
        logger.warning("monitor de robôs indisponível: %s", e)
        return jsonify({"error": f"indicadores_painel.json indisponível: {str(e)[:120]}"}), 503

    saida = {}
    for r in _robos_do_painel(painel):
        if not r["posto"] or (postos and r["posto"] not in postos):
            continue
        posto = saida.setdefault(r["posto"], dict(robos=[], total=0, parados=0, contagem={}))
        posto["robos"].append(r)
        if r.get("vigia") is False:
            continue    # aparece com os números, mas não conta como robô parado
        posto["total"] += 1
        posto["parados"] += int(r["dias"] >= 1)
        posto["contagem"][r["status"]] = posto["contagem"].get(r["status"], 0) + 1
    for posto in saida.values():
        posto["robos"].sort(key=lambda r: (-r["dias"], r["nome"]))

    return jsonify({"postos": saida, "rotulos": ROTULO_STATUS,
                    "gerado_em": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")})


# ── Monitor de leads ────────────────────────────────────────────────────────
# JSON do export_monitor_leads.py (roda de hora em hora), que já traz as séries
# quebradas por posto — aqui é só recortar o posto e contar.

LEADS_JSON = os.getenv("MONITOR_LEADS_JSON",
                       "/opt/relatorio_h_t/json_consolidado/monitor_leads.json")


@avisos_bp.get("/api/avisos/leads")
def api_leads():
    """Leads criados: hoje até agora, ontem inteiro, média diária de 30 dias e
    as fontes de hoje — por posto."""
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401
    postos = _postos_pedidos()
    try:
        with open(LEADS_JSON, encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError) as e:
        logger.warning("monitor de leads indisponível: %s", e)
        return jsonify({"error": f"monitor_leads.json indisponível: {str(e)[:120]}"}), 503

    # Virou o dia, o contador ZERA — e o que veio na foto de ontem vira "ontem".
    # O robô roda de hora em hora; entre a meia-noite e a primeira rodada não
    # existe leitura do dia novo. Mostrar a foto de ontem como se fosse de hoje
    # trava o gestor num número velho (e, pior, dava "0 hoje" com a lista de
    # fontes de ontem somando 22 na mesma tela — visto às 00:02 de 12/09/2026).
    # Então: dia novo começa zerado, e a tela diz que está esperando a leitura.
    try:
        foto = date.fromisoformat(str(d.get("gerado_em"))[:10])
    except ValueError:
        foto = date.today()
    referencia = date.today()
    aguardando = foto < referencia
    hoje = referencia.isoformat()
    ontem = (referencia - timedelta(days=1)).isoformat()
    dias_posto = d.get("dias_posto") or {}
    horas_posto = d.get("horas_posto") or {}
    fontes_posto = d.get("fontes_posto") or {}
    ultima_hora = (d.get("ultima_hora") or {}).get("label") or ""
    balde = f"{hoje} {ultima_hora}" if ultima_hora else ""

    saida = {}
    for posto in (postos or sorted(dias_posto)):
        serie = dias_posto.get(posto) or []
        # A média de 30 dias ignora HOJE, que ainda está contando
        fechados = [x for x in serie if x.get("d") != hoje][-30:]
        total = sum(x.get("n", 0) for x in fechados)
        saida[posto] = dict(
            # Sem leitura do dia novo, hoje é 0 de verdade — não é o de ontem
            hoje=0 if aguardando else sum(x.get("n", 0) for x in serie if x.get("d") == hoje),
            ontem=sum(x.get("n", 0) for x in serie if x.get("d") == ontem),
            media30=round(total / len(fechados), 1) if fechados else 0,
            dias_considerados=len(fechados),
            ultima_hora=sum(x.get("n", 0) for x in (horas_posto.get(posto) or [])
                            if x.get("h") == balde),
            # As fontes vieram da foto: se a foto é de ontem, elas são de ontem
            fontes_hoje=[] if aguardando else sorted(
                ({"fonte": x.get("fonte"), "n": x.get("n", 0)} for x in (fontes_posto.get(posto) or [])),
                key=lambda x: -x["n"]),
            por_dia=[{"d": x.get("d"), "n": x.get("n", 0)} for x in serie[-14:]],
        )

    return jsonify({
        "postos": saida, "hora": "" if aguardando else ultima_hora, "dia": hoje,
        "aguardando": aguardando, "foto": foto.isoformat(),
        "gerado_em": d.get("gerado_em"),
    })


# ── Qualidade da agenda ─────────────────────────────────────────────────────
# A foto diária do export_qualidade_agenda: para cada posto × especialidade,
# quantos dias até a próxima vaga e os prazos da ANS e da CAMIM daquele CBOS.
# ATENÇÃO à ordem dos prazos: o da ANS é o APERTADO. A régua da página é
#   dias <= prazo_ans        → no prazo
#   dias <= prazo_camim      → furou a ANS, ainda dentro do nosso
#   acima dos dois           → crítico
# Quem agrupa posto em corredor é o avisos: aqui só se devolve a linha crua.

AGENDA_DIR = os.getenv("QUALIDADE_AGENDA_DIR") or "/opt/relatorio_h_t/json_consolidado/qualidade_agenda"


@avisos_bp.get("/api/avisos/agenda")
def api_agenda():
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401
    import glob

    pedidos = {p.strip().upper() for p in (request.args.get("postos") or "").split(",") if p.strip()}
    arquivos = sorted(glob.glob(os.path.join(AGENDA_DIR, "*.json")))
    if not arquivos:
        return jsonify({"error": "sem foto da qualidade da agenda"}), 503
    try:
        with open(arquivos[-1], encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError) as e:
        return jsonify({"error": f"foto da agenda ilegível: {str(e)[:120]}"}), 503

    cbos = d.get("cbos") or {}
    postos: dict[str, list] = {}
    for linha in d.get("dados") or []:
        posto = (linha.get("posto") or "").strip().upper()
        if not posto or (pedidos and posto not in pedidos):
            continue
        prazos = cbos.get(linha.get("cbos_casado") or "", {})
        postos.setdefault(posto, []).append(dict(
            especialidade=(linha.get("Especialidade") or "").strip(),
            dias=linha.get("DiasAteProximaVaga"),
            data=linha.get("DataProximaVaga"),
            vagas=linha.get("QuantidadeVagasDisponivelNaData") or 0,
            prazo_ans=int(prazos.get("prazoconsultaans") or 0),
            prazo_camim=int(prazos.get("prazoconsultacamim") or 0),
        ))

    meta = d.get("meta") or {}
    return jsonify({
        "postos": postos,
        "data": meta.get("data_referencia"),
        "gerado_em": meta.get("gerado_em"),
        "nomes": {k: v.get("nome") for k, v in (d.get("postos_info") or {}).items()},
    })


# ── Prescrições ─────────────────────────────────────────────────────────────
# O `prescricao_hoje.json` do export_governanca (de 15 em 15 min) — o MESMO
# arquivo que a página KPI_prescricao.html lê. As contas repetem as do JS de
# lá (renderKPIs), no filtro padrão da página (1º do mês até hoje):
#   hoje / ontem           soma de `valor` no dia
#   semana atual           segunda até hoje; semana passada = segunda a domingo
#   média diária           total do mês ÷ dias corridos do mês (hoje incluso)
#   médicos ativos hoje    médicos distintos com prescrição hoje
# Única diferença consciente: a página corta a semana no 1º do mês porque o
# filtro de datas corta; aqui semana é semana (no dia 3, "semana atual" conta
# a segunda do mês anterior). Mudou a régua lá, muda aqui.
# Só voltam postos com prescrição neste mês ou no anterior: um posto que parou
# de prescrever pelo sistema há um ano não é "zero hoje", é "não usa".

PRESCRICAO_JSON = os.getenv("PRESCRICAO_HOJE_JSON",
                            "/opt/relatorio_h_t/json_consolidado/prescricao_hoje.json")
_PRESCRICAO_CACHE: dict = {}
TOP_MEDICOS = 15


def _linhas_prescricao() -> tuple[list, float]:
    """(linhas, mtime) — o JSON tem ~7 MB; só relê quando o arquivo muda."""
    mtime = os.path.getmtime(PRESCRICAO_JSON)
    if _PRESCRICAO_CACHE.get("mtime") != mtime:
        with open(PRESCRICAO_JSON, encoding="utf-8") as f:
            d = json.load(f)
        _PRESCRICAO_CACHE.update(mtime=mtime, linhas=d.get("linhas") or [])
    return _PRESCRICAO_CACHE["linhas"], mtime


def resumo_prescricoes(linhas, postos=None, hoje: date | None = None) -> dict:
    hoje = hoje or date.today()
    h = hoje.isoformat()
    ontem = (hoje - timedelta(days=1)).isoformat()
    seg = hoje - timedelta(days=hoje.weekday())
    seg_atual = seg.isoformat()
    seg_passada, dom_passado = (seg - timedelta(days=7)).isoformat(), (seg - timedelta(days=1)).isoformat()
    ini_mes = hoje.replace(day=1)
    ini_anterior = (ini_mes - timedelta(days=1)).replace(day=1).isoformat()
    ini_mes = ini_mes.isoformat()
    quinze = (hoje - timedelta(days=13)).isoformat()

    acc: dict = {}
    for r in linhas:
        d = str(r.get("data_consulta") or "")[:10]
        posto = str(r.get("posto") or "").strip().upper()
        if len(d) != 10 or d < min(ini_anterior, seg_passada) or d > h or len(posto) != 1:
            continue
        if postos and posto not in postos:
            continue
        v = int(_num(r.get("valor")) or 1)
        p = acc.setdefault(posto, dict(hoje=0, ontem=0, semana_atual=0, semana_passada=0, mes=0,
                                       mes_anterior=0, medicos_hoje=set(), medicos_mes=set(),
                                       tipos={}, medicos={}, por_dia={}))
        medico = str(r.get("medico") or "").strip()
        if d == h:
            p["hoje"] += v
            if medico:
                p["medicos_hoje"].add(medico)
        if d == ontem:
            p["ontem"] += v
        if d >= seg_atual:
            p["semana_atual"] += v
        if seg_passada <= d <= dom_passado:
            p["semana_passada"] += v
        if d >= quinze:
            p["por_dia"][d] = p["por_dia"].get(d, 0) + v
        if d >= ini_mes:
            p["mes"] += v
            tipo = str(r.get("tipo_prescricao") or "").strip() or "Sem tipo"
            p["tipos"][tipo] = p["tipos"].get(tipo, 0) + v
            if medico:
                p["medicos_mes"].add(medico)
                p["medicos"][medico] = p["medicos"].get(medico, 0) + v
        elif d >= ini_anterior:
            p["mes_anterior"] += v

    saida = {}
    for posto, p in sorted(acc.items()):
        if not (p["mes"] or p["mes_anterior"]):
            continue    # só a semana passada caiu na janela: posto que não usa mais
        por_n = lambda dic: sorted(dic.items(), key=lambda kv: (-kv[1], kv[0]))
        saida[posto] = dict(
            hoje=p["hoje"], ontem=p["ontem"],
            semana_atual=p["semana_atual"], semana_passada=p["semana_passada"],
            mes=p["mes"], mes_anterior=p["mes_anterior"],
            media_diaria=round(p["mes"] / hoje.day),
            medicos_hoje=len(p["medicos_hoje"]), medicos_mes=len(p["medicos_mes"]),
            tipos_mes=[{"tipo": t, "n": n} for t, n in por_n(p["tipos"])],
            medicos_top_mes=[{"medico": m, "n": n} for m, n in por_n(p["medicos"])[:TOP_MEDICOS]],
            por_dia=[{"d": d, "n": n} for d, n in sorted(p["por_dia"].items())],
        )
    # O total do conjunto pedido: médico que atende em dois postos é UM médico
    # ativo, então os distintos não se somam posto a posto.
    somar = lambda campo: sum(p[campo] for p in saida.values())
    total = {c: somar(c) for c in ("hoje", "ontem", "semana_atual", "semana_passada", "mes", "mes_anterior")}
    total["media_diaria"] = round(total["mes"] / hoje.day)
    for campo, conj in (("medicos_hoje", "medicos_hoje"), ("medicos_mes", "medicos_mes")):
        total[campo] = len(set().union(*(acc[c][conj] for c in saida))) if saida else 0
    return {"postos": saida, "total": total, "dia": h, "ym": h[:7], "dia_do_mes": hoje.day,
            "semana_desde": seg_atual}


@avisos_bp.get("/api/avisos/prescricoes")
def api_prescricoes():
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401
    try:
        linhas, mtime = _linhas_prescricao()
    except (OSError, ValueError) as e:
        logger.warning("prescrições indisponíveis: %s", e)
        return jsonify({"error": f"prescricao_hoje.json indisponível: {str(e)[:120]}"}), 503
    saida = resumo_prescricoes(linhas, set(_postos_pedidos()))
    saida["gerado_em"] = datetime.fromtimestamp(mtime).strftime("%Y-%m-%dT%H:%M:%S")
    return jsonify(saida)
