"""
custos_ti.py — Núcleo do módulo "Custos de TI" (antigo "Custos com IA").

O que muda em relação ao custos_ia:
  o custos_ia continua existindo e continua sendo a fonte da verdade do gasto
  com IA (Costs API da OpenAI, print/texto da Groq, assinaturas). Ele virou UM
  centro de custo dentro do Custos de TI. Este módulo é a camada de cima:
  centros de custo genéricos (Infra, Banco de Dados, Comunicação...), cadastro
  de formas de pagamento, lançamento de despesas e a consolidação de tudo.

Moeda: a home consolida em BRL, porque boleto/nota fiscal daqui é em real.
Cada lançamento guarda o valor ORIGINAL + a moeda + a cotação usada, e o
valor_brl congelado. Histórico não muda quando o dólar mexe.

Nada aqui gasta dinheiro: só leitura de API (Graph da Meta, cotação) e CRUD no
Postgres. O único ponto que custa centavos é a leitura de print por visão, que
segue morando no custos_ia e é disparada à mão.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func

import custos_ti_db as db
from custos_ti_db import (
    CentroCusto, Conta, Cotacao, FormaPagamento, Lancamento, TiSession,
)

log = logging.getLogger(__name__)

_BRT = timezone(timedelta(hours=-3))
_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# Cotação de partida quando o mês ainda não tem nenhuma definida e não existe
# nenhum mês anterior de onde herdar. Só afeta o primeiro lançamento em USD.
COTACAO_FALLBACK = float(os.environ.get("CUSTOS_TI_USD_BRL", "5.40") or 5.40)

MESES_PADRAO = 12   # janela do gráfico de evolução

# Paleta categórica validada para daltonismo (mesma ordem fixa do TI_PALETA no
# front). A cor identifica o CENTRO em todos os gráficos e no menu, então é
# atribuída na ordem dos slots e nunca reciclada — do 9º centro em diante o
# gráfico agrupa em "Outros" em vez de inventar uma cor nova.
PALETA = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
          "#e87ba4", "#008300", "#4a3aa7", "#e34948"]

# Centros criados na primeira execução da migration. O 'ia' é especial: aponta
# para a página que já existe e o total dele vem dos snapshots do custos_ia.
CENTROS_SEED = [
    {"key": "ia", "nome": "IA", "icone": "fas fa-robot", "cor": PALETA[0],
     "ordem": 10, "fonte": "ia", "href": "/custos_ia",
     "descricao": "OpenAI, Groq, Anthropic e assinaturas de IA. "
                  "Os valores vêm do painel Custos com IA."},
    {"key": "comunicacao", "nome": "Comunicação", "icone": "fab fa-whatsapp",
     "cor": PALETA[1], "ordem": 20, "fonte": "manual", "integracao": "meta",
     "descricao": "WhatsApp/Meta, SMS, e-mail transacional, telefonia."},
    {"key": "infra", "nome": "Infraestrutura", "icone": "fas fa-server",
     "cor": PALETA[2], "ordem": 30, "fonte": "manual",
     "descricao": "VMs, hospedagem, domínios, certificados, CDN, backup."},
    {"key": "banco_dados", "nome": "Banco de Dados", "icone": "fas fa-database",
     "cor": PALETA[3], "ordem": 40, "fonte": "manual",
     "descricao": "RDS, instâncias SQL Server, storage e réplicas."},
    {"key": "software", "nome": "Software e Licenças", "icone": "fas fa-box-open",
     "cor": PALETA[4], "ordem": 50, "fonte": "manual",
     "descricao": "SaaS, licenças, ferramentas de desenvolvimento."},
]


# ─────────────────────────────────────────────────────────────────────────────
# Datas / competências
# ─────────────────────────────────────────────────────────────────────────────
def mes_atual() -> str:
    return datetime.now(_BRT).strftime("%Y-%m")


def valid_month(m: Optional[str], default: Optional[str] = None) -> str:
    m = (m or "").strip()
    if _MONTH_RE.match(m):
        return m
    return default or mes_atual()


def _add_meses(competencia: str, n: int) -> str:
    ano, mes = (int(x) for x in competencia.split("-"))
    total = ano * 12 + (mes - 1) + n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def range_meses(de: str, ate: str) -> list[str]:
    """Lista de competências de `de` até `ate`, inclusive, crescente."""
    de, ate = valid_month(de), valid_month(ate)
    if de > ate:
        de, ate = ate, de
    out, cur = [], de
    while cur <= ate and len(out) < 120:   # teto de sanidade (10 anos)
        out.append(cur)
        cur = _add_meses(cur, 1)
    return out


def ultimos_meses(n: int = MESES_PADRAO, ate: Optional[str] = None) -> list[str]:
    ate = valid_month(ate)
    return range_meses(_add_meses(ate, -(max(1, n) - 1)), ate)


def _now() -> datetime:
    return db.now_brt()


# ─────────────────────────────────────────────────────────────────────────────
# Cotação USD→BRL
# ─────────────────────────────────────────────────────────────────────────────
def get_cotacao(sess, competencia: str) -> float:
    """Cotação do mês, com carry-forward do último mês definido antes dele."""
    competencia = valid_month(competencia)
    row = sess.get(Cotacao, competencia)
    if row:
        return float(row.usd_brl)
    anterior = (sess.query(Cotacao)
                .filter(Cotacao.competencia < competencia)
                .order_by(Cotacao.competencia.desc())
                .first())
    if anterior:
        return float(anterior.usd_brl)
    return COTACAO_FALLBACK


def set_cotacao(sess, competencia: str, usd_brl, fonte: str = "manual") -> dict:
    competencia = valid_month(competencia)
    # aceita "5,42" — o usuário digita em pt-BR e a API pode ser chamada direto
    valor = _dec_ou_none(usd_brl)
    if valor is None:
        raise ValueError(f"cotação inválida: {usd_brl!r}")
    valor = round(valor, 6)
    if valor <= 0:
        raise ValueError("cotação precisa ser maior que zero")
    row = sess.get(Cotacao, competencia)
    if row:
        row.usd_brl, row.fonte = valor, fonte
    else:
        row = Cotacao(competencia=competencia, usd_brl=valor, fonte=fonte)
        sess.add(row)
    sess.commit()
    return row.to_dict()


_PTAX_URL = ("https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
             "CotacaoDolarPeriodo(dataInicial=@dataInicial,"
             "dataFinalCotacao=@dataFinalCotacao)")


def fetch_ptax(ate: date, dias_atras: int = 10) -> dict:
    """PTAX de venda do Banco Central — a última disponível até `ate`.

    É a cotação OFICIAL brasileira, a mesma usada para fins contábeis e fiscais.
    Tem histórico, então serve para preencher mês passado (a AwesomeAPI só dá
    a de hoje). Não publica em fim de semana e feriado: por isso a janela de
    `dias_atras` e o "última disponível".
    """
    import requests
    ini = ate - timedelta(days=dias_atras)
    try:
        r = requests.get(_PTAX_URL, timeout=25, params={
            "@dataInicial": f"'{ini.strftime('%m-%d-%Y')}'",
            "@dataFinalCotacao": f"'{ate.strftime('%m-%d-%Y')}'",
            "$format": "json",
            "$select": "cotacaoVenda,dataHoraCotacao",
        })
        r.raise_for_status()
        linhas = (r.json() or {}).get("value") or []
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"PTAX: {e}", "usd_brl": None}
    if not linhas:
        return {"ok": False, "usd_brl": None,
                "error": f"PTAX sem cotação entre {ini} e {ate}."}
    ultima = max(linhas, key=lambda x: x["dataHoraCotacao"])
    dia = ultima["dataHoraCotacao"][:10]
    return {"ok": True, "usd_brl": round(float(ultima["cotacaoVenda"]), 6),
            "fonte": f"PTAX/BCB {dia}", "data": dia}


def fetch_cotacao_usd_brl(ate: Optional[date] = None) -> dict:
    """Cotação USD→BRL: PTAX do Banco Central, com AwesomeAPI de reserva.

    PTAX primeiro porque é oficial e tem histórico. A AwesomeAPI só entra se o
    BCB falhar E o alvo for hoje — ela não serve para data passada, e ainda por
    cima devolve 429 quando se pede demais.
    """
    import requests
    alvo = ate or datetime.now(_BRT).date()
    ptax = fetch_ptax(alvo)
    if ptax.get("ok"):
        return ptax
    if ate is not None:
        return ptax          # data passada: sem reserva, PTAX é a fonte
    try:
        r = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL", timeout=12)
        r.raise_for_status()
        bid = float((r.json().get("USDBRL") or {}).get("bid"))
        return {"ok": True, "usd_brl": round(bid, 6), "fonte": "awesomeapi"}
    except Exception as e:  # noqa: BLE001 — cotação é conveniência, não derruba a tela
        return {"ok": False, "usd_brl": None,
                "error": f"{ptax.get('error')} · reserva AwesomeAPI: {e}"}


def _fim_do_mes(competencia: str) -> date:
    ano, mes = (int(x) for x in competencia.split("-"))
    prox = date(ano + 1, 1, 1) if mes == 12 else date(ano, mes + 1, 1)
    return prox - timedelta(days=1)


def preencher_cotacoes(sess, de: str, ate: str, *, sobrescrever: bool = False) -> dict:
    """Preenche a cotação de cada mês do período com a PTAX de fechamento.

    Convenção: PTAX de venda do último dia útil do mês. É verificável — dá para
    conferir no site do BCB. Mês corrente usa a última PTAX disponível.
    Por padrão NÃO sobrescreve mês que já tem cotação definida à mão.
    """
    meses = range_meses(de, ate)
    hoje = datetime.now(_BRT).date()
    definidos, pulados, falhas = [], [], []
    for m in meses:
        if not sobrescrever and sess.get(Cotacao, m):
            pulados.append(m)
            continue
        alvo = min(_fim_do_mes(m), hoje)
        r = fetch_ptax(alvo)
        if not r.get("ok"):
            falhas.append({"competencia": m, "error": r.get("error")})
            continue
        set_cotacao(sess, m, r["usd_brl"], fonte=r["fonte"])
        definidos.append({"competencia": m, "usd_brl": r["usd_brl"], "fonte": r["fonte"]})
    return {"definidos": definidos, "pulados": pulados, "falhas": falhas}


def recalcular_conversoes(sess, de: str, ate: str) -> dict:
    """Recalcula valor_brl/valor_usd dos lançamentos com a cotação atual do mês.

    O valor da moeda ORIGINAL nunca muda — só o convertido. Serve para consertar
    lançamentos gravados quando o mês ainda não tinha cotação de verdade e caiu
    no fallback do sistema.
    """
    meses = set(range_meses(de, ate))
    cot = {m: get_cotacao(sess, m) for m in meses}
    mexidos = 0
    for l in (sess.query(Lancamento)
              .filter(Lancamento.competencia.in_(meses)).all()):
        nova = cot[l.competencia]
        if l.cotacao is not None and abs(float(l.cotacao) - nova) < 1e-9:
            continue
        valor, moeda = float(l.valor or 0), l.moeda
        l.cotacao = nova
        l.valor_brl = para_brl(valor, moeda, nova)
        l.valor_usd = para_usd(valor, moeda, nova)
        mexidos += 1
    sess.commit()
    return {"recalculados": mexidos, "meses": sorted(meses)}


def para_brl(valor: float, moeda: str, cotacao: float) -> float:
    """Converte para BRL. EUR não tem cotação própria — cai em USD e avisa na UI."""
    valor = float(valor or 0.0)
    if (moeda or "BRL").upper() == "BRL":
        return round(valor, 4)
    return round(valor * float(cotacao or COTACAO_FALLBACK), 4)


def para_usd(valor: float, moeda: str, cotacao: float) -> float:
    """Converte para USD. Despesa já em USD passa direto — sem tocar em cotação."""
    valor = float(valor or 0.0)
    if (moeda or "BRL").upper() == "USD":
        return round(valor, 4)
    cot = float(cotacao or COTACAO_FALLBACK)
    return round(valor / cot, 4) if cot else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Centros de custo
# ─────────────────────────────────────────────────────────────────────────────
# Integrações que um centro pode ter. Só "meta" por enquanto; AWS entra aqui.
INTEGRACOES = ("meta",)

_SLUG_RE = re.compile(r"[^a-z0-9_]+")


def slugify(texto: str) -> str:
    import unicodedata
    base = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    slug = _SLUG_RE.sub("_", base.strip().lower()).strip("_")
    return (slug or "centro")[:60]


def _proxima_cor_livre(sess) -> str:
    """Primeiro slot da paleta ainda não usado por outro centro."""
    usadas = {(c.cor or "").lower() for c in sess.query(CentroCusto).all()}
    return next((c for c in PALETA if c.lower() not in usadas), PALETA[0])


def listar_centros(sess, incluir_inativos: bool = False) -> list[CentroCusto]:
    q = sess.query(CentroCusto)
    if not incluir_inativos:
        q = q.filter(CentroCusto.ativo.is_(True))
    return q.order_by(CentroCusto.ordem, CentroCusto.nome).all()


def get_centro(sess, key: str) -> Optional[CentroCusto]:
    return sess.query(CentroCusto).filter(CentroCusto.key == key).first()


def salvar_centro(sess, dados: dict) -> dict:
    """Cria ou atualiza um centro. Criar um centro = criar a página e o menu."""
    cid = dados.get("id")
    nome = (dados.get("nome") or "").strip()
    if not nome:
        raise ValueError("nome do centro de custo é obrigatório")

    centro = sess.get(CentroCusto, int(cid)) if cid else None
    if centro is None:
        key = slugify(dados.get("key") or nome)
        if get_centro(sess, key):
            sufixo = 2
            while get_centro(sess, f"{key}_{sufixo}"):
                sufixo += 1
            key = f"{key}_{sufixo}"
        maior = sess.query(func.max(CentroCusto.ordem)).scalar() or 0
        centro = CentroCusto(key=key, ordem=int(dados.get("ordem") or maior + 10))

    centro.nome = nome
    centro.descricao = (dados.get("descricao") or "").strip() or None
    centro.icone = (dados.get("icone") or "").strip() or "fas fa-folder"
    centro.cor = (dados.get("cor") or "").strip() or _proxima_cor_livre(sess)
    if dados.get("ordem") is not None:
        centro.ordem = int(dados["ordem"])
    if dados.get("ativo") is not None:
        centro.ativo = bool(dados["ativo"])
    if "forma_pagamento_id" in dados:
        centro.forma_pagamento_id = _int_ou_none(dados.get("forma_pagamento_id"))
    if "integracao" in dados:
        integ = (dados.get("integracao") or "").strip() or None
        if integ and integ not in INTEGRACOES:
            raise ValueError(f"integração inválida: {integ}")
        centro.integracao = integ
    sess.add(centro)
    sess.commit()
    return centro.to_dict()


def excluir_centro(sess, centro_id: int) -> dict:
    """Remove um centro. Recusa se ainda tiver lançamento (não apaga histórico)."""
    centro = sess.get(CentroCusto, int(centro_id))
    if not centro:
        raise ValueError("centro de custo não encontrado")
    if centro.fonte == "ia":
        raise ValueError("o centro de IA é fixo (a fonte é o painel Custos com IA)")
    n = sess.query(func.count(Lancamento.id))\
            .filter(Lancamento.centro_id == centro.id).scalar() or 0
    if n:
        raise ValueError(
            f"este centro tem {n} lançamento(s). Desative-o em vez de excluir, "
            "ou mova/exclua os lançamentos antes."
        )
    sess.query(Conta).filter(Conta.centro_id == centro.id).delete()
    sess.delete(centro)
    sess.commit()
    return {"removido": True}


# ─────────────────────────────────────────────────────────────────────────────
# Formas de pagamento
# ─────────────────────────────────────────────────────────────────────────────
def listar_formas(sess, incluir_inativas: bool = True) -> list[FormaPagamento]:
    q = sess.query(FormaPagamento)
    if not incluir_inativas:
        q = q.filter(FormaPagamento.ativo.is_(True))
    return q.order_by(FormaPagamento.ativo.desc(), FormaPagamento.nome).all()


def salvar_forma(sess, dados: dict) -> dict:
    fid = dados.get("id")
    nome = (dados.get("nome") or "").strip()
    if not nome:
        raise ValueError("nome da forma de pagamento é obrigatório")
    tipo = (dados.get("tipo") or "cartao_credito").strip()
    if tipo not in db.TIPOS_PAGAMENTO:
        raise ValueError(f"tipo inválido: {tipo}")
    moeda = (dados.get("moeda_padrao") or "BRL").upper()
    if moeda not in db.MOEDAS:
        raise ValueError(f"moeda inválida: {moeda}")

    ultimos4 = re.sub(r"\D", "", str(dados.get("ultimos4") or ""))[-4:] or None
    bandeira = (dados.get("bandeira") or "").strip() or None
    dia = dados.get("dia_vencimento")
    dia = int(dia) if str(dia or "").strip().isdigit() else None
    if dia is not None and not (1 <= dia <= 31):
        raise ValueError("dia de vencimento precisa estar entre 1 e 31")

    forma = sess.get(FormaPagamento, int(fid)) if fid else None
    if forma is None:
        existente = _forma_por_cartao(sess, bandeira, ultimos4)
        if existente:
            raise ValueError(
                f"já existe uma forma de pagamento {bandeira} ···· {ultimos4} "
                f"({existente.nome})."
            )
        forma = FormaPagamento()

    forma.nome = nome
    forma.tipo = tipo
    forma.bandeira = bandeira
    forma.ultimos4 = ultimos4
    forma.titular = (dados.get("titular") or "").strip() or None
    forma.dia_vencimento = dia
    forma.moeda_padrao = moeda
    forma.obs = (dados.get("obs") or "").strip() or None
    if dados.get("ativo") is not None:
        forma.ativo = bool(dados["ativo"])
    sess.add(forma)
    sess.commit()
    return forma.to_dict()


def _forma_por_cartao(sess, bandeira: Optional[str],
                      ultimos4: Optional[str]) -> Optional[FormaPagamento]:
    if not ultimos4:
        return None
    return (sess.query(FormaPagamento)
            .filter(FormaPagamento.ultimos4 == ultimos4,
                    FormaPagamento.bandeira == bandeira)
            .first())


def excluir_forma(sess, forma_id: int) -> dict:
    forma = sess.get(FormaPagamento, int(forma_id))
    if not forma:
        raise ValueError("forma de pagamento não encontrada")
    n = sess.query(func.count(Lancamento.id))\
            .filter(Lancamento.forma_pagamento_id == forma.id).scalar() or 0
    if n:
        # Apagar deixaria o histórico sem saber em que cartão foi pago.
        forma.ativo = False
        sess.commit()
        return {"removido": False, "desativado": True, "lancamentos": n}
    sess.query(Conta).filter(Conta.forma_pagamento_id == forma.id)\
        .update({Conta.forma_pagamento_id: None})
    sess.delete(forma)
    sess.commit()
    return {"removido": True}


# ─────────────────────────────────────────────────────────────────────────────
# Contas (serviços contratados dentro de um centro)
# ─────────────────────────────────────────────────────────────────────────────
def listar_contas(sess, centro_id: Optional[int] = None,
                  incluir_inativas: bool = True) -> list[Conta]:
    q = sess.query(Conta)
    if centro_id:
        q = q.filter(Conta.centro_id == int(centro_id))
    if not incluir_inativas:
        q = q.filter(Conta.ativo.is_(True))
    return q.order_by(Conta.ativo.desc(), Conta.nome).all()


def salvar_conta(sess, dados: dict) -> dict:
    nome = (dados.get("nome") or "").strip()
    if not nome:
        raise ValueError("nome da conta é obrigatório")
    centro_id = dados.get("centro_id")
    conta = sess.get(Conta, int(dados["id"])) if dados.get("id") else None
    if conta is None:
        if not centro_id:
            raise ValueError("centro de custo é obrigatório")
        conta = Conta(centro_id=int(centro_id))
    elif centro_id:
        conta.centro_id = int(centro_id)

    recorrencia = (dados.get("recorrencia") or "mensal").strip()
    if recorrencia not in db.RECORRENCIAS:
        raise ValueError(f"recorrência inválida: {recorrencia}")
    moeda = (dados.get("moeda") or "BRL").upper()
    if moeda not in db.MOEDAS:
        raise ValueError(f"moeda inválida: {moeda}")

    conta.nome = nome
    conta.fornecedor = (dados.get("fornecedor") or "").strip() or None
    conta.forma_pagamento_id = _int_ou_none(dados.get("forma_pagamento_id"))
    conta.recorrencia = recorrencia
    conta.valor_previsto = _dec_ou_none(dados.get("valor_previsto"))
    conta.moeda = moeda
    conta.dia_vencimento = _int_ou_none(dados.get("dia_vencimento"))
    conta.desde = valid_month(dados.get("desde")) if dados.get("desde") else None
    conta.ate = valid_month(dados.get("ate")) if dados.get("ate") else None
    conta.url_painel = (dados.get("url_painel") or "").strip() or None
    conta.obs = (dados.get("obs") or "").strip() or None
    if dados.get("ativo") is not None:
        conta.ativo = bool(dados["ativo"])
    sess.add(conta)
    sess.commit()
    return conta.to_dict()


def excluir_conta(sess, conta_id: int) -> dict:
    conta = sess.get(Conta, int(conta_id))
    if not conta:
        raise ValueError("conta não encontrada")
    n = sess.query(func.count(Lancamento.id))\
            .filter(Lancamento.conta_id == conta.id).scalar() or 0
    if n:
        conta.ativo = False
        sess.commit()
        return {"removido": False, "desativado": True, "lancamentos": n}
    sess.delete(conta)
    sess.commit()
    return {"removido": True}


def _int_ou_none(v):
    s = str(v if v is not None else "").strip()
    return int(s) if s.lstrip("-").isdigit() else None


def _dec_ou_none(v):
    if v in (None, "", "null"):
        return None
    try:
        return round(float(str(v).replace(",", ".")), 4)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Lançamentos
# ─────────────────────────────────────────────────────────────────────────────
def salvar_lancamento(sess, dados: dict, *, email: Optional[str] = None) -> dict:
    """Cria ou atualiza um lançamento.

    O objeto novo só entra na sessão DEPOIS de preenchido: get_cotacao() faz um
    SELECT no meio do caminho e o autoflush do SQLAlchemy tentaria gravar a
    linha ainda em branco, estourando NOT NULL em centro_id.
    """
    lanc = sess.get(Lancamento, int(dados["id"])) if dados.get("id") else None
    novo = lanc is None
    if novo:
        lanc = Lancamento()
        lanc.created_by = email
        origem = (dados.get("origem") or "manual").strip()
        if origem not in db.ORIGENS_LANC:
            raise ValueError(f"origem inválida: {origem}")
        lanc.origem = origem

    descricao = (dados.get("descricao") or "").strip()
    if not descricao:
        raise ValueError("descrição é obrigatória")
    centro_id = _int_ou_none(dados.get("centro_id")) or (lanc.centro_id if not novo else None)
    if not centro_id:
        raise ValueError("centro de custo é obrigatório")

    moeda = (dados.get("moeda") or "BRL").upper()
    if moeda not in db.MOEDAS:
        raise ValueError(f"moeda inválida: {moeda}")
    status = (dados.get("status") or "pago").strip()
    if status not in db.STATUS_LANC:
        raise ValueError(f"status inválido: {status}")

    valor = _dec_ou_none(dados.get("valor"))
    if valor is None:
        raise ValueError("valor é obrigatório")

    data_pag = _parse_date(dados.get("data_pagamento"))
    # Competência default = mês do pagamento; é o que o extrato dá de graça.
    competencia = valid_month(
        dados.get("competencia") or (data_pag.strftime("%Y-%m") if data_pag else None)
    )
    cotacao = _dec_ou_none(dados.get("cotacao")) or get_cotacao(sess, competencia)

    lanc.centro_id = int(centro_id)
    lanc.conta_id = _int_ou_none(dados.get("conta_id"))
    lanc.competencia = competencia
    lanc.data_pagamento = data_pag
    lanc.descricao = descricao[:240]
    lanc.fornecedor = (dados.get("fornecedor") or "").strip() or None
    lanc.forma_pagamento_id = _int_ou_none(dados.get("forma_pagamento_id"))
    lanc.valor = valor
    lanc.moeda = moeda
    # A cotação fica registrada sempre — mesmo em BRL, porque é ela que gerou o
    # valor_usd. Sem isso não dá para auditar de onde saiu o número convertido.
    lanc.cotacao = cotacao
    lanc.valor_brl = para_brl(valor, moeda, cotacao)
    lanc.valor_usd = para_usd(valor, moeda, cotacao)
    lanc.status = status
    lanc.obs = (dados.get("obs") or "").strip() or None
    if dados.get("external_id"):
        lanc.external_id = str(dados["external_id"])[:120]
    if novo:
        sess.add(lanc)
    sess.commit()
    return lanc.to_dict()


def excluir_lancamento(sess, lanc_id: int) -> dict:
    lanc = sess.get(Lancamento, int(lanc_id))
    if not lanc:
        raise ValueError("lançamento não encontrado")
    sess.delete(lanc)
    sess.commit()
    return {"removido": True}


def _parse_date(v) -> Optional[date]:
    s = str(v or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def listar_lancamentos(sess, de: str, ate: str, centro_id: Optional[int] = None,
                       forma_id: Optional[int] = None) -> list[Lancamento]:
    q = (sess.query(Lancamento)
         .filter(Lancamento.competencia >= de, Lancamento.competencia <= ate))
    if centro_id:
        q = q.filter(Lancamento.centro_id == int(centro_id))
    if forma_id:
        q = q.filter(Lancamento.forma_pagamento_id == int(forma_id))
    return q.order_by(Lancamento.competencia.desc(),
                      Lancamento.data_pagamento.desc().nullslast(),
                      Lancamento.id.desc()).all()


# ─────────────────────────────────────────────────────────────────────────────
# Integração com o painel Custos com IA (centro fonte='ia')
# ─────────────────────────────────────────────────────────────────────────────
def ia_totais_usd(meses: list[str]) -> dict[str, dict]:
    """{competência: {openai, groq, subs, total}} em USD, lido do custos_ia.

    Tolerante: se o módulo não carregar (dev sem a pasta de dados), devolve zeros
    — a home continua funcionando, só sem a fatia de IA.
    """
    vazio = {m: {"openai": 0.0, "groq": 0.0, "subs": 0.0, "total": 0.0} for m in meses}
    try:
        import custos_ia
    except Exception as e:  # noqa: BLE001
        log.warning("custos_ia indisponível para o Custos de TI: %s", e)
        return vazio

    out = {}
    for m in meses:
        try:
            o = custos_ia.load_openai_snapshot(m) or {}
            g = custos_ia.load_groq_snapshot(m) or {}
            subs = custos_ia.subs_total(m)
            openai_t = float(o.get("total_usd") or 0.0)
            groq_t = float(g.get("total_usd") or 0.0)
            out[m] = {
                "openai": round(openai_t, 4), "groq": round(groq_t, 4),
                "subs": round(float(subs or 0.0), 4),
                "total": round(openai_t + groq_t + float(subs or 0.0), 4),
            }
        except Exception as e:  # noqa: BLE001
            log.warning("custos_ia falhou no mês %s: %s", m, e)
            out[m] = vazio[m]
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Consolidação / payload das telas
# ─────────────────────────────────────────────────────────────────────────────
def ultimo_mes_com_dado(sess, centro_id: Optional[int] = None) -> Optional[str]:
    """Competência mais recente que tem lançamento — o mês que vale abrir.

    Existe porque no dia 2 o mês corrente está vazio e a tela parecia quebrada.
    Voltar só um mês não bastaria: se o anterior também estiver vazio o problema
    volta, então a tela pula direto para o último com movimento.
    """
    q = sess.query(func.max(Lancamento.competencia))
    if centro_id:
        q = q.filter(Lancamento.centro_id == int(centro_id))
    return q.scalar()


def _periodo(de: Optional[str], ate: Optional[str]) -> tuple[str, str]:
    """Default do filtro = mês atual, como pedido."""
    if not de and not ate:
        m = mes_atual()
        return m, m
    ate = valid_month(ate or de)
    de = valid_month(de or ate)
    return (de, ate) if de <= ate else (ate, de)


def resumo(sess, de: Optional[str] = None, ate: Optional[str] = None) -> dict:
    """Consolidação do período: total, por centro, por forma, por mês, top contas.

    Tudo em BRL (valor_brl congelado no lançamento) e também em USD equivalente,
    usando a cotação do mês de cada linha.
    """
    de, ate = _periodo(de, ate)
    meses = range_meses(de, ate)
    centros = listar_centros(sess, incluir_inativos=True)
    por_id = {c.id: c for c in centros}
    cot = {m: get_cotacao(sess, m) for m in meses}

    lancs = listar_lancamentos(sess, de, ate)

    # Cada balde acumula BRL e USD lado a lado. A tela escolhe qual mostrar, e
    # nenhum dos dois é derivado do outro na hora de exibir — ambos vêm dos
    # valores congelados no lançamento.
    def _bal():
        return {"brl": 0.0, "usd": 0.0}

    def _soma(d: dict, chave, brl: float, usd: float):
        b = d.setdefault(chave, _bal())
        b["brl"] = round(b["brl"] + brl, 2)
        b["usd"] = round(b["usd"] + usd, 2)

    por_centro: dict[int, dict] = {}
    por_centro_mes: dict[int, dict[str, dict]] = {}
    por_forma: dict[Optional[int], dict] = {}
    por_conta: dict[str, dict] = {}
    por_mes: dict[str, dict] = {m: _bal() for m in meses}
    por_status = {"pago": _bal(), "previsto": _bal()}
    por_origem: dict[str, dict] = {}
    previstos: list[dict] = []
    # quantos lançamentos daquela moeda são valor ORIGINAL (não convertido)
    exatos = {"brl": 0, "usd": 0}

    for l in lancs:
        brl = float(l.valor_brl or 0.0)
        usd = float(l.valor_usd or 0.0)
        _soma(por_status, l.status, brl, usd)
        # REALIZADO vs PREVISTO: só o que foi pago entra nos totais e gráficos.
        # Sem isso a estimativa da Graph API (que entra como 'previsto') somaria
        # EM CIMA da cobrança real do cartão vinda do extrato — o mesmo gasto
        # contado duas vezes, com um número que a própria Meta erra.
        if l.status != "pago":
            previstos.append(l.to_dict())
            continue
        exatos["usd" if l.moeda == "USD" else "brl"] += 1
        _soma(por_centro, l.centro_id, brl, usd)
        _soma(por_centro_mes.setdefault(l.centro_id, {}), l.competencia, brl, usd)
        _soma(por_forma, l.forma_pagamento_id, brl, usd)
        rotulo = l.conta.nome if l.conta else (l.fornecedor or l.descricao)
        _soma(por_conta, rotulo, brl, usd)
        _soma(por_mes, l.competencia, brl, usd)
        _soma(por_origem, l.origem, brl, usd)

    # Centro de IA: o total NÃO vem de ti_lancamento, vem dos snapshots do
    # custos_ia (a fonte da verdade daquele painel). Lá o valor nasce em USD,
    # então o dólar é o exato e o real é que é convertido.
    ia_centro = next((c for c in centros if c.fonte == "ia"), None)
    ia_usd = ia_totais_usd(meses) if ia_centro else {}
    ia_detalhe = []
    if ia_centro:
        for m in meses:
            d = ia_usd.get(m, {})
            usd = round(float(d.get("total") or 0.0), 2)
            brl = round(usd * cot[m], 2)
            if usd:
                exatos["usd"] += 1
                _soma(por_centro, ia_centro.id, brl, usd)
                _soma(por_centro_mes.setdefault(ia_centro.id, {}), m, brl, usd)
                _soma(por_mes, m, brl, usd)
                _soma(por_status, "pago", brl, usd)
                _soma(por_origem, "ia_snapshot", brl, usd)
                # sem isto o gasto de IA sumia do gráfico por forma de pagamento
                # e ele não fechava com o total (medido: faltavam R$ 3.118,93
                # em jul/26). Cai em "— sem forma —" se o centro não tiver uma.
                _soma(por_forma, ia_centro.forma_pagamento_id, brl, usd)
            ia_detalhe.append({"competencia": m, "usd": usd, "brl": brl,
                               "cotacao": cot[m], "openai": d.get("openai", 0.0),
                               "groq": d.get("groq", 0.0), "subs": d.get("subs", 0.0)})
            for chave, rotulo in (("openai", "OpenAI (API)"), ("groq", "Groq"),
                                  ("subs", "Assinaturas de IA")):
                u = round(float(d.get(chave) or 0.0), 2)
                if u:
                    _soma(por_conta, rotulo, round(u * cot[m], 2), u)

    total = {"brl": round(sum(b["brl"] for b in por_mes.values()), 2),
             "usd": round(sum(b["usd"] for b in por_mes.values()), 2)}
    n = len(meses) or 1

    def _pct(b: dict) -> dict:
        return {"brl": round(b["brl"] / total["brl"] * 100, 1) if total["brl"] else 0.0,
                "usd": round(b["usd"] / total["usd"] * 100, 1) if total["usd"] else 0.0}

    def _ordena(d: dict):
        return sorted(d.items(), key=lambda kv: kv[1]["brl"], reverse=True)

    formas = {f.id: f for f in listar_formas(sess)}
    return {
        "periodo": {"de": de, "ate": ate, "meses": meses,
                    "mes_atual": mes_atual(), "um_mes": len(meses) == 1},
        "cotacao": {"por_mes": cot, "fallback": COTACAO_FALLBACK,
                    "detalhe": cotacoes_detalhe(sess, meses)},
        "total_brl": total["brl"], "total_usd": total["usd"],
        "media_mensal_brl": round(total["brl"] / n, 2),
        "media_mensal_usd": round(total["usd"] / n, 2),
        # quantos lançamentos têm valor ORIGINAL em cada moeda — a tela usa para
        # dizer se o número que está na frente do usuário é exato ou convertido
        "exatos": exatos,
        "por_centro": [
            {"centro_id": cid, "key": por_id[cid].key, "nome": por_id[cid].nome,
             "cor": por_id[cid].cor, "icone": por_id[cid].icone,
             "url": por_id[cid].url, "fonte": por_id[cid].fonte,
             "total_brl": b["brl"], "total_usd": b["usd"],
             "pct_brl": _pct(b)["brl"], "pct_usd": _pct(b)["usd"],
             "por_mes_brl": {m: v["brl"] for m, v in por_centro_mes.get(cid, {}).items()},
             "por_mes_usd": {m: v["usd"] for m, v in por_centro_mes.get(cid, {}).items()}}
            for cid, b in _ordena(por_centro) if cid in por_id
        ],
        "por_mes": [{"competencia": m, "total_brl": por_mes[m]["brl"],
                     "total_usd": por_mes[m]["usd"], "cotacao": cot[m]} for m in meses],
        "por_forma": [
            {"forma_id": fid,
             "rotulo": formas[fid].rotulo if fid in formas else "— sem forma —",
             "tipo": formas[fid].tipo if fid in formas else None,
             "total_brl": b["brl"], "total_usd": b["usd"],
             "pct_brl": _pct(b)["brl"], "pct_usd": _pct(b)["usd"]}
            for fid, b in _ordena(por_forma)
        ],
        "top_contas": [{"nome": k, "total_brl": b["brl"], "total_usd": b["usd"]}
                       for k, b in _ordena(por_conta)[:15]],
        "por_status": {k: v for k, v in por_status.items()},
        "por_origem": por_origem,
        # Previstos ficam FORA do total de propósito — são estimativa ou conta a
        # vencer. Vão à parte para a tela mostrar sem misturar.
        "previstos": previstos,
        "total_previsto_brl": por_status["previsto"]["brl"],
        "total_previsto_usd": por_status["previsto"]["usd"],
        "ia": {"detalhe": ia_detalhe,
               "centro_key": ia_centro.key if ia_centro else None},
        "qtd_lancamentos": len(lancs) - len(previstos),
        "qtd_previstos": len(previstos),
    }


def cotacoes_detalhe(sess, meses: list[str]) -> list[dict]:
    """Cotação de cada mês do período, dizendo de ONDE ela veio.

    Sem isso o usuário não tem como saber se o número em real foi convertido por
    uma cotação que alguém digitou, que veio de API, ou por um chute do sistema.
    """
    out = []
    for m in meses:
        row = sess.get(Cotacao, m)
        if row:
            out.append({"competencia": m, "usd_brl": float(row.usd_brl),
                        "fonte": row.fonte or "manual", "propria": True,
                        "definida_em": row.updated_at.isoformat(timespec="minutes")
                        if row.updated_at else None})
            continue
        herdada = (sess.query(Cotacao).filter(Cotacao.competencia < m)
                   .order_by(Cotacao.competencia.desc()).first())
        if herdada:
            out.append({"competencia": m, "usd_brl": float(herdada.usd_brl),
                        "fonte": f"herdada de {herdada.competencia}",
                        "propria": False, "definida_em": None})
        else:
            out.append({"competencia": m, "usd_brl": COTACAO_FALLBACK,
                        "fonte": "padrão do sistema (não confiável)",
                        "propria": False, "definida_em": None})
    return out


def home_payload(sess, de: Optional[str] = None, ate: Optional[str] = None) -> dict:
    """Payload da home: consolidação do período + evolução dos últimos 12 meses."""
    de, ate = _periodo(de, ate)
    dados = resumo(sess, de, ate)

    # Evolução sempre olha 12 meses até o fim do período, independente do filtro —
    # é o gráfico de tendência, não o do período.
    meses_evo = ultimos_meses(MESES_PADRAO, ate)
    evo = resumo(sess, meses_evo[0], meses_evo[-1])
    dados["evolucao"] = {
        "meses": meses_evo,
        "series": [
            {"key": c["key"], "nome": c["nome"], "cor": c["cor"],
             "valores_brl": [c["por_mes_brl"].get(m, 0.0) for m in meses_evo],
             "valores_usd": [c["por_mes_usd"].get(m, 0.0) for m in meses_evo]}
            for c in evo["por_centro"]
        ],
    }
    dados["ultimo_mes_com_dado"] = ultimo_mes_com_dado(sess)
    dados["centros"] = [c.to_dict() for c in listar_centros(sess)]
    dados["formas"] = [f.to_dict() for f in listar_formas(sess)]
    dados["gerado_em"] = _now().isoformat(timespec="seconds")
    return dados


def centro_payload(sess, key: str, de: Optional[str] = None,
                   ate: Optional[str] = None) -> dict:
    """Payload da página de um centro: contas cadastradas + lançamentos do período."""
    centro = get_centro(sess, key)
    if not centro:
        raise ValueError(f"centro de custo '{key}' não existe")
    de, ate = _periodo(de, ate)
    meses = range_meses(de, ate)
    lancs = listar_lancamentos(sess, de, ate, centro_id=centro.id)

    def _bal():
        return {"brl": 0.0, "usd": 0.0}

    def _soma(d: dict, chave, brl: float, usd: float):
        b = d.setdefault(chave, _bal())
        b["brl"] = round(b["brl"] + brl, 2)
        b["usd"] = round(b["usd"] + usd, 2)

    por_mes: dict[str, dict] = {m: _bal() for m in meses}
    por_conta: dict[str, dict] = {}
    previsto = _bal()
    cot = {m: get_cotacao(sess, m) for m in meses}
    for l in lancs:
        brl, usd = float(l.valor_brl or 0.0), float(l.valor_usd or 0.0)
        # Mesma regra da home: previsto não entra no realizado. Ver resumo().
        if l.status != "pago":
            previsto["brl"] = round(previsto["brl"] + brl, 2)
            previsto["usd"] = round(previsto["usd"] + usd, 2)
            continue
        _soma(por_mes, l.competencia, brl, usd)
        rotulo = l.conta.nome if l.conta else (l.fornecedor or l.descricao)
        _soma(por_conta, rotulo, brl, usd)

    ia = None
    if centro.fonte == "ia":
        usd_mes = ia_totais_usd(meses)
        ia = []
        for m in meses:
            d = usd_mes.get(m, {})
            u = round(float(d.get("total") or 0.0), 2)
            b = round(u * cot[m], 2)
            _soma(por_mes, m, b, u)
            ia.append({"competencia": m, "usd": u, "brl": b,
                       "openai": d.get("openai", 0.0), "groq": d.get("groq", 0.0),
                       "subs": d.get("subs", 0.0), "cotacao": cot[m]})
            for chave, rotulo in (("openai", "OpenAI (API)"), ("groq", "Groq"),
                                  ("subs", "Assinaturas de IA")):
                uu = round(float(d.get(chave) or 0.0), 2)
                if uu:
                    _soma(por_conta, rotulo, round(uu * cot[m], 2), uu)

    total = {"brl": round(sum(b["brl"] for b in por_mes.values()), 2),
             "usd": round(sum(b["usd"] for b in por_mes.values()), 2)}
    return {
        "centro": centro.to_dict(),
        "periodo": {"de": de, "ate": ate, "meses": meses,
                    "mes_atual": mes_atual(), "um_mes": len(meses) == 1},
        "total_brl": total["brl"], "total_usd": total["usd"],
        "total_previsto_brl": previsto["brl"], "total_previsto_usd": previsto["usd"],
        "media_mensal_brl": round(total["brl"] / (len(meses) or 1), 2),
        "media_mensal_usd": round(total["usd"] / (len(meses) or 1), 2),
        "por_mes": [{"competencia": m, "total_brl": por_mes[m]["brl"],
                     "total_usd": por_mes[m]["usd"], "cotacao": cot[m]} for m in meses],
        "por_conta": [{"nome": k, "total_brl": b["brl"], "total_usd": b["usd"]}
                      for k, b in sorted(por_conta.items(),
                                         key=lambda kv: kv[1]["brl"], reverse=True)],
        "contas": [c.to_dict() for c in listar_contas(sess, centro.id)],
        "lancamentos": [l.to_dict() for l in lancs],
        "formas": [f.to_dict() for f in listar_formas(sess)],
        "centros": [c.to_dict() for c in listar_centros(sess)],
        "ia": ia,
        "cotacao": get_cotacao(sess, ate),
        "cotacoes": cotacoes_detalhe(sess, meses),
        "ultimo_mes_com_dado": ultimo_mes_com_dado(sess, centro.id),
        "gerado_em": _now().isoformat(timespec="seconds"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Detalhamento Meta — por telefone e por categoria de preço
# ─────────────────────────────────────────────────────────────────────────────
# Rótulos das categorias da Meta. SERVICE aparece com custo 0 no modelo atual
# (resposta dentro da janela de 24h), mas o volume dela importa: é o que mostra
# quanto do tráfego é conversa de verdade e não disparo.
CATEGORIAS_META = {
    "MARKETING": "Marketing",
    "UTILITY": "Utilidade",
    "AUTHENTICATION": "Autenticação",
    "SERVICE": "Serviço (resposta em 24h)",
}


def meta_detalhe(sess, de: Optional[str] = None, ate: Optional[str] = None) -> dict:
    """Custo da Meta no período, quebrado por telefone e por categoria de preço.

    Uma chamada à Graph API por mês (a API não aceita intervalo maior que o
    mês com granularidade diária de forma confiável). Só GET, sem custo.
    """
    import custos_ti_meta as meta

    de, ate = _periodo(de, ate)
    meses = range_meses(de, ate)
    cot = {m: get_cotacao(sess, m) for m in meses}

    por_tel: dict[str, dict] = {}
    por_cat: dict[str, dict] = {}
    por_mes: list[dict] = []
    total = {"usd": 0.0, "brl": 0.0, "volume": 0}
    erros: list[str] = []

    for m in meses:
        d = meta.fetch_pricing_analytics(m)
        if not d.get("ok"):
            erros.append(f"{m}: {d.get('error')}")
            continue
        if d.get("error"):          # respondeu, mas sem ponto naquele mês
            continue
        usd = float(d["total"] or 0.0)
        brl = round(usd * cot[m], 2)
        total["usd"] = round(total["usd"] + usd, 2)
        total["brl"] = round(total["brl"] + brl, 2)
        total["volume"] += int(d.get("volume") or 0)
        por_mes.append({"competencia": m, "usd": round(usd, 2), "brl": brl,
                        "volume": int(d.get("volume") or 0), "cotacao": cot[m]})

        for tel, linha in (d.get("por_telefone") or {}).items():
            alvo = por_tel.setdefault(tel, {
                "numero": tel, "rotulo": linha["rotulo"],
                "usd": 0.0, "brl": 0.0, "volume": 0, "categorias": {}})
            alvo["usd"] = round(alvo["usd"] + linha["total"], 2)
            alvo["brl"] = round(alvo["brl"] + linha["total"] * cot[m], 2)
            alvo["volume"] += linha["volume"]
            for cat, c in linha["categorias"].items():
                cc = alvo["categorias"].setdefault(cat, {"usd": 0.0, "brl": 0.0,
                                                         "volume": 0})
                cc["usd"] = round(cc["usd"] + c["custo"], 2)
                cc["brl"] = round(cc["brl"] + c["custo"] * cot[m], 2)
                cc["volume"] += c["volume"]

        for cat, custo in (d.get("por_categoria") or {}).items():
            alvo = por_cat.setdefault(cat, {"categoria": cat,
                                            "rotulo": CATEGORIAS_META.get(cat, cat),
                                            "usd": 0.0, "brl": 0.0, "volume": 0})
            alvo["usd"] = round(alvo["usd"] + custo, 2)
            alvo["brl"] = round(alvo["brl"] + custo * cot[m], 2)

    # volume por categoria vem da soma dos telefones (a chave de categoria do
    # payload da Meta traz só custo)
    for linha in por_tel.values():
        for cat, c in linha["categorias"].items():
            if cat in por_cat:
                por_cat[cat]["volume"] += c["volume"]

    def _pct(v: float) -> float:
        return round(v / total["usd"] * 100, 1) if total["usd"] else 0.0

    telefones = sorted(por_tel.values(), key=lambda x: x["usd"], reverse=True)
    for t in telefones:
        t["pct"] = _pct(t["usd"])
        t["categorias"] = [
            {"categoria": c, "rotulo": CATEGORIAS_META.get(c, c), **v}
            for c, v in sorted(t["categorias"].items(),
                               key=lambda kv: kv[1]["usd"], reverse=True)
        ]
    categorias = sorted(por_cat.values(), key=lambda x: x["usd"], reverse=True)
    for c in categorias:
        c["pct"] = _pct(c["usd"])

    return {
        "periodo": {"de": de, "ate": ate, "meses": meses},
        "total_usd": total["usd"], "total_brl": total["brl"],
        "volume": total["volume"],
        "telefones": telefones, "categorias": categorias, "por_mes": por_mes,
        "erros": erros,
        "nota": ("Estimativa da Graph API (pricing_analytics) — é o consumo das "
                 "mensagens, não a cobrança do cartão. Serviço custa 0 no modelo "
                 "atual, mas o volume conta."),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Importação Meta → lançamentos
# ─────────────────────────────────────────────────────────────────────────────
def importar_meta_texto(sess, texto: str, *, centro_key: str = "comunicacao",
                        conta_id: Optional[int] = None,
                        criar_forma: bool = True,
                        salvar: bool = True,
                        email: Optional[str] = None) -> dict:
    """Lê o texto da Atividade de pagamento da Meta e vira lançamentos.

    `salvar=False` só pré-visualiza (é o botão "Conferir" da tela).
    Dedupe por external_id: reimportar o mesmo extrato não duplica nada, então
    dá para colar o mês inteiro toda vez sem medo.
    """
    import custos_ti_meta as meta

    parsed = meta.parse_payment_activity(texto)
    itens = parsed["items"]
    if not itens:
        return {"ok": False, "error": "Nenhuma transação reconhecida no texto colado.",
                "novos": [], "duplicados": [], "total": 0.0}

    centro = get_centro(sess, centro_key)
    if not centro:
        raise ValueError(f"centro de custo '{centro_key}' não existe")

    existentes = {
        r[0] for r in sess.query(Lancamento.external_id)
        .filter(Lancamento.origem == "meta_texto",
                Lancamento.external_id.in_([i["external_id"] for i in itens])).all()
    }

    novos, duplicados, formas_criadas = [], [], []
    for it in itens:
        registro = {
            "external_id": it["external_id"],
            "data_pagamento": it["data"].isoformat() if it["data"] else None,
            "competencia": it["data"].strftime("%Y-%m") if it["data"] else mes_atual(),
            "valor": it["valor"], "moeda": it["moeda"],
            "bandeira": it["bandeira"], "ultimos4": it["ultimos4"],
            "status": it["status"],
        }
        if it["external_id"] in existentes:
            duplicados.append(registro)
            continue
        novos.append(registro)

    total_novos = round(sum(n["valor"] for n in novos), 2)
    if not salvar:
        return {"ok": True, "preview": True, "centro": centro.to_dict(),
                "novos": novos, "duplicados": duplicados,
                "total": total_novos, "moeda": parsed["moeda"],
                "formas_criadas": []}

    forma_cache: dict[tuple, Optional[int]] = {}
    for n in novos:
        chave = (n["bandeira"], n["ultimos4"])
        if chave not in forma_cache:
            forma = _forma_por_cartao(sess, n["bandeira"], n["ultimos4"])
            if forma is None and criar_forma and n["ultimos4"]:
                forma = FormaPagamento(
                    nome=n["bandeira"] or "Cartão",
                    tipo="cartao_credito",
                    bandeira=n["bandeira"], ultimos4=n["ultimos4"],
                    moeda_padrao=n["moeda"] if n["moeda"] in db.MOEDAS else "USD",
                    obs="Criada automaticamente na importação da Meta.",
                )
                sess.add(forma)
                sess.flush()
                formas_criadas.append(forma.to_dict())
            forma_cache[chave] = forma.id if forma else None

        salvar_lancamento(sess, {
            "centro_id": centro.id,
            "conta_id": conta_id,
            "competencia": n["competencia"],
            "data_pagamento": n["data_pagamento"],
            "descricao": f"WhatsApp Business (Meta) — {n['external_id'][:20]}…",
            "fornecedor": "Meta Platforms",
            "forma_pagamento_id": forma_cache[chave],
            "valor": n["valor"], "moeda": n["moeda"],
            "status": "pago" if n["status"] == "pago" else "previsto",
            "origem": "meta_texto",
            "external_id": n["external_id"],
        }, email=email)

    return {"ok": True, "preview": False, "centro": centro.to_dict(),
            "novos": novos, "duplicados": duplicados,
            "total": total_novos, "moeda": parsed["moeda"],
            "formas_criadas": formas_criadas}


def importar_meta_api(sess, competencia: str, *, centro_key: str = "comunicacao",
                      salvar: bool = True, email: Optional[str] = None) -> dict:
    """Custo ESTIMADO das mensagens do mês, via Graph API, como lançamento previsto.

    Entra com origem 'meta_api' e status 'previsto' — não se mistura com a
    cobrança real do cartão (origem 'meta_texto'), que é o que fecha o mês.
    O external_id é fixo por mês, então re-sincronizar ATUALIZA a linha em vez
    de criar outra.
    """
    import custos_ti_meta as meta

    competencia = valid_month(competencia)
    dados = meta.fetch_custo_mensagens(competencia)
    if not dados.get("ok"):
        return {"ok": False, "error": dados.get("error"), "analytics": dados}

    centro = get_centro(sess, centro_key)
    if not centro:
        raise ValueError(f"centro de custo '{centro_key}' não existe")

    ext = f"meta_analytics::{competencia}"
    if salvar and dados["total"] > 0:
        existente = (sess.query(Lancamento)
                     .filter(Lancamento.origem == "meta_api",
                             Lancamento.external_id == ext).first())
        payload = {
            "id": existente.id if existente else None,
            "centro_id": centro.id, "competencia": competencia,
            "descricao": f"WhatsApp — custo estimado de mensagens ({competencia})",
            "fornecedor": "Meta Platforms",
            "valor": dados["total"],
            "moeda": dados.get("moeda") or "USD",
            "status": "previsto", "origem": "meta_api", "external_id": ext,
            "obs": f"Estimativa da Graph API ({dados.get('fonte')}). "
                   "A cobrança real do cartão entra pela importação do extrato.",
        }
        salvar_lancamento(sess, payload, email=email)

    return {"ok": True, "analytics": dados, "centro": centro.to_dict(),
            "salvo": bool(salvar and dados["total"] > 0)}


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────────────────────────────────────
def seed_centros(sess) -> list[dict]:
    """Cria os centros iniciais que ainda não existem (idempotente)."""
    criados = []
    for c in CENTROS_SEED:
        if get_centro(sess, c["key"]):
            continue
        centro = CentroCusto(**c)
        sess.add(centro)
        criados.append(c["key"])
    sess.commit()
    return criados
