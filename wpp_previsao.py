"""
wpp_previsao.py
Central de controle e auditoria dos disparos de cobrança WhatsApp.

Responde, para uma data escolhida, linha a linha (cliente a cliente):
  - quantas mensagens serão/foram enviadas, para quem, por qual campanha,
    em que horário e por qual regra;
  - quem NÃO vai receber e o MOTIVO exato, em linguagem leiga.

Três regimes por data:
  - PASSADO: auditoria pura das tabelas envios/nao_enviados (o que de fato
    aconteceu). Não toca o SQL Server.
  - HOJE: simulação fiel do cron (mesmas queries, mesmos filtros, mesma
    ordem de rodada) cruzada com os envios reais já feitos no dia.
  - FUTURO: simulação com a régua de dias deslocada (atraso/pré-vencimento),
    marcada como estimativa — pagamentos e envios até lá mudam o resultado.

SEGURANÇA — este módulo é SOMENTE-LEITURA por construção:
  - importa do motor (send_whatsapp_cobranca) apenas funções puras:
    buscar_faturas (SELECT), limpar_telefone, fmt_*, montar_params_template,
    _expandir_template. NUNCA chama enviar*/registrar_*.
  - nenhuma escrita em SQLite nem em SQL Server.
A fidelidade vem de REUSAR as funções do próprio cron em vez de reimplicar
as regras — se o cron mudar, a previsão muda junto.

Usado por wpp_cobranca_routes.py (rotas /wpp/previsao*). A simulação roda em
thread de fundo (mesmo padrão do cache-refresh) porque varre os 13 postos no
SQL Server e pode levar minutos — request síncrono derrubaria o worker único.
"""

import copy
import logging
import os
import threading
from datetime import date, datetime, timedelta

import wpp_cobranca_db as db
import wpp_cobranca_sql as sql_helper
from wpp_cobranca_sql import (
    get_conn_posto,
    modo_envio,
    MODO_ATRASO,
    MODO_PRE_VENCIMENTO,
    MODO_CLIENTES,
    MODO_CLIENTE_NOVO,
    MODO_FALTA_MEDICO,
)

log = logging.getLogger(__name__)

# Rodada do cron: sync_wpp.sh roda a cada 15 min (cron/relatorio_ht linha */15).
RODADA_MIN = 15

# Preço Meta por mensagem em US$, por CATEGORIA do template (a categoria vem
# da própria API /templates — mesma classificação que a Meta usa pra cobrar).
# Defaults = padrão Meta Brasil exibido no wpp_dashboard; ajustável por env.
import os as _os
PRECO_META_USD = {
    "MARKETING":      float(_os.getenv("WPP_PRECO_MKT_USD",  "0.0625")),
    "UTILITY":        float(_os.getenv("WPP_PRECO_UTIL_USD", "0.0068")),
    "AUTHENTICATION": float(_os.getenv("WPP_PRECO_AUTH_USD", "0.0315")),
}
# Fallback de cotação quando a awesomeapi não responder (marcado como tal).
USD_BRL_FALLBACK = float(_os.getenv("WPP_USD_BRL", "5.40"))
# Teto de linhas detalhadas por campanha no JSON (contagens continuam completas
# e a truncagem é AVISADA na tela — nada de teto silencioso).
MAX_LINHAS_POR_CAMPANHA = 4000

_DIAS_NOMES = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]

# ---------------------------------------------------------------------------
# Legenda dos motivos — código técnico → rótulo curto + explicação leiga.
# Toda linha carrega o código E a frase pronta (com datas/nomes interpolados);
# esta legenda alimenta o bloco "Como ler esta página".
# ---------------------------------------------------------------------------
MOTIVOS_LEGENDA = {
    "prevista": {
        "rotulo": "Será enviada",
        "explica": "Passou em todas as regras da campanha e nos controles de "
                   "repetição. Sai na próxima passada do robô (a cada "
                   f"{RODADA_MIN} min) dentro da janela de horário.",
    },
    "enviada": {
        "rotulo": "Enviada",
        "explica": "A mensagem já foi enviada — o horário efetivo está na linha.",
    },
    "janela_encerrada_hoje": {
        "rotulo": "Janela do dia fechou",
        "explica": "O cliente está elegível, mas o horário de envio da campanha "
                   "já terminou hoje. Se continuar elegível, entra no próximo "
                   "dia permitido.",
    },
    "dia_nao_permitido": {
        "rotulo": "Dia não permitido",
        "explica": "A campanha não envia neste dia da semana.",
    },
    "sem_telefone_valido": {
        "rotulo": "Telefone inválido",
        "explica": "O telefone do cadastro não é um número de WhatsApp válido "
                   "(vazio, incompleto ou anotação tipo 'NÃO TEM').",
    },
    "nome_de_teste": {
        "rotulo": "Cadastro de teste",
        "explica": "O nome contém 'TESTE' ou começa com 'MATRICULA' — o sistema "
                   "nunca envia para cadastros de teste (proteção criada após "
                   "incidente real).",
    },
    "bloqueado_intervalo_global": {
        "rotulo": "Recebeu há pouco tempo",
        "explica": "Regra de silêncio: depois de receber qualquer mensagem, o "
                   "contato fica N dias sem receber outra (N = intervalo da "
                   "campanha). Evita bombardear o cliente.",
    },
    "bloqueado_rodada_global": {
        "rotulo": "Outra campanha envia hoje",
        "explica": "O mesmo telefone já será atendido hoje por uma campanha que "
                   "roda antes na fila. O sistema envia no máximo 1 mensagem "
                   "por telefone por rodada.",
    },
    "duplicado_na_campanha": {
        "rotulo": "Telefone repetido",
        "explica": "O mesmo telefone aparece em mais de uma matrícula/fatura "
                   "desta campanha — só a primeira recebe.",
    },
    "ja_enviado_campanha": {
        "rotulo": "Já recebeu (envio único)",
        "explica": "Campanha configurada para enviar UMA vez por contato; este "
                   "contato já recebeu.",
    },
    "erro_api": {
        "rotulo": "Erro no envio",
        "explica": "O sistema tentou enviar e a API de WhatsApp devolveu erro. "
                   "O detalhe técnico está na linha — merece atenção.",
    },
    "posto_indisponivel": {
        "rotulo": "Posto não consultado",
        "explica": "Não foi possível ler o banco deste posto no momento da "
                   "análise — os clientes dele não aparecem na lista.",
    },
}


# ---------------------------------------------------------------------------
# Estado da execução em background — EM ARQUIVO, não em memória.
#
# camim-auth e wpp-campanhas rodam a mesma rota, e o gunicorn pode ter mais
# de um worker: estado em memória fazia o "Iniciar" cair num processo e o
# "Status" noutro, que respondia "nada rodando / nenhum resultado" (medido em
# 2026-08-10: botão travava e destravava sem nada acontecer). Os arquivos
# ficam no MESMO diretório do SQLite de controle (os serviços já têm escrita
# lá) e a exclusão mútua entre processos é por flock no arquivo de lock.
# ---------------------------------------------------------------------------
import fcntl
import json as _json

_STATE_DIR = os.path.dirname(db.DB_PATH)
_STATUS_PATH = os.path.join(_STATE_DIR, "wpp_previsao_status.json")
_RESULT_PATH = os.path.join(_STATE_DIR, "wpp_previsao_resultado.json")
_LOCK_PATH = os.path.join(_STATE_DIR, "wpp_previsao.lock")

_lock_fh = None  # handle do flock, vivo enquanto a thread roda NESTE processo


def _write_json_atomic(path: str, payload: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        _json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, path)


def _read_json(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return None


def _alguem_rodando() -> bool:
    """True se ALGUM processo segura o flock da análise agora."""
    try:
        with open(_LOCK_PATH, "a") as fh:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fh, fcntl.LOCK_UN)
                return False
            except OSError:
                return True
    except OSError:
        return False


def _grava_status(**kw) -> None:
    s = _read_json(_STATUS_PATH) or {}
    s.update(kw)
    _write_json_atomic(_STATUS_PATH, s)


def status() -> dict:
    s = _read_json(_STATUS_PATH) or {"running": False, "pct": 0, "msg": "",
                                     "erro": None, "data": None}
    if s.get("running") and not _alguem_rodando():
        # Arquivo diz "rodando" mas ninguém segura o lock: o processo morreu
        # no meio (deploy/restart). Sem este guard a tela ficaria em
        # "Analisando…" para sempre.
        s["running"] = False
        s["erro"] = (s.get("erro")
                     or "Análise interrompida (o serviço foi reiniciado no meio). "
                        "Clique em Analisar para refazer.")
        _write_json_atomic(_STATUS_PATH, s)
    return s


def resultado() -> dict | None:
    return _read_json(_RESULT_PATH)


def iniciar(data_iso: str) -> tuple[bool, str]:
    """Dispara a análise em thread de fundo. Retorna (ok, mensagem)."""
    global _lock_fh
    try:
        alvo = date.fromisoformat(data_iso)
    except (TypeError, ValueError):
        return False, "data inválida (use AAAA-MM-DD)"
    fh = open(_LOCK_PATH, "a")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return False, "já existe uma análise em andamento"
    _lock_fh = fh  # mantém o fd aberto → lock vivo enquanto a thread roda
    _write_json_atomic(_STATUS_PATH, {
        "running": True, "pct": 0, "msg": "Iniciando…", "erro": None,
        "data": alvo.isoformat(),
    })
    t = threading.Thread(target=_rodar, args=(alvo,), daemon=True)
    t.start()
    return True, "análise iniciada"


def _rodar(alvo: date) -> None:
    global _lock_fh
    try:
        if alvo < date.today():
            res = _auditar_passado(alvo)
        else:
            res = _simular(alvo)
        _write_json_atomic(_RESULT_PATH, res)
        _grava_status(running=False, pct=100, msg="Concluído",
                      gerado_em=res["gerado_em"],
                      resumo=res.get("resumo"), data_resultado=res.get("data"))
    except Exception as e:
        log.exception("previsao: falha na análise")
        _grava_status(running=False,
                      erro=f"{type(e).__name__}: {str(e)[:300]}")
    finally:
        try:
            if _lock_fh is not None:
                fcntl.flock(_lock_fh, fcntl.LOCK_UN)
                _lock_fh.close()
        except Exception:
            pass
        _lock_fh = None


def _progresso(pct: int, msg: str) -> None:
    _grava_status(pct=max(0, min(99, int(pct))), msg=msg)


# ---------------------------------------------------------------------------
# Helpers comuns
# ---------------------------------------------------------------------------

def _hm(s: str | None, default: tuple[int, int]) -> tuple[int, int]:
    try:
        h, m = str(s).strip().split(":")
        return int(h), int(m)
    except Exception:
        return default


def _fmt_hm(hm: tuple[int, int]) -> str:
    return f"{hm[0]:02d}:{hm[1]:02d}"


def _dias_semana_txt(c: dict) -> str:
    dias = sorted({int(d) for d in str(c.get("dias_semana") or "0,1,2,3,4").split(",")
                   if d.strip().isdigit()})
    return ", ".join(_DIAS_NOMES[d] for d in dias if 0 <= d <= 6) or "—"


def _regra_humana(c: dict) -> str:
    m = modo_envio(c)
    if m == MODO_PRE_VENCIMENTO:
        mx = c.get("dias_ref_max")
        fim = f" a {mx}" if mx is not None else " ou mais"
        return (f"Aviso antes do vencimento: fatura em aberto que vence daqui a "
                f"{c.get('dias_ref_min', 4)}{fim} dias")
    if m == MODO_CLIENTE_NOVO:
        return "Boas-vindas: cliente que pagou a 1ª mensalidade nos últimos 7 dias"
    if m == MODO_CLIENTES:
        ini = (c.get("adm_data_ini") or "?")[:10]
        fim = (c.get("adm_data_fim") or "hoje")[:10]
        return f"Clientes por data de admissão ({ini} até {fim})"
    if m == MODO_FALTA_MEDICO:
        return "Aviso de falta de médico — disparado na hora pelo cadastro da falta, não pelo robô"
    mx = c.get("dias_atraso_max")
    fim = f" a {mx}" if mx else " ou mais"
    extra = " · apenas não-recorrentes" if c.get("nao_recorrente") else ""
    return f"Cobrança: fatura vencida há {c.get('dias_atraso_min', 1)}{fim} dias{extra}"


def _canal_txt(c: dict) -> str:
    partes = []
    if c.get("enviar_meta"):
        partes.append("WhatsApp (Meta)")
    if c.get("enviar_chat", 1):
        partes.append("chat interno")
    return " + ".join(partes) or "nenhum canal ativo"


def _resumo_campanha_base(c: dict, ordem: int) -> dict:
    return {
        "id": c["id"],
        "nome": c["nome"],
        "ativa": bool(c.get("ativa")),
        "modo": modo_envio(c),
        "regra_humana": _regra_humana(c),
        "template": c.get("template"),
        "canal": _canal_txt(c),
        "enviar_meta": bool(c.get("enviar_meta")),
        "numero_saida": c.get("numero_saida") or "2455-9600",
        "janela": f"{_fmt_hm(_hm(c.get('hora_inicio'), (8, 0)))}–"
                  f"{_fmt_hm(_hm(c.get('hora_fim'), (20, 0)))}",
        "dias_semana_txt": _dias_semana_txt(c),
        "intervalo_dias": int(c.get("intervalo_dias") or 7),
        "ignorar_intervalo": bool(c.get("ignorar_intervalo")),
        "ordem_rodada": ordem,
        "postos": sorted(c.get("postos") or []),
        "status_campanha": "ok",
        "motivo_campanha": None,
        "hora_prevista_txt": None,
        "template_body": "",
        "contagem": {"previstas": 0, "enviadas": 0, "bloqueadas": 0, "erros": 0,
                     "por_motivo": {}},
        "custo_estimado_meta": None,
        "custo_detalhe": None,
        "erros_postos": [],
        "linhas": [],
        "linhas_truncadas": 0,
    }


def _conta(camp: dict, linha: dict) -> None:
    """Adiciona a linha na campanha respeitando o teto (contagens sempre completas)."""
    cont = camp["contagem"]
    st = linha["status"]
    if st == "prevista":
        cont["previstas"] += 1
    elif st == "enviada":
        cont["enviadas"] += 1
    elif st == "erro":
        cont["erros"] += 1
    else:
        cont["bloqueadas"] += 1
    cod = linha.get("motivo_codigo") or st
    cont["por_motivo"][cod] = cont["por_motivo"].get(cod, 0) + 1
    if len(camp["linhas"]) < MAX_LINHAS_POR_CAMPANHA:
        camp["linhas"].append(linha)
    else:
        camp["linhas_truncadas"] += 1


def _ultimo_envio_info(telefone: str) -> dict | None:
    """Último envio accepted deste telefone em QUALQUER campanha, com nome da
    campanha — versão informativa do db.ultimo_envio_aceito()."""
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT e.enviado_em, e.campanha_id, c.nome AS campanha_nome "
            "FROM envios e JOIN campanhas c ON c.id = e.campanha_id "
            "WHERE e.telefone=? AND e.status LIKE 'accepted%' "
            "ORDER BY e.enviado_em DESC LIMIT 1",
            (telefone,),
        ).fetchone()
    return dict(row) if row else None


def _fmt_data_br(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _hora_de_iso(iso: str | None) -> str:
    try:
        return datetime.fromisoformat(iso).strftime("%H:%M")
    except Exception:
        return "?"


# ---------------------------------------------------------------------------
# Custo Meta: cotação ao vivo + categoria real do template
# ---------------------------------------------------------------------------

def _cotacao_usd_brl() -> dict:
    """Dólar comercial ao vivo (awesomeapi, mesma fonte do wpp_dashboard e do
    custos_ti). Nunca quebra a análise: sem resposta, cai no fallback do
    sistema E DIZ que caiu — cotação de origem desconhecida induz decisão
    errada (lição do custos_ti em 2026-08-02)."""
    import requests
    try:
        r = requests.get("https://economia.awesomeapi.com.br/json/last/USD-BRL",
                         timeout=8)
        r.raise_for_status()
        bid = float(r.json()["USDBRL"]["bid"])
        return {"usd_brl": round(bid, 4), "fonte": "awesomeapi.com.br",
                "ao_vivo": True}
    except Exception as e:
        log.warning("previsao: cotação awesomeapi falhou (%s) — usando fallback", e)
        return {"usd_brl": USD_BRL_FALLBACK, "fonte": "padrão do sistema (cotação ao vivo indisponível)",
                "ao_vivo": False}


def _categorias_templates() -> dict:
    """{nome_template: categoria Meta} via API /templates. Vazio se a API falhar."""
    import requests
    try:
        r = requests.get(
            f"{_os.getenv('WAPP_API_URL', 'https://whatsapp-api.camim.com.br')}/templates",
            headers={"Authorization": f"Bearer {_os.getenv('WAPP_TOKEN', '')}"},
            timeout=10,
        )
        r.raise_for_status()
        return {t.get("name"): str(t.get("category") or "").upper()
                for t in r.json().get("items", [])}
    except Exception as e:
        log.warning("previsao: não foi possível ler categorias dos templates: %s", e)
        return {}


# ---------------------------------------------------------------------------
# SIMULAÇÃO (hoje e futuro)
# ---------------------------------------------------------------------------

def _campanha_ajustada(c: dict, delta: int) -> dict:
    """Cópia da campanha com a régua de dias deslocada para simular data futura.

    O SQL calcula diasdebito/dias-até-vencimento com GETDATE(); para D dias à
    frente basta deslocar os limites — a query do cron fica intocada:
      atraso:        diasdebito(D) = diasdebito(hoje) + delta → limites - delta
      pré-vencimento: dias_ate(D)  = dias_ate(hoje)  - delta → limites + delta
    """
    if delta <= 0:
        return c
    c2 = copy.deepcopy(c)
    m = modo_envio(c)
    if m == MODO_ATRASO:
        c2["dias_atraso_min"] = max(1, int(c.get("dias_atraso_min") or 1) - delta)
        if c.get("dias_atraso_max"):
            c2["dias_atraso_max"] = max(1, int(c["dias_atraso_max"]) - delta)
    elif m == MODO_PRE_VENCIMENTO:
        c2["dias_ref_min"] = int(c.get("dias_ref_min") or 4) + delta
        if c.get("dias_ref_max") is not None:
            c2["dias_ref_max"] = int(c["dias_ref_max"]) + delta
    return c2


def _buscar_faturas_data(cursor, campanha: dict, alvo: date, delta: int) -> list[dict]:
    """buscar_faturas do cron, com um caso especial: cliente_novo em data
    futura precisa da janela de 7 dias deslocada (a função do cron usa hoje)."""
    import send_whatsapp_cobranca as engine
    if modo_envio(campanha) == MODO_CLIENTE_NOVO and delta > 0:
        ini = alvo - timedelta(days=sql_helper.CLIENTE_NOVO_LOOKBACK_DIAS)
        fim = alvo + timedelta(days=1)
        cursor.execute(sql_helper._SQL_CLIENTE_NOVO, [ini, fim])
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    return engine.buscar_faturas(cursor, _campanha_ajustada(campanha, delta))


def _hora_prevista(c: dict, alvo: date, agora: datetime) -> tuple[str, str | None]:
    """(frase de horário previsto, código de bloqueio de janela ou None)."""
    hi = _hm(c.get("hora_inicio"), (8, 0))
    hf = _hm(c.get("hora_fim"), (20, 0))
    hoje = date.today()
    if alvo > hoje:
        return (f"entre {_fmt_hm(hi)} e {_fmt_hm(hf)}, na primeira passada do robô "
                f"(a cada {RODADA_MIN} min)"), None
    agora_hm = (agora.hour, agora.minute)
    if agora_hm < hi:
        return (f"a partir de {_fmt_hm(hi)} (primeira passada do robô na janela)"), None
    if agora_hm >= hf:
        return (f"janela de hoje ({_fmt_hm(hi)}–{_fmt_hm(hf)}) já encerrou"), "janela_encerrada_hoje"
    return (f"na próxima passada do robô (até {RODADA_MIN} min; campanhas pesadas "
            f"na frente da fila podem empurrar o horário)"), None


def _dia_permitido(c: dict, alvo: date) -> bool:
    dias = {int(d) for d in str(c.get("dias_semana") or "0,1,2,3,4").split(",")
            if d.strip().isdigit()}
    return alvo.weekday() in dias


def _envios_do_dia(alvo: date) -> tuple[dict, dict]:
    """Envios accepted do dia. Retorna:
    (por_camp_tel: {(campanha_id, telefone): envio}, por_tel: {telefone: envio})."""
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT e.*, c.nome AS campanha_nome FROM envios e "
            "JOIN campanhas c ON c.id = e.campanha_id "
            "WHERE date(e.enviado_em)=? AND e.status LIKE 'accepted%'",
            (alvo.isoformat(),),
        ).fetchall()
    por_camp_tel, por_tel = {}, {}
    for r in rows:
        d = dict(r)
        por_camp_tel[(d["campanha_id"], d["telefone"])] = d
        por_tel.setdefault(d["telefone"], d)
    return por_camp_tel, por_tel


def _erros_do_dia(alvo: date) -> list[dict]:
    """Registros de nao_enviados do dia com motivo de erro de API."""
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT n.*, c.nome AS campanha_nome FROM nao_enviados n "
            "JOIN campanhas c ON c.id = n.campanha_id "
            "WHERE date(n.rodada_em)=? AND n.motivo LIKE 'erro%'",
            (alvo.isoformat(),),
        ).fetchall()
    return [dict(r) for r in rows]


def _simular(alvo: date) -> dict:
    import re
    import send_whatsapp_cobranca as engine

    hoje = date.today()
    delta = (alvo - hoje).days
    agora = datetime.now()
    eh_hoje = (delta == 0)

    engine._load_template_bodies()
    cot = _cotacao_usd_brl()
    categorias = _categorias_templates()

    todas = db.listar_campanhas()
    ativas = [c for c in todas if c.get("ativa")]

    # Mesma ordem da rodada do cron: janela que fecha mais cedo primeiro.
    def _hora_fim_key(c: dict):
        return _hm(c.get("hora_fim"), (20, 0))
    ativas.sort(key=_hora_fim_key)

    envios_camp_tel, envios_tel = _envios_do_dia(alvo) if eh_hoje else ({}, {})
    erros_dia = _erros_do_dia(alvo) if eh_hoje else []

    resultado = {
        "data": alvo.isoformat(),
        "tipo": "hoje" if eh_hoje else "futuro",
        "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
        "agora": agora.strftime("%H:%M"),
        "avisos": [],
        "campanhas": [],
        "motivos_legenda": MOTIVOS_LEGENDA,
    }
    if delta > 0:
        resultado["avisos"].append(
            f"Previsão para {_fmt_data_br(alvo)} calculada com os dados de AGORA: "
            "pagamentos, novas faturas e envios feitos até lá mudam o resultado. "
            "Use como ordem de grandeza, não como lista final.")
    if eh_hoje:
        resultado["avisos"].append(
            "Análise de hoje: cruza a fila prevista (mesmas regras do robô) com "
            "o que já foi enviado no dia. Recalcule para atualizar o retrato.")

    # Dedup global da rodada: telefone → campanha que "ganhou" o contato.
    telefones_rodada: dict[str, dict] = {}
    # Pré-carrega quem já recebeu HOJE (esses telefones já estão comprometidos).
    for tel, env in envios_tel.items():
        telefones_rodada.setdefault(tel, {"campanha": env["campanha_nome"],
                                          "ja_real": True})

    total_passos = sum(len(c.get("postos") or []) for c in ativas) or 1
    passo = 0

    campanhas_out = []
    for c in todas:
        if not c.get("ativa"):
            camp = _resumo_campanha_base(c, 0)
            camp["status_campanha"] = "pulada"
            camp["motivo_campanha"] = "Campanha suspensa — não dispara enquanto estiver pausada."
            campanhas_out.append(camp)

    ordem = 0
    for c in ativas:
        ordem += 1
        camp = _resumo_campanha_base(c, ordem)
        camp["template_body"] = engine._TEMPLATE_BODIES.get(c.get("template") or "", "")
        campanhas_out.append(camp)

        modo_raw = str(c.get("modo_envio") or "").strip().lower()
        if modo_raw == MODO_FALTA_MEDICO or modo_envio(c) == MODO_FALTA_MEDICO:
            camp["status_campanha"] = "pulada"
            camp["motivo_campanha"] = (
                "Não passa pelo robô de cobrança: o envio acontece na hora em que "
                "a falta do médico é cadastrada (tela Falta de Médico).")
            # Envios do dia desta campanha ainda aparecem (auditoria).
            for (cid, tel), env in envios_camp_tel.items():
                if cid == c["id"]:
                    _conta(camp, _linha_de_envio(env))
            continue

        if not (c.get("postos") or []):
            camp["status_campanha"] = "pulada"
            camp["motivo_campanha"] = "Sem posto configurado — não há de onde buscar clientes."
            continue

        if not _dia_permitido(c, alvo):
            camp["status_campanha"] = "pulada"
            camp["motivo_campanha"] = (
                f"{_DIAS_NOMES[alvo.weekday()].capitalize()} não é dia de envio "
                f"desta campanha (envia: {_dias_semana_txt(c)}).")
            continue

        hora_txt, cod_janela = _hora_prevista(c, alvo, agora)
        camp["hora_prevista_txt"] = hora_txt

        intervalo = int(c.get("intervalo_dias") or 7)
        ignorar_intervalo = bool(c.get("ignorar_intervalo"))
        telefones_camp: set[str] = set()

        for posto in (c.get("postos") or []):
            passo += 1
            _progresso(passo * 100 // total_passos,
                       f"[{c['nome']}] lendo posto {posto}…")
            conn = get_conn_posto(posto)
            if not conn:
                camp["erros_postos"].append(
                    f"Posto {posto}: sem conexão com o banco — clientes deste "
                    "posto não entraram na análise.")
                continue
            cursor = conn.cursor()
            try:
                faturas = _buscar_faturas_data(cursor, c, alvo, delta)
            except Exception as e:
                camp["erros_postos"].append(
                    f"Posto {posto}: erro na consulta ({str(e)[:150]}).")
                cursor.close()
                conn.close()
                continue

            for f in faturas:
                f["_valor_fmt"] = engine.fmt_valor(f.get("valor"))
                f["_venc_fmt"] = engine.fmt_venc(f.get("venc"))
                linha = _linha_base(c, posto, f, delta)

                # 1) cadastro de teste (mesma regex do cron)
                nome_upper = str(f.get("nome") or "").strip().upper()
                if re.search(r"\bTESTE\b", nome_upper) or nome_upper.startswith("MATRICULA "):
                    linha.update(status="bloqueada", motivo_codigo="nome_de_teste",
                                 motivo="Cadastro de teste — o sistema nunca envia "
                                        "para nomes com 'TESTE'/'MATRICULA'.")
                    _conta(camp, linha)
                    continue

                # 2) telefone
                tel = engine.limpar_telefone(f.get("telefonewhatsapp"))
                if not tel:
                    bruto = str(f.get("telefonewhatsapp") or "").strip() or "(vazio)"
                    linha.update(status="bloqueada", motivo_codigo="sem_telefone_valido",
                                 motivo=f"O telefone do cadastro ({bruto}) não é um "
                                        "WhatsApp válido.")
                    _conta(camp, linha)
                    continue
                linha["telefone"] = tel

                # 2.5) já enviado HOJE por esta campanha (retrato real do dia)
                env_hoje = envios_camp_tel.get((c["id"], tel)) if eh_hoje else None
                if env_hoje:
                    linha.update(status="enviada", motivo_codigo="enviada",
                                 motivo=f"Enviada hoje às {_hora_de_iso(env_hoje['enviado_em'])} "
                                        "por esta campanha.",
                                 quando=f"enviada {_hora_de_iso(env_hoje['enviado_em'])}")
                    telefones_camp.add(tel)
                    _conta(camp, linha)
                    continue

                # 3) dedupe — mesmas duas políticas do cron
                if ignorar_intervalo:
                    if tel in telefones_camp:
                        linha.update(status="bloqueada", motivo_codigo="duplicado_na_campanha",
                                     motivo="Mesmo telefone de outra matrícula já "
                                            "contemplada nesta campanha — só a "
                                            "primeira recebe.")
                        _conta(camp, linha)
                        continue
                    if db.ja_enviado_na_campanha(c["id"], tel):
                        telefones_camp.add(tel)
                        linha.update(status="bloqueada", motivo_codigo="ja_enviado_campanha",
                                     motivo="Campanha de envio único: este contato "
                                            "já recebeu esta campanha antes.")
                        _conta(camp, linha)
                        continue
                else:
                    if tel in telefones_rodada:
                        dono = telefones_rodada[tel]
                        if dono.get("ja_real"):
                            motivo = (f"O mesmo telefone já recebeu HOJE pela campanha "
                                      f"'{dono['campanha']}' — no máximo 1 mensagem "
                                      "por telefone por dia/rodada.")
                        else:
                            motivo = (f"O mesmo telefone já será atendido pela campanha "
                                      f"'{dono['campanha']}', que roda antes na fila — "
                                      "no máximo 1 mensagem por telefone por rodada.")
                        linha.update(status="bloqueada",
                                     motivo_codigo="bloqueado_rodada_global",
                                     motivo=motivo)
                        _conta(camp, linha)
                        continue

                    ult = _ultimo_envio_info(tel)
                    if ult:
                        try:
                            dt_ult = datetime.fromisoformat(ult["enviado_em"]).date()
                        except Exception:
                            dt_ult = None
                        if dt_ult is not None:
                            dias_desde = (alvo - dt_ult).days
                            if dias_desde < intervalo:
                                volta = dt_ult + timedelta(days=intervalo)
                                quando_rec = ("HOJE" if dias_desde == 0
                                              else f"em {_fmt_data_br(dt_ult)} (há {dias_desde}d)")
                                linha.update(
                                    status="bloqueada",
                                    motivo_codigo="bloqueado_intervalo_global",
                                    motivo=(f"Recebeu mensagem {quando_rec} pela campanha "
                                            f"'{ult['campanha_nome']}'. Regra de silêncio de "
                                            f"{intervalo} dias — volta a poder receber em "
                                            f"{_fmt_data_br(volta)}."))
                                telefones_rodada[tel] = {"campanha": ult["campanha_nome"],
                                                         "ja_real": dias_desde == 0}
                                _conta(camp, linha)
                                continue

                # 4) elegível — vai enviar (ou janela já fechou hoje)
                params = engine.montar_params_template(c.get("template") or "", f)
                linha["params"] = params
                if cod_janela == "janela_encerrada_hoje":
                    linha.update(status="bloqueada", motivo_codigo="janela_encerrada_hoje",
                                 motivo=f"Elegível, mas a janela de envio de hoje "
                                        f"({camp['janela']}) já fechou antes de o robô "
                                        "chegar até este cliente. Se continuar elegível, "
                                        "entra no próximo dia permitido.")
                else:
                    linha.update(status="prevista", motivo_codigo="prevista",
                                 motivo="Passa em todas as regras — envio previsto "
                                        f"{hora_txt}.",
                                 quando=f"≈ {hora_txt}")
                if ignorar_intervalo:
                    telefones_camp.add(tel)
                else:
                    telefones_rodada.setdefault(
                        tel, {"campanha": c["nome"], "ja_real": False})
                _conta(camp, linha)

            cursor.close()
            conn.close()

        # Envios reais do dia desta campanha que NÃO apareceram na consulta
        # atual (ex.: cliente pagou depois de receber — a fatura saiu da view).
        if eh_hoje:
            vistos = {l.get("telefone") for l in camp["linhas"] if l.get("status") == "enviada"}
            for (cid, tel), env in envios_camp_tel.items():
                if cid == c["id"] and tel not in vistos:
                    li = _linha_de_envio(env)
                    li["motivo"] += (" Obs.: este cliente não aparece mais na "
                                     "consulta atual — a fatura pode ter sido paga "
                                     "depois do envio.")
                    _conta(camp, li)

        if camp["enviar_meta"] and camp["contagem"]["previstas"]:
            cat = categorias.get(c.get("template") or "")
            preco = PRECO_META_USD.get(cat)
            cat_txt = (cat or "").lower() or "?"
            if preco is None:
                # Sem categoria → assume o preço MAIOR (marketing): estimativa
                # conservadora é melhor que custo subestimado.
                preco = PRECO_META_USD["MARKETING"]
                cat_txt = "categoria desconhecida, assumido marketing"
            n = camp["contagem"]["previstas"]
            camp["custo_estimado_meta"] = round(n * preco * cot["usd_brl"], 2)
            camp["custo_detalhe"] = (
                f"{n} msgs × US$ {preco:g} ({cat_txt}) × "
                f"R$ {cot['usd_brl']:.4f}/US$ — {cot['fonte']}")

    # Erros de API do dia (nao_enviados com motivo erro%)
    err_por_camp: dict[int, list[dict]] = {}
    for e in erros_dia:
        err_por_camp.setdefault(e["campanha_id"], []).append(e)
    for camp in campanhas_out:
        for e in err_por_camp.get(camp["id"], []):
            _conta(camp, {
                "posto": e.get("posto"), "matricula": e.get("matricula"),
                "nome": e.get("nome"), "telefone": e.get("telefone_ok") or e.get("telefone_raw"),
                "ref": "", "valor": "", "venc": "", "dias_atraso": e.get("dias_atraso"),
                "status": "erro", "motivo_codigo": "erro_api",
                "motivo": "O sistema tentou enviar e a API devolveu erro "
                          f"(detalhe técnico: {e.get('motivo')}).",
                "quando": f"tentado {_hora_de_iso(e.get('rodada_em'))}",
                "por_que_entrou": "", "params": None,
            })

    resultado["custo_info"] = {
        "usd_brl": cot["usd_brl"],
        "fonte": cot["fonte"],
        "ao_vivo": cot["ao_vivo"],
        "precos_usd": PRECO_META_USD,
    }
    if not cot["ao_vivo"]:
        resultado["avisos"].append(
            "Cotação do dólar ao vivo indisponível — o custo estimado usa o "
            f"valor padrão do sistema (R$ {cot['usd_brl']:.2f}/US$).")
    resultado["campanhas"] = campanhas_out
    resultado["resumo"] = _totalizar(campanhas_out)
    return resultado


def _linha_base(c: dict, posto: str, f: dict, delta: int) -> dict:
    m = modo_envio(c)
    dias = (f.get("diasdebito") or 0) + (delta if m == MODO_ATRASO else 0)
    if m == MODO_ATRASO:
        entrou = (f"Fatura '{f.get('ref') or ''}' de R$ {f.get('_valor_fmt')} vencida "
                  f"há {dias} dias — dentro da regra da campanha.")
    elif m == MODO_PRE_VENCIMENTO:
        entrou = (f"Fatura '{f.get('ref') or ''}' de R$ {f.get('_valor_fmt')} com "
                  f"vencimento em {f.get('_venc_fmt')} — dentro da janela de aviso.")
    elif m == MODO_CLIENTE_NOVO:
        entrou = "Pagou a 1ª mensalidade nos últimos 7 dias — entra nas boas-vindas."
    else:
        entrou = f"Admissão em {f.get('ref') or '?'} — dentro do recorte da campanha."
    return {
        "posto": posto,
        "matricula": str(f.get("matricula") or ""),
        "nome": str(f.get("nome") or ""),
        "telefone": None,
        "ref": str(f.get("ref") or ""),
        "valor": f.get("_valor_fmt") or "",
        "venc": f.get("_venc_fmt") or "",
        "dias_atraso": dias,
        "status": None,
        "motivo_codigo": None,
        "motivo": None,
        "quando": None,
        "por_que_entrou": entrou,
        "params": None,
    }


def _linha_de_envio(env: dict) -> dict:
    return {
        "posto": env.get("posto"),
        "matricula": env.get("matricula"),
        "nome": env.get("nome"),
        "telefone": env.get("telefone"),
        "ref": env.get("ref"),
        "valor": env.get("valor"),
        "venc": env.get("venc"),
        "dias_atraso": env.get("dias_atraso"),
        "status": "enviada",
        "motivo_codigo": "enviada",
        "motivo": f"Enviada às {_hora_de_iso(env.get('enviado_em'))} "
                  f"(status da API: {env.get('status')}).",
        "quando": f"enviada {_hora_de_iso(env.get('enviado_em'))}",
        "por_que_entrou": "",
        "params": None,
    }


def _totalizar(campanhas: list[dict]) -> dict:
    tot = {"previstas": 0, "enviadas": 0, "bloqueadas": 0, "erros": 0,
           "total_analisadas": 0, "custo_estimado_meta": 0.0,
           "campanhas_ativas": 0, "campanhas_puladas": 0, "alertas": 0}
    for camp in campanhas:
        cont = camp["contagem"]
        for k in ("previstas", "enviadas", "bloqueadas", "erros"):
            tot[k] += cont[k]
        tot["total_analisadas"] += (cont["previstas"] + cont["enviadas"]
                                    + cont["bloqueadas"] + cont["erros"])
        if camp.get("custo_estimado_meta"):
            tot["custo_estimado_meta"] += camp["custo_estimado_meta"]
        if camp["status_campanha"] == "ok":
            tot["campanhas_ativas"] += 1
        else:
            tot["campanhas_puladas"] += 1
        tot["alertas"] += len(camp.get("erros_postos") or []) + cont["erros"]
    tot["custo_estimado_meta"] = round(tot["custo_estimado_meta"], 2)
    return tot


# ---------------------------------------------------------------------------
# AUDITORIA DE DATA PASSADA — só as tabelas de controle, sem SQL Server
# ---------------------------------------------------------------------------

def _auditar_passado(alvo: date) -> dict:
    resultado = {
        "data": alvo.isoformat(),
        "tipo": "passado",
        "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
        "agora": datetime.now().strftime("%H:%M"),
        "avisos": [
            f"Auditoria de {_fmt_data_br(alvo)}: mostra o que ficou REGISTRADO "
            "(envios feitos e recusas gravadas). Bloqueios de cadência — regra de "
            "silêncio e telefone repetido na rodada — não são gravados no banco "
            "por decisão de projeto (seriam milhares de linhas por dia), então "
            "não aparecem aqui.",
        ],
        "campanhas": [],
        "motivos_legenda": MOTIVOS_LEGENDA,
    }
    _progresso(30, "Lendo envios do dia…")
    with db.get_conn() as conn:
        envs = conn.execute(
            "SELECT e.*, c.nome AS campanha_nome FROM envios e "
            "JOIN campanhas c ON c.id = e.campanha_id "
            "WHERE date(e.enviado_em)=? ORDER BY e.enviado_em",
            (alvo.isoformat(),),
        ).fetchall()
        naos = conn.execute(
            "SELECT n.*, c.nome AS campanha_nome FROM nao_enviados n "
            "JOIN campanhas c ON c.id = n.campanha_id "
            "WHERE date(n.rodada_em)=? ORDER BY n.rodada_em",
            (alvo.isoformat(),),
        ).fetchall()

    _progresso(70, "Montando auditoria…")
    todas = {c["id"]: c for c in db.listar_campanhas()}
    camps: dict[int, dict] = {}

    def _camp(cid: int, nome_fallback: str) -> dict:
        if cid not in camps:
            base = todas.get(cid) or {"id": cid, "nome": nome_fallback, "postos": []}
            camps[cid] = _resumo_campanha_base(base, 0)
        return camps[cid]

    for r in envs:
        e = dict(r)
        camp = _camp(e["campanha_id"], e.get("campanha_nome") or f"Campanha {e['campanha_id']}")
        if str(e.get("status") or "").startswith("accepted") or e.get("status") == "dry_run":
            _conta(camp, _linha_de_envio(e))
        else:
            li = _linha_de_envio(e)
            li.update(status="erro", motivo_codigo="erro_api",
                      motivo=f"Tentativa de envio terminou em erro "
                             f"(detalhe técnico: {e.get('status')}).")
            _conta(camp, li)

    for r in naos:
        n = dict(r)
        camp = _camp(n["campanha_id"], n.get("campanha_nome") or f"Campanha {n['campanha_id']}")
        motivo_raw = str(n.get("motivo") or "")
        eh_erro = motivo_raw.startswith("erro")
        cod = "erro_api" if eh_erro else (
            motivo_raw if motivo_raw in MOTIVOS_LEGENDA else "outro")
        leiga = {
            "sem_telefone_valido": "O telefone do cadastro não era um WhatsApp válido.",
            "nome_de_teste": "Cadastro de teste — o sistema nunca envia para esses nomes.",
            "ja_enviado_campanha": "Campanha de envio único: o contato já havia recebido.",
            "janela_fechou_entre_fases": "A janela de horário fechou no meio do "
                                         "processamento do lote — o envio Meta foi abortado.",
        }.get(motivo_raw, None)
        _conta(camp, {
            "posto": n.get("posto"), "matricula": n.get("matricula"),
            "nome": n.get("nome"),
            "telefone": n.get("telefone_ok") or n.get("telefone_raw"),
            "ref": "", "valor": "", "venc": "", "dias_atraso": n.get("dias_atraso"),
            "status": "erro" if eh_erro else "bloqueada",
            "motivo_codigo": cod,
            "motivo": leiga or f"Não enviada (registro do robô: {motivo_raw}).",
            "quando": f"registrado {_hora_de_iso(n.get('rodada_em'))}",
            "por_que_entrou": "", "params": None,
        })

    resultado["campanhas"] = sorted(camps.values(), key=lambda x: x["id"])
    resultado["resumo"] = _totalizar(resultado["campanhas"])
    return resultado
