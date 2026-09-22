"""
wpp_diagnostico.py — POR QUE uma campanha WhatsApp não enviou nada num posto.

Nasceu em 2026-09-22: o gestor do posto D recebeu às 08:30 "Horrível —
campanha parada — pode ser robô travado", abriu o painel às 08:48 com tudo
verde e não entendeu nada. O que tinha acontecido: a campanha *Seja Bem
Vindo!* não tinha NENHUM cliente novo em D desde quinta (o robô rodou 7x e
achou 0), e o alerta disparou antes da rodada das 08:00 chegar às campanhas
do Couto (chega ~09:00). Nada estava quebrado — a mensagem é que era vaga.

Este módulo responde, em linguagem leiga e com os números na mão:
  1. o robô está rodando?               (robo_rodadas — batimento do cron)
  2. ele chegou a olhar ESTE posto?      (robo_rodada_posto)
  3. quantos clientes estavam nas condições da campanha e por que cada um
     não recebeu? (candidatos / sem_telefone / bloqueado / erro_api)
  4. quais são as condições, para o gestor conferir no contas a receber.

SOMENTE LEITURA: lê o SQLite de controle (envios + batimento) e, quando
precisa, faz UM COUNT no SQL Server do posto (fallback sem batimento e
detalhe "quantos estão nas condições mas sem telefone"). Nunca envia nada.

Usado por disparar_alarmes.py (mensagem do alarme wpp_campanha) e por
alarmes_routes.py (o "por quê" no monitor). Se a régua do status mudar em
disparar_alarmes.status_wpp_campanha, mudar DIAS_GATILHO aqui junto.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta

import wpp_cobranca_db as db

log = logging.getLogger(__name__)

# Régua: ≥ 5 dias sem envio accepted = Horrível (mesma de
# disparar_alarmes._dias_para_status e do painel monitorarrobos).
DIAS_GATILHO = 5
# Rodada do cron = */15 min. Robô "vivo" = última rodada terminou há menos
# que isso OU está em andamento há menos de 3h (a das 08:00 leva ~1h).
ROBO_MAX_MIN_SEM_RODADA = 45
ROBO_MAX_MIN_RODADA_ABERTA = 180

DIAS_NOMES = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
DIAS_NOMES_LONGO = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira",
                    "sexta-feira", "sábado", "domingo"]

MODO_ATRASO = "atraso"
MODO_PRE_VENCIMENTO = "pre_vencimento"
MODO_CLIENTES = "clientes_admissao"
MODO_CLIENTE_NOVO = "cliente_novo"
MODO_FALTA_MEDICO = "falta_medico"

# Classificação → gravidade + rótulo curto (usado no monitor e na central).
CLASSES = {
    "robo_parado":       ("critico", "Robô parado"),
    "posto_inacessivel": ("critico", "Robô não lê o banco do posto"),
    "falha_envio":       ("critico", "API do WhatsApp recusando"),
    "nao_processada":    ("critico", "Robô pulou esta campanha"),
    "sem_telefone":      ("atencao", "Clientes sem telefone de WhatsApp"),
    "verificar":         ("atencao", "Dados inconsistentes — verificar"),
    "sem_dados":         ("atencao", "Sem registro de rodadas ainda"),
    "sem_clientes":      ("normal",  "Ninguém nas condições — robô ok"),
    "bloqueado_regras":  ("normal",  "Todos já receberam outra mensagem — robô ok"),
    "fora_da_agenda":    ("normal",  "Campanha não envia nestes dias — robô ok"),
    "em_dia":            ("normal",  "Enviando normalmente"),
}


# ---------------------------------------------------------------------------
# Helpers de tempo / texto
# ---------------------------------------------------------------------------

def _dt(s: str | None) -> datetime | None:
    """ISO (com ou sem offset) → datetime naive local. Os valores gravados
    são hora local do servidor; o offset, quando existe, é só decoração."""
    if not s:
        return None
    try:
        d = datetime.fromisoformat(str(s).strip())
        return d.replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


def _dia_txt(d: date | datetime, com_ano: bool = False) -> str:
    fmt = "%d/%m/%Y" if com_ano else "%d/%m"
    return f"{DIAS_NOMES[d.weekday()]} {d.strftime(fmt)}"


def _quando_txt(dt: datetime | None, hoje: date) -> str:
    if not dt:
        return "nunca"
    if dt.date() == hoje:
        return f"hoje às {dt:%H:%M}"
    if dt.date() == hoje - timedelta(days=1):
        return f"ontem ({_dia_txt(dt)}) às {dt:%H:%M}"
    return f"{_dia_txt(dt)} às {dt:%H:%M}"


def _hm(s: str | None, default: str) -> str:
    s = str(s or "").strip()
    return s if len(s) == 5 and s[2] == ":" else default


def dias_permitidos(c: dict) -> set[int]:
    return {int(d) for d in str(c.get("dias_semana") or "0,1,2,3,4").split(",")
            if d.strip().isdigit() and 0 <= int(d) <= 6}


def _dias_txt(c: dict) -> str:
    ds = sorted(dias_permitidos(c))
    if ds == [0, 1, 2, 3, 4]:
        return "segunda a sexta"
    if ds == [0, 1, 2, 3, 4, 5, 6]:
        return "todos os dias"
    if ds == [0, 1, 2, 3, 4, 5]:
        return "segunda a sábado"
    return ", ".join(DIAS_NOMES[d] for d in ds) or "nenhum dia"


def _modo(c: dict) -> str:
    m = str(c.get("modo_envio") or MODO_ATRASO).strip().lower()
    return m if m in (MODO_ATRASO, MODO_PRE_VENCIMENTO, MODO_CLIENTES,
                      MODO_CLIENTE_NOVO, MODO_FALTA_MEDICO) else MODO_ATRASO


def _postos_de(c: dict) -> list[str]:
    p = c.get("postos")
    if isinstance(p, str):
        try:
            p = json.loads(p or "[]")
        except Exception:
            p = []
    return [str(x).upper() for x in (p or [])]


def _plural(n: int, um: str, muitos: str) -> str:
    return f"{n} {um if n == 1 else muitos}"


# ---------------------------------------------------------------------------
# Condições da campanha em linguagem leiga
# ---------------------------------------------------------------------------

def condicoes_campanha(c: dict) -> list[str]:
    """Lista, em português de balcão, quem recebe a mensagem desta campanha.
    É o que o gestor confere no contas a receber."""
    m = _modo(c)
    out: list[str] = []
    if m == MODO_ATRASO:
        mn = int(c.get("dias_atraso_min") or 1)
        mx = c.get("dias_atraso_max")
        if mx:
            out.append(f"Mensalidade em atraso de {mn} a {int(mx)} dias")
        else:
            out.append(f"Mensalidade em atraso há {mn} dias ou mais")
    elif m == MODO_PRE_VENCIMENTO:
        mn = int(c.get("dias_ref_min") or 0)
        mx = c.get("dias_ref_max")
        if mx is not None:
            out.append(f"Mensalidade em aberto que vence daqui a {mn} a {int(mx)} dias")
        else:
            out.append(f"Mensalidade em aberto que vence daqui a {mn} dias ou mais")
    elif m == MODO_CLIENTE_NOVO:
        out.append("Cliente novo que pagou a 1ª mensalidade nos últimos 7 dias "
                   "(mensalidade do mês da admissão, cliente ativo)")
    elif m == MODO_CLIENTES:
        ini = (c.get("adm_data_ini") or "?")[:10]
        fim = (c.get("adm_data_fim") or "?")[:10]
        out.append(f"Cliente admitido entre {ini} e {fim}")
        for campo, rotulo in (("tipo_cliente", "Tipo de cliente"),
                              ("titular_dependente", "Titular/dependente"),
                              ("situacao_cliente", "Situação"),
                              ("tipo_fj", "Pessoa física/jurídica"),
                              ("origem", "Origem")):
            if c.get(campo):
                out.append(f"{rotulo}: {c[campo]}")
        if c.get("pagador_atrasado"):
            out.append("Somente pagador em atraso")
    elif m == MODO_FALTA_MEDICO:
        out.append("Paciente com consulta marcada com médico que faltou (disparo manual, não é do robô)")

    if m in (MODO_ATRASO, MODO_PRE_VENCIMENTO):
        if not c.get("incluir_cancelados"):
            out.append("Cliente ativo (cancelado na ANS não entra)")
        if c.get("nao_recorrente"):
            out.append("Que NÃO paga por cartão recorrente")
        if c.get("sem_email"):
            out.append("Sem e-mail no cadastro")
        if c.get("sexo"):
            out.append(f"Sexo: {c['sexo']}")
        if c.get("idade_min") is not None or c.get("idade_max") is not None:
            out.append(f"Idade entre {c.get('idade_min') or 0} e {c.get('idade_max') or '…'} anos")
        for campo, rotulo in (("operadora", "Operadora"), ("cobrador", "Cobrador"),
                              ("corretor", "Corretor"), ("bairro", "Bairro"), ("rua", "Rua")):
            if c.get(campo):
                out.append(f"{rotulo}: {c[campo]}")
        if m == MODO_ATRASO:
            out.append("Mensalidade de plano (referência mês/ano), não pré-cadastro")

    out.append("Telefone de WhatsApp válido no cadastro")
    if c.get("ignorar_intervalo"):
        out.append("Recebe esta mensagem uma única vez")
    else:
        iv = int(c.get("intervalo_dias") or 7)
        out.append(f"Sem ter recebido nenhuma outra mensagem nossa nos últimos {iv} dias")
    return out


def janela_txt(c: dict) -> str:
    return (f"{_dias_txt(c)}, das {_hm(c.get('hora_inicio'), '08:00')} "
            f"às {_hm(c.get('hora_fim'), '20:00')}")


# ---------------------------------------------------------------------------
# Leituras do SQLite de controle
# ---------------------------------------------------------------------------

def campanhas_do_posto(posto: str) -> list[dict]:
    """Campanhas ATIVAS do robô (cron) que incluem o posto. falta_medico fica
    fora — dispara pelo /medico_falta, não pelo cron."""
    posto = str(posto).upper()
    out = []
    for c in db.listar_campanhas():
        if not c.get("ativa"):
            continue
        if _modo(c) == MODO_FALTA_MEDICO or str(c.get("modo_envio") or "").strip().lower() == MODO_FALTA_MEDICO:
            continue
        if posto in _postos_de(c):
            out.append(c)
    return out


def ultimo_envio(campanha_id: int, posto: str) -> datetime | None:
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(enviado_em) AS u FROM envios "
            "WHERE campanha_id=? AND posto=? AND status LIKE 'accepted%'",
            (int(campanha_id), str(posto).upper()),
        ).fetchone()
    return _dt(row["u"] if row else None)


def estado_robo(agora: datetime) -> dict:
    """Última rodada real (dry_run=0) do cron e se o robô está vivo."""
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT id, rodada_em, terminada_em, campanhas, total_enviado "
            "FROM robo_rodadas WHERE dry_run=0 ORDER BY rodada_em DESC LIMIT 1"
        ).fetchone()
        total = conn.execute("SELECT COUNT(*) FROM robo_rodadas").fetchone()[0]
    if not row:
        return {"tem_batimento": False, "vivo": None, "ultima_rodada": None,
                "ultima_rodada_fim": None, "em_andamento": False, "minutos": None,
                "total_rodadas": int(total or 0)}
    ini = _dt(row["rodada_em"])
    fim = _dt(row["terminada_em"])
    em_andamento = fim is None
    ref = fim or ini
    minutos = int((agora - ref).total_seconds() // 60) if ref else None
    if em_andamento:
        vivo = minutos is not None and minutos <= ROBO_MAX_MIN_RODADA_ABERTA
    else:
        vivo = minutos is not None and minutos <= ROBO_MAX_MIN_SEM_RODADA
    return {"tem_batimento": True, "vivo": bool(vivo),
            "ultima_rodada": ini.isoformat(timespec="minutes") if ini else None,
            "ultima_rodada_fim": fim.isoformat(timespec="minutes") if fim else None,
            "em_andamento": em_andamento, "minutos": minutos,
            "total_rodadas": int(total or 0)}


def batimento(campanha_id: int, posto: str, desde: datetime, agora: datetime) -> dict:
    """Agrega robo_rodada_posto de campanha×posto desde `desde`.

    Os contadores são MÁXIMOS por dia (e no período), não somas: o mesmo
    cliente reaparece em todas as 48 rodadas do dia até ser atendido —
    somar diria "1.900 clientes" onde havia 40."""
    posto = str(posto).upper()
    with db.get_conn() as conn:
        rows = conn.execute(
            """SELECT p.rodada_em, p.processado_em, p.resultado, p.erro,
                      p.candidatos, p.enviados, p.sem_telefone, p.nome_teste,
                      p.bloq_intervalo, p.bloq_rodada, p.ja_enviado, p.erro_api, p.outros
               FROM robo_rodada_posto p
               JOIN robo_rodadas r ON r.id = p.rodada_id
               WHERE p.campanha_id=? AND p.posto=? AND r.dry_run=0 AND p.rodada_em >= ?
               ORDER BY p.rodada_em""",
            (int(campanha_id), posto, desde.isoformat(timespec="seconds")),
        ).fetchall()
        # Outras campanhas processadas para o posto no período — distingue
        # "tabela nova/vazia" de "o robô pulou só esta campanha".
        outras = conn.execute(
            """SELECT COUNT(*) FROM robo_rodada_posto p JOIN robo_rodadas r ON r.id=p.rodada_id
               WHERE p.posto=? AND p.campanha_id<>? AND r.dry_run=0 AND p.rodada_em >= ?""",
            (posto, int(campanha_id), desde.isoformat(timespec="seconds")),
        ).fetchone()[0]

    cont_keys = ("candidatos", "enviados", "sem_telefone", "nome_teste",
                 "bloq_intervalo", "bloq_rodada", "ja_enviado", "erro_api", "outros")
    por_dia: dict[str, dict] = {}
    resultados: dict[str, int] = {}
    ultimo_erro = None
    soma_enviados = 0
    soma_erro_api = 0
    for r in rows:
        dia = str(r["rodada_em"])[:10]
        d = por_dia.setdefault(dia, {"dia": dia, "rodadas": 0, "primeira": None, "ultima": None,
                                     **{k: 0 for k in cont_keys}})
        d["rodadas"] += 1
        pe = str(r["processado_em"])[11:16]
        d["primeira"] = pe if d["primeira"] is None else d["primeira"]
        d["ultima"] = pe
        for k in cont_keys:
            d[k] = max(d[k], int(r[k] or 0))
        resultados[r["resultado"]] = resultados.get(r["resultado"], 0) + 1
        if r["erro"]:
            ultimo_erro = str(r["erro"])[:160]
        soma_enviados += int(r["enviados"] or 0)
        soma_erro_api += int(r["erro_api"] or 0)

    maximos = {k: max([d[k] for d in por_dia.values()] or [0]) for k in cont_keys}
    hoje = agora.date().isoformat()
    d_hoje = por_dia.get(hoje)
    return {
        "desde": desde.isoformat(timespec="minutes"),
        "rodadas": len(rows),
        "dias_com_rodada": len(por_dia),
        "dias_com_candidatos": sum(1 for d in por_dia.values() if d["candidatos"] > 0),
        "maximos": maximos,
        "enviados_total": soma_enviados,
        "erro_api_total": soma_erro_api,
        "resultados": resultados,
        "ultimo_erro": ultimo_erro,
        "por_dia": sorted(por_dia.values(), key=lambda d: d["dia"]),
        "hoje": {"rodadas": d_hoje["rodadas"], "primeira": d_hoje["primeira"],
                 "ultima": d_hoje["ultima"], "candidatos": d_hoje["candidatos"]} if d_hoje else None,
        "outras_campanhas_no_posto": int(outras or 0),
    }


def _primeira_passada_recente(campanha_id: int, posto: str, hoje: date) -> str | None:
    """Hora (HH:MM) em que a rodada chegou a esta campanha×posto no dia mais
    recente anterior a hoje — para dizer 'costuma chegar por volta das…'."""
    with db.get_conn() as conn:
        row = conn.execute(
            """SELECT MIN(p.processado_em) AS pe FROM robo_rodada_posto p
               JOIN robo_rodadas r ON r.id=p.rodada_id
               WHERE p.campanha_id=? AND p.posto=? AND r.dry_run=0
                 AND substr(p.rodada_em,1,10) = (
                     SELECT MAX(substr(p2.rodada_em,1,10)) FROM robo_rodada_posto p2
                     WHERE p2.campanha_id=? AND p2.posto=? AND substr(p2.rodada_em,1,10) < ?)""",
            (int(campanha_id), str(posto).upper(), int(campanha_id), str(posto).upper(),
             hoje.isoformat()),
        ).fetchone()
    return str(row["pe"])[11:16] if row and row["pe"] else None


# ---------------------------------------------------------------------------
# COUNT ao vivo no SQL Server do posto (fallback + detalhe "sem telefone")
# ---------------------------------------------------------------------------

_FILTRO_TEL = "telefonewhatsapp IS NOT NULL AND telefonewhatsapp <> '' AND "


def contar_candidatos_agora(c: dict, posto: str, timeout_s: int = 30) -> dict | None:
    """Quantos clientes do posto estão AGORA nas condições da campanha, com e
    sem telefone de WhatsApp. UM COUNT por chamada, só SELECT. Devolve None
    se o posto não responder (a mensagem diz isso, não inventa zero)."""
    try:
        from wpp_cobranca_sql import (build_where, source_sql, where_extras,
                                      get_query_cliente_novo, get_conn_posto)
    except Exception as e:  # pyodbc ausente etc.
        log.warning("contar_candidatos_agora: sem wpp_cobranca_sql (%s)", e)
        return None
    m = _modo(c)
    conn = None
    try:
        conn = get_conn_posto(str(posto).upper())
        if not conn:
            return {"erro": "sem conexão com o banco do posto"}
        try:
            conn.timeout = int(timeout_s)
        except Exception:
            pass
        cur = conn.cursor()
        if m == MODO_CLIENTE_NOVO:
            sql, params = get_query_cliente_novo()
            base = sql.replace("ORDER BY r.idCliente", "").strip()
            cur.execute(f"SELECT COUNT(*) FROM ({base}) t", params)
            total = int(cur.fetchone()[0] or 0)
            cur.execute(f"SELECT COUNT(*) FROM ({base}) t "
                        "WHERE t.telefonewhatsapp IS NOT NULL AND t.telefonewhatsapp <> ''", params)
            com_tel = int(cur.fetchone()[0] or 0)
        else:
            where, params = build_where(c)
            src = source_sql(c)
            extra = where_extras(c)
            cur.execute(f"SELECT COUNT(*) FROM {src} WHERE {where}{extra}", params)
            com_tel = int(cur.fetchone()[0] or 0)
            if where.startswith(_FILTRO_TEL):
                where_sem = where[len(_FILTRO_TEL):]
                cur.execute(f"SELECT COUNT(*) FROM {src} WHERE {where_sem}{extra}", params)
                total = int(cur.fetchone()[0] or 0)
            else:
                total = com_tel
        cur.close()
        return {"com_telefone": com_tel, "sem_telefone": max(0, total - com_tel), "total": total}
    except Exception as e:
        log.warning("contar_candidatos_agora(%s/%s): %s", c.get("id"), posto, e)
        return {"erro": str(e)[:160]}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Diagnóstico de UMA campanha × posto
# ---------------------------------------------------------------------------

def dias_de_envio_perdidos(c: dict, ultimo: datetime | None, agora: datetime,
                           limite: int = 14) -> list[str]:
    """Dias em que a campanha PODIA enviar e não enviou, do dia seguinte ao
    último envio até hoje. Hoje entra só se a janela já abriu."""
    permitidos = dias_permitidos(c)
    hoje = agora.date()
    ini = (ultimo.date() + timedelta(days=1)) if ultimo else hoje - timedelta(days=limite)
    ini = max(ini, hoje - timedelta(days=limite))
    out = []
    d = ini
    h_ini = _hm(c.get("hora_inicio"), "08:00")
    while d <= hoje:
        if d.weekday() in permitidos:
            if d == hoje:
                if agora.strftime("%H:%M") >= h_ini:
                    out.append("hoje (até agora)")
            else:
                out.append(_dia_txt(d))
        d += timedelta(days=1)
    return out


def diagnosticar(c: dict, posto: str, agora: datetime | None = None,
                 dias: int = DIAS_GATILHO, consultar_sql: bool = True) -> dict:
    agora = agora or datetime.now()
    hoje = agora.date()
    posto = str(posto).upper()
    ult = ultimo_envio(c["id"], posto)
    dias_sem = (hoje - ult.date()).days if ult else 999
    parada = dias_sem >= dias
    perdidos = dias_de_envio_perdidos(c, ult, agora)
    envia_hoje = hoje.weekday() in dias_permitidos(c)
    desde = agora - timedelta(days=dias)
    robo = estado_robo(agora)
    bat = batimento(c["id"], posto, desde, agora)
    mx = bat["maximos"]

    diag = {
        "campanha_id": c["id"], "campanha_nome": c.get("nome"), "modo": _modo(c),
        "posto": posto,
        "ultimo_envio": ult.isoformat(timespec="minutes") if ult else None,
        "ultimo_envio_txt": _quando_txt(ult, hoje),
        "dias_sem_envio": dias_sem, "parada": parada,
        "dias_envio_perdidos": perdidos, "envia_hoje": envia_hoje,
        "envia_fim_de_semana": bool({5, 6} & dias_permitidos(c)),
        "janela": janela_txt(c), "condicoes": condicoes_campanha(c),
        "robo": robo, "batimento": bat, "agora_sql": None,
        "hoje_pendente": False, "hoje_previsao": None,
    }

    if not parada:
        diag.update(classificacao="em_dia", gravidade="normal",
                    resumo=f"Enviando normalmente — último envio {diag['ultimo_envio_txt']}.",
                    acao="Nada a fazer.")
        return diag

    # A rodada de hoje ainda não chegou a esta campanha×posto?
    if envia_hoje and robo.get("vivo") and robo.get("em_andamento") and not bat["hoje"] \
            and agora.strftime("%H:%M") >= _hm(c.get("hora_inicio"), "08:00"):
        diag["hoje_pendente"] = True
        diag["hoje_previsao"] = _primeira_passada_recente(c["id"], posto, hoje)

    # ── Classificação ─────────────────────────────────────────────────
    elegiveis_max = max(0, mx["candidatos"] - mx["sem_telefone"] - mx["nome_teste"])
    bloqueados_max = mx["bloq_intervalo"] + mx["bloq_rodada"] + mx["ja_enviado"]
    so_erros_posto = bat["rodadas"] > 0 and all(
        k in ("sem_conexao", "erro_query") for k in bat["resultados"])

    if not robo["tem_batimento"] or (bat["rodadas"] == 0 and bat["outras_campanhas_no_posto"] == 0
                                     and not robo["vivo"] and robo["total_rodadas"] == 0):
        cls = "sem_dados"
    elif robo["vivo"] is False:
        cls = "robo_parado"
    elif bat["rodadas"] == 0:
        if not perdidos:
            cls = "fora_da_agenda"
        elif bat["outras_campanhas_no_posto"] == 0:
            cls = "sem_dados"
        else:
            cls = "nao_processada"
    elif so_erros_posto:
        cls = "posto_inacessivel"
    elif bat["erro_api_total"] > 0 and bat["enviados_total"] == 0:
        cls = "falha_envio"
    elif mx["candidatos"] == 0:
        cls = "sem_clientes"
    elif elegiveis_max == 0:
        cls = "sem_telefone"
    elif bat["enviados_total"] == 0 and bloqueados_max >= elegiveis_max:
        cls = "bloqueado_regras"
    else:
        cls = "verificar"

    # COUNT ao vivo: sem batimento (única fonte) ou para dizer quantos estão
    # nas condições mas sem telefone (a query do robô já exclui esses).
    if consultar_sql and cls in ("sem_dados", "sem_clientes", "robo_parado", "nao_processada"):
        diag["agora_sql"] = contar_candidatos_agora(c, posto)

    grav, _rot = CLASSES[cls]
    diag.update(classificacao=cls, gravidade=grav, rotulo=_rot)
    diag["resumo"], diag["acao"] = _resumo_e_acao(diag)
    return diag


def _resumo_e_acao(d: dict) -> tuple[str, str]:
    """Frases leigas: o que o robô encontrou e o que o gestor faz."""
    cls = d["classificacao"]
    bat = d["batimento"]
    mx = bat["maximos"]
    robo = d["robo"]
    posto = d["posto"]
    n_rod = bat["rodadas"]
    ag = d.get("agora_sql") or {}
    aviso_tel = ""
    if ag.get("sem_telefone"):
        aviso_tel = (f" Neste momento há {_plural(ag['sem_telefone'], 'cliente', 'clientes')} do posto "
                     f"{posto} nas condições mas SEM telefone de WhatsApp no cadastro — o robô não "
                     f"enxerga quem não tem WhatsApp.")

    if cls == "robo_parado":
        ult = _dt(robo.get("ultima_rodada_fim") or robo.get("ultima_rodada"))
        quando = _quando_txt(ult, datetime.now().date()) if ult else "não há registro"
        fila = (f" Neste momento há {_plural(ag['com_telefone'], 'cliente', 'clientes')} do posto "
                f"{posto} aguardando esta mensagem." if ag.get("com_telefone") else "")
        return (f"🚨 O robô de WhatsApp NÃO está rodando. A última rodada foi {quando} — desde então "
                f"nenhuma passada, para nenhum posto. É defeito de sistema, não do seu contas a receber.{fila}",
                "Acione o TI AGORA e peça para verificar o robô de WhatsApp (cron sync_wpp na VM).")

    if cls == "posto_inacessivel":
        erro = bat.get("ultimo_erro") or "sem conexão"
        return (f"🚨 O robô está rodando, mas não consegue ler o banco do posto {posto}: nas últimas "
                f"{n_rod} passadas todas falharam ({erro}). Os outros postos seguem normais.",
                f"Acione o TI: servidor ou banco do posto {posto} fora do ar, ou senha alterada.")

    if cls == "falha_envio":
        erro_txt = f" Último erro: {bat['ultimo_erro']}." if bat.get("ultimo_erro") else ""
        return (f"🚨 O robô encontrou até {_plural(mx['candidatos'], 'cliente', 'clientes')} nas condições e "
                f"tentou enviar, mas a API do WhatsApp recusou TODAS as tentativas "
                f"({bat['erro_api_total']} tentativas em {n_rod} passadas nos últimos {DIAS_GATILHO} dias, "
                f"0 enviadas).{erro_txt}",
                "Acione o TI para verificar a conta Meta/WhatsApp do número de saída desta campanha.")

    if cls == "nao_processada":
        return (f"🚨 O robô rodou e processou as outras campanhas do posto {posto}, mas NÃO passou por esta "
                f"nos últimos {DIAS_GATILHO} dias. Pode ser configuração da campanha (dias/horário) ou defeito.",
                "Acione o TI e peça para conferir a configuração desta campanha no painel de campanhas.")

    if cls == "sem_telefone":
        return (f"🤖 O robô está funcionando. Encontrou até {_plural(mx['candidatos'], 'cliente', 'clientes')} do "
                f"posto {posto} nas condições por passada, mas NENHUM tem telefone de WhatsApp válido no "
                f"cadastro — todos foram pulados. Não é defeito do robô.",
                "Corrigir o telefone de WhatsApp no cadastro desses clientes. A lista está no painel de "
                "campanhas → esta campanha → Não enviados.")

    if cls == "bloqueado_regras":
        return (f"🤖 O robô está funcionando. Os {_plural(mx['candidatos'], 'cliente', 'clientes')} do posto "
                f"{posto} nas condições já receberam outra mensagem nossa há menos de 7 dias (regra de "
                f"silêncio: 1 mensagem por telefone a cada 7 dias) ou já receberam esta campanha.",
                "Nada a fazer — eles voltam a receber quando completar o intervalo.")

    if cls == "fora_da_agenda":
        return (f"🤖 O robô está funcionando. Esta campanha não envia nestes dias ({d['janela']}); o último "
                f"dia de envio possível foi o do último envio.",
                "Nada a fazer.")

    if cls == "sem_clientes":
        dias_txt = _plural(bat["dias_com_rodada"], "dia", "dias")
        return (f"🤖 O robô está funcionando. Rodou {_plural(n_rod, 'vez', 'vezes')} para esta campanha no posto {posto} nos "
                f"últimos {DIAS_GATILHO} dias ({dias_txt} com rodada) e não encontrou NENHUM cliente nas "
                f"condições abaixo. Não há defeito para acionar o TI.{aviso_tel}",
                f"Confira no contas a receber se realmente não houve cliente do posto {posto} nas condições "
                f"desde {d['ultimo_envio_txt'].replace('às', 'às')}. Se houve, o robô não o viu: veja se o "
                f"lançamento está no sistema e se o telefone de WhatsApp está no cadastro.")

    if cls == "sem_dados":
        base = ("ℹ️ Ainda não há registro das rodadas do robô para este período (recurso novo em "
                "2026-09-22) — não dá para afirmar se ele rodou ou não.")
        if ag.get("erro"):
            return (f"{base} A consulta ao vivo ao banco do posto também falhou ({ag['erro']}).",
                    "Acione o TI: sem batimento do robô e sem acesso ao banco do posto, não há como "
                    "saber se as mensagens estão saindo.")
        if ag:
            n = int(ag.get("com_telefone") or 0)
            sem = int(ag.get("sem_telefone") or 0)
            if n == 0:
                return (f"{base} Consultei agora o banco do posto {posto}: ZERO clientes nas condições"
                        + (f" ({sem} nas condições mas sem telefone de WhatsApp)" if sem else "")
                        + " — é normal não ter enviado.",
                        f"Nada a acionar. Confira no contas a receber se realmente não houve cliente nas "
                        f"condições desde {d['ultimo_envio_txt']}." +
                        (" Os sem telefone só recebem depois de corrigir o cadastro." if sem else ""))
            return (f"{base} Consultei agora o banco do posto {posto}: {_plural(n, 'cliente', 'clientes')} "
                    f"nas condições aguardando esta mensagem" + (f" (+{sem} sem telefone)" if sem else "") + ".",
                    "Acione o TI para verificar se o robô de WhatsApp está rodando — há cliente esperando.")
        return (base, "Acione o TI para conferir se o robô de WhatsApp está rodando.")

    # verificar
    return (f"⚠️ Dados inconsistentes: o robô registrou {bat['enviados_total']} envio(s) em {n_rod} passadas, "
            f"mas o histórico de envios desta campanha no posto {posto} não mostra nada há "
            f"{d['dias_sem_envio']} dias.",
            "Acione o TI para conferir o registro de envios desta campanha.")


# ---------------------------------------------------------------------------
# Diagnóstico do POSTO (o que o alarme wpp_campanha manda)
# ---------------------------------------------------------------------------

def diagnostico_posto(posto: str, agora: datetime | None = None,
                      dias: int = DIAS_GATILHO, consultar_sql: bool = True) -> dict:
    agora = agora or datetime.now()
    hoje = agora.date()
    posto = str(posto).upper()
    camps = campanhas_do_posto(posto)
    paradas, em_dia = [], []
    for c in camps:
        ult = ultimo_envio(c["id"], posto)
        dias_sem = (hoje - ult.date()).days if ult else 999
        if dias_sem >= dias:
            paradas.append(diagnosticar(c, posto, agora, dias, consultar_sql))
        else:
            em_dia.append({"campanha_id": c["id"], "campanha_nome": c.get("nome"),
                           "ultimo_envio_txt": _quando_txt(ult, hoje), "dias_sem_envio": dias_sem})
    pior = max([p["dias_sem_envio"] for p in paradas] + [e["dias_sem_envio"] for e in em_dia] + [0])
    grav_ordem = {"critico": 0, "atencao": 1, "normal": 2}
    gravidade = min((p["gravidade"] for p in paradas), key=lambda g: grav_ordem[g], default="normal")
    return {
        "posto": posto, "gerado_em": agora.isoformat(timespec="minutes"),
        "campanhas": [p["campanha_id"] for p in paradas],
        "paradas": paradas, "em_dia": em_dia,
        "pior_dias": pior, "gravidade": gravidade,
        "robo": estado_robo(agora),
        "aguardar_rodada": any(p.get("hoje_pendente") for p in paradas),
    }


# ---------------------------------------------------------------------------
# Renderização — WhatsApp (texto) e e-mail (HTML)
# ---------------------------------------------------------------------------

def _saudacao(nome: str | None, posto: str) -> str:
    n = (nome or "").strip()
    return n.split()[0].capitalize() if n else f"Gestor(a) do posto {posto}"


def _blocos(dp: dict, nome_gerente: str | None) -> list[dict]:
    """Estrutura comum às duas renderizações. Cada bloco: {titulo, linhas,
    lista}. Frases curtas; números com o que significam."""
    posto = dp["posto"]
    blocos: list[dict] = []
    quem = _saudacao(nome_gerente, posto)

    for i, d in enumerate(dp["paradas"]):
        nome_c = d["campanha_nome"] or f"campanha {d['campanha_id']}"
        dias = d["dias_sem_envio"]
        if d["ultimo_envio"]:
            abertura = (f"{quem if i == 0 else 'Também'}, a campanha *{nome_c}* não envia mensagem no posto "
                        f"{posto} há *{_plural(dias, 'dia', 'dias')}*. O último envio foi "
                        f"{d['ultimo_envio_txt']}. No painel isso aparece como *Horrível*.")
        else:
            abertura = (f"{quem if i == 0 else 'Também'}, a campanha *{nome_c}* NUNCA enviou mensagem no posto "
                        f"{posto}. No painel isso aparece como *Horrível*.")
        linhas = [abertura]
        if d["dias_envio_perdidos"]:
            dp_ = d["dias_envio_perdidos"]
            lista = dp_[0] if len(dp_) == 1 else ", ".join(dp_[:-1]) + " e " + dp_[-1]
            fds = "" if d["envia_fim_de_semana"] else " A campanha não envia sábado e domingo."
            linhas.append(f"Dias em que podia enviar e não enviou: {lista}.{fds}")
        if d.get("hoje_pendente"):
            prev = f" — ontem chegou às {d['hoje_previsao']}" if d.get("hoje_previsao") else ""
            linhas.append(f"⏳ Hoje a rodada começou às {str(d['robo'].get('ultima_rodada') or '')[11:16]} "
                          f"e ainda não chegou a esta campanha{prev}. Este aviso considera os dias anteriores.")
        blocos.append({"titulo": None, "linhas": linhas})

        blocos.append({"titulo": "O que o robô encontrou", "linhas": [d["resumo"]]})
        blocos.append({"titulo": "Quem recebe esta mensagem", "lista": d["condicoes"],
                       "linhas_depois": [f"Envio: {d['janela']}."]})
        blocos.append({"titulo": "O que fazer", "linhas": [d["acao"]]})

    if dp["em_dia"]:
        blocos.append({"titulo": f"As outras campanhas do posto {posto} estão em dia",
                       "lista": [f"{e['campanha_nome']} — {e['ultimo_envio_txt']}" for e in dp["em_dia"]]})
    return blocos


def texto_whatsapp(dp: dict, nome_gerente: str | None, posto_nome: str, app_url: str,
                   papel: str = "gerente", nome_destinatario: str | None = None) -> str:
    """Mensagem de WhatsApp. `papel` ≠ gerente → cópia com cabeçalho dizendo
    a quem o aviso original foi. Sem o link de ciência — quem chama anexa."""
    posto = dp["posto"]
    agora = _dt(dp.get("gerado_em")) or datetime.now()
    out = [f"⚠️ ALERTA CAMIM — Posto {posto} ({posto_nome})",
           f"{DIAS_NOMES_LONGO[agora.weekday()]}, {agora:%d/%m/%Y} às {agora:%H:%M}"]
    if papel != "gerente":
        rot = {"diretor": "DIRETORIA", "auditor": "AUDITORIA", "extra": "CÓPIA"}.get(papel, papel.upper())
        ger = (nome_gerente or "").strip() or f"gestor(a) do posto {posto}"
        out.append(f"[{rot}] Cópia do aviso enviado a {ger}.")
    out.append("")
    for b in _blocos(dp, nome_gerente):
        if b.get("titulo"):
            # "O que o robô encontrou" não ganha emoji: o resumo já começa com
            # o dele (🚨 defeito · 🤖 robô ok · ℹ️ sem dados · ⚠️ verificar).
            emoji = {"O que o robô encontrou": "", "Quem recebe esta mensagem": "📋 ",
                     "O que fazer": "👉 "}.get(b["titulo"], "✅ ")
            out.append(f"{emoji}*{b['titulo']}:*")
        for l in b.get("linhas") or []:
            out.append(l)
        for item in b.get("lista") or []:
            out.append(f"• {item}")
        for l in b.get("linhas_depois") or []:
            out.append(l)
        out.append("")
    out.append(f"🔎 Painel: {app_url}/monitorarrobos.html?posto={posto}")
    return "\n".join(out).strip()


def html_email(dp: dict, nome_gerente: str | None, posto_nome: str, app_url: str,
               titulo_alarme: str, papel: str = "gerente") -> str:
    import html as _h
    posto = dp["posto"]
    agora = _dt(dp.get("gerado_em")) or datetime.now()
    cor = {"critico": "#c62828", "atencao": "#ef6c00", "normal": "#2e7d32"}[dp["gravidade"]]
    cab = {"critico": "Defeito — acionar o TI", "atencao": "Atenção — cadastro/dados",
           "normal": "Robô funcionando — sem clientes nas condições"}[dp["gravidade"]]

    def _fmt(s: str) -> str:
        # *negrito* do WhatsApp → <b>
        import re
        s = _h.escape(s)
        return re.sub(r"\*([^*]+)\*", r"<b>\1</b>", s)

    partes = []
    if papel != "gerente":
        ger = _h.escape((nome_gerente or "").strip() or f"gestor(a) do posto {posto}")
        partes.append(f'<p style="color:#666;font-size:13px;margin:0 0 14px">Cópia do aviso enviado a '
                      f'<b>{ger}</b>.</p>')
    for b in _blocos(dp, nome_gerente):
        if b.get("titulo"):
            partes.append(f'<h3 style="font-size:15px;margin:18px 0 6px">{_h.escape(b["titulo"])}</h3>')
        for l in b.get("linhas") or []:
            partes.append(f'<p style="margin:0 0 8px;line-height:1.45">{_fmt(l)}</p>')
        if b.get("lista"):
            partes.append('<ul style="margin:0 0 8px 18px;padding:0;line-height:1.5">' +
                          "".join(f"<li>{_fmt(x)}</li>" for x in b["lista"]) + "</ul>")
        for l in b.get("linhas_depois") or []:
            partes.append(f'<p style="margin:0 0 8px;color:#555">{_fmt(l)}</p>')
    corpo = "\n".join(partes)
    return f"""<!doctype html>
<html lang="pt-br"><body style="margin:0;padding:0;background:#f4f4f4;font-family:sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px"><tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.10)">
  <tr><td style="background:{cor};padding:18px 32px">
    <div style="color:#fff;font-size:12px;opacity:.9">{_h.escape(cab)}</div>
    <h2 style="margin:4px 0 0;color:#fff;font-size:19px">&#9888; Posto {posto} &mdash; {_h.escape(posto_nome)}</h2>
    <div style="color:#fff;font-size:12px;opacity:.9;margin-top:4px">{_h.escape(titulo_alarme)} &middot;
      {DIAS_NOMES_LONGO[agora.weekday()]}, {agora:%d/%m/%Y} &agrave;s {agora:%H:%M}</div>
  </td></tr>
  <tr><td style="padding:22px 32px;font-size:14px;color:#222">
    {corpo}
    <p style="margin:22px 0 0">
      <a href="{app_url}/monitorarrobos.html?posto={posto}"
         style="background:#1565c0;color:#fff;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:600;display:inline-block">
        Abrir o painel Monitorar Rob&ocirc;s</a>
    </p>
  </td></tr>
  <tr><td style="background:#f8f8f8;padding:12px 32px;border-top:1px solid #eee">
    <p style="margin:0;color:#aaa;font-size:11px">CAMIM &mdash; Sistema de Alertas Operacionais</p>
  </td></tr>
</table></td></tr></table></body></html>"""


def resumo_curto(dp: dict) -> str:
    """Uma frase para o monitor/central: por que o alerta saiu."""
    if not dp.get("paradas"):
        return "Nenhuma campanha parada no momento."
    partes = []
    for d in dp["paradas"]:
        rot = CLASSES.get(d["classificacao"], ("", d["classificacao"]))[1]
        partes.append(f"{d['campanha_nome']}: {_plural(d['dias_sem_envio'], 'dia', 'dias')} sem envio — {rot}")
    return " · ".join(partes)


def compactar(dp: dict) -> dict:
    """Versão para guardar em disparo.detalhes (sem listas longas)."""
    out = {k: dp[k] for k in ("posto", "gerado_em", "campanhas", "pior_dias", "gravidade",
                              "aguardar_rodada") if k in dp}
    out["robo"] = dp.get("robo")
    out["em_dia"] = dp.get("em_dia")
    out["paradas"] = []
    for d in dp.get("paradas") or []:
        c = {k: d.get(k) for k in ("campanha_id", "campanha_nome", "modo", "ultimo_envio",
                                   "ultimo_envio_txt", "dias_sem_envio", "dias_envio_perdidos",
                                   "janela", "condicoes", "classificacao", "gravidade", "rotulo",
                                   "resumo", "acao", "hoje_pendente", "hoje_previsao", "agora_sql")}
        b = d.get("batimento") or {}
        c["batimento"] = {k: b.get(k) for k in ("rodadas", "dias_com_rodada", "dias_com_candidatos",
                                                "maximos", "enviados_total", "erro_api_total",
                                                "resultados", "ultimo_erro", "hoje")}
        out["paradas"].append(c)
    out["resumo_curto"] = resumo_curto(dp)
    return out


if __name__ == "__main__":  # uso manual: python wpp_diagnostico.py D [--sem-sql]
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    posto_arg = (sys.argv[1] if len(sys.argv) > 1 else "D").upper()
    dp = diagnostico_posto(posto_arg, consultar_sql="--sem-sql" not in sys.argv)
    print(texto_whatsapp(dp, None, posto_arg, "https://kpi.camim.com.br"))
    print("\n--- resumo:", resumo_curto(dp))
    print(json.dumps(compactar(dp), ensure_ascii=False, indent=1)[:4000])
