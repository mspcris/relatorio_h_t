"""
cancelados_robo_routes.py — Lista operacional: quem o robô de pré-agendamento cancelou.

Uso: a recepção abre a página e vê, por dia (ou período) de consulta, quem foi
cancelado automaticamente por não confirmar na janela 5-2 dias. Essas pessoas
normalmente NÃO sabem que foram canceladas e aparecem no balcão no horário marcado.

Por que consulta ao vivo e não o preagendamento.json:
  - o robô cancela a cada ~20 min; o ETL roda 1x/dia às 02:30 → lista sempre velha
  - o JSON tem ~190 MB; carregar isso no balcão pra ver 30 nomes não faz sentido
  - aqui a query é estreita (só Desistencia=1 do robô, período curto) → ~3-4s por posto

Acesso (desde 2026-08-31, pedido do Petterson/Campo Grande via Cristiano):
  - a LEITURA mostra a rede inteira para qualquer usuário logado — o ACL de
    postos do usuário não limita mais o que ele vê aqui;
  - a MARCAÇÃO de "tratado" continua exigindo o posto no ACL: quem trata o
    cancelamento é a recepção do posto, não quem só acompanha o número.

Identificação do robô: assinatura textual em Cad_LancamentoServico.MotivoDesistencia,
  "Consulta pré agendada não foi confirmada pelo cliente e foi cancelada
   automaticamente em <data>"

ATENÇÃO — existe um SEGUNDO cancelamento automático no CAMIM, com texto
  "CANCELAMENTO AUTOMÁTICO FEITO PELO SISTEMA. NÃO FOI ENCONTRADO O REGISTRO DE
   PAGAMENTO DA SESSÃO", que NÃO é o robô de pré-agendamento. Filtrar por
  "automátic" pegaria os dois juntos. Por isso o LIKE exige "confirmada pelo
  cliente" — é o que separa um do outro.
"""
import logging
import os
import re
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

from flask import Blueprint, jsonify, request

from medico_novo_routes import _check_admin, _conn_for_posto, _require_posto_in_acl

logger = logging.getLogger(__name__)

cancelados_robo_bp = Blueprint("cancelados_robo_bp", __name__)

# Assinatura do robô de pré-agendamento. Ver docstring do módulo sobre o
# porquê de "confirmada pelo cliente" ser obrigatório no padrão.
ROBO_LIKE = "%confirmada pelo cliente%cancelada automaticamente%"

# A view é pesada; sem cache um F5 no balcão bate em todos os postos de novo.
_CACHE_TTL = 60
_cache: dict = {}
_cache_lock = threading.Lock()

# Postos consultados = TODOS os configurados no .env, sem lista fixa de "postos
# com robô". O piloto começou em B/G/X/Y, mas fixar isso aqui viraria um
# kill-switch implícito: quando o robô fosse habilitado numa filial nova, a
# página esconderia os cancelamentos dela em silêncio. Posto sem robô só
# devolve 0 linhas.
_MAX_WORKERS = 8

# A view é pesada demais para range aberto — a consulta ao vivo é operacional,
# não é o lugar de puxar um ano de histórico (para isso existe o dashboard de
# pré-agendamento, que lê o JSON do ETL).
_MAX_DIAS = 92


def _postos_disponiveis() -> list:
    """Toda letra com DB_HOST_<L> no .env. Derivado do ambiente (não de lista
    fixa) pelo mesmo motivo do comentário acima: filial nova aparece sozinha."""
    return [p for p in string.ascii_uppercase if os.getenv(f"DB_HOST_{p}", "").strip()]

SQL = """
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT ls.idLancamentoServico,
       f3.Matricula,
       f3.Paciente,
       f3.idadePaciente,
       f3.TelefoneResidencial,
       f3.NomeMedico,
       f3.Especialidade,
       CONVERT(varchar(10), l.DataConsulta, 120)          AS data_consulta,
       CONVERT(varchar(5),  l.HoraPrevistaConsulta, 108)  AS hora_consulta,
       CONVERT(varchar(19), ls.DataDesistencia, 120)      AS data_desistencia
FROM Cad_LancamentoServico ls WITH (NOLOCK)
JOIN cad_lancamento l WITH (NOLOCK)
  ON ls.idLancamento = l.idLancamento
JOIN vw_Cad_LancamentoProntuarioComDesistencia f3
  ON ls.idLancamentoServico = f3.idLancamentoServico
WHERE ls.Desistencia = 1
  AND CAST(ls.MotivoDesistencia AS varchar(max)) LIKE ?
  AND l.DataConsulta >= ?
  AND l.DataConsulta <  ?
ORDER BY l.HoraPrevistaConsulta
"""


def _fmt_data_sql(d: date) -> str:
    """YYYYMMDD — formato básico ISO.

    O CLAUDE.md manda DD/MM/YYYY em views e ISO em tabelas, e aqui a query
    mistura as duas coisas. O formato básico sem separadores é interpretado
    igual sob qualquer SET DATEFORMAT, então resolve os dois casos de uma vez.
    """
    return d.strftime("%Y%m%d")


_RE_TELEFONE = re.compile(r"\(?\s*(\d{2})\s*\)?[\s.\-]*(\d{4,5})[\s.\-]*(\d{4})")


def _telefones(raw) -> list:
    """Extrai os telefones de um campo que pode conter vários no mesmo texto.

    A formatação de TelefoneResidencial varia por filial e é comum vir
    "(21) 99999-8888 / 3333-4444". Concatenar todos os dígitos produziria um
    número inválido, então casamos o padrão DDD+número e devolvemos cada um
    separado, já com o 55 na frente para o link do WhatsApp.
    """
    if not raw:
        return []
    out = []
    for ddd, meio, fim in _RE_TELEFONE.findall(str(raw)):
        num = f"55{ddd}{meio}{fim}"
        if num not in out:
            out.append(num)
    return out


def _buscar_posto(posto: str, ini: date, fim: date) -> list:
    """Cancelados do robô com DataConsulta entre ini e fim (inclusivos)."""
    d_ini = _fmt_data_sql(ini)
    d_fim = _fmt_data_sql(fim + timedelta(days=1))

    ck = (posto, d_ini, d_fim)
    with _cache_lock:
        hit = _cache.get(ck)
        if hit and (time.time() - hit[0]) < _CACHE_TTL:
            return hit[1]

    con = _conn_for_posto(posto)
    try:
        cur = con.cursor()
        cur.execute(SQL, ROBO_LIKE, d_ini, d_fim)
        out = []
        for r in cur.fetchall():
            out.append({
                "posto":                 posto,
                "id_lancamento_servico": int(r[0]),
                "matricula":             (str(r[1]).strip() if r[1] is not None else ""),
                "paciente":              (r[2] or "").strip(),
                "idade":                 (int(r[3]) if r[3] is not None else None),
                "telefones":             _telefones(r[4]),
                "telefone_exibicao":     (str(r[4]).strip() if r[4] is not None else ""),
                "medico":                (r[5] or "").strip(),
                "especialidade":         (r[6] or "").strip(),
                "data_consulta":         r[7],
                "hora_consulta":         r[8],
                "data_desistencia":      r[9],
            })
    finally:
        con.close()

    with _cache_lock:
        # Com filtro de período cada combinação (posto, ini, fim) vira uma chave
        # nova; sem poda o dict só cresce no worker de vida longa do camim-auth.
        agora = time.time()
        for k in [k for k, v in _cache.items() if (agora - v[0]) >= _CACHE_TTL]:
            del _cache[k]
        _cache[ck] = (agora, out)
    return out


def _marcacoes(postos: list) -> dict:
    """(posto, idLancamentoServico) → dados de quem já tratou."""
    from auth_db import SessionLocal, CanceladoRoboTratado
    db = SessionLocal()
    try:
        q = db.query(CanceladoRoboTratado).filter(
            CanceladoRoboTratado.posto.in_(postos or [""])
        ).all()
        return {
            (m.posto, m.id_lancamento_servico): {
                "tratado_por": m.tratado_por,
                "observacao":  m.observacao or "",
                "tratado_em":  m.tratado_em.strftime("%d/%m/%Y %H:%M") if m.tratado_em else "",
            }
            for m in q
        }
    finally:
        db.close()


@cancelados_robo_bp.get("/api/cancelados_robo")
def api_listar():
    """Lista os cancelados pelo robô para um período de data de consulta.

    ?ini=YYYY-MM-DD&fim=YYYY-MM-DD (default: hoje)  ·  ?data=YYYY-MM-DD é o
    atalho antigo de 1 dia e continua aceito  ·  ?posto=X (default: rede inteira)

    A leitura é da rede inteira de propósito — ver docstring do módulo.
    """
    email, postos_acl, _ = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    raw_ini = (request.args.get("ini") or request.args.get("data") or "").strip()
    raw_fim = (request.args.get("fim") or "").strip()
    try:
        d_ini = datetime.strptime(raw_ini, "%Y-%m-%d").date() if raw_ini else date.today()
        d_fim = datetime.strptime(raw_fim, "%Y-%m-%d").date() if raw_fim else d_ini
    except ValueError:
        return jsonify({"error": "data inválida (use YYYY-MM-DD)"}), 400
    if d_fim < d_ini:
        d_ini, d_fim = d_fim, d_ini
    if (d_fim - d_ini).days + 1 > _MAX_DIAS:
        return jsonify({"error": f"período máximo de {_MAX_DIAS} dias por consulta"}), 400

    disponiveis = _postos_disponiveis()
    posto_req = (request.args.get("posto") or "").strip().upper()
    if posto_req:
        if posto_req not in disponiveis:
            return jsonify({"error": f"posto {posto_req} não configurado"}), 400
        alvo = [posto_req]
    else:
        alvo = disponiveis

    linhas, erros = [], {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futs = {pool.submit(_buscar_posto, p, d_ini, d_fim): p for p in alvo}
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                linhas.extend(fut.result())
            except Exception as e:
                # Posto fora do ar não pode derrubar a lista dos outros — a
                # recepção precisa ver o que dá pra ver. O erro vai no payload
                # para a página avisar que aquele posto ficou de fora.
                logger.warning("cancelados_robo: posto %s falhou: %s", p, e)
                erros[p] = str(e)[:200]

    marc = _marcacoes(alvo)
    for ln in linhas:
        m = marc.get((ln["posto"], ln["id_lancamento_servico"]))
        ln["tratado"] = bool(m)
        ln["tratado_por"] = m["tratado_por"] if m else ""
        ln["tratado_em"] = m["tratado_em"] if m else ""
        ln["observacao"] = m["observacao"] if m else ""

    linhas.sort(key=lambda x: (x["data_consulta"] or "9999-99-99",
                               x["hora_consulta"] or "99:99",
                               x["posto"], x["paciente"]))

    return jsonify({
        "ini":           d_ini.strftime("%Y-%m-%d"),
        "fim":           d_fim.strftime("%Y-%m-%d"),
        # o front marca "tratado" só nos postos do ACL do usuário
        "postos_acl":    sorted(postos_acl or []),
        "postos":        alvo,
        "postos_erro":   erros,
        "total":         len(linhas),
        "total_tratados": sum(1 for x in linhas if x["tratado"]),
        "cancelados":    linhas,
    })


# ---------------------------------------------------------------------------
# RELATÓRIO PARA GESTÃO (2026-09-22) — as 9 perguntas do Petterson/Cristiano
# ---------------------------------------------------------------------------
# Hoje o Petterson responde isso comparando agenda por agenda com ajuda de IA.
# Aqui é uma consulta agregada por posto × dia de consulta, sobre as MESMAS
# tabelas-base e os MESMOS filtros do sql/preagendamento.sql (F3, estornos,
# Codigo > 0), para o número bater com o dashboard de pré-agendamento.
#
# Definições (fechadas com o Petterson em 2026-09-22 — mudar aqui E no bloco
# "Como foi calculado" do cancelados_robo.html):
#   falta médica   consulta cujo horário cai DENTRO de uma falta registrada do
#                  médico (Cad_MedicoFalta, qualquer duração — não só dia
#                  inteiro como o dashboard) e foi cancelada, por quem for.
#                  "Pacientes prejudicados, não importa se remarcados."
#   atendidos      StatusAtendimento = 1.
#   marcou já      DataConfirmacaoAgendamentoConsulta gravada até 1 min depois
#   confirmado     do lançamento (o ERP preenche no ato quando marca dentro da
#                  janela ou quando o atendente já confirma ao marcar).
#   confirmou      DataConfirmacaoAgendamentoConsulta gravada DEPOIS disso —
#   (app ou F5)    pelo app ou pela central no F5. O ERP não grava a origem e a
#                  auditoria (Sis_Historico) não registra essa alteração
#                  (medido: 1 em 940), então app × F5 NÃO é separável aqui.
#   falta          regra do dashboard: não compareceu e já passou 1h da hora
#                  prevista; antes disso é pendente. Médico faltou fica fora.
#   reaproveitada  vaga (agenda + dia + número) de um cancelado pelo robô que
#                  recebeu marcação ativa feita com MENOS DE 2 DIAS de
#                  antecedência, depois do cancelamento ("encaixe").
#
# Medido em G, ago/2026: 313 cancelados pelo robô → 132 vagas reaproveitadas
# (110 atendidas), 178 ficaram vazias. Base ~5 s por posto para 3 meses.

_CACHE_REL_TTL = 300
_cache_rel: dict = {}

SQL_RELATORIO = """
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT l.idLancamento, l.idEspecialidade, l.DataConsulta, l.consulta,
       DATEADD(minute, DATEPART(hour, l.HoraPrevistaConsulta) * 60 + DATEPART(minute, l.HoraPrevistaConsulta),
               CAST(CAST(l.DataConsulta AS date) AS datetime))                        AS dt_consulta,
       DATEDIFF(day, l.[Data], l.DataConsulta)                                          AS dif_dias,
       CAST(ISNULL(ls.Desistencia, 0) AS int)                                           AS desist,
       ls.DataDesistencia,
       CASE WHEN ls.Desistencia = 1 AND CAST(ls.MotivoDesistencia AS varchar(max)) LIKE ? THEN 1 ELSE 0 END AS canc_robo,
       CASE WHEN ls.Desistencia = 1 AND (ls.MotivoDesistencia LIKE 'CANCELADO PELO CLIENTE NO SITE OU APP%'
                                     OR  ls.MotivoDesistencia LIKE 'CANCELAMENTO E REMARCA%') THEN 1 ELSE 0 END AS canc_app,
       CASE WHEN ls.Desistencia = 1 AND ls.MotivoDesistencia LIKE 'CANCELAMENTO AUTOM%PAGAMENTO%' THEN 1 ELSE 0 END AS canc_sem_pag,
       CASE WHEN l.DataConfirmacaoAgendamentoConsulta IS NULL THEN 0
            WHEN DATEDIFF(minute, l.[Data], l.DataConfirmacaoAgendamentoConsulta) <= 1 THEN 1 ELSE 0 END AS conf_no_ato,
       CASE WHEN l.DataConfirmacaoAgendamentoConsulta IS NULL THEN 0
            WHEN DATEDIFF(minute, l.[Data], l.DataConfirmacaoAgendamentoConsulta) <= 1 THEN 0 ELSE 1 END AS conf_depois,
       CASE WHEN l.DataHoraNotificacaoPreAgendamento IS NOT NULL THEN 1 ELSE 0 END      AS push,
       CASE WHEN l.MarcadoViaWeb = 1 THEN 1 ELSE 0 END                                  AS via_app,
       CASE WHEN mf.idFalta IS NOT NULL THEN 1 ELSE 0 END                               AS mf,
       ls.StatusAtendimento, ls.StatusAguardar, l.Falta, ls.DataMaterial, sc.AtendidoComDataMaterial
INTO #b
FROM cad_lancamento          l  WITH (NOLOCK)
JOIN Cad_LancamentoServico   ls WITH (NOLOCK) ON ls.idLancamento    = l.idLancamento
JOIN Cad_Especialidade       es               ON es.idEspecialidade = l.idEspecialidade AND es.ExibirNoF3 = 1
JOIN Cad_Servico             ss               ON ss.idServico       = ls.idServico
JOIN Cad_ServicoClasse       sc               ON sc.idClasse        = ss.idClasse
OUTER APPLY (SELECT TOP 1 mf.idFalta
             FROM Cad_MedicoFalta mf WITH (NOLOCK)
             WHERE mf.idMedico = l.idMedico AND mf.Desativado = 0 AND mf.Especialidade = es.Especialidade
               AND DATEADD(minute, DATEPART(hour, l.HoraPrevistaConsulta) * 60 + DATEPART(minute, l.HoraPrevistaConsulta),
                           CAST(CAST(l.DataConsulta AS date) AS datetime)) BETWEEN mf.DataHora AND mf.DatahoraFim) mf
WHERE l.consulta             IS NOT NULL
  AND l.desativado           = 0
  AND l.HoraPrevistaConsulta IS NOT NULL
  AND l.DataConsulta         >= ?
  AND l.DataConsulta         <  ?
  AND l.Codigo               > 0
  AND l.DataEstorno          IS NULL
  AND (ss.ExibenoProntuarioF3 = 1 OR ss.PermitirAgendamentoF6eCTRLF6 = 1);

-- categoria (mesma régua do preagendamento.html: Atendido ganha de médico
-- faltou; falta só depois de 1h da hora prevista; antes disso é pendente)
SELECT b.*,
       CASE WHEN b.desist = 1 THEN 'cancelada'
            WHEN b.StatusAtendimento = 1 THEN 'compareceu'
            WHEN b.mf = 1 THEN 'medico_faltou'
            WHEN b.StatusAguardar = 1 THEN 'compareceu'
            WHEN ISNULL(b.Falta, 0) = 0 AND CAST(b.DataConsulta AS date) > CAST(GETDATE() - 1 AS date)
                 AND b.StatusAtendimento IS NULL AND b.DataMaterial IS NOT NULL
                 AND b.AtendidoComDataMaterial = 1 THEN 'compareceu'
            WHEN DATEADD(hour, 1, b.dt_consulta) < GETDATE() THEN 'falta'
            ELSE 'pendente' END AS cat
INTO #c
FROM #b b;

-- reaproveitamento: outra marcação ATIVA na mesma vaga, feita depois do
-- cancelamento; "<2 dias de antecedência" é a régua do Petterson
SELECT c.idLancamento,
       MAX(CASE WHEN a.[Data] > c.DataDesistencia AND DATEDIFF(day, a.[Data], a.DataConsulta) < 2 THEN 1 ELSE 0 END) AS r2d,
       MAX(CASE WHEN a.[Data] > c.DataDesistencia THEN 1 ELSE 0 END)                                             AS rqq,
       MAX(CASE WHEN a.[Data] > c.DataDesistencia AND DATEDIFF(day, a.[Data], a.DataConsulta) < 2
                 AND s2.StatusAtendimento = 1 THEN 1 ELSE 0 END)                                                 AS r2d_at
INTO #r
FROM #c c
JOIN cad_lancamento        a  WITH (NOLOCK) ON a.idEspecialidade = c.idEspecialidade AND a.DataConsulta = c.DataConsulta
                                            AND a.consulta = c.consulta AND a.idLancamento <> c.idLancamento
                                            AND a.desativado = 0 AND a.HoraPrevistaConsulta IS NOT NULL
JOIN Cad_LancamentoServico s2 WITH (NOLOCK) ON s2.idLancamento = a.idLancamento AND ISNULL(s2.Desistencia, 0) = 0
WHERE c.canc_robo = 1
GROUP BY c.idLancamento;

SELECT CONVERT(varchar(10), c.DataConsulta, 120)                                             AS dia,
       COUNT(*)                                                                               AS total,
       SUM(c.desist)                                                                          AS canceladas,
       SUM(c.canc_robo)                                                                       AS canc_robo,
       SUM(c.canc_app)                                                                        AS canc_app,
       SUM(c.canc_sem_pag)                                                                    AS canc_sem_pag,
       SUM(CASE WHEN c.desist = 1 AND c.mf = 1 THEN 1 ELSE 0 END)                             AS canc_falta_medico,
       SUM(CASE WHEN c.desist = 1 AND c.mf = 1 AND c.canc_robo = 1 THEN 1 ELSE 0 END)          AS canc_falta_medico_robo,
       SUM(CASE WHEN c.desist = 1 AND c.mf = 1 AND c.canc_app = 1 THEN 1 ELSE 0 END)           AS canc_falta_medico_app,
       SUM(CASE WHEN c.cat = 'medico_faltou' THEN 1 ELSE 0 END)                                AS medico_faltou,
       SUM(CASE WHEN c.desist = 0 AND c.StatusAtendimento = 1 THEN 1 ELSE 0 END)               AS atendidos,
       SUM(CASE WHEN c.cat = 'compareceu' THEN 1 ELSE 0 END)                                   AS compareceram,
       SUM(CASE WHEN c.cat = 'falta' THEN 1 ELSE 0 END)                                        AS faltas,
       SUM(CASE WHEN c.cat = 'pendente' THEN 1 ELSE 0 END)                                     AS pendentes,
       SUM(c.conf_no_ato)                                                                     AS conf_no_ato_total,
       SUM(CASE WHEN c.desist = 0 AND c.conf_no_ato = 1 THEN 1 ELSE 0 END)                     AS conf_no_ato,
       SUM(CASE WHEN c.cat = 'falta' AND c.conf_no_ato = 1 THEN 1 ELSE 0 END)                  AS conf_no_ato_falta,
       SUM(CASE WHEN c.cat = 'compareceu' AND c.conf_no_ato = 1 THEN 1 ELSE 0 END)             AS conf_no_ato_compareceu,
       SUM(CASE WHEN c.desist = 0 AND c.conf_depois = 1 THEN 1 ELSE 0 END)                     AS conf_depois,
       SUM(CASE WHEN c.cat = 'falta' AND c.conf_depois = 1 THEN 1 ELSE 0 END)                  AS conf_depois_falta,
       SUM(CASE WHEN c.cat = 'compareceu' AND c.conf_depois = 1 THEN 1 ELSE 0 END)             AS conf_depois_compareceu,
       SUM(CASE WHEN c.desist = 0 AND c.conf_depois = 1 AND c.via_app = 1 THEN 1 ELSE 0 END)   AS conf_depois_marc_app,
       SUM(CASE WHEN c.desist = 0 AND c.conf_no_ato = 0 AND c.conf_depois = 0 THEN 1 ELSE 0 END) AS sem_conf,
       SUM(CASE WHEN c.cat = 'falta' AND c.conf_no_ato = 0 AND c.conf_depois = 0 THEN 1 ELSE 0 END) AS sem_conf_falta,
       SUM(CASE WHEN c.cat = 'compareceu' AND c.conf_no_ato = 0 AND c.conf_depois = 0 THEN 1 ELSE 0 END) AS sem_conf_compareceu,
       SUM(CASE WHEN c.desist = 0 AND c.dif_dias <= 5 THEN 1 ELSE 0 END)                       AS dentro5,
       SUM(CASE WHEN c.cat = 'falta' AND c.dif_dias <= 5 THEN 1 ELSE 0 END)                    AS dentro5_falta,
       SUM(CASE WHEN c.desist = 0 AND c.push = 1 THEN 1 ELSE 0 END)                            AS push,
       SUM(ISNULL(r.r2d, 0))                                                                  AS reaprov2d,
       SUM(ISNULL(r.rqq, 0))                                                                  AS reaprov_qq,
       SUM(ISNULL(r.r2d_at, 0))                                                               AS reaprov2d_atendido
FROM #c c
LEFT JOIN #r r ON r.idLancamento = c.idLancamento
GROUP BY CONVERT(varchar(10), c.DataConsulta, 120)
ORDER BY 1;
"""

# Lista das vagas reaproveitadas (para o Petterson conferir uma a uma).
SQL_REAPROVEITADAS = """
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT TOP 3000
       CONVERT(varchar(10), l.DataConsulta, 120)                 AS data_consulta,
       CONVERT(varchar(5),  l.HoraPrevistaConsulta, 108)         AS hora_consulta,
       l.consulta                                                AS vaga,
       CASE WHEN sc.ExibenoProntuario = 1 THEN sc.Classe ELSE es.Especialidade END AS especialidade,
       m.Nome                                                    AS medico,
       ISNULL(d.Nome, cli.Nome)                                  AS cancelado_paciente,
       CONVERT(varchar(16), ls.DataDesistencia, 120)             AS cancelado_em,
       ISNULL(d2.Nome, cli2.Nome)                                AS novo_paciente,
       CONVERT(varchar(16), a.[Data], 120)                       AS novo_marcado_em,
       DATEDIFF(day, a.[Data], a.DataConsulta)                   AS novo_antecedencia,
       CASE WHEN a.MarcadoViaWeb = 1 THEN 'app' WHEN a.MarcadoViaAgendaUnificada = 1 THEN 'central' ELSE 'balcão' END AS novo_canal,
       CASE WHEN s2.StatusAtendimento = 1 THEN 1 ELSE 0 END      AS novo_atendido
FROM cad_lancamento          l   WITH (NOLOCK)
JOIN Cad_LancamentoServico   ls  WITH (NOLOCK) ON ls.idLancamento = l.idLancamento
JOIN Cad_Especialidade       es                ON es.idEspecialidade = l.idEspecialidade AND es.ExibirNoF3 = 1
JOIN Cad_Medico              m                 ON m.idMedico = l.idMedico
JOIN Cad_Servico             ss                ON ss.idServico = ls.idServico
JOIN Cad_ServicoClasse       sc                ON sc.idClasse = ss.idClasse
JOIN Cad_Cliente             cli               ON cli.idCliente = l.idCliente
LEFT JOIN Cad_ClienteDependente d              ON d.idDependente = l.idDependente
CROSS APPLY (SELECT TOP 1 a.*
             FROM cad_lancamento a WITH (NOLOCK)
             JOIN Cad_LancamentoServico s2 WITH (NOLOCK) ON s2.idLancamento = a.idLancamento AND ISNULL(s2.Desistencia, 0) = 0
             WHERE a.idEspecialidade = l.idEspecialidade AND a.DataConsulta = l.DataConsulta AND a.consulta = l.consulta
               AND a.idLancamento <> l.idLancamento AND a.desativado = 0 AND a.HoraPrevistaConsulta IS NOT NULL
               AND a.[Data] > ls.DataDesistencia AND DATEDIFF(day, a.[Data], a.DataConsulta) < 2
             ORDER BY a.[Data]) a
JOIN Cad_LancamentoServico   s2  WITH (NOLOCK) ON s2.idLancamento = a.idLancamento
JOIN Cad_Cliente             cli2              ON cli2.idCliente = a.idCliente
LEFT JOIN Cad_ClienteDependente d2             ON d2.idDependente = a.idDependente
WHERE ls.Desistencia = 1
  AND CAST(ls.MotivoDesistencia AS varchar(max)) LIKE ?
  AND l.consulta IS NOT NULL AND l.desativado = 0 AND l.HoraPrevistaConsulta IS NOT NULL
  AND l.DataConsulta >= ? AND l.DataConsulta < ?
  AND l.Codigo > 0 AND l.DataEstorno IS NULL
  AND (ss.ExibenoProntuarioF3 = 1 OR ss.PermitirAgendamentoF6eCTRLF6 = 1)
ORDER BY l.DataConsulta, l.HoraPrevistaConsulta
"""

# Colunas numéricas do relatório, na ordem do SELECT (dia é a primeira).
_COLS_REL = (
    "total", "canceladas", "canc_robo", "canc_app", "canc_sem_pag",
    "canc_falta_medico", "canc_falta_medico_robo", "canc_falta_medico_app", "medico_faltou",
    "atendidos", "compareceram", "faltas", "pendentes",
    "conf_no_ato_total", "conf_no_ato", "conf_no_ato_falta", "conf_no_ato_compareceu",
    "conf_depois", "conf_depois_falta", "conf_depois_compareceu", "conf_depois_marc_app",
    "sem_conf", "sem_conf_falta", "sem_conf_compareceu",
    "dentro5", "dentro5_falta", "push",
    "reaprov2d", "reaprov_qq", "reaprov2d_atendido",
)


def _relatorio_posto(posto: str, ini: date, fim: date) -> list:
    """Linhas por dia de consulta para o posto. Cache de 5 min por (posto,
    período): é relatório de gestão, não balcão."""
    d_ini = _fmt_data_sql(ini)
    d_fim = _fmt_data_sql(fim + timedelta(days=1))
    ck = ("rel", posto, d_ini, d_fim)
    with _cache_lock:
        hit = _cache_rel.get(ck)
        if hit and (time.time() - hit[0]) < _CACHE_REL_TTL:
            return hit[1]

    con = _conn_for_posto(posto)
    try:
        con.timeout = 120
        cur = con.cursor()
        cur.execute(SQL_RELATORIO, ROBO_LIKE, d_ini, d_fim)
        # SET NOCOUNT ON: só o SELECT final devolve linhas; os SELECT INTO não
        # geram result set, mas pyodbc pode precisar pular sets vazios.
        while cur.description is None and cur.nextset():
            pass
        out = []
        for r in cur.fetchall():
            item = {"posto": posto, "dia": r[0]}
            for i, k in enumerate(_COLS_REL, start=1):
                item[k] = int(r[i] or 0)
            out.append(item)
    finally:
        con.close()

    with _cache_lock:
        agora = time.time()
        for k in [k for k, v in _cache_rel.items() if (agora - v[0]) >= _CACHE_REL_TTL]:
            del _cache_rel[k]
        _cache_rel[ck] = (agora, out)
    return out


def _reaproveitadas_posto(posto: str, ini: date, fim: date) -> list:
    d_ini = _fmt_data_sql(ini)
    d_fim = _fmt_data_sql(fim + timedelta(days=1))
    con = _conn_for_posto(posto)
    try:
        con.timeout = 120
        cur = con.cursor()
        cur.execute(SQL_REAPROVEITADAS, ROBO_LIKE, d_ini, d_fim)
        while cur.description is None and cur.nextset():
            pass
        cols = ["data_consulta", "hora_consulta", "vaga", "especialidade", "medico",
                "cancelado_paciente", "cancelado_em", "novo_paciente", "novo_marcado_em",
                "novo_antecedencia", "novo_canal", "novo_atendido"]
        out = []
        for r in cur.fetchall():
            item = {"posto": posto}
            for i, k in enumerate(cols):
                v = r[i]
                item[k] = v.strip() if isinstance(v, str) else v
            out.append(item)
        return out
    finally:
        con.close()


def _periodo_e_postos():
    """Valida ?ini&fim&posto — mesma régua da lista (92 dias, rede inteira)."""
    raw_ini = (request.args.get("ini") or "").strip()
    raw_fim = (request.args.get("fim") or "").strip()
    try:
        d_ini = datetime.strptime(raw_ini, "%Y-%m-%d").date() if raw_ini else date.today().replace(day=1)
        d_fim = datetime.strptime(raw_fim, "%Y-%m-%d").date() if raw_fim else date.today()
    except ValueError:
        return None, None, None, (jsonify({"error": "data inválida (use YYYY-MM-DD)"}), 400)
    if d_fim < d_ini:
        d_ini, d_fim = d_fim, d_ini
    if (d_fim - d_ini).days + 1 > _MAX_DIAS:
        return None, None, None, (jsonify({"error": f"período máximo de {_MAX_DIAS} dias por consulta"}), 400)
    disponiveis = _postos_disponiveis()
    posto_req = (request.args.get("posto") or "").strip().upper()
    if posto_req:
        alvo = [p for p in posto_req.split(",") if p]
        fora = [p for p in alvo if p not in disponiveis]
        if fora:
            return None, None, None, (jsonify({"error": f"posto {','.join(fora)} não configurado"}), 400)
    else:
        alvo = disponiveis
    return d_ini, d_fim, alvo, None


def _paralelo(fn, alvo: list, d_ini: date, d_fim: date):
    linhas, erros = [], {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futs = {pool.submit(fn, p, d_ini, d_fim): p for p in alvo}
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                linhas.extend(fut.result())
            except Exception as e:
                logger.warning("cancelados_robo/relatorio: posto %s falhou: %s", p, e)
                erros[p] = str(e)[:200]
    return linhas, erros


@cancelados_robo_bp.get("/api/cancelados_robo/relatorio")
def api_relatorio():
    """Relatório para gestão: as 9 perguntas, por posto e por dia de consulta.
    ?ini&fim (default: mês corrente até hoje) · ?posto=G ou G,B (default: rede)."""
    email, _postos_acl, _ = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    d_ini, d_fim, alvo, erro = _periodo_e_postos()
    if erro:
        return erro

    por_dia, erros = _paralelo(_relatorio_posto, alvo, d_ini, d_fim)

    por_posto = {}
    for ln in por_dia:
        acc = por_posto.setdefault(ln["posto"], {"posto": ln["posto"], **{k: 0 for k in _COLS_REL}})
        for k in _COLS_REL:
            acc[k] += ln[k]
    totais = {k: 0 for k in _COLS_REL}
    for acc in por_posto.values():
        for k in _COLS_REL:
            totais[k] += acc[k]
    por_dia.sort(key=lambda x: (x["dia"], x["posto"]))

    return jsonify({
        "ini": d_ini.strftime("%Y-%m-%d"), "fim": d_fim.strftime("%Y-%m-%d"),
        "postos": alvo, "postos_erro": erros,
        "postos_com_dados": sorted(por_posto.keys()),
        "totais": totais,
        "por_posto": [por_posto[p] for p in sorted(por_posto)],
        "por_dia": por_dia,
        "gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
    })


@cancelados_robo_bp.get("/api/cancelados_robo/relatorio/reaproveitadas")
def api_reaproveitadas():
    """Lista das vagas de cancelados pelo robô que foram reaproveitadas
    (marcação ativa com < 2 dias de antecedência, depois do cancelamento)."""
    email, _postos_acl, _ = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    d_ini, d_fim, alvo, erro = _periodo_e_postos()
    if erro:
        return erro
    linhas, erros = _paralelo(_reaproveitadas_posto, alvo, d_ini, d_fim)
    linhas.sort(key=lambda x: (x["data_consulta"] or "", x["hora_consulta"] or "", x["posto"]))
    return jsonify({"ini": d_ini.strftime("%Y-%m-%d"), "fim": d_fim.strftime("%Y-%m-%d"),
                    "postos": alvo, "postos_erro": erros, "total": len(linhas), "linhas": linhas})


@cancelados_robo_bp.post("/api/cancelados_robo/tratado")
def api_marcar():
    """Marca/desmarca 'a recepção já tratou'. Grava só no SQLite local."""
    email, postos_acl, _ = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    posto = (body.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos_acl)
    if erro:
        return jsonify({"error": erro}), 400
    try:
        idls = int(body.get("id_lancamento_servico") or 0)
    except (TypeError, ValueError):
        idls = 0
    if idls <= 0:
        return jsonify({"error": "id_lancamento_servico inválido"}), 400

    tratado = bool(body.get("tratado"))
    obs = (body.get("observacao") or "").strip()[:300]

    from auth_db import SessionLocal, CanceladoRoboTratado
    db = SessionLocal()
    try:
        row = db.query(CanceladoRoboTratado).filter_by(
            posto=posto, id_lancamento_servico=idls
        ).one_or_none()

        if tratado:
            if row:
                row.tratado_por = email
                row.observacao = obs
            else:
                db.add(CanceladoRoboTratado(
                    posto=posto, id_lancamento_servico=idls,
                    tratado_por=email, observacao=obs,
                ))
        elif row:
            db.delete(row)

        db.commit()
        return jsonify({"ok": True, "tratado": tratado})
    except Exception as e:
        db.rollback()
        logger.exception("cancelados_robo: falha ao marcar tratado")
        return jsonify({"error": str(e)[:200]}), 500
    finally:
        db.close()
