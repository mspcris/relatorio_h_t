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
