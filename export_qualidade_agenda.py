#!/usr/bin/env python3
# export_qualidade_agenda.py
# Exporta dados de Qualidade da Agenda Médica (vagas por especialidade por posto)
# Fonte única: Anchieta (posto A) — view retorna todos os postos.
# Mapeia idEndereco → letra via cad_endereco.
# Gera: json_consolidado/qualidade_agenda.json
# Cron recomendado: 0 5,12 * * *

import os, sys, json, argparse, time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from qualidade_agenda_regras import proxima_vaga, pct_livre
from dotenv import load_dotenv

from etl_meta import ETLMeta

# =========================
# Constantes
# =========================

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
JSON_DIR         = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR          = os.path.join(BASE_DIR, "logs")
LOG_FILE         = os.path.join(LOG_DIR, f"export_qualidade_agenda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

POSTO_MASTER     = "A"   # Anchieta — fonte canônica de todos os dados
ODBC_DRIVER      = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# Mapeamento idEndereco → letra (obtido de cad_endereco, mantido como fallback)
ID_ENDERECO_TO_LETRA = {
    1: "R",   # Realengo
    2: "C",   # Campinho
    3: "A",   # Anchieta
    4: "J",   # Jacarepaguá
    5: "G",   # Campo Grande
    6: "I",   # Nova Iguaçu
    7: "B",   # Bangu
    12: "N",  # Nilópolis
    20: "D",  # Del Castilho
    21: "X",  # X Campo Grande
    25: "M",  # Madureira
    26: "P",  # Rio das Pedras
    51: "Y",  # Y Campo Grande
}

# =========================
# SQLs
# =========================

SQL_CAD_ENDERECO = """
SELECT
    idEndereco,
    Codigo   AS letra,
    Descricao AS nome
FROM cad_endereco
WHERE AtendimentoAtivoPosto = 1
"""

SQL_CAD_CBOS = """
SELECT
    Especialidade,
    ISNULL(prazoconsultaans, 0)               AS prazoconsultaans,
    ISNULL(prazoconsultacamim, 0)             AS prazoconsultacamim,
    ISNULL(ValorPMinimoVagaDisponivel, 0)     AS ValorPMinimoVagaDisponivel
FROM cad_cbos
WHERE Desativado = 0
"""

# CALENDÁRIO DE VAGAS GERADO DO cad_agenda — por que não lemos a view (2026-11-04)
#
# Até 04/11/2026 a fonte era uma cópia de vw_Rel_EspecialidadeProximaVagaPai
# (cópia porque a original estoura em DECIMAL(5,2) quando há mais agendados que
# capacidade — 10/09/2026, PSIQUIATRIA em Nova Iguaçu). A view nasce de
# `cad_agendadata`, e essa tabela só ganha linha na PRIMEIRA MARCAÇÃO: medido
# 4.352 linhas futuras, nenhuma com 0 agendados. Dia inteiro sem ninguém marcado
# não existe para ela. Agenda lotada por semanas e vazia depois virava SEM VAGA
# (Y/Pediatria: 14/14 até 15/12 e 22/12 com 0 de 14 — o ERP mostrava a vaga, o KPI
# não; 7 linhas SEM VAGA em 04/11, todas com agenda ativa) e o dia vazio sumia da
# lista `dias_livres` (G/Pediatria 22/12, 0 de 18).
#
# A view ainda ignora DataInicioExibicao/DataFimExibicao (médico com agenda fixa +
# plantão de 1 dia: capacidade e marcados em DOBRO) e junta cad_agendadata só por
# médico (373 médico×dia com uma linha por idEspecialidade → multiplica quando o
# médico tem duas agendas no posto; 111 têm).
#
# REGRAS (Cristiano, 2026-11-04):
# - "Dia sem nenhuma marcação tem todas as vagas livres e devem ser vistas."
# - "DataFimExibicao é quando um médico vai encerrar seu atendimento conosco;
#    vagas após a data fim não devem existir."
#
# Então o calendário é GERADO: hoje + HORIZONTE_DIAS × cad_agenda ativa, dia em
# que a agenda atende (bit <Dia>), com <Dia>QuantidadeMaxima > 0, dentro de
# [DataInicioExibicao, DataFimExibicao], sem falta em Cad_AgendaMedicoFalta, e
# quinzenal só nas semanas pares contadas do início (conferido em 7 de 7 agendas
# quinzenais com marcação). Marcados vêm de cad_agendadata por LEFT JOIN e valem
# 0 quando não há linha. A tabela guarda DOIS contadores por dia: a linha POR
# ESPECIALIDADE (idMedico NULL, idEspecialidade = cad_agenda.idEspecialidade —
# a agenda em si; 2.266 linhas futuras) e a linha POR MÉDICO (idEspecialidade
# NULL, 2.090 linhas — a SOMA de todas as agendas do médico no dia). A view junta
# pela do médico, então médico com duas agendas (Clínica Geral 13 + Reumatologia
# 19 = 32) levava 32 marcados em CADA uma. Aqui a linha por especialidade manda e
# a do médico é só reserva quando aquela não existe (22 de 2.090; nesse caso um
# médico com duas agendas fica com marcados a mais, nunca a menos).
# Reserva de vaga (ReservaMinimo/Maximo/Dias) e
# CapacidadeConsiderada são a MESMA conta da view: fora da janela de reserva a
# capacidade é QuantidadeMaxima − reservadas; dentro dela é QuantidadeMaxima.
#
# Validado em 04/11/2026 (dry-run contra a view, 400 dias, posto A): 1.656 dias
# batem; 112 diferem — agenda sem marcação (invisível para a view), agenda lotada
# ou o dobro da view; nenhum caso em que o gerado conte MENOS do que a grade do
# ERP. Y/Pediatria → 22/12 com 14 de 14; G/Pediatria 22/12 → 18 de 18 (iguais à
# Agenda Semanal Unificada do ERP). 32.962 linhas posto×esp×dia em ~60 s.
#
# "Próxima vaga" continua exigindo pelo menos 1 vaga livre (desvio consciente da
# view, 2026-09-13: dia lotado nunca é próxima vaga) E % livre da SOMA DO DIA >=
# mínimo do cad_cbos casado por radical — a escolha é feita em Python por
# qualidade_agenda_regras.proxima_vaga(), não aqui.
#
# AGENDA RESTRITA (Cristiano, 2026-11-04): `PermitirAgendamentoquenuncaconsultou`
# desligado = só quem já se consultou com o médico marca (o "R" da grade do ERP;
# 2 agendas ativas na rede em 04/11, ex.: Vera Lucia/Pediatria/G). Cada dia sai
# com dois pares: (vagas, capacidade) com TODAS as agendas — o padrão da tela —
# e (vagas_sr, cap_sr) SEM as restritas, para o interruptor "Incluir agendas
# restritas" da página. Os campos de próxima vaga da linha são do padrão; a
# página recalcula a partir de `dias_livres` quando o interruptor é desligado.
#
# ARMADILHA: `text()` do SQLAlchemy lê qualquer `:nome` como bind param, até em
# comentário. Nada de dois-pontos neste SQL. Datas literais, se um dia
# precisar, em DD/MM/YYYY (DATEFORMAT dmy).

HORIZONTE_DIAS = 400   # dias à frente; nada além disso interessa (o front corta em 400 também)

_DIAS_SEMANA = ["Domingo", "Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado"]  # DATEPART(DW) 1..7


def _case_dia(sufixo):
    """CASE DATEPART(DW, d.Data) → coluna <Dia><sufixo> do cad_agenda (1 = Domingo, como a view)."""
    return ("CASE DATEPART(DW, d.Data) "
            + " ".join(f"WHEN {i + 1} THEN ISNULL(a.{n}{sufixo}, 0)" for i, n in enumerate(_DIAS_SEMANA))
            + " END")


SQL_CALENDARIO = f"""
WITH n AS (
    SELECT TOP ({HORIZONTE_DIAS + 1}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i
    FROM sys.all_objects
),
dias AS (
    SELECT i AS offd, DATEADD(day, i, CAST(GETDATE() AS DATE)) AS Data FROM n
),
ag AS (
    SELECT a.idEndereco, a.Especialidade, a.idMedico, a.idEspecialidade, d.offd, d.Data,
           CASE WHEN ISNULL(a.PermitirAgendamentoquenuncaconsultou, 0) = 1 THEN 0 ELSE 1 END AS restrita,
           {_case_dia('')}                         AS atende,
           {_case_dia('QuantidadeMaxima')}         AS QM,
           {_case_dia('QuantidadeReservaMaximo')}  AS RMax,
           {_case_dia('QuantidadeReservaMinimo')}  AS RMin,
           {_case_dia('QuantidadeReservaDias')}    AS RDias
    FROM cad_agenda a
    CROSS JOIN dias d
    WHERE a.Desativado = 0
      AND (a.DataInicioExibicao IS NULL OR d.Data >= CAST(a.DataInicioExibicao AS DATE))
      AND (a.DataFimExibicao    IS NULL OR d.Data <= CAST(a.DataFimExibicao    AS DATE))
      AND (ISNULL(a.AgendaQuinzenal, 0) = 0 OR a.DataInicioExibicao IS NULL
           OR DATEDIFF(day, CAST(a.DataInicioExibicao AS DATE), d.Data) % 14 < 7)
),
base AS (
    SELECT ag.idEndereco, ag.Especialidade, ag.offd, ag.Data, ag.QM, ag.RDias, ag.restrita,
           CASE WHEN ag.RMax = 0 AND ag.RMin = 0 THEN 0 ELSE ag.RMax - ag.RMin + 1 END AS Reservadas,
           COALESCE(xe.QuantidadeAgendada, xm.QuantidadeAgendada, 0) AS QA
    FROM ag
    LEFT JOIN cad_agendadata xe              -- contador da AGENDA (especialidade)
           ON xe.idEndereco     = ag.idEndereco
          AND xe.Data           = ag.Data
          AND xe.Desativado     = 0
          AND xe.idMedico       IS NULL
          AND xe.idEspecialidade = ag.idEspecialidade
    LEFT JOIN cad_agendadata xm              -- contador do MÉDICO (soma das agendas dele), só reserva
           ON xm.idEndereco     = ag.idEndereco
          AND xm.Data           = ag.Data
          AND xm.Desativado     = 0
          AND xm.idMedico       = ag.idMedico
          AND ISNULL(xm.idEspecialidade, 0) = 0
    LEFT JOIN Cad_AgendaMedicoFalta f
           ON f.idEndereco = ag.idEndereco
          AND f.idMedico   = ag.idMedico
          AND CAST(f.DataHora AS DATE) = ag.Data
    WHERE ag.atende = 1 AND ag.QM > 0 AND f.idAgendaMedicoFalta IS NULL
),
dia AS (
    SELECT *, CASE WHEN CAST(GETDATE() AS DATE) > DATEADD(day, -RDias, Data) THEN QM ELSE QM - Reservadas END AS cap
    FROM base
)
SELECT idEndereco, Especialidade, offd, Data,
       SUM(cap)        AS capacidade,
       SUM(cap - QA)   AS vagas,
       SUM(QM)         AS maxima,
       SUM(Reservadas) AS reservadas,
       MAX(RDias)      AS reserva_dias,
       SUM(CASE WHEN restrita = 0 THEN cap      ELSE 0 END) AS cap_sr,
       SUM(CASE WHEN restrita = 0 THEN cap - QA ELSE 0 END) AS vagas_sr
FROM dia
GROUP BY idEndereco, Especialidade, offd, Data
ORDER BY idEndereco, Especialidade, offd
"""

# =========================
# Logging
# =========================

class Logger:
    def __init__(self, log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.file_handle = open(log_file, 'w', encoding='utf-8')

    def write(self, msg: str):
        ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] {msg}"
        print(msg)
        self.file_handle.write(line + '\n')
        self.file_handle.flush()

    def close(self):
        if self.file_handle:
            self.file_handle.close()


logger = Logger(LOG_FILE)


class Timer:
    def __init__(self, name: str):
        self.name    = name
        self.elapsed = 0

    def __enter__(self):
        self._t0 = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = time.time() - self._t0

    def format(self):
        e = self.elapsed
        if e < 1:    return f"{e*1000:.0f}ms"
        if e < 60:   return f"{e:.1f}s"
        return f"{int(e//60)}m {e%60:.0f}s"


# =========================
# Helpers
# =========================

def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def _set_mtime(path: str):
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))


def sanitize(obj):
    if isinstance(obj, dict):  return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):  return [sanitize(v) for v in obj]
    if isinstance(obj, datetime): return obj.isoformat()
    if isinstance(obj, date):     return obj.isoformat()
    try:
        if pd.isna(obj): return None
    except Exception:
        pass
    if hasattr(obj, 'item'):
        try: return obj.item()
        except Exception: pass
    return obj


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


# =========================
# Conexão
# =========================

def build_conn_str(posto: str):
    p    = (posto or "").strip().upper()
    host = _env(f"DB_HOST_{p}")
    base = _env(f"DB_BASE_{p}")
    if not host or not base:
        return None
    user       = _env(f"DB_USER_{p}")
    pwd        = _env(f"DB_PASSWORD_{p}")
    port       = _env(f"DB_PORT_{p}", "1433") or "1433"
    encrypt    = _env("DB_ENCRYPT",    "yes")
    trust_cert = _env("DB_TRUST_CERT", "yes")
    timeout    = _env("DB_TIMEOUT",    "20")
    server     = f"tcp:{host},{port}"
    common     = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server};DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
    )
    return common + (f"UID={user};PWD={pwd}" if user else "Trusted_Connection=yes")


def make_engine(odbc_str: str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}",
        pool_pre_ping=True,
        future=True,
    )


# =========================
# Status
# =========================

# =========================
# Especialidade da agenda × especialidade do cad_cbos
# =========================
# A agenda nomeia a ÁREA ("CARDIOLOGIA"); o cad_cbos, que guarda os prazos,
# nomeia o PROFISSIONAL ("CARDIOLOGISTA"). O join era por nome exato e acertava
# 17 de 60: nas outras 43 o prazo vinha 0, e como calc_status manda tudo sem
# prazo para CRITICO, o mapa de calor pintava 261 de 295 células de vermelho —
# inclusive CLÍNICA GERAL com ZERO dia de espera. Medido em 11/09/2026; com o
# casamento por radical o mapa vira 232 OK, 28 alerta e 35 críticos de verdade.
#
# Ao mexer aqui, rodar o de-para inteiro e conferir a olho: é matching por
# aproximação, e errar o par é pior que não casar. Dois pares que JÁ deram
# errado e por isso existem os pisos: NEUROPSICOLOGIA não pode virar
# NEUROPEDIATRA (prefixo comum NEUROP), nem PSICOLOGIA virar PSICOPEDAGOGO.

import re
import unicodedata

_STOP = {"DE", "DA", "DO", "DAS", "DOS", "E", "COM", "EM", "A", "O", "ON", "LINE"}

# Sufixos que distinguem profissão de área. O radical é o que sobra.
_SUFIXOS = [
    ("OLOGISTAS", "OLOG"), ("OLOGISTA", "OLOG"), ("OLOGOS", "OLOG"),
    ("OLOGOG", "OLOG"), ("OLOGO", "OLOG"), ("OLOGIA", "OLOG"),
    ("IATRIA", "IATR"), ("IATRA", "IATR"), ("IATRICO", "IATR"), ("IATRICA", "IATR"),
    ("ETRISTA", "ETR"), ("ETRIA", "ETR"),
    ("TERAPEUTA", "TERAP"), ("TERAPIA", "TERAP"),
    ("GRAFISTA", "GRAF"), ("GRAFIA", "GRAF"),
    ("SCOPISTA", "SCOP"), ("SCOPIA", "SCOP"),
    ("CIRURGIAO", "CIRURG"), ("CIRURGICO", "CIRURG"), ("CIRURGIA", "CIRURG"),
    ("CLINICO", "CLINIC"), ("CLINICA", "CLINIC"),
    ("MEDICO", "MEDIC"), ("MEDICINA", "MEDIC"),
    ("OBSTETRICIA", "OBSTETR"), ("OBSTETRA", "OBSTETR"), ("OBSTETRICO", "OBSTETR"),
    ("NUTRICIONISTA", "NUTRIC"), ("NUTRICAO", "NUTRIC"),
    ("PLASTICO", "PLASTIC"), ("PLASTICA", "PLASTIC"),
    ("PEDAGOGIA", "PEDAGOG"), ("PEDAGOGO", "PEDAGOG"),
    ("HOMEOPATIA", "HOMEOPAT"), ("HOMEOPATA", "HOMEOPAT"),
    ("ISTA", ""), ("ISTAS", ""),
]


def _sem_acento(t):
    return "".join(c for c in unicodedata.normalize("NFD", t)
                   if unicodedata.category(c) != "Mn")


def radical(token):
    t = token
    for suf, troca in _SUFIXOS:
        # `>=` e não `>`: quando o token INTEIRO é o sufixo (NUTRICAO,
        # CIRURGIA, CLINICA, MEDICINA, OBSTETRICIA) a regra também vale.
        if t.endswith(suf) and len(t) >= len(suf):
            return t[: -len(suf)] + troca
    return t


def tokens(nome):
    t = _sem_acento(str(nome or "")).upper()
    t = t.replace("-", "")   # ULTRA-SONOGRAFISTA e uma palavra so
    t = re.sub(r"SS", "S", t)          # ULTRASSONOGRAFIA ≈ ULTRA-SONOGRAFISTA
    t = re.sub(r"[^A-Z0-9 ]+", " ", t)
    partes = [p for p in t.split() if p and p not in _STOP]
    return [radical(p) for p in partes]


def _prefixo_comum(a, b):
    n = 0
    for x, y in zip(a, b):
        if x != y:
            break
        n += 1
    return n


def _casa_token(a, b, minimo=5, minimo_prefixo=9):
    """Dois radicais são o mesmo se um é prefixo do outro (ALERG ⊂ ALERGOLOG)
    ou se compartilham prefixo longo (HOMEOPATIA ≈ HOMEOPATA).

    O piso alto no prefixo comum é o que separa NEUROPSICOLOG de NEUROPEDIATR
    (comum = NEUROP, 6) e PSICOLOG de PSICOPEDAGOG (comum = PSICO, 5) — esses
    NÃO podem casar, são especialidades diferentes. Com 6 o matcher pôs
    NEUROPSICOLOGIA em NEUROPEDIATRA.
    """
    if a == b:
        return True
    curto, longo = (a, b) if len(a) <= len(b) else (b, a)
    if len(curto) >= minimo and longo.startswith(curto):
        return True
    return _prefixo_comum(a, b) >= minimo_prefixo


def _peso(x, lista):
    """1.0 quando o radical é idêntico, 0.85 quando só se parece.

    Sem isso, NEUROPSICOLOGIA empatava 1.0 com NEUROPSICÓLOGO (igual) e
    NEUROPEDIATRA (parecido), e o desempate por nome mais curto escolhia o
    errado. Igualdade tem que valer mais que semelhança.
    """
    if any(x == y for y in lista):
        return 1.0
    return 0.85 if any(_casa_token(x, y) for y in lista) else 0.0


def pontua(ag, cb):
    """0..1 — quanto os dois nomes descrevem a mesma coisa."""
    if not ag or not cb:
        return 0.0
    casados = sum(_peso(x, cb) for x in ag)
    casados_inv = sum(_peso(y, ag) for y in cb)
    # penaliza sobra dos dois lados: "NEFROLOGIA" não deve virar
    # "ENFERMEIRO NEFROLOGISTA" quando existe "NEFROLOGISTA"
    return (casados + casados_inv) / (len(ag) + len(cb))


_CACHE_CASAMENTO = {}


def casar_cbos(especialidade, cbos_map):
    """Chave do cad_cbos para esta especialidade de agenda, ou None.

    Com cache: são 295 linhas para ~60 especialidades distintas, e cada
    resolução varre as 158 chaves do cad_cbos.
    """
    if especialidade in _CACHE_CASAMENTO:
        return _CACHE_CASAMENTO[especialidade]
    chave, _score = casar(especialidade, cbos_map)
    _CACHE_CASAMENTO[especialidade] = chave
    return chave


def casar(especialidade, chaves_cbos, minimo=0.6):
    """Devolve (chave_do_cbos, score) ou (None, 0.0) se nada for bom o bastante."""
    if especialidade in chaves_cbos:
        return especialidade, 1.0
    ag = tokens(especialidade)
    melhor, melhor_score = None, 0.0
    for chave in chaves_cbos:
        s = pontua(ag, tokens(chave))
        if s > melhor_score or (s == melhor_score and melhor and len(chave) < len(melhor)):
            melhor, melhor_score = chave, s
    return (melhor, melhor_score) if melhor_score >= minimo else (None, melhor_score)


def calc_status(dias, prazo_ans: int, prazo_camim: int) -> str:
    """
    SEM_VAGA  → sem data de próxima vaga
    OK        → dias <= prazo_ans  (dentro do prazo ANS)
    ALERTA    → dias <= prazo_camim (além do ANS mas dentro do prazo CAMIM)
    CRITICO   → além de ambos os prazos
    """
    if prazo_ans <= 0 and prazo_camim <= 0:
        # Sem prazo cadastrado não dá para dizer se está no prazo. Antes isto
        # caía em CRITICO e virava alarme falso; agora tem cor própria (cinza)
        # e sai da conta de crítico — é pendência de CADASTRO, não de agenda.
        return "SEM_PRAZO"
    if dias is None:
        return "SEM_VAGA"
    if prazo_ans > 0 and dias <= prazo_ans:
        return "OK"
    if prazo_camim > 0 and dias <= prazo_camim:
        return "ALERTA"
    return "CRITICO"


# =========================
# Execução principal
# =========================

def run():
    meta = ETLMeta('export_qualidade_agenda', 'json_consolidado')

    load_dotenv(os.path.join(BASE_DIR, ".env"))
    ensure_dir(JSON_DIR)

    total_start = time.time()
    hoje        = date.today()

    logger.write("\n" + "="*70)
    logger.write("EXPORT QUALIDADE DA AGENDA MEDICA")
    logger.write(f"  Fonte: posto master '{POSTO_MASTER}' (Anchieta)")
    logger.write(f"  Data referencia: {hoje.isoformat()}")
    logger.write("="*70)

    conn_str = build_conn_str(POSTO_MASTER)
    if not conn_str:
        logger.write(f"ERRO FATAL: sem configuracao de conexao para '{POSTO_MASTER}'")
        meta.error('geral', f"sem configuracao de conexao para '{POSTO_MASTER}'")
        meta.save()
        sys.exit(1)

    eng = make_engine(conn_str)

    # ── 1. cad_endereco → mapa idEndereco → {letra, nome} ────────────────────
    logger.write(f"\n[ETAPA 1/4] Carregando cad_endereco...")
    endereco_map = {}   # {idEndereco: {letra, nome}}
    letra_map    = {}   # {letra: {idEndereco, nome}}  (para postos_info)

    try:
        with Timer("cad_endereco") as t:
            with eng.connect() as con:
                rows = con.execute(text(SQL_CAD_ENDERECO)).fetchall()

        for row in rows:
            id_end = int(row[0])
            letra  = str(row[1] or "").strip().upper()
            nome   = str(row[2] or "").strip()
            if letra:
                endereco_map[id_end] = {"letra": letra, "nome": nome}
                letra_map[letra]     = {"letra": letra, "idEndereco": id_end, "nome": nome}

        # Complementar com fallback hardcoded para ids que possam não estar
        for id_end, letra in ID_ENDERECO_TO_LETRA.items():
            if id_end not in endereco_map:
                endereco_map[id_end] = {"letra": letra, "nome": letra}

        logger.write(f"  OK: {len(endereco_map)} postos mapeados ({t.format()})")
    except Exception as e:
        logger.write(f"  ERRO cad_endereco: {e} — usando mapeamento hardcoded")
        for id_end, letra in ID_ENDERECO_TO_LETRA.items():
            endereco_map[id_end] = {"letra": letra, "nome": letra}

    # ── 2. cad_cbos → prazos e thresholds ────────────────────────────────────
    logger.write(f"\n[ETAPA 2/4] Carregando cad_cbos...")
    cbos_map = {}

    try:
        with Timer("cad_cbos") as t:
            with eng.connect() as con:
                rows = con.execute(text(SQL_CAD_CBOS)).fetchall()

        for row in rows:
            esp = str(row[0] or "").strip().upper()
            cbos_map[esp] = {
                "prazoconsultaans":           int(row[1] or 0),
                "prazoconsultacamim":         int(row[2] or 0),
                "ValorPMinimoVagaDisponivel": float(row[3] or 0.0),
            }
        logger.write(f"  OK: {len(cbos_map)} especialidades ({t.format()})")
    except Exception as e:
        logger.write(f"  ERRO cad_cbos: {e}")
        meta.error('geral', str(e))
        meta.save()
        sys.exit(1)

    # ── 3. Calendário de vagas gerado do cad_agenda — todos os postos de uma vez ──
    logger.write(f"\n[ETAPA 3/4] Gerando calendário de vagas do cad_agenda ({HORIZONTE_DIAS} dias)...")

    all_dados     = []
    postos_vistos = set()

    try:
        with Timer("calendario") as t:
            with eng.connect() as con:
                rows = con.execute(text(SQL_CALENDARIO)).fetchall()
        logger.write(f"  OK: {len(rows)} linhas posto×especialidade×dia ({t.format()})")

        # (idEndereco, ESPECIALIDADE) → {offset: [offset, vagas, capacidade, máximo, reservadas,
        #                                          dias de reserva, vagas SEM restritas, capacidade SEM restritas]}
        calendario = {}
        for id_end_l, esp_l, off_l, _data_l, cap_l, vagas_l, max_l, res_l, resd_l, cap_sr_l, vagas_sr_l in rows:
            k = (int(id_end_l), str(esp_l or "").strip().upper())
            off = int(off_l)
            calendario.setdefault(k, {})[off] = [off, int(vagas_l or 0), int(cap_l or 0),
                                                 int(max_l or 0), int(res_l or 0), int(resd_l or 0),
                                                 int(vagas_sr_l or 0), int(cap_sr_l or 0)]
        logger.write(f"  {len(calendario)} pares posto×especialidade com agenda ativa no horizonte")

        def _safe_float(v):
            try:
                f = float(v)
                return None if pd.isna(f) else round(f, 2)
            except Exception:
                return None

        def _safe_int(v):
            try:   return int(v)
            except Exception: return None

        sem_vaga = 0
        for (id_end, esp), dias_map in sorted(calendario.items()):
            info  = endereco_map.get(id_end, {})
            letra = info.get("letra", f"?{id_end}")
            nome  = info.get("nome",  str(id_end))

            chave_cbos = casar_cbos(esp, cbos_map)
            cbos_info  = cbos_map.get(chave_cbos or esp,
                                      {"prazoconsultaans": 0, "prazoconsultacamim": 0,
                                       "ValorPMinimoVagaDisponivel": 0.0})
            prazo_ans  = cbos_info["prazoconsultaans"]
            prazo_cam  = cbos_info["prazoconsultacamim"]
            minimo     = cbos_info["ValorPMinimoVagaDisponivel"]

            lista_dias   = [dias_map[o] for o in sorted(dias_map)]
            lista_livres = [x for x in lista_dias if x[1] > 0]      # dias com >= 1 vaga, sem o % mínimo

            # Próxima vaga = primeiro dia com >= 1 vaga E % livre (soma do dia)
            # >= mínimo do cad_cbos casado por radical.
            escolhido = proxima_vaga(lista_livres, minimo)
            if escolhido:
                _off, _vagas, _cap, _max, _res, _resd, _v_sr, _c_sr = escolhido
                dias          = _off
                data_str      = (hoje + timedelta(days=_off)).isoformat()
                vagas_na_data = _vagas
                cap_na_data   = _cap
                pct_na_data   = pct_livre(_vagas, _cap)
                total_medicos = _cap
                reservadas    = _res
                reserva_dias  = _resd
                total_c_res   = _max
            else:
                sem_vaga += 1
                dias = None; data_str = None
                vagas_na_data = cap_na_data = pct_na_data = None
                total_medicos = reservadas = reserva_dias = total_c_res = None
            status = calc_status(dias, prazo_ans, prazo_cam)

            postos_vistos.add(letra)

            all_dados.append({
                "posto":            letra,
                "idEndereco":       id_end,
                "Endereco":         nome,
                "Especialidade":    esp,
                "DataProximaVaga":  data_str,
                "DiasAteProximaVaga": dias,
                "QuantidadeVagasDisponivelNaData":                    vagas_na_data,
                "QuantidadeVagasTotalMedicosAtendem":                 total_medicos,
                # mínimo do cad_cbos CASADO (não o do join exato, que era None em 46 de 59)
                "ValorPMinimoVagaDisponivel":                         _safe_float(minimo),
                "ValorPercentualVagasLivres":                         pct_na_data,
                "QuantidadeVagasReservadas":                          reservadas,
                "QuantidadeReservaDias":                              reserva_dias,
                "CapacidadeConsiderada":                              cap_na_data,
                "QuantidadeVagasTotalMedicosAtendemIncluindoReserva": total_c_res,
                "cbos_casado":       chave_cbos,
                "prazoconsultaans":  prazo_ans,
                "prazoconsultacamim": prazo_cam,
                "Status":            status,
                # [[offset, vagas, capacidade, vagas_sem_restritas, cap_sem_restritas], ...]
                # ordenado por offset — TODO dia com >= 1 vaga, SEM o % mínimo aplicado: a
                # tela aplica (diaPassa) com o % atual do cad_cbos e escolhe o par conforme
                # o interruptor "Incluir agendas restritas".
                "dias_livres":       [[x[0], x[1], x[2], x[6], x[7]] for x in lista_livres],
                # alguma agenda restrita (PermitirAgendamentoquenuncaconsultou = 0) no horizonte
                "tem_restrita":      any(x[7] < x[2] for x in lista_dias),
            })

        logger.write(f"  Sem vaga no horizonte (>= 1 vaga e % mínimo): {sem_vaga} de {len(all_dados)} linhas")

        # Resumo por posto
        from collections import Counter
        por_posto = Counter(d["posto"] for d in all_dados)
        for letra_p, cnt in sorted(por_posto.items()):
            info_p = letra_map.get(letra_p, endereco_map.get(None, {}))
            nome_p = letra_map.get(letra_p, {}).get("nome", letra_p)
            logger.write(f"  [{letra_p}] {nome_p}: {cnt} especialidades")

    except Exception as e:
        logger.write(f"  ERRO consulta vagas: {e}")
        meta.error('geral', str(e))
        meta.save()
        sys.exit(1)

    # ── 4. Salvar JSON ────────────────────────────────────────────────────────
    logger.write(f"\n[ETAPA 4/4] Gerando JSON...")

    postos_info       = {l: letra_map[l] for l in sorted(postos_vistos) if l in letra_map}
    especialidades    = sorted(set(d["Especialidade"] for d in all_dados if d["Especialidade"]))
    postos_ordenados  = sorted(postos_vistos)

    payload = sanitize({
        "meta": {
            "gerado_em":       datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "data_referencia": hoje.isoformat(),
            "origem":          "export_qualidade_agenda.py",
            "total_registros": len(all_dados),
        },
        "cbos":           cbos_map,
        "especialidades": especialidades,
        "postos":         postos_ordenados,
        "postos_info":    postos_info,
        "dados":          all_dados,
    })

    out_path = os.path.join(JSON_DIR, "qualidade_agenda.json")
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, out_path)
    _set_mtime(out_path)

    # Snapshot diário — permite consulta histórica
    snap_dir  = os.path.join(JSON_DIR, "qualidade_agenda")
    ensure_dir(snap_dir)
    snap_path = os.path.join(snap_dir, f"{hoje.isoformat()}.json")
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _set_mtime(snap_path)
    logger.write(f"  Snapshot diário: {snap_path}")

    elapsed = time.time() - total_start
    logger.write(f"  JSON salvo: {out_path}")
    logger.write(f"  Total registros: {len(all_dados)}")
    logger.write(f"  Postos: {len(postos_ordenados)} — {', '.join(postos_ordenados)}")
    logger.write(f"  Especialidades: {len(especialidades)}")
    logger.write(f"  Tempo total: {elapsed:.1f}s")
    # Registra cada posto presente nos dados como OK no meta — assim o widget
    # ETLStatus mostra feedback granular por posto. A query é única (view
    # consolidada via Anchieta), mas conceitualmente cobre todos os postos.
    for letra in sorted(postos_vistos):
        meta.ok(letra)
    meta.save()

    logger.write(f"\nFinalizado export_qualidade_agenda.")
    logger.write(f"Log salvo em: {LOG_FILE}\n")
    logger.close()


if __name__ == "__main__":
    run()
