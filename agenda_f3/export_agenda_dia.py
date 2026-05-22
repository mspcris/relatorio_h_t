#!/usr/bin/env python3
"""
export_agenda_dia.py — ETL Agenda do Dia (agenda_f3, independente do KPI)

Fluxo:
  Fase 1: Conecta em todos os postos (paralelo)
  Fase 2: Busca agendas de cada posto (paralelo)
  Fase 3: Agrupa matrículas por posto de ORIGEM (cfcliente, cross-posto)
  Fase 4: Busca status + pagamento por posto de origem (paralelo)
  Fase 5: Identifica postos "fully valid" (todas as fases sem erro)
  Fase 6: Para cada posto válido → REPLACE atômico em Postgres (f3.agenda_dia)
  Fase 7: Para postos com falha → mark_posto_failure (snapshot antigo permanece)
  Fase 8: Escreve JSON consolidado em disco (fallback se Postgres cair)

ATOMICIDADE: um posto P só tem o snapshot reescrito se TUDO necessário
foi baixado com sucesso (conexão + agenda + status/pagou de TODAS as
origens cross-posto). Se faltar algo, o snapshot antigo permanece intacto
e a UI mostra banner alertando "última atualização há X minutos".

Saída:
  - Postgres f3.agenda_dia (fonte primária)
  - JSON: /opt/agenda_f3/json_consolidado/agenda_dia.json (fallback)
"""
import os, sys, json, time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Path relativo ao próprio script — funciona via cron como root sem cwd.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# CRÍTICO: load_dotenv ANTES de importar f3_db, porque f3_db cria o engine
# do Postgres em tempo de import (precisa de PG_RDS_HOST/USER/PASSWORD).
load_dotenv(os.path.join(BASE_DIR, ".env"))

from f3_db import replace_posto, mark_posto_failure, update_run


# =========================
# Constantes
# =========================

JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR  = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, f"export_agenda_dia_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
OUT_FILE = os.path.join(JSON_DIR, "agenda_dia.json")

POSTOS      = list("ABCDGIJMNPRXY")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
MAX_WORKERS = 6


# =========================
# Logging
# =========================

class Logger:
    def __init__(self, log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.fh = open(log_file, 'w', encoding='utf-8')

    def write(self, msg: str):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] {msg}"
        print(msg, flush=True)
        self.fh.write(line + '\n')
        self.fh.flush()

    def close(self):
        self.fh.close()


def fmt_time(elapsed):
    if elapsed < 1:    return f"{elapsed*1000:.0f}ms"
    if elapsed < 60:   return f"{elapsed:.1f}s"
    return f"{int(elapsed//60)}m {elapsed%60:.0f}s"


logger = Logger(LOG_FILE)


# =========================
# Helpers
# =========================

def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def build_conn_str(posto: str):
    p = posto.strip().upper()
    host = _env(f"DB_HOST_{p}")
    base = _env(f"DB_BASE_{p}")
    if not host or not base:
        return None
    user       = _env(f"DB_USER_{p}")
    pwd        = _env(f"DB_PASSWORD_{p}")
    port       = _env(f"DB_PORT_{p}", "1433") or "1433"
    encrypt    = _env("DB_ENCRYPT", "yes")
    trust_cert = _env("DB_TRUST_CERT", "yes")
    timeout    = _env("DB_TIMEOUT", "20")
    server     = f"tcp:{host},{port}"
    common = (
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


def safe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def format_hora(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%H:%M")
    if isinstance(v, timedelta):
        total = int(v.total_seconds())
        return f"{total // 3600:02d}:{(total % 3600) // 60:02d}"
    return str(v)[:5]


# =========================
# FASE 1 — Conectar
# =========================

def connect_posto(posto: str):
    conn_str = build_conn_str(posto)
    if not conn_str:
        return posto, None, "sem config no .env"
    try:
        eng = make_engine(conn_str)
        with eng.connect() as con:
            con.execute(text("SELECT 1"))
        return posto, eng, None
    except Exception as e:
        return posto, None, str(e)


def connect_all_postos():
    engines = {}
    failures = {}
    logger.write("")
    logger.write("FASE 1 — Conectando em todos os postos (paralelo)...")
    logger.write("-" * 70)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(connect_posto, p): p for p in POSTOS}
        for future in as_completed(futures):
            posto, eng, err = future.result()
            if eng:
                engines[posto] = eng
                logger.write(f"  [{posto}] ✓ conectado")
            else:
                failures[posto] = err
                logger.write(f"  [{posto}] ✗ {err}")
    logger.write(f"  → {len(engines)}/{len(POSTOS)} postos online | {fmt_time(time.time()-t0)}")
    return engines, failures


# =========================
# FASE 2 — Agendas
# =========================

SQL_AGENDA = """
SET NOCOUNT ON;
SELECT
    idendereco,
    matricula,
    codigo       AS cfcliente,
    paciente,
    idadePaciente,
    especialidade,
    nomemedico   AS medico,
    HoraPrevistaConsulta,
    CONVERT(varchar(5), dataconfirmacaoconsulta, 108) AS hora_confirmacao,
    Dif_dias_agend_cons,
    Atendido,
    Desistencia
FROM vw_Cad_LancamentoProntuarioComDesistencia
WHERE dataconsulta >= :dt_ini
  AND dataconsulta <  :dt_fim
ORDER BY nomemedico, HoraPrevistaConsulta ASC
"""


def fetch_agenda_posto(eng, posto: str, dt: date):
    """Busca agenda de um posto/data. Retorna (posto, dt_iso, rows, err)."""
    dt_dmy  = dt.strftime("%d/%m/%Y")
    dt_next = (dt + timedelta(days=1)).strftime("%d/%m/%Y")
    dt_iso  = dt.isoformat()
    try:
        with eng.connect() as con:
            rows = con.execute(text(SQL_AGENDA),
                               {"dt_ini": dt_dmy, "dt_fim": dt_next}).mappings().all()
        return posto, dt_iso, list(rows), None
    except Exception as e:
        return posto, dt_iso, [], str(e)


def fetch_all_agendas(engines: dict, datas: list):
    agendas = defaultdict(dict)
    erros = defaultdict(list)
    logger.write("")
    logger.write("FASE 2 — Buscando agendas (paralelo)...")
    logger.write("-" * 70)
    t0 = time.time()
    tasks = [(eng, p, dt) for p, eng in engines.items() for dt in datas]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_agenda_posto, eng, p, dt): (p, dt) for eng, p, dt in tasks}
        for future in as_completed(futures):
            posto, dt_iso, rows, err = future.result()
            agendas[posto][dt_iso] = rows
            if err:
                erros[posto].append(f"agenda {dt_iso}: {err}")
                logger.write(f"  [{posto}] {dt_iso}: ERRO — {err}")
            else:
                logger.write(f"  [{posto}] {dt_iso}: {len(rows)} pacientes")
    total = sum(len(r) for p in agendas.values() for r in p.values())
    logger.write(f"  → {total} registros | {fmt_time(time.time()-t0)}")
    return agendas, erros


# =========================
# FASE 3 — Cad_endereco + agregação cross-posto
# =========================

def load_endereco_map(engines: dict):
    id_to_letra = {}
    letra_to_id = {}
    for p, eng in engines.items():
        try:
            with eng.connect() as con:
                rows = con.execute(text(
                    "SET NOCOUNT ON; SELECT idEndereco, Codigo FROM cad_endereco"
                )).mappings().all()
            for r in rows:
                id_end = int(r["idEndereco"])
                letra = (r["Codigo"] or "").strip().upper()
                if letra:
                    id_to_letra[id_end] = letra
                    letra_to_id[letra] = id_end
            if letra_to_id:
                logger.write(f"  Mapa cad_endereco carregado de [{p}]: {len(letra_to_id)} postos")
                return id_to_letra, letra_to_id
        except Exception:
            continue
    return id_to_letra, letra_to_id


def aggregate_matriculas(agendas: dict):
    """Por posto P agendado → quais postos de ORIGEM (cfcliente) precisamos consultar?

    Retorna:
      mats_por_origem: {letra_origem: set(matriculas)} — pra fase 4
      origens_por_posto: {posto_agenda: set(letras_origem)} — pra fase 5 (validação)
    """
    logger.write("")
    logger.write("FASE 3 — Agrupando matrículas por posto de origem...")
    logger.write("-" * 70)
    mats_por_origem = defaultdict(set)
    origens_por_posto = defaultdict(set)
    for posto, datas in agendas.items():
        for dt_iso, rows in datas.items():
            for r in rows:
                mat = safe_int(r.get("matricula"))
                if not mat:
                    continue
                cf = (str(r.get("cfcliente") or "")).strip().upper()
                letra = cf if cf else posto
                mats_por_origem[letra].add(mat)
                origens_por_posto[posto].add(letra)
    for letra in sorted(mats_por_origem):
        logger.write(f"  [{letra}] {len(mats_por_origem[letra])} matrículas únicas")
    total = sum(len(s) for s in mats_por_origem.values())
    logger.write(f"  → {total} matrículas em {len(mats_por_origem)} origens")
    return mats_por_origem, origens_por_posto


# =========================
# FASE 4 — Status + Pagamento
# =========================

def fetch_status_for_posto(eng, posto: str, matriculas: list, id_endereco: int):
    """Retorna {matricula: {situacao, plano, cobrador_fatura, situacao_clube}}.

    Junta vw_Cad_Cliente pra trazer SituaçãoClube (usado em planos clube como
    Camim Liberty). Agrupa por matrícula com MAX pra evitar duplicatas vindas
    de múltiplas prestações da mesma matrícula.
    """
    status = {}
    for i in range(0, len(matriculas), 500):
        batch = matriculas[i:i+500]
        phs = ",".join(str(m) for m in batch)
        id_filter = f"AND r.idendereco = {id_endereco}" if id_endereco else ""
        sql = f"""
        SET NOCOUNT ON;
        SELECT
            r.matricula,
            MAX(ISNULL(r.[Cliente Situação], '')) AS situacao,
            MAX(ISNULL(r.[Plano], ''))            AS plano,
            MAX(ISNULL(r.[Cobrador fatura], ''))  AS cobrador_fatura,
            MAX(ISNULL(c.SituaçãoClube, ''))      AS situacao_clube
        FROM vw_fin_receita2 r
        LEFT JOIN vw_Cad_Cliente c ON c.idCliente = r.idcliente
        WHERE r.matricula IN ({phs}) {id_filter}
        GROUP BY r.matricula
        """
        with eng.connect() as con:
            rows = con.execute(text(sql)).mappings().all()
        for r in rows:
            status[int(r["matricula"])] = {
                "situacao":        (r["situacao"]        or "").strip(),
                "plano":           (r["plano"]           or "").strip(),
                "cobrador_fatura": (r["cobrador_fatura"] or "").strip(),
                "situacao_clube":  (r["situacao_clube"]  or "").strip(),
            }
    return status


def fetch_pagou_for_posto(eng, posto: str, matriculas: list, id_endereco: int, datas: list):
    pagou_por_data = defaultdict(set)
    for dt in datas:
        dt_dmy  = dt.strftime("%d/%m/%Y")
        dt_next = (dt + timedelta(days=1)).strftime("%d/%m/%Y")
        dt_iso  = dt.isoformat()
        for i in range(0, len(matriculas), 500):
            batch = matriculas[i:i+500]
            phs = ",".join(str(m) for m in batch)
            id_filter = f"AND idendereco = {id_endereco}" if id_endereco else ""
            sql = f"""
            SET NOCOUNT ON;
            SELECT DISTINCT matricula
            FROM vw_fin_receita2
            WHERE matricula IN ({phs}) {id_filter}
              AND idcontatipo = 5
              AND [data prestação] >= :dt_ini
              AND [data prestação] <  :dt_fim
            """
            with eng.connect() as con:
                rows = con.execute(text(sql),
                                   {"dt_ini": dt_dmy, "dt_fim": dt_next}).mappings().all()
            for r in rows:
                pagou_por_data[dt_iso].add(int(r["matricula"]))
    return pagou_por_data


def fetch_financeiro_for_letra(args):
    """Worker: status + pagou de um posto de origem. Retorna erro se algo falhou."""
    posto_letra, matriculas, id_endereco, engines, datas = args
    eng = engines.get(posto_letra)
    if not eng:
        cs = build_conn_str(posto_letra)
        if not cs:
            return posto_letra, {}, {}, f"sem config .env"
        try:
            eng = make_engine(cs)
            with eng.connect() as con:
                con.execute(text("SELECT 1"))
            engines[posto_letra] = eng
        except Exception as e:
            return posto_letra, {}, {}, str(e)

    mat_list = sorted(matriculas)
    try:
        status = fetch_status_for_posto(eng, posto_letra, mat_list, id_endereco)
        pagou  = fetch_pagou_for_posto(eng, posto_letra, mat_list, id_endereco, datas)
        return posto_letra, status, pagou, None
    except Exception as e:
        return posto_letra, {}, {}, str(e)


def fetch_all_financeiro(engines: dict, mats_por_origem: dict, letra_to_id: dict, datas: list):
    logger.write("")
    logger.write("FASE 4 — Buscando status/pagou por posto de origem (paralelo)...")
    logger.write("-" * 70)
    t0 = time.time()
    status_global = {}
    pagou_global  = defaultdict(set)
    origens_ok    = set()       # letras de origem que sucederam
    origens_err   = {}          # {letra: erro}
    tasks = []
    for letra, mats in mats_por_origem.items():
        id_end = letra_to_id.get(letra)
        tasks.append((letra, mats, id_end, engines, datas))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_financeiro_for_letra, t): t[0] for t in tasks}
        for future in as_completed(futures):
            letra, status, pagou, err = future.result()
            if err:
                origens_err[letra] = err
                logger.write(f"  [{letra}] ✗ {err}")
            else:
                origens_ok.add(letra)
                status_global.update(status)
                for dt_iso, mats in pagou.items():
                    pagou_global[dt_iso].update(mats)
                logger.write(f"  [{letra}] ✓ {len(status)} status | pagou: {sum(len(s) for s in pagou.values())}")
    logger.write(f"  → {len(origens_ok)} origens OK, {len(origens_err)} com erro | {fmt_time(time.time()-t0)}")
    return status_global, pagou_global, origens_ok, origens_err


# =========================
# FASE 5 — Validar postos (regra de atomicidade)
# =========================

def validate_postos(engines: dict, agendas: dict, erros_agenda: dict,
                    origens_por_posto: dict, origens_ok: set, origens_err: dict):
    """Para cada posto, determina se TUDO chegou. Posto "válido" = pode ser
    reescrito. Posto inválido → snapshot antigo permanece intocado."""
    logger.write("")
    logger.write("FASE 5 — Validando completude por posto (regra de atomicidade)...")
    logger.write("-" * 70)
    postos_validos = set()
    postos_invalidos = {}   # {posto: motivo}

    for posto in POSTOS:
        if posto not in engines:
            postos_invalidos[posto] = "conexão falhou"
            continue
        if erros_agenda.get(posto):
            postos_invalidos[posto] = "; ".join(erros_agenda[posto])
            continue
        # Verifica cross-posto: todas as origens referenciadas por este posto
        # precisam ter sucedido em status/pagou.
        origens = origens_por_posto.get(posto, set())
        faltando = [o for o in origens if o not in origens_ok]
        if faltando:
            erros_str = ", ".join(f"{o}({origens_err.get(o,'?')})" for o in faltando)
            postos_invalidos[posto] = f"origem(ns) c/ erro: {erros_str}"
            continue
        postos_validos.add(posto)

    for p in sorted(postos_validos):
        logger.write(f"  [{p}] ✓ válido — será reescrito")
    for p, motivo in sorted(postos_invalidos.items()):
        logger.write(f"  [{p}] ⊘ inválido — {motivo}")
    logger.write(f"  → {len(postos_validos)} válidos, {len(postos_invalidos)} preservados")
    return postos_validos, postos_invalidos


# =========================
# FASE 6 — Montar pacientes + REPLACE atômico
# =========================

def resolve_situacao(matricula, status_data):
    """Aplica as regras de negócio do CAMIM pra Situação Financeira:

      1. matrícula == 0           → "Particular" (regra de ouro — vence tudo)
      2. sem dados financeiros    → "Égide" (matrícula > 0 mas não está em vw_fin_receita2)
      3. Plano "Camim Liberty"    → SituaçãoClube (vw_Cad_Cliente)
      4. Plano vazio + Cobrador "ÉGIDE" → "Égide"
      5. Plano vazio + outro     → Cliente Situação (fallback)
      6. Outros planos            → Cliente Situação (regra atual)

    Validado com cliente em 2026-05-21 (incidente Malvinete/Nathan/Letícia).
    """
    # 1) Regra de ouro
    if not matricula or matricula == 0:
        return "Particular"

    # 2) Sem dados financeiros pra essa matrícula
    if not status_data:
        return "Égide"

    plano    = (status_data.get("plano")           or "").strip()
    cobrador = (status_data.get("cobrador_fatura") or "").strip()
    sit_view = (status_data.get("situacao")        or "").strip()
    sit_club = (status_data.get("situacao_clube")  or "").strip()

    # 3) Liberty (clube) → SituaçãoClube
    if "liberty" in plano.lower():
        return sit_club or sit_view or "(sem info)"

    # 4) Sem plano + Cobrador ÉGIDE → Égide
    if not plano:
        cob_upper = cobrador.upper()
        if "ÉGIDE" in cob_upper or "EGIDE" in cob_upper:
            return "Égide"
        # 5) Sem plano + outro cobrador → fallback Cliente Situação
        return sit_view or "Égide"

    # 6) Outros planos → Cliente Situação (regra atual)
    return sit_view or "(sem info)"


def build_pacientes(rows, dt_iso: str, status_global: dict, pagou_global: dict):
    pacientes = []
    exame_visto = set()
    for r in rows:
        mat = safe_int(r.get("matricula"))
        esp = (r.get("especialidade") or "").strip()
        if esp.upper() == "EXAME" and mat:
            medico = (r.get("medico") or "").strip()
            chave = f"{mat}_{medico}_EXAME"
            if chave in exame_visto:
                continue
            exame_visto.add(chave)
        cf = (str(r.get("cfcliente") or "")).strip().upper()
        # Aplica regras de negócio (matrícula 0 = Particular, Liberty = clube, etc)
        situacao = resolve_situacao(mat, status_global.get(mat) if mat else None)
        pagou = mat in pagou_global.get(dt_iso, set()) if mat else False
        pacientes.append({
            "matricula":        mat,
            "cfcliente":        cf,
            "posto_cliente":    cf if cf else "?",
            "paciente":         (r.get("paciente") or "").strip(),
            "idade":            safe_int(r.get("idadePaciente")),
            "especialidade":    esp,
            "medico":           (r.get("medico") or "").strip(),
            "hora_prevista":    format_hora(r.get("HoraPrevistaConsulta")),
            "hora_confirmacao": (r.get("hora_confirmacao") or "").strip(),
            "dias_agend_cons":  safe_int(r.get("Dif_dias_agend_cons")),
            "atendido":         (str(r.get("Atendido") or "")).strip(),
            "desistencia":      1 if r.get("Desistencia") else 0,
            "situacao":         situacao,
            "pagou_no_dia":     pagou,
            "idendereco":       safe_int(r.get("idendereco")),
        })
    return pacientes


def write_postos_validos(postos_validos: set, agendas: dict, datas: list,
                         status_global: dict, pagou_global: dict, gerado_em):
    """Para cada posto válido, monta pacientes (todas as datas) e faz REPLACE atômico."""
    logger.write("")
    logger.write("FASE 6 — Escrevendo em Postgres (transação por posto)...")
    logger.write("-" * 70)
    write_errs = {}
    dados_para_json = {}          # {posto: {dt_iso: [pacientes]}}  — só dos válidos
    total_pacientes = 0

    for posto in sorted(postos_validos):
        try:
            todos_pacientes = []   # pra fase 6: pacientes c/ campo "data"
            dados_para_json[posto] = {}
            for dt in datas:
                dt_iso = dt.isoformat()
                rows = agendas[posto].get(dt_iso, [])
                pacs = build_pacientes(rows, dt_iso, status_global, pagou_global)
                dados_para_json[posto][dt_iso] = pacs
                for p in pacs:
                    p_with_date = dict(p)
                    p_with_date["data"] = dt
                    todos_pacientes.append(p_with_date)

            # REPLACE atômico (BEGIN/DELETE/INSERT/COMMIT)
            replace_posto(posto, datas, todos_pacientes, gerado_em)
            n = len(todos_pacientes)
            total_pacientes += n
            logger.write(f"  [{posto}] ✓ {n} registros gravados")
        except Exception as e:
            write_errs[posto] = str(e)
            logger.write(f"  [{posto}] ✗ ERRO no COMMIT — {e}")
            # Se o COMMIT falhar, removemos do dados_para_json também — não
            # queremos JSON refletindo algo que não foi pra Postgres.
            dados_para_json.pop(posto, None)

    return dados_para_json, write_errs, total_pacientes


# =========================
# FASE 7 — Marcar falhas
# =========================

def mark_failures(postos_invalidos: dict, write_errs: dict, gerado_em):
    logger.write("")
    logger.write("FASE 7 — Marcando falhas em agenda_dia_meta...")
    logger.write("-" * 70)
    todas_falhas = {**postos_invalidos, **write_errs}
    for posto, motivo in sorted(todas_falhas.items()):
        try:
            mark_posto_failure(posto, motivo, gerado_em)
            logger.write(f"  [{posto}] marcado falha — snapshot antigo preservado")
        except Exception as e:
            logger.write(f"  [{posto}] FALHA ao marcar meta: {e}")
    return len(todas_falhas)


# =========================
# FASE 8 — JSON fallback
# =========================

def write_json_fallback(dados_validos: dict, datas: list, gerado_em):
    """Escreve JSON consolidado com TODOS os postos válidos. Fallback se
    Postgres cair. Se um posto não estava em dados_validos, ele simplesmente
    não aparece no JSON (igual à versão Postgres)."""
    logger.write("")
    logger.write("FASE 8 — Escrevendo JSON fallback...")
    logger.write("-" * 70)
    os.makedirs(JSON_DIR, exist_ok=True)
    payload = {
        "meta": {
            "gerado_em": gerado_em.isoformat(timespec="seconds"),
            "datas": [d.isoformat() for d in datas],
            "postos_no_json": sorted(dados_validos.keys()),
            "origem": "export_agenda_dia.py",
        },
        "dados": dados_validos,
    }
    tmp = OUT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)
    os.replace(tmp, OUT_FILE)
    logger.write(f"  → JSON salvo: {OUT_FILE}")


# =========================
# Main
# =========================

def run():
    t0 = time.time()
    iniciado_em = datetime.now(timezone.utc)
    hoje   = date.today()
    amanha = hoje + timedelta(days=1)
    datas  = [hoje, amanha]

    logger.write("=" * 70)
    logger.write("EXPORT AGENDA DO DIA — agenda_f3 (Postgres + JSON fallback)")
    logger.write("=" * 70)
    logger.write(f"  Datas: {hoje.isoformat()}, {amanha.isoformat()}")
    logger.write(f"  Postos: {', '.join(POSTOS)}")
    logger.write(f"  Início: {datetime.now().strftime('%H:%M:%S')}")
    logger.write("=" * 70)

    engines, conn_failures = connect_all_postos()
    if not engines:
        logger.write("ERRO FATAL: nenhum posto online")
        update_run(iniciado_em, datetime.now(timezone.utc),
                   int(time.time()-t0), 0, len(POSTOS), 0)
        sys.exit(1)

    id_to_letra, letra_to_id = load_endereco_map(engines)
    if not letra_to_id:
        logger.write("ERRO FATAL: não conseguiu carregar cad_endereco de nenhum posto")
        update_run(iniciado_em, datetime.now(timezone.utc),
                   int(time.time()-t0), 0, len(POSTOS), 0)
        sys.exit(1)

    agendas, erros_agenda = fetch_all_agendas(engines, datas)
    mats_por_origem, origens_por_posto = aggregate_matriculas(agendas)
    status_global, pagou_global, origens_ok, origens_err = fetch_all_financeiro(
        engines, mats_por_origem, letra_to_id, datas
    )

    postos_validos, postos_invalidos = validate_postos(
        engines, agendas, erros_agenda, origens_por_posto, origens_ok, origens_err
    )

    # Inclui falhas de conexão nos inválidos
    for p, err in conn_failures.items():
        postos_invalidos.setdefault(p, f"conexão: {err}")

    gerado_em = datetime.now(timezone.utc)
    dados_validos, write_errs, total_pacientes = write_postos_validos(
        postos_validos, agendas, datas, status_global, pagou_global, gerado_em
    )

    n_falhas = mark_failures(postos_invalidos, write_errs, gerado_em)

    write_json_fallback(dados_validos, datas, gerado_em)

    terminou_em = datetime.now(timezone.utc)
    duracao = int(time.time() - t0)

    update_run(iniciado_em, terminou_em, duracao,
               len(dados_validos), n_falhas, total_pacientes)

    logger.write("")
    logger.write("=" * 70)
    logger.write("RESUMO FINAL")
    logger.write("=" * 70)
    logger.write(f"  Postos reescritos:        {len(dados_validos)}")
    logger.write(f"  Postos preservados (erro): {n_falhas}")
    logger.write(f"  Total pacientes gravados:  {total_pacientes}")
    logger.write(f"  Duração:                   {fmt_time(duracao)}")
    logger.write(f"  Log:                       {LOG_FILE}")
    logger.close()


if __name__ == "__main__":
    run()
