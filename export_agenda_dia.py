#!/usr/bin/env python3
# export_agenda_dia.py
# ETL: Agenda do Dia — gera JSON com agenda, status financeiro e pagamento
# Roda para cada posto, para hoje e amanhã.
# Gera: json_consolidado/agenda_dia.json
# Cron recomendado: 0 * * * * (a cada hora)
#
# Otimização v2:
#   - Fase 1: conecta em todos os postos em paralelo
#   - Fase 2: busca agendas de todos os postos em paralelo
#   - Fase 3: agrupa matrículas por posto de ORIGEM (cfcliente) cross-posto
#   - Fase 4: busca status/pagamento UMA VEZ por posto de origem (não por consulta)
#   Resultado: ~1-2 min em vez de ~7 min

import os, sys, json, time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from etl_meta import ETLMeta

# =========================
# Constantes
# =========================

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
JSON_DIR    = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR     = os.path.join(BASE_DIR, "logs")
LOG_FILE    = os.path.join(LOG_DIR, f"export_agenda_dia_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
OUT_FILE    = os.path.join(JSON_DIR, "agenda_dia.json")

POSTOS      = list("ABCDGIJMNPRXY")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
MAX_WORKERS = 6   # threads paralelas para conexão/queries


# =========================
# Logging & Timing
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


class Timer:
    def __init__(self, name: str):
        self.name    = name
        self.elapsed = 0

    def __enter__(self):
        self._t0 = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = time.time() - self._t0

    def fmt(self):
        e = self.elapsed
        if e < 1:    return f"{e*1000:.0f}ms"
        if e < 60:   return f"{e:.1f}s"
        return f"{int(e//60)}m {e%60:.0f}s"


class Stats:
    """Acumula estatísticas de queries para relatório final."""
    def __init__(self):
        self.queries     = 0
        self.queries_ok  = 0
        self.queries_err = 0
        self.rows        = 0
        self.db_time     = 0
        self.call_times  = []   # (posto, fase, query, elapsed)
        self.errors      = []   # (posto, fase, query, error)

    def add(self, posto, fase, query, elapsed, ok, rows=0, error=None):
        self.queries += 1
        self.db_time += elapsed
        self.call_times.append((posto, fase, query, elapsed))
        if ok:
            self.queries_ok += 1
            self.rows += rows
        else:
            self.queries_err += 1
            self.errors.append((posto, fase, query, str(error)))

    def report(self):
        logger.write("")
        logger.write("=" * 70)
        logger.write("ESTATÍSTICAS FINAIS")
        logger.write("=" * 70)
        logger.write(f"  Queries executadas: {self.queries} ({self.queries_ok} ok, {self.queries_err} erro)")
        logger.write(f"  Linhas retornadas:  {self.rows}")
        logger.write(f"  Tempo total de DB:  {fmt_time(self.db_time)}")

        # Resumo por posto
        posto_times = {}
        for posto, fase, query, elapsed in self.call_times:
            posto_times.setdefault(posto, []).append(elapsed)
        logger.write("")
        logger.write("RESUMO POR POSTO:")
        logger.write("-" * 70)
        for p in sorted(posto_times):
            times = posto_times[p]
            total = sum(times)
            logger.write(f"  [{p}] {len(times)} queries | Total: {fmt_time(total)} | Média: {fmt_time(total/len(times))}")

        # Top 10 mais lentas
        logger.write("")
        logger.write("TOP 10 QUERIES MAIS LENTAS:")
        logger.write("-" * 70)
        top = sorted(self.call_times, key=lambda x: x[3], reverse=True)[:10]
        for i, (posto, fase, query, elapsed) in enumerate(top, 1):
            logger.write(f"  {i:2d}. [{posto}] {fase} {query} → {fmt_time(elapsed)}")

        # Erros
        if self.errors:
            logger.write("")
            logger.write(f"ERROS ({len(self.errors)}):")
            logger.write("-" * 70)
            for posto, fase, query, err in self.errors:
                logger.write(f"  [{posto}] {fase} {query}: {err}")


logger = Logger(LOG_FILE)
stats  = Stats()


def fmt_time(elapsed):
    if elapsed < 1:    return f"{elapsed*1000:.0f}ms"
    if elapsed < 60:   return f"{elapsed:.1f}s"
    return f"{int(elapsed//60)}m {elapsed%60:.0f}s"


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


def safe_str(v):
    if v is None:
        return None
    return str(v).strip()


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
# FASE 1 — Conectar em todos os postos
# =========================

def connect_posto(posto: str):
    """Tenta conectar em um posto. Retorna (posto, engine) ou (posto, None)."""
    conn_str = build_conn_str(posto)
    if not conn_str:
        return posto, None, "sem config no .env"

    with Timer(f"connect_{posto}") as t:
        try:
            eng = make_engine(conn_str)
            with eng.connect() as con:
                con.execute(text("SELECT 1"))
            return posto, eng, None
        except Exception as e:
            return posto, None, str(e)


def connect_all_postos():
    """Conecta em todos os postos em paralelo. Retorna dict {letra: engine}."""
    engines = {}
    logger.write("")
    logger.write("FASE 1 — Conectando em todos os postos (paralelo)...")
    logger.write("-" * 70)

    with Timer("fase1_total") as t_total:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(connect_posto, p): p for p in POSTOS}
            for future in as_completed(futures):
                posto, eng, err = future.result()
                if eng:
                    engines[posto] = eng
                    logger.write(f"  [{posto}] ✓ conectado")
                else:
                    logger.write(f"  [{posto}] ✗ {err}")

    logger.write(f"  → {len(engines)}/{len(POSTOS)} postos online | {t_total.fmt()}")
    return engines


# =========================
# FASE 2 — Buscar agendas em paralelo
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
    Atendido
FROM vw_Cad_LancamentoProntuarioComDesistencia
WHERE dataconsulta >= :dt_ini
  AND dataconsulta <  :dt_fim
  AND desistencia = 0
  AND atendido <> 'MÉDICO faltou'
ORDER BY nomemedico, HoraPrevistaConsulta ASC
"""


def fetch_agenda_posto(eng, posto: str, dt: date):
    """Busca agenda de um posto para uma data. Retorna (posto, dt_iso, rows)."""
    dt_dmy  = dt.strftime("%d/%m/%Y")
    dt_next = (dt + timedelta(days=1)).strftime("%d/%m/%Y")
    dt_iso  = dt.isoformat()

    with Timer(f"agenda_{posto}_{dt_iso}") as t:
        try:
            with eng.connect() as con:
                rows = con.execute(
                    text(SQL_AGENDA), {"dt_ini": dt_dmy, "dt_fim": dt_next}
                ).mappings().all()
            stats.add(posto, "agenda", dt_iso, t.elapsed, True, rows=len(rows))
            return posto, dt_iso, rows, None
        except Exception as e:
            stats.add(posto, "agenda", dt_iso, t.elapsed, False, error=e)
            return posto, dt_iso, [], str(e)


def fetch_all_agendas(engines: dict, datas: list):
    """Busca agendas de todos os postos/datas em paralelo."""
    # {posto: {dt_iso: [rows]}}
    agendas = defaultdict(dict)

    logger.write("")
    logger.write("FASE 2 — Buscando agendas de todos os postos (paralelo)...")
    logger.write("-" * 70)

    tasks = []
    for posto, eng in engines.items():
        for dt in datas:
            tasks.append((eng, posto, dt))

    with Timer("fase2_total") as t_total:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(fetch_agenda_posto, eng, posto, dt): (posto, dt)
                for eng, posto, dt in tasks
            }
            for future in as_completed(futures):
                posto, dt_iso, rows, err = future.result()
                agendas[posto][dt_iso] = rows
                if err:
                    logger.write(f"  [{posto}] {dt_iso}: ERRO — {err}")
                else:
                    logger.write(f"  [{posto}] {dt_iso}: {len(rows)} pacientes")

    total_rows = sum(len(r) for p in agendas.values() for r in p.values())
    logger.write(f"  → {total_rows} registros de agenda | {t_total.fmt()}")
    return agendas


# =========================
# FASE 3 — Agrupar matrículas por posto de origem (cross-posto)
# =========================

def load_endereco_map(engines: dict):
    """Carrega mapa idEndereco ↔ letra de cad_endereco."""
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
                logger.write(f"  Mapa endereços carregado de [{p}]: {len(letra_to_id)} postos")
                return id_to_letra, letra_to_id
        except Exception:
            continue
    return id_to_letra, letra_to_id


def aggregate_matriculas(agendas: dict):
    """
    Agrupa TODAS as matrículas por posto de origem (cfcliente), cross-posto.
    Retorna: {letra_origem: set(matriculas)}
    """
    logger.write("")
    logger.write("FASE 3 — Agrupando matrículas por posto de origem...")
    logger.write("-" * 70)

    mats_por_origem = defaultdict(set)  # letra → {matriculas}

    for posto, datas in agendas.items():
        for dt_iso, rows in datas.items():
            for r in rows:
                mat = safe_int(r.get("matricula"))
                if not mat:
                    continue
                cf = (str(r.get("cfcliente") or "")).strip().upper()
                letra = cf if cf else posto
                mats_por_origem[letra].add(mat)

    for letra in sorted(mats_por_origem):
        logger.write(f"  [{letra}] {len(mats_por_origem[letra])} matrículas únicas")

    total = sum(len(s) for s in mats_por_origem.values())
    logger.write(f"  → {total} matrículas para consultar status/pagamento em {len(mats_por_origem)} postos")
    return mats_por_origem


# =========================
# FASE 4 — Buscar status e pagamento UMA VEZ por posto de origem
# =========================

def fetch_status_for_posto(eng, posto_letra: str, matriculas: list, id_endereco: int):
    """Busca [Cliente Situação] para TODAS as matrículas de um posto."""
    status = {}
    total_rows = 0

    for i in range(0, len(matriculas), 500):
        batch = matriculas[i:i+500]
        phs = ",".join(str(m) for m in batch)
        id_filter = f"AND idendereco = {id_endereco}" if id_endereco else ""
        sql = f"""
        SET NOCOUNT ON;
        SELECT DISTINCT matricula, [Cliente Situação] AS situacao
        FROM vw_fin_receita2
        WHERE matricula IN ({phs}) {id_filter}
        """
        with Timer(f"status_{posto_letra}_batch{i}") as t:
            try:
                with eng.connect() as con:
                    rows = con.execute(text(sql)).mappings().all()
                for r in rows:
                    status[int(r["matricula"])] = (r["situacao"] or "").strip()
                total_rows += len(rows)
                stats.add(posto_letra, "status", f"batch_{i}_{i+len(batch)}", t.elapsed, True, rows=len(rows))
            except Exception as e:
                stats.add(posto_letra, "status", f"batch_{i}_{i+len(batch)}", t.elapsed, False, error=e)

    return status, total_rows


def fetch_pagou_for_posto(eng, posto_letra: str, matriculas: list, id_endereco: int, datas: list):
    """Verifica pagamento (idcontatipo=5) para TODAS as matrículas, TODAS as datas."""
    # {dt_iso: set(matricula)}
    pagou_por_data = defaultdict(set)
    total_rows = 0

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
            with Timer(f"pagou_{posto_letra}_{dt_iso}_batch{i}") as t:
                try:
                    with eng.connect() as con:
                        rows = con.execute(text(sql), {"dt_ini": dt_dmy, "dt_fim": dt_next}).mappings().all()
                    for r in rows:
                        pagou_por_data[dt_iso].add(int(r["matricula"]))
                    total_rows += len(rows)
                    stats.add(posto_letra, "pagou", f"{dt_iso}_batch_{i}", t.elapsed, True, rows=len(rows))
                except Exception as e:
                    stats.add(posto_letra, "pagou", f"{dt_iso}_batch_{i}", t.elapsed, False, error=e)

    return pagou_por_data, total_rows


def fetch_financeiro_for_letra(args):
    """Worker: busca status + pagamento para um posto de origem."""
    posto_letra, matriculas, id_endereco, engines, datas = args
    eng = engines.get(posto_letra)
    if not eng:
        # Tentar conectar
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
    status, n_status = fetch_status_for_posto(eng, posto_letra, mat_list, id_endereco)
    pagou, n_pagou   = fetch_pagou_for_posto(eng, posto_letra, mat_list, id_endereco, datas)

    return posto_letra, status, pagou, None


def fetch_all_financeiro(engines: dict, mats_por_origem: dict, letra_to_id: dict, datas: list):
    """Busca status e pagamento para todos os postos de origem em paralelo."""
    logger.write("")
    logger.write("FASE 4 — Buscando status/pagamento por posto de origem (paralelo)...")
    logger.write("-" * 70)

    # status_global: {matricula: situacao}
    # pagou_global:  {dt_iso: set(matricula)}
    status_global = {}
    pagou_global  = defaultdict(set)

    tasks = []
    for letra, mats in mats_por_origem.items():
        id_end = letra_to_id.get(letra)
        tasks.append((letra, mats, id_end, engines, datas))

    with Timer("fase4_total") as t_total:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(fetch_financeiro_for_letra, t): t[0] for t in tasks}
            for future in as_completed(futures):
                letra, status, pagou, err = future.result()
                if err:
                    logger.write(f"  [{letra}] ✗ {err}")
                else:
                    status_global.update(status)
                    for dt_iso, mats in pagou.items():
                        pagou_global[dt_iso].update(mats)
                    logger.write(f"  [{letra}] ✓ {len(status)} status | pagou: {sum(len(s) for s in pagou.values())}")

    logger.write(f"  → {len(status_global)} status | {sum(len(s) for s in pagou_global.values())} pagamentos | {t_total.fmt()}")
    return status_global, pagou_global


# =========================
# FASE 5 — Montar payload
# =========================

def build_payload(agendas: dict, status_global: dict, pagou_global: dict, datas: list):
    """Monta o payload final com todos os pacientes."""
    logger.write("")
    logger.write("FASE 5 — Montando payload...")
    logger.write("-" * 70)

    dados = {}  # {posto: {dt_iso: [pacientes]}}

    for posto in sorted(agendas):
        dados[posto] = {}
        for dt in datas:
            dt_iso = dt.isoformat()
            rows = agendas[posto].get(dt_iso, [])

            pacientes = []
            exame_visto = set()

            for r in rows:
                mat = safe_int(r.get("matricula"))
                esp = (r.get("especialidade") or "").strip()

                # Deduplicar exames
                if esp.upper() == "EXAME" and mat:
                    medico = (r.get("medico") or "").strip()
                    chave = f"{mat}_{medico}_EXAME"
                    if chave in exame_visto:
                        continue
                    exame_visto.add(chave)

                cf = (str(r.get("cfcliente") or "")).strip().upper()
                situacao = status_global.get(mat, "") if mat else ""
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
                    "situacao":         situacao,
                    "pagou_no_dia":     pagou,
                    "idendereco":       safe_int(r.get("idendereco")),
                })

            dados[posto][dt_iso] = pacientes
            if pacientes:
                logger.write(f"  [{posto}] {dt_iso}: {len(pacientes)} pacientes")

    total_pac = sum(
        len(pacs)
        for posto_data in dados.values()
        for pacs in posto_data.values()
    )
    logger.write(f"  → {total_pac} pacientes no payload")
    return dados, total_pac


# =========================
# Main
# =========================

def run():
    meta = ETLMeta('export_agenda_dia', 'json_consolidado')

    load_dotenv(os.path.join(BASE_DIR, ".env"))
    os.makedirs(JSON_DIR, exist_ok=True)

    t0 = time.time()
    hoje   = date.today()
    amanha = hoje + timedelta(days=1)
    datas  = [hoje, amanha]

    logger.write("=" * 70)
    logger.write("EXPORT AGENDA DO DIA  (v2 — paralelo)")
    logger.write("=" * 70)
    logger.write(f"  Datas:    {hoje.isoformat()}, {amanha.isoformat()}")
    logger.write(f"  Postos:   {', '.join(POSTOS)}")
    logger.write(f"  Workers:  {MAX_WORKERS}")
    logger.write(f"  Início:   {datetime.now().strftime('%H:%M:%S')}")
    logger.write("=" * 70)

    # FASE 1 — Conectar
    engines = connect_all_postos()
    if not engines:
        logger.write("ERRO FATAL: nenhum posto online")
        meta.error('geral', 'nenhum posto online')
        meta.save()
        sys.exit(1)

    # Carregar mapa endereços
    with Timer("cad_endereco") as t:
        id_to_letra, letra_to_id = load_endereco_map(engines)
    logger.write(f"  Mapa endereços: {len(letra_to_id)} postos | {t.fmt()}")

    if not letra_to_id:
        logger.write("ERRO FATAL: não conseguiu carregar cad_endereco")
        meta.error('geral', 'não conseguiu carregar cad_endereco')
        meta.save()
        sys.exit(1)

    # FASE 2 — Buscar agendas
    agendas = fetch_all_agendas(engines, datas)

    # FASE 3 — Agrupar matrículas por posto de origem
    mats_por_origem = aggregate_matriculas(agendas)

    # FASE 4 — Status/pagamento
    status_global, pagou_global = fetch_all_financeiro(engines, mats_por_origem, letra_to_id, datas)

    # FASE 5 — Montar payload
    dados, total_pac = build_payload(agendas, status_global, pagou_global, datas)

    # Salvar JSON
    payload = {
        "meta": {
            "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "datas": [d.isoformat() for d in datas],
            "postos": POSTOS,
            "origem": "export_agenda_dia.py",
        },
        "dados": dados,
    }

    # Track per-posto results
    for posto in POSTOS:
        if posto in engines and posto in dados and any(dados[posto].values()):
            meta.ok(posto)
        elif posto not in engines:
            meta.error(posto, 'conexao falhou')

    with Timer("salvar_json") as t:
        tmp = OUT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, OUT_FILE)
    meta.save()
    logger.write(f"\nJSON salvo: {OUT_FILE} | {t.fmt()}")

    # Relatório final
    elapsed = time.time() - t0
    logger.write("")
    logger.write("=" * 70)
    logger.write("RESUMO FINAL")
    logger.write("=" * 70)
    logger.write(f"  Total pacientes: {total_pac}")
    logger.write(f"  Postos online:   {len(engines)}/{len(POSTOS)}")
    logger.write(f"  Tempo total:     {fmt_time(elapsed)}")
    logger.write(f"  Log:             {LOG_FILE}")

    # Contagem por situação
    sit_counts = defaultdict(int)
    for posto_data in dados.values():
        for pacs in posto_data.values():
            for p in pacs:
                sit = p.get("situacao") or "(vazio)"
                sit_counts[sit] += 1
    if sit_counts:
        logger.write("")
        logger.write("DISTRIBUIÇÃO POR SITUAÇÃO FINANCEIRA:")
        logger.write("-" * 40)
        for sit, cnt in sorted(sit_counts.items(), key=lambda x: -x[1]):
            logger.write(f"  {sit:30s} {cnt:>6d}")

    # Contagem por atendimento
    atend_counts = defaultdict(int)
    for posto_data in dados.values():
        for pacs in posto_data.values():
            for p in pacs:
                at = p.get("atendido") or "(vazio)"
                atend_counts[at] += 1
    if atend_counts:
        logger.write("")
        logger.write("DISTRIBUIÇÃO POR STATUS ATENDIMENTO:")
        logger.write("-" * 40)
        for at, cnt in sorted(atend_counts.items(), key=lambda x: -x[1]):
            logger.write(f"  {at:30s} {cnt:>6d}")

    stats.report()
    logger.write("")
    logger.write(f"Fim: {datetime.now().strftime('%H:%M:%S')} | Total: {fmt_time(elapsed)}")
    logger.close()


if __name__ == "__main__":
    run()
