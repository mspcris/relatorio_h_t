#!/usr/bin/env python3
# export_agenda_dia.py
# ETL: Agenda do Dia — gera JSON com agenda, status financeiro e pagamento
# Roda para cada posto, para hoje e amanhã.
# Gera: json_consolidado/agenda_dia.json
# Cron recomendado: 0 * * * * (a cada hora)

import os, sys, json, time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus
from collections import defaultdict

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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
        print(msg)
        self.fh.write(line + '\n')
        self.fh.flush()

    def close(self):
        self.fh.close()


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
# ETL
# =========================

def load_endereco_map(eng):
    """Carrega mapa idEndereco → letra de cad_endereco."""
    id_to_letra = {}
    letra_to_id = {}
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
    except Exception as e:
        logger.write(f"  WARN: falha ao carregar cad_endereco: {e}")
    return id_to_letra, letra_to_id


def fetch_agenda(eng, dt: date):
    """Busca agenda do dia para o posto conectado."""
    dt_dmy = dt.strftime("%d/%m/%Y")
    dt_next = (dt + timedelta(days=1)).strftime("%d/%m/%Y")

    sql = """
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
    with eng.connect() as con:
        return con.execute(text(sql), {"dt_ini": dt_dmy, "dt_fim": dt_next}).mappings().all()


def fetch_status_batch(eng, matriculas, id_endereco):
    """Busca [Cliente Situação] para lote de matrículas no posto correto."""
    status = {}
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
        try:
            with eng.connect() as con:
                for r in con.execute(text(sql)).mappings().all():
                    status[int(r["matricula"])] = (r["situacao"] or "").strip()
        except Exception:
            pass
    return status


def fetch_pagou_batch(eng, matriculas, id_endereco, dt: date):
    """Verifica quais matrículas pagaram mensalidade (idcontatipo=5) no dia."""
    dt_dmy = dt.strftime("%d/%m/%Y")
    dt_next = (dt + timedelta(days=1)).strftime("%d/%m/%Y")
    pagou = set()
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
        try:
            with eng.connect() as con:
                for r in con.execute(text(sql), {"dt_ini": dt_dmy, "dt_fim": dt_next}).mappings().all():
                    pagou.add(int(r["matricula"]))
        except Exception:
            pass
    return pagou


def process_posto(posto: str, datas: list[date], engines_cache: dict, letra_to_id: dict):
    """Processa um posto para as datas fornecidas. Retorna dict {data_iso: [pacientes]}."""
    conn_str = build_conn_str(posto)
    if not conn_str:
        logger.write(f"  [{posto}] sem config no .env — skip")
        return {}

    try:
        eng = make_engine(conn_str)
        with eng.connect() as con:
            con.execute(text("SELECT 1"))
        engines_cache[posto] = eng
    except Exception as e:
        logger.write(f"  [{posto}] offline: {e}")
        return {}

    resultado = {}

    for dt in datas:
        dt_iso = dt.isoformat()
        try:
            rows = fetch_agenda(eng, dt)
        except Exception as e:
            logger.write(f"  [{posto}] erro agenda {dt_iso}: {e}")
            resultado[dt_iso] = []
            continue

        if not rows:
            resultado[dt_iso] = []
            continue

        # Agrupar matrículas por posto de origem (cfcliente)
        mats_por_origem = defaultdict(set)  # letra -> {matriculas}
        for r in rows:
            mat = safe_int(r.get("matricula"))
            if not mat:
                continue
            cf = (str(r.get("cfcliente") or "")).strip().upper()
            letra = cf if cf else posto
            mats_por_origem[letra].add(mat)

        # Buscar status e pagamento por posto de origem
        status_map = {}
        pagou_map = set()

        for letra_orig, mats in mats_por_origem.items():
            id_end = letra_to_id.get(letra_orig)
            # Conectar no banco do posto de origem
            if letra_orig in engines_cache:
                eng_orig = engines_cache[letra_orig]
            else:
                cs = build_conn_str(letra_orig)
                if not cs:
                    continue
                try:
                    eng_orig = make_engine(cs)
                    with eng_orig.connect() as con:
                        con.execute(text("SELECT 1"))
                    engines_cache[letra_orig] = eng_orig
                except Exception:
                    continue

            st = fetch_status_batch(eng_orig, list(mats), id_end)
            status_map.update(st)

            pg = fetch_pagou_batch(eng_orig, list(mats), id_end, dt)
            pagou_map.update(pg)

        # Montar pacientes (dedup exames)
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
                "situacao":         status_map.get(mat, "") if mat else "",
                "pagou_no_dia":     mat in pagou_map if mat else False,
                "idendereco":       safe_int(r.get("idendereco")),
            })

        resultado[dt_iso] = pacientes
        logger.write(f"  [{posto}] {dt_iso}: {len(pacientes)} pacientes")

    return resultado


def run():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    os.makedirs(JSON_DIR, exist_ok=True)

    t0 = time.time()
    hoje = date.today()
    amanha = hoje + timedelta(days=1)
    datas = [hoje, amanha]

    logger.write("=" * 60)
    logger.write("EXPORT AGENDA DO DIA")
    logger.write(f"  Datas: {hoje.isoformat()}, {amanha.isoformat()}")
    logger.write(f"  Postos: {', '.join(POSTOS)}")
    logger.write("=" * 60)

    # Carregar mapa de endereços do primeiro posto disponível
    letra_to_id = {}
    engines_cache = {}

    for p in POSTOS:
        cs = build_conn_str(p)
        if not cs:
            continue
        try:
            eng = make_engine(cs)
            with eng.connect() as con:
                con.execute(text("SELECT 1"))
            engines_cache[p] = eng
            _, letra_to_id = load_endereco_map(eng)
            if letra_to_id:
                logger.write(f"  Mapa endereços carregado de [{p}]: {len(letra_to_id)} postos")
                break
        except Exception:
            continue

    if not letra_to_id:
        logger.write("ERRO: não conseguiu carregar cad_endereco de nenhum posto")
        sys.exit(1)

    # Processar cada posto
    payload = {
        "meta": {
            "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "datas": [d.isoformat() for d in datas],
            "postos": POSTOS,
            "origem": "export_agenda_dia.py",
        },
        "dados": {},  # {posto: {data_iso: [pacientes]}}
    }

    for p in POSTOS:
        logger.write(f"\nProcessando [{p}]...")
        resultado = process_posto(p, datas, engines_cache, letra_to_id)
        if resultado:
            payload["dados"][p] = resultado

    # Salvar JSON
    tmp = OUT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, OUT_FILE)

    elapsed = time.time() - t0
    total_pac = sum(
        len(pacs)
        for posto_data in payload["dados"].values()
        for pacs in posto_data.values()
    )
    logger.write(f"\nJSON salvo: {OUT_FILE}")
    logger.write(f"Total pacientes: {total_pac}")
    logger.write(f"Tempo: {elapsed:.1f}s")
    logger.write(f"Log: {LOG_FILE}")
    logger.close()


if __name__ == "__main__":
    run()
