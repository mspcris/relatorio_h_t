#!/usr/bin/env python3
# export_qualidade_agenda.py
# Exporta dados de Qualidade da Agenda Médica (vagas por especialidade por posto)
# Fonte única: Anchieta (posto A) — view retorna todos os postos.
# Mapeia idEndereco → letra via cad_endereco.
# Gera: json_consolidado/qualidade_agenda.json
# Cron recomendado: 0 5,12 * * *

import os, sys, json, argparse, time
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
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

SQL_VAGAS = """
SELECT
    idEndereco,
    Endereco,
    Especialidade,
    DataProximaVaga,
    QuantidadeVagasDisponivelNaData,
    QuantidadeVagasTotalMedicosAtendem,
    Desativado,
    ValorPMinimoVagaDisponivel,
    ValorPercentualVagasLivres,
    QuantidadeVagasReservadas,
    QuantidadeReservaDias,
    CapacidadeConsiderada,
    QuantidadeVagasTotalMedicosAtendemIncluindoReserva
FROM vw_rel_especialidadeProximaVaga
WHERE ISNULL(Desativado, 0) = 0
ORDER BY idEndereco, Especialidade
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

def calc_status(dias, prazo_ans: int, prazo_camim: int) -> str:
    """
    SEM_VAGA  → sem data de próxima vaga
    OK        → dias <= prazo_ans  (dentro do prazo ANS)
    ALERTA    → dias <= prazo_camim (além do ANS mas dentro do prazo CAMIM)
    CRITICO   → além de ambos os prazos
    """
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

    # ── 3. vw_rel_especialidadeProximaVaga → todos os postos de uma vez ───────
    logger.write(f"\n[ETAPA 3/4] Consultando vw_rel_especialidadeProximaVaga...")

    all_dados    = []
    postos_vistos = set()

    try:
        with Timer("vagas") as t:
            with eng.connect() as con:
                result    = con.execute(text(SQL_VAGAS))
                col_names = list(result.keys())
                rows      = result.fetchall()

        logger.write(f"  OK: {len(rows)} linhas brutas ({t.format()})")

        def _safe_float(v):
            try:
                f = float(v)
                return None if pd.isna(f) else round(f, 2)
            except Exception:
                return None

        def _safe_int(v):
            try:   return int(v)
            except Exception: return None

        for row in rows:
            d = dict(zip(col_names, row))

            id_end = _safe_int(d.get("idEndereco"))
            info   = endereco_map.get(id_end, {})
            letra  = info.get("letra", f"?{id_end}")
            nome   = info.get("nome",  str(id_end))

            # Normalizar data
            data_prox = d.get("DataProximaVaga")
            dias      = None
            data_str  = None
            if data_prox is not None:
                try:
                    if isinstance(data_prox, datetime):
                        dp = data_prox.date()
                    elif isinstance(data_prox, date):
                        dp = data_prox
                    else:
                        dp = pd.to_datetime(data_prox).date()
                    dias     = (dp - hoje).days
                    data_str = dp.isoformat()
                except Exception:
                    pass

            esp        = str(d.get("Especialidade") or "").strip().upper()
            cbos_info  = cbos_map.get(esp, {"prazoconsultaans": 0, "prazoconsultacamim": 0, "ValorPMinimoVagaDisponivel": 0.0})
            prazo_ans  = cbos_info["prazoconsultaans"]
            prazo_cam  = cbos_info["prazoconsultacamim"]
            status     = calc_status(dias, prazo_ans, prazo_cam)

            postos_vistos.add(letra)

            all_dados.append({
                "posto":            letra,
                "idEndereco":       id_end,
                "Endereco":         nome,
                "Especialidade":    esp,
                "DataProximaVaga":  data_str,
                "DiasAteProximaVaga": dias,
                "QuantidadeVagasDisponivelNaData":                    _safe_int(d.get("QuantidadeVagasDisponivelNaData")),
                "QuantidadeVagasTotalMedicosAtendem":                 _safe_int(d.get("QuantidadeVagasTotalMedicosAtendem")),
                "ValorPMinimoVagaDisponivel":                         _safe_float(d.get("ValorPMinimoVagaDisponivel")),
                "ValorPercentualVagasLivres":                         _safe_float(d.get("ValorPercentualVagasLivres")),
                "QuantidadeVagasReservadas":                          _safe_int(d.get("QuantidadeVagasReservadas")),
                "QuantidadeReservaDias":                              _safe_int(d.get("QuantidadeReservaDias")),
                "CapacidadeConsiderada":                              _safe_int(d.get("CapacidadeConsiderada")),
                "QuantidadeVagasTotalMedicosAtendemIncluindoReserva": _safe_int(d.get("QuantidadeVagasTotalMedicosAtendemIncluindoReserva")),
                "prazoconsultaans":  prazo_ans,
                "prazoconsultacamim": prazo_cam,
                "Status":            status,
            })

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
