#!/usr/bin/env python3
# export_qualidade_agenda.py
# Exporta dados de Qualidade da Agenda Médica (vagas por especialidade por posto)
# Salva em json_consolidado/qualidade_agenda.json para consumo pelo frontend

import os, sys, json, argparse, time
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# Constantes
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR  = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, f"export_qualidade_agenda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

POSTOS = list("ANXYBRPCDGIMJ")
POSTO_MASTER_CBOS = "A"  # Anchieta — fonte canônica do cad_cbos

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

POSTO_NOMES = {
    "A": "Anchieta",
    "N": "Nova Iguaçu",
    "X": "Xerém",
    "Y": "Nilópolis",
    "B": "Belford Roxo",
    "R": "Realengo",
    "P": "Pavuna",
    "C": "Campo Grande",
    "D": "Duque de Caxias",
    "G": "Guadalupe",
    "I": "Ilha do Governador",
    "M": "Méier",
    "J": "Jacarepaguá",
}

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
"""

# =========================
# Logging
# =========================

class Logger:
    """Escreve simultaneamente em console e arquivo."""
    def __init__(self, log_file):
        self.log_file = log_file
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.file_handle = open(log_file, 'w', encoding='utf-8')

    def write(self, msg: str):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{timestamp}] {msg}"
        print(msg)
        self.file_handle.write(line + '\n')
        self.file_handle.flush()

    def close(self):
        if self.file_handle:
            self.file_handle.close()

logger = Logger(LOG_FILE)


class Timer:
    def __init__(self, name: str):
        self.name = name
        self.start = None
        self.elapsed = 0

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = time.time() - self.start

    def format(self):
        if self.elapsed < 1:
            return f"{self.elapsed*1000:.0f}ms"
        elif self.elapsed < 60:
            return f"{self.elapsed:.1f}s"
        else:
            mins = self.elapsed // 60
            secs = self.elapsed % 60
            return f"{int(mins)}m {secs:.0f}s"


# =========================
# Helpers de ambiente
# =========================

def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def _set_mtime(path: str) -> None:
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))


def sanitize(obj):
    """Converte NaN/NaT/inf/date/datetime em tipos JSON-safe de forma recursiva."""
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    # numpy scalar → python native
    if hasattr(obj, 'item'):
        try:
            return obj.item()
        except Exception:
            pass
    return obj


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


# =========================
# Conexão com SQL Server
# =========================

def build_conn_str(posto: str):
    p = (posto or "").strip().upper()
    host = _env(f"DB_HOST_{p}")
    base = _env(f"DB_BASE_{p}")
    if not host or not base:
        return None
    user = _env(f"DB_USER_{p}")
    pwd  = _env(f"DB_PASSWORD_{p}")
    port = _env(f"DB_PORT_{p}", "1433") or "1433"
    encrypt    = _env("DB_ENCRYPT", "yes")
    trust_cert = _env("DB_TRUST_CERT", "yes")
    timeout    = _env("DB_TIMEOUT", "20")
    server = f"tcp:{host},{port}"
    common = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server};DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
    )
    if user:
        return common + f"UID={user};PWD={pwd}"
    return common + "Trusted_Connection=yes"


def make_engine(odbc_str: str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}",
        pool_pre_ping=True,
        future=True,
    )


# =========================
# Cálculo de status
# =========================

def calc_status(dias: int | None, prazo_ans: int, prazo_camim: int) -> str:
    """
    Regra de negócio:
      - SEM_VAGA   → dias is None (sem data de próxima vaga)
      - OK         → dias <= prazo_ans (e prazo_ans > 0)
      - ALERTA     → dias <= prazo_camim (e prazo_camim > 0), mas fora do ANS
      - CRITICO    → todos os demais casos
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

def run(postos_filtro=None):
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    ensure_dir(JSON_DIR)

    postos_alvo = postos_filtro or POSTOS
    total_start = time.time()

    logger.write("\n" + "="*70)
    logger.write("EXPORT QUALIDADE DA AGENDA MEDICA")
    logger.write("="*70)
    logger.write(f"  Postos: {', '.join(postos_alvo)}")
    logger.write(f"  Data referencia: {date.today().isoformat()}")
    logger.write("="*70)

    # ── 1. Carregar cad_cbos do posto master (A = Anchieta) ──────────────────
    logger.write(f"\n[ETAPA 1/3] Carregando cad_cbos do posto master '{POSTO_MASTER_CBOS}'...")

    cbos_map = {}  # {especialidade: {prazoconsultaans, prazoconsultacamim, ValorPMinimoVagaDisponivel}}

    conn_str_master = build_conn_str(POSTO_MASTER_CBOS)
    if not conn_str_master:
        logger.write(f"  ERRO: sem configuracao de conexao para posto {POSTO_MASTER_CBOS}")
        sys.exit(1)

    try:
        with Timer("cad_cbos") as t_cbos:
            eng = make_engine(conn_str_master)
            with eng.connect() as con:
                rows = con.execute(text(SQL_CAD_CBOS)).fetchall()
                cols = con.execute(text(SQL_CAD_CBOS)).keys() if False else None

        df_cbos = pd.DataFrame(rows, columns=["Especialidade", "prazoconsultaans",
                                               "prazoconsultacamim", "ValorPMinimoVagaDisponivel"])
        for _, row in df_cbos.iterrows():
            esp = str(row["Especialidade"]).strip().upper()
            cbos_map[esp] = {
                "prazoconsultaans":           int(row["prazoconsultaans"] or 0),
                "prazoconsultacamim":         int(row["prazoconsultacamim"] or 0),
                "ValorPMinimoVagaDisponivel": float(row["ValorPMinimoVagaDisponivel"] or 0.0),
            }
        logger.write(f"  OK: {len(cbos_map)} especialidades carregadas ({t_cbos.format()})")
    except Exception as e:
        logger.write(f"  ERRO ao carregar cad_cbos: {e}")
        sys.exit(1)

    # ── 2. Consultar vw_rel_especialidadeProximaVaga por posto ───────────────
    logger.write(f"\n[ETAPA 2/3] Consultando vagas por posto...")
    logger.write("-" * 70)

    all_dados = []
    postos_info = {}
    postos_com_dados = []
    hoje = date.today()

    for posto in postos_alvo:
        conn_str = build_conn_str(posto)
        if not conn_str:
            logger.write(f"  [{posto}] SKIP: sem configuracao de conexao")
            continue

        try:
            with Timer(f"vagas_{posto}") as t_posto:
                eng = make_engine(conn_str)
                with eng.connect() as con:
                    rows = con.execute(text(SQL_VAGAS)).fetchall()
                    if rows:
                        keys = rows[0]._fields if hasattr(rows[0], '_fields') else [
                            "idEndereco", "Endereco", "Especialidade", "DataProximaVaga",
                            "QuantidadeVagasDisponivelNaData", "QuantidadeVagasTotalMedicosAtendem",
                            "Desativado", "ValorPMinimoVagaDisponivel", "ValorPercentualVagasLivres",
                            "QuantidadeVagasReservadas", "QuantidadeReservaDias",
                            "CapacidadeConsiderada", "QuantidadeVagasTotalMedicosAtendemIncluindoReserva"
                        ]
                        df = pd.DataFrame([dict(zip(keys, r)) for r in rows])
                    else:
                        df = pd.DataFrame()

            if df.empty:
                logger.write(f"  [{posto}] OK (vazio) — {t_posto.format()}")
                continue

            # Registrar info do posto (usa primeiro idEndereco encontrado)
            id_end = int(df["idEndereco"].iloc[0]) if "idEndereco" in df.columns else 0
            nome_end = str(df["Endereco"].iloc[0]) if "Endereco" in df.columns else POSTO_NOMES.get(posto, posto)
            postos_info[posto] = {
                "letra":     posto,
                "idEndereco": id_end,
                "nome":      POSTO_NOMES.get(posto, nome_end),
            }
            postos_com_dados.append(posto)

            # Calcular DiasAteProximaVaga e Status por linha
            for _, row in df.iterrows():
                data_prox = row.get("DataProximaVaga")
                dias = None
                data_str = None

                if data_prox is not None and not (isinstance(data_prox, float) and pd.isna(data_prox)):
                    try:
                        if isinstance(data_prox, (datetime,)):
                            data_prox_date = data_prox.date()
                        elif isinstance(data_prox, date):
                            data_prox_date = data_prox
                        else:
                            data_prox_date = pd.to_datetime(data_prox).date()
                        dias = (data_prox_date - hoje).days
                        data_str = data_prox_date.isoformat()
                    except Exception:
                        pass

                esp = str(row.get("Especialidade", "") or "").strip().upper()
                cbos_info = cbos_map.get(esp, {
                    "prazoconsultaans": 0,
                    "prazoconsultacamim": 0,
                    "ValorPMinimoVagaDisponivel": 0.0,
                })
                prazo_ans   = cbos_info["prazoconsultaans"]
                prazo_camim = cbos_info["prazoconsultacamim"]
                status = calc_status(dias, prazo_ans, prazo_camim)

                def _safe_float(v):
                    try:
                        f = float(v)
                        return None if pd.isna(f) else round(f, 2)
                    except Exception:
                        return None

                def _safe_int(v):
                    try:
                        return int(v)
                    except Exception:
                        return None

                all_dados.append({
                    "posto":                  posto,
                    "idEndereco":             _safe_int(row.get("idEndereco")),
                    "Endereco":               str(row.get("Endereco", "") or ""),
                    "Especialidade":          esp,
                    "DataProximaVaga":        data_str,
                    "DiasAteProximaVaga":     dias,
                    "QuantidadeVagasDisponivelNaData":                    _safe_int(row.get("QuantidadeVagasDisponivelNaData")),
                    "QuantidadeVagasTotalMedicosAtendem":                 _safe_int(row.get("QuantidadeVagasTotalMedicosAtendem")),
                    "ValorPMinimoVagaDisponivel":                         _safe_float(row.get("ValorPMinimoVagaDisponivel")),
                    "ValorPercentualVagasLivres":                         _safe_float(row.get("ValorPercentualVagasLivres")),
                    "QuantidadeVagasReservadas":                          _safe_int(row.get("QuantidadeVagasReservadas")),
                    "QuantidadeReservaDias":                              _safe_int(row.get("QuantidadeReservaDias")),
                    "CapacidadeConsiderada":                              _safe_int(row.get("CapacidadeConsiderada")),
                    "QuantidadeVagasTotalMedicosAtendemIncluindoReserva": _safe_int(row.get("QuantidadeVagasTotalMedicosAtendemIncluindoReserva")),
                    "prazoconsultaans":        prazo_ans,
                    "prazoconsultacamim":      prazo_camim,
                    "Status":                  status,
                })

            logger.write(f"  [{posto}] OK: {len(df)} linhas — {t_posto.format()}")

        except Exception as e:
            logger.write(f"  [{posto}] ERRO: {str(e)[:80]}")
            continue

    # ── 3. Montar e salvar JSON ──────────────────────────────────────────────
    logger.write(f"\n[ETAPA 3/3] Gerando JSON...")

    especialidades_set = sorted(set(d["Especialidade"] for d in all_dados if d["Especialidade"]))

    payload = sanitize({
        "meta": {
            "gerado_em":       datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "data_referencia": hoje.isoformat(),
            "origem":          "export_qualidade_agenda.py",
            "total_registros": len(all_dados),
        },
        "cbos":          cbos_map,
        "especialidades": especialidades_set,
        "postos":         postos_com_dados,
        "postos_info":    postos_info,
        "dados":          all_dados,
    })

    out_path = os.path.join(JSON_DIR, "qualidade_agenda.json")
    tmp_path = out_path + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, out_path)
    _set_mtime(out_path)

    elapsed_total = time.time() - total_start
    logger.write(f"  JSON salvo: {out_path}")
    logger.write(f"  Total registros: {len(all_dados)}")
    logger.write(f"  Postos com dados: {len(postos_com_dados)} — {', '.join(postos_com_dados)}")
    logger.write(f"  Especialidades: {len(especialidades_set)}")
    logger.write(f"  Tempo total: {elapsed_total:.1f}s")
    logger.write(f"\nFinalizado export_qualidade_agenda.")
    logger.write(f"Log salvo em: {LOG_FILE}\n")
    logger.close()


def parse_args():
    p = argparse.ArgumentParser(
        description="Qualidade da Agenda Médica: exporta vagas por especialidade por posto.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--postos", default="".join(POSTOS),
                   help="Subset de postos. Ex: ANX ou Y.")
    p.add_argument("--dry-run", action="store_true",
                   help="Executa sem gravar JSON.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    postos_filtro = [c.upper() for c in args.postos if c.isalpha()] or POSTOS
    if not args.dry_run:
        run(postos_filtro)
    else:
        print("[DRY-RUN] Nenhuma alteracao realizada.")
