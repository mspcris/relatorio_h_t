# ctrlq_desbloqueio.py
# Exporta registros de desbloqueio de agenda (cad_especialidade com DataFimExibicao)
# por posto para json_ctrlq_desbloqueio/.
#
# Saídas:
#   json_ctrlq_desbloqueio/CTRLQ_DESBLOQUEIO_<POSTO>.json  — por posto
#   json_ctrlq_desbloqueio/CTRLQ_DESBLOQUEIO_CONSOLIDADO.json — todos os postos

import os
import re
import json
import decimal
from datetime import datetime, date, time as time_type, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
SQL_PATH     = os.path.join(BASE_DIR, "sql_ctrlq_desbloqueio", "sql_ctrlq_desbloqueio.sql")
SQL_AUD_PATH = os.path.join(BASE_DIR, "sql_ctrlq_desbloqueio", "sql_ctrlq_desbloqueio_aud.sql")
SQL_IRM_PATH = os.path.join(BASE_DIR, "sql_ctrlq_desbloqueio", "sql_ctrlq_desbloqueio_irmaos.sql")
JSON_DIR     = os.path.join(BASE_DIR, "json_ctrlq_desbloqueio")

ODBC_DRIVER     = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS_FALLBACK = list("ANXYBRPCDGIMJ")


# ── utilidades ────────────────────────────────────────────────────────────────

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def atomic_write(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)

def cleanup_json_dir(d):
    ensure_dir(d)
    n = 0
    for name in os.listdir(d):
        if name.lower().endswith(".json"):
            try: os.remove(os.path.join(d, name)); n += 1
            except Exception: pass
    print(f"[CLEANUP] removidos {n} .json em {d}")

def normalize(v):
    if isinstance(v, time_type):
        return v.strftime('%H:%M:%S')
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if pd.isna(v) if not isinstance(v, (list, dict)) else False:
        return None
    try:
        import numpy as np
        if isinstance(v, np.generic):
            return v.item()
    except Exception:
        pass
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")
    return v

def normalize_row(row):
    return {k: normalize(v) for k, v in row.items()}


# ── conexão ───────────────────────────────────────────────────────────────────

def build_conn_str(host, base, user, pwd, port="1433"):
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};DATABASE={base};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        + (f"UID={user};PWD={pwd}" if user else "Trusted_Connection=yes")
    )

def make_engine(odbc_str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}",
        future=True, pool_pre_ping=True
    )

def build_conns_from_env(postos=None):
    postos = postos or POSTOS_FALLBACK
    conns = {}
    for p in postos:
        host = env(f"DB_HOST_{p}"); base = env(f"DB_BASE_{p}")
        if not host or not base: continue
        conns[p] = build_conn_str(host, base, env(f"DB_USER_{p}"),
                                  env(f"DB_PASSWORD_{p}"), env(f"DB_PORT_{p}", "1433"))
    if not conns:
        host0 = env("DB_HOST"); base0 = env("DB_BASE")
        if host0 and base0:
            conns["SINGLE"] = build_conn_str(host0, base0, env("DB_USER"),
                                             env("DB_PASSWORD"), env("DB_PORT", "1433"))
    return conns


# ── auditoria (vw_Sis_Historico) ─────────────────────────────────────────────

def fetch_audit(engine, aud_sql):
    """Retorna dict {idEspecialidade(int): [lista de registros]} ou {} se indisponível."""
    try:
        with engine.connect() as con:
            df = pd.read_sql_query(text(aud_sql), con)
        result = {}
        for r in df.to_dict(orient="records"):
            ide = r.get("idEspecialidade")
            if ide is not None:
                entry = {
                    "aud_idHistorico": normalize(r.get("aud_idHistorico")),
                    "aud_data":        normalize(r.get("aud_data")),
                    "aud_usuario":     normalize(r.get("aud_usuario")),
                    "aud_detalhe":     normalize(r.get("aud_detalhe")),
                    "aud_comando":     normalize(r.get("aud_comando")),
                    "aud_descricao":   normalize(r.get("aud_descricao")),
                    "aud_computador":  normalize(r.get("aud_computador")),
                    "aud_fallback":    bool(r.get("aud_fallback", 0)),
                }
                result.setdefault(int(ide), []).append(entry)
        return result
    except Exception as e:
        print(f"(auditoria indisponível: {type(e).__name__})", end=" ")
        return {}

# ── parsing do Detalhe da vw_Sis_Historico ───────────────────────────────────
#
# Formato gravado pelo ERP (uma alteração por linha, \r\n entre elas):
#   "\rAlteração DataFimExibicao de (vazio) para 11/09/2026 15:54:03\r\n
#    ValorCustoSexta de 1245 para 1245,01\r\n
#    ObservacaoDesbloqueio de (vazio) para  - Custo Semanal Anterior de R$ 2.534,00 para 2.534,01\r\n"
# A linha de ObservacaoDesbloqueio é sempre a última e carrega " para " várias
# vezes (é o log inteiro anexado) — por isso o parse para nela.

_RE_MUDANCA = re.compile(r"^\s*(?:Altera[çc][ãa]o\s+)?([A-Za-z0-9_]+)\s+de\s+(.*?)\s+para\s+(.*)$", re.S)
_RE_DATA_BR = re.compile(r"(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}):(\d{2}))?")

def parse_detalhe(detalhe):
    """→ lista de {campo, de, para} (sem a ObservacaoDesbloqueio, que vai à parte
    em 'obs_para')."""
    out, obs = [], None
    if not detalhe:
        return out, obs
    txt = str(detalhe).replace("\r\n", "\n")
    for ln in txt.split("\n"):
        ln = ln.strip("\r ").strip()
        if not ln:
            continue
        m = _RE_MUDANCA.match(ln)
        if not m:
            continue
        campo, de, para = m.group(1), m.group(2).strip(), m.group(3).strip()
        if campo.lower() == "observacaodesbloqueio":
            obs = para
            break          # é a última linha; o resto é log anexado
        out.append({"campo": campo, "de": de, "para": para})
    return out, obs

def _data_br_para_iso(txt):
    m = _RE_DATA_BR.search(txt or "")
    if not m:
        return None
    d, mo, y, hh, mi, ss = m.groups()
    return f"{y}-{mo}-{d}T{hh or '00'}:{mi or '00'}:{ss or '00'}"

def _fmt_br(iso):
    """'2026-09-11T15:54:03' → '11/09/2026 15:54:03' (formato do Detalhe)."""
    try:
        dt = datetime.fromisoformat(str(iso)[:19])
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return None

def detectar_gatilho(aud_list, datafim_atual):
    """Acha, na auditoria ordenada por idHistorico, o evento que ABRIU o
    desbloqueio vigente e a eventual prorrogação.

    gatilho     = última linha com "DataFimExibicao de (vazio) para X"
                  (o ERP grava isso quando custo/tempo semanal muda)
    prorrogacao = última linha DEPOIS do gatilho que trocou DataFimExibicao
                  de um valor para outro (KPI 'Prorrogar Agenda' ou ERP)
    Se a data fim atual foi definida por uma linha "de X para Y" cujo X não é
    vazio e não há (vazio) antes, a própria linha vira o gatilho.
    """
    alvo = _fmt_br(datafim_atual)
    set_atual = None
    for e in reversed(aud_list):
        for m in e["mudancas"]:
            if m["campo"].lower() == "datafimexibicao" and (alvo is None or m["para"].startswith(alvo[:16])):
                set_atual = e; break
        if set_atual: break
    if set_atual is None:
        for e in reversed(aud_list):
            if any(m["campo"].lower() == "datafimexibicao" for m in e["mudancas"]):
                set_atual = e; break
    if set_atual is None:
        return None, None
    def _de_vazio(e):
        return any(m["campo"].lower() == "datafimexibicao" and m["de"].lower().strip("() ") in ("vazio", "")
                   for m in e["mudancas"])
    if _de_vazio(set_atual):
        return set_atual, None
    gat = None
    for e in reversed(aud_list):
        if e["aud_idHistorico"] <= set_atual["aud_idHistorico"] and _de_vazio(e):
            gat = e; break
    return (gat or set_atual), (set_atual if gat else None)

# O ERP define DataFimExibicao = momento da mudança + 8 dias EXATOS (medido em
# 4 casos: Ilona 03/09 15:54:03 → 11/09 15:54:03; Milton 24/07 16:20:19 →
# 01/08 16:20:19; José Vinicius 02/09 13:39:03 → 10/09 13:39:03; Ilona jul/26).
# Quando a agenda é CRIADA já com data fim, a inclusão não gera linha de
# auditoria com "DataFimExibicao de (vazio) para" — então o gatilho é
# estimado subtraindo esses 8 dias. Se o ERP mudar o prazo, mudar aqui.
DIAS_DATAFIM_ERP = 8

def gatilho_estimado(datafim_atual):
    try:
        dt = datetime.fromisoformat(str(datafim_atual)[:19])
    except Exception:
        return None
    from datetime import timedelta
    est = dt - timedelta(days=DIAS_DATAFIM_ERP)
    return {"aud_idHistorico": None, "aud_data": est.isoformat(timespec="seconds"),
            "aud_usuario": None, "mudancas": [], "obs_para": None, "estimado": True}

# campos que a auditoria consegue desfazer (nome do ERP → nome no registro)
def _campo_registro(rec, campo):
    alvo = campo.lower()
    for k in rec:
        if k.lower() == alvo and not k.startswith("hist_"):
            return k
    return None

_RE_NUM = re.compile(r"^-?\d{1,3}(\.\d{3})*(,\d+)?$|^-?\d+(,\d+)?$")

def _valor_auditoria(txt):
    t = (txt or "").strip()
    if t.lower() in ("(vazio)", ""):
        return None
    if t in ("Sim", "Não"):
        return t == "Sim"
    if re.match(r"^\d{1,2}:\d{2}$", t):
        return t + ":00"
    if _RE_NUM.match(t):
        return float(t.replace(".", "").replace(",", "."))
    return t

def reconstruir_antes(r, ciclo):
    """'Antes' = estado atual com as mudanças do ciclo DESFEITAS, da mais nova
    para a mais antiga (inclusive o gatilho). Usado quando não existe foto em
    Cad_EspecialidadeHistorico anterior ao gatilho — agenda criada no mesmo
    dia (Dr. Milton, R, 24/07/2026)."""
    dias = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
    campos = ["PermitirAgendamentoquenuncaconsultou"]
    for d in dias:
        campos += [d, d + "HoraInicio", d + "HoraFim", d + "Almoco", d + "Almocoinicio", d + "AlmocoFim",
                   "ValorCusto" + d, "QuantidadeCusto" + d]
    estado = {c: r.get(_campo_registro(r, c) or c) for c in campos}
    estado["PermitirAgendamentoquenuncaconsultou"] = r.get("atual_PermitirSemConsulta")
    desfeitas = 0
    for e in reversed([x for x in ciclo if x.get("aud_no_ciclo")]):
        for m in e.get("mudancas") or []:
            k = next((c for c in campos if c.lower() == m["campo"].lower()), None)
            if k is None:
                continue
            estado[k] = _valor_auditoria(m["de"])
            desfeitas += 1
    for c in campos:
        k = "hist_" + ("PermitirSemConsulta" if c == "PermitirAgendamentoquenuncaconsultou" else c)
        r[k] = estado[c]
    r["hist_DataHoraInclusao"] = None
    r["hist_fonte"] = "reconstruido_auditoria"
    r["hist_mudancas_desfeitas"] = desfeitas

def merge_audit(rows, audit_map):
    empty_scalar = {"aud_idHistorico": None, "aud_data": None,
                    "aud_usuario": None, "aud_detalhe": None}
    for r in rows:
        ide = r.get("idEspecialidade")
        aud_all = audit_map.get(int(ide)) if ide is not None else None
        r["aud_gatilho"] = None
        r["aud_prorrogacao"] = None
        if not aud_all:
            r["aud_historico"] = []
            r.update(empty_scalar)
            g = gatilho_estimado(r.get("DataFimExibicao"))
            if g:
                r["aud_gatilho"] = {"idHistorico": None, "data": g["aud_data"], "usuario": None, "mudancas": [],
                                    "datafim_definida": None, "obs_apos": None, "estimado": True}
            continue
        for e in aud_all:
            e["mudancas"], e["obs_para"] = parse_detalhe(e.get("aud_detalhe"))
        gat, pro = detectar_gatilho(aud_all, r.get("DataFimExibicao"))
        if gat is None:
            est = gatilho_estimado(r.get("DataFimExibicao"))
            if est:
                # insere o gatilho estimado na posição cronológica certa
                aud_all = sorted(aud_all + [est], key=lambda e: str(e["aud_data"]))
                gat = est
        if gat:
            # o card mostra o ciclo vigente: do gatilho em diante (mais 3
            # linhas anteriores para contexto)
            idx = next(i for i, e in enumerate(aud_all) if e is gat)
            ciclo = aud_all[max(0, idx - 3):]
            for e in ciclo:
                e["aud_no_ciclo"] = (aud_all.index(e) >= idx)
                e["aud_gatilho"] = e is gat
                e["aud_prorrogacao"] = bool(pro) and e is pro
            r["aud_historico"] = ciclo
            r["aud_gatilho"] = {
                "idHistorico": gat["aud_idHistorico"], "data": gat["aud_data"],
                "usuario": gat["aud_usuario"], "mudancas": gat["mudancas"],
                "datafim_definida": next((m["para"] for m in gat["mudancas"]
                                          if m["campo"].lower() == "datafimexibicao"), None),
                "obs_apos": gat.get("obs_para"),
                "estimado": bool(gat.get("estimado")),
            }
            if gat.get("estimado"):
                ciclo = [e for e in ciclo if not e.get("estimado")]
                r["aud_historico"] = ciclo
            if pro:
                r["aud_prorrogacao"] = {
                    "idHistorico": pro["aud_idHistorico"], "data": pro["aud_data"],
                    "usuario": pro["aud_usuario"],
                    "de":   next((m["de"]   for m in pro["mudancas"] if m["campo"].lower() == "datafimexibicao"), None),
                    "para": next((m["para"] for m in pro["mudancas"] if m["campo"].lower() == "datafimexibicao"), None),
                }
            principal = gat
        else:
            for e in aud_all:
                e["aud_no_ciclo"] = False; e["aud_gatilho"] = False; e["aud_prorrogacao"] = False
            r["aud_historico"] = aud_all[-10:]
            principal = aud_all[-1]
        if principal.get("estimado"):
            # sem linha própria: a primeira linha real do ciclo é quem mexeu
            principal = next((e for e in r["aud_historico"] if e.get("aud_no_ciclo")), None) or aud_all[-1]
        r["aud_idHistorico"] = principal["aud_idHistorico"]
        r["aud_data"]        = principal["aud_data"]
        r["aud_usuario"]     = principal["aud_usuario"]
        r["aud_detalhe"]     = principal.get("aud_detalhe")
    return rows


# ── snapshot da VÉSPERA do gatilho (Cad_EspecialidadeHistorico) ──────────────
#
# Cad_EspecialidadeHistorico é foto DIÁRIA (23:59:59). O SQL principal traz o
# snapshot anterior à DataFimExibicao — que está no FUTURO, então é sempre a
# foto de ontem, já com todas as mudanças, e "como era antes" ficava igual a
# "como está agora" (Dr. Milton, 2026-09-03). O correto é a foto anterior ao
# GATILHO: o último estado antes da mudança que abriu o registro.

_HIST_CAMPOS = None

def _hist_campos(engine):
    global _HIST_CAMPOS
    if _HIST_CAMPOS is None:
        with engine.connect() as con:
            cols = {r[0] for r in con.execute(text(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='Cad_EspecialidadeHistorico'"))}
        _HIST_CAMPOS = cols
    return _HIST_CAMPOS

def merge_snapshot_gatilho(rows, engine):
    dias = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
    campos = ["PermitirAgendamentoquenuncaconsultou"]
    for d in dias:
        campos += [d, d + "HoraInicio", d + "HoraFim", d + "Almoco", d + "Almocoinicio", d + "AlmocoFim",
                   "ValorCusto" + d, "QuantidadeCusto" + d]
    try:
        existentes = _hist_campos(engine)
    except Exception as e:
        print(f"(snapshot indisponível: {type(e).__name__})", end=" ")
        return rows
    campos = [c for c in campos if c in existentes]
    sql = ("SELECT TOP 1 DataHoraInclusao, " + ", ".join(campos) +
           " FROM Cad_EspecialidadeHistorico WHERE idEspecialidade = :i AND DataHoraInclusao < :g "
           "ORDER BY DataHoraInclusao DESC")
    for r in rows:
        g = r.get("aud_gatilho")
        r["hist_fonte"] = "antes_datafim"
        if not g or not g.get("data"):
            continue
        try:
            gdt = datetime.fromisoformat(str(g["data"])[:19])
            with engine.connect() as con:
                row = con.execute(text(sql), {"i": int(r["idEspecialidade"]), "g": gdt}).mappings().first()
        except Exception as e:
            print(f"(snapshot {r.get('idEspecialidade')} falhou: {type(e).__name__})", end=" ")
            continue
        if not row:
            reconstruir_antes(r, r.get("aud_historico") or [])
            continue
        r["hist_fonte"] = "vespera_gatilho"
        r["hist_DataHoraInclusao"] = normalize(row["DataHoraInclusao"])
        for c in campos:
            k = "hist_" + ("PermitirSemConsulta" if c == "PermitirAgendamentoquenuncaconsultou" else c)
            r[k] = normalize(row[c])
    return rows


# ── irmãos (outros registros ativos do mesmo médico+especialidade) ──────────

def fetch_irmaos(engine, irm_sql):
    """Retorna dict {parent_idEspecialidade(int): [lista de registros irmãos]}."""
    try:
        with engine.connect() as con:
            df = pd.read_sql_query(text(irm_sql), con)
        result = {}
        for r in df.to_dict(orient="records"):
            parent = r.pop("parent_idEspecialidade", None)
            if parent is not None:
                result.setdefault(int(parent), []).append(normalize_row(r))
        return result
    except Exception as e:
        print(f"(irmãos indisponível: {type(e).__name__})", end=" ")
        return {}

def merge_irmaos(rows, irmaos_map):
    for r in rows:
        ide = r.get("idEspecialidade")
        r["irmaos"] = irmaos_map.get(int(ide), []) if ide is not None else []
    return rows


# ── exportação ────────────────────────────────────────────────────────────────

def main():
    print("=== CTRLQ Desbloqueio Exporter ===")
    # .env do próprio diretório; rodando de outro lugar (teste em /tmp na VM)
    # cai no .env de produção.
    for _env_path in (os.path.join(BASE_DIR, ".env"), "/opt/relatorio_h_t/.env"):
        if os.path.isfile(_env_path):
            load_dotenv(_env_path); break
    ensure_dir(JSON_DIR)

    if not os.path.isfile(SQL_PATH):
        print(f"ERRO: SQL não encontrado em {SQL_PATH}"); return
    sql = open(SQL_PATH, encoding="utf-8").read().strip()
    if not sql:
        print("ERRO: SQL vazio"); return

    aud_sql = ""
    if os.path.isfile(SQL_AUD_PATH):
        aud_sql = open(SQL_AUD_PATH, encoding="utf-8").read().strip()

    irm_sql = ""
    if os.path.isfile(SQL_IRM_PATH):
        irm_sql = open(SQL_IRM_PATH, encoding="utf-8").read().strip()

    conns = build_conns_from_env()
    if not conns:
        print("ERRO: nenhuma conexão no .env"); return

    print(f"Postos: {list(conns.keys())}")
    cleanup_json_dir(JSON_DIR)

    por_posto = {}

    for posto, odbc in conns.items():
        print(f"[{posto}] executando...", end=" ")
        try:
            engine = make_engine(odbc)
            with engine.connect() as con:
                df = pd.read_sql_query(text(sql), con)
            rows = [normalize_row(r) for r in df.to_dict(orient="records")]
            if aud_sql:
                audit_map = fetch_audit(engine, aud_sql)
                rows = merge_audit(rows, audit_map)
                rows = merge_snapshot_gatilho(rows, engine)
            if irm_sql:
                irmaos_map = fetch_irmaos(engine, irm_sql)
                rows = merge_irmaos(rows, irmaos_map)
            por_posto[posto] = rows
            out = os.path.join(JSON_DIR, f"CTRLQ_DESBLOQUEIO_{posto}.json")
            atomic_write(out, rows)
            print(f"OK ({len(rows)} registros)")
        except Exception as e:
            print(f"ERRO: {e}")

    if not por_posto:
        print("Nenhum posto exportado."); return

    agora = datetime.now(timezone.utc).astimezone()
    consolidado = {
        "meta": {
            "gerado_em":        agora.isoformat(timespec="seconds"),
            "gerado_em_br":     agora.strftime("%d/%m/%Y, %H:%M"),
            "postos":           sorted(por_posto.keys()),
        },
        "postos": por_posto,
    }
    cons_path = os.path.join(JSON_DIR, "CTRLQ_DESBLOQUEIO_CONSOLIDADO.json")
    atomic_write(cons_path, consolidado)
    total = sum(len(v) for v in por_posto.values())
    print(f"[CONSOLIDADO] {cons_path}  (total={total})")
    print("=== Concluído ===")


if __name__ == "__main__":
    main()
