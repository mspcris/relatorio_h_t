# export_cad_cliente_incremental.py
# --------------------------------------------------------
# Agora totalmente DINÂMICO E CORRIGIDO:
# - Gera:
#     1) pormatricula.json                (flat antigo)
#     2) pormatricula_tipo.json           (PF/PJ separado)
#     3) pormatricula_consolidado.json    (PF+PJ somado)
#     4) porvida.json                     (vidas PF)
#     5) porvida_beneficiarios.json       (beneficiários PF)
#     6) porvida_beneficiarios_pj.json    (novo — só PJ)
# --------------------------------------------------------

import os
import json
import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from export_governanca import (
    build_conns_from_env,
    ensure_dir,
    make_engine,
    POSTOS,
    load_sql_strip_go,
)

# --------------------------------------------------------
# METADADOS
# --------------------------------------------------------

def add_metadata(payload: dict) -> dict:
    payload["_meta"] = {
        "gerado_em": datetime.now(timezone.utc).astimezone().isoformat()
    }
    return payload


# --------------------------------------------------------
# PATHS
# --------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR  = os.path.join(BASE_DIR, "SQL_cadastro")
JSON_DIR = os.path.join(BASE_DIR, "json_cadastro")


# --------------------------------------------------------
# UTIL
# --------------------------------------------------------

def save_json(path: str, data):
    ensure_dir(os.path.dirname(path))
    data = add_metadata(data)

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# --------------------------------------------------------
# EXPORTAÇÕES
# --------------------------------------------------------

def export_matriculas(sql_path: str, json_out: str, conns: dict, postos: list):
    print(f"\n=== EXPORTANDO {json_out} ===")

    sql_text = load_sql_strip_go(sql_path)
    final = {}

    for posto, conn_str in conns.items():
        if posto not in postos:
            continue

        print(f"[{json_out}] Posto {posto} → consultando...")
        try:
            engine = make_engine(conn_str)
            with engine.connect() as con:
                df = pd.read_sql_query(text(sql_text), con)
        except Exception as e:
            print(f"[ERRO] Exec SQL {posto}: {e}")
            continue

        required = {"posto", "total"}
        if not required.issubset(df.columns):
            print(f"[ERRO] SQL deve retornar: posto, total")
            print(f"Recebido: {df.columns.tolist()}")
            return

        total_post = int(df["total"].sum()) if not df.empty else 0
        final[posto] = total_post

    save_json(json_out, final)
    print(f"[OK] Gerado: {json_out}")


def export_vidas(sql_path: str, json_out: str, conns: dict, postos: list, col_total="total"):
    print(f"\n=== EXPORTANDO {json_out} ===")

    sql_text = load_sql_strip_go(sql_path)
    final = {}

    for posto, conn_str in conns.items():
        if posto not in postos:
            continue

        print(f"[{json_out}] Posto {posto} → consultando...")
        try:
            engine = make_engine(conn_str)
            with engine.connect() as con:
                df = pd.read_sql_query(text(sql_text), con)
        except Exception as e:
            print(f"[ERRO] Exec SQL {posto}: {e}")
            continue

        required = {"faixa_etaria", col_total}
        if not required.issubset(df.columns):
            print(f"[ERRO] SQL deve retornar: faixa_etaria, {col_total}")
            print(f"Recebido: {df.columns.tolist()}")
            return

        if df.empty:
            final[posto] = {}
        else:
            final[posto] = (
                df.set_index("faixa_etaria")[col_total]
                .astype(int)
                .to_dict()
            )

    save_json(json_out, final)
    print(f"[OK] Gerado: {json_out}")


# ---------------------- NOVOS EXPORTADORES ------------------------------

def export_matriculas_tipo(sql_path: str, json_out: str, conns: dict, postos: list):
    print(f"\n=== EXPORTANDO {json_out} (PF/PJ) ===")

    sql_text = load_sql_strip_go(sql_path)
    final = {}

    for posto, conn_str in conns.items():
        if posto not in postos:
            continue

        try:
            engine = make_engine(conn_str)
            with engine.connect() as con:
                df = pd.read_sql_query(text(sql_text), con)
        except Exception as e:
            print(f"[ERRO] Exec SQL {posto}: {e}")
            continue

        df = df[df["posto"] == posto]
        if df.empty:
            final[posto] = {}
            continue

        mapa = {}

        # salva PF e PJ
        for _, row in df.iterrows():
            tipo = row["tipo"]          # "F" ou "J"
            mapa[tipo] = int(row["matriculas"])

        # campos corrigidos — AGORA USA ticket_medio_real / ticket_medio_previsto
        if "ticket_medio_real" in df.columns:
            mapa["ticket_medio_real"] = float(df["ticket_medio_real"].iloc[0])

        if "ticket_medio_previsto" in df.columns:
            mapa["ticket_medio_previsto"] = float(df["ticket_medio_previsto"].iloc[0])

        final[posto] = mapa

    save_json(json_out, final)
    print(f"[OK] Gerado: {json_out}")


# --------------------------------------------------------
# REGISTRO DE TIPOS DE SQL
# --------------------------------------------------------

SQL_HANDLERS = {
    "matriculas": {
        "required": {"posto", "total"},
        "func": lambda sql, out, conns, postos: export_matriculas(sql, out, conns, postos)
    },
    "matriculas_tipo": {
        "required": {"posto", "tipo", "matriculas"},
        "func": lambda sql, out, conns, postos: export_matriculas_tipo(sql, out, conns, postos)
    },
    "matriculas_consolidado": {
        "required": {"posto", "tipo", "matriculas"},
        "func": lambda sql, out, conns, postos: export_matriculas_consolidado(sql, out, conns, postos)
    },
    "vidas": {
        "required": {"posto", "faixa_etaria", "total"},
        "func": lambda sql, out, conns, postos: export_vidas(sql, out, conns, postos, col_total="total")
    },
    "beneficiarios": {
        "required": {"posto", "faixa_etaria", "beneficiarios"},
        "func": lambda sql, out, conns, postos: export_vidas(sql, out, conns, postos, col_total="beneficiarios")
    },
    "beneficiarios_pj": {
        "required": {"posto", "faixa_etaria", "total"},
        "func": lambda sql, out, conns, postos: export_vidas(sql, out, conns, postos, col_total="total")
    }
}


# --------------------------------------------------------
# DETECTOR INTELIGENTE
# --------------------------------------------------------

def detect_sql_type(df: pd.DataFrame):
    cols = set(df.columns)

    if {"posto", "tipo", "matriculas"}.issubset(cols):
        return "matriculas_tipo"

    if {"posto", "tipo", "matriculas"}.issubset(cols):
        return "matriculas_consolidado"

    if {"posto", "faixa_etaria", "beneficiarios"}.issubset(cols):
        return "beneficiarios"

    if {"posto", "faixa_etaria", "total"}.issubset(cols):
        if "tipo" not in cols:
            return "beneficiarios_pj"
        return "vidas"

    if {"posto", "total"}.issubset(cols):
        return "matriculas"

    return None


# --------------------------------------------------------
# EXECUÇÃO DINÂMICA
# --------------------------------------------------------

def run_dynamic(conns, postos):
    print("========== EXPORT DINÂMICO DE SQLs ==========")

    for file in os.listdir(SQL_DIR):
        if not file.endswith(".sql"):
            continue

        sql_path = os.path.join(SQL_DIR, file)
        base_name = file.replace("sql_", "").replace(".sql", "")
        json_out = os.path.join(JSON_DIR, f"{base_name}.json")

        print(f"\n=== PROCESSANDO {file} → {base_name}.json ===")

        sql_text = load_sql_strip_go(sql_path)
        if not sql_text:
            print(f"[ERRO] SQL vazio: {sql_path}")
            continue

        sample_posto = next(iter(conns))
        try:
            engine = make_engine(conns[sample_posto])
            with engine.connect() as con:
                df_sample = pd.read_sql_query(text(sql_text), con)
        except Exception as e:
            print(f"[ERRO] Ao amostrar SQL {file}: {e}")
            continue

        sql_type = detect_sql_type(df_sample)
        if not sql_type:
            print(f"[ERRO] Estrutura não reconhecida. Colunas: {df_sample.columns.tolist()}")
            continue

        handler = SQL_HANDLERS[sql_type]["func"]
        handler(sql_path, json_out, conns, postos)

    print("\n✔ Concluído.")


# --------------------------------------------------------
# MAIN
# --------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--postos", default="".join(POSTOS))
    return parser.parse_args()


def run():
    args = parse_args()
    ensure_dir(JSON_DIR)

    postos = [c for c in args.postos if c.isalpha()]
    conns = build_conns_from_env(postos)

    if not conns:
        print("ERRO: Nenhuma conexão configurada no .env")
        return

    run_dynamic(conns, postos)


if __name__ == "__main__":
    run()
