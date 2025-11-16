# export_cad_cliente_incremental.py
# --------------------------------------------------------
# Gera JSONs simplificados, sem incremental.
#
# pormatricula.json:
#   { "A": 1234, "B": 455, ... }
#
# porvida.json:
#   { "A": { "0 a 18": 200, "19 a 23": 120 }, "B": {...} }
#
# Os SQLs devem retornar:
#   Matrícula → posto, total
#   Vidas     → posto, faixa_etaria, total
# --------------------------------------------------------

import os
import json
import argparse

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
# PATHS
# --------------------------------------------------------

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SQL_DIR    = os.path.join(BASE_DIR, "SQL_cadastro")
JSON_DIR   = os.path.join(BASE_DIR, "json_cadastro")

SQL_POR_MATRICULA = os.path.join(SQL_DIR, "sql_pormatricula.sql")
SQL_POR_VIDA      = os.path.join(SQL_DIR, "sql_porvida.sql")


# --------------------------------------------------------
# UTIL
# --------------------------------------------------------

def save_json(path: str, data):
    """Salva JSON de forma segura."""
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# --------------------------------------------------------
# EXPORTAÇÃO — MATRÍCULAS
# --------------------------------------------------------

def export_matriculas(sql_path: str, json_out: str, conns: dict, postos: list):
    print(f"\n=== EXPORTANDO {json_out} ===")

    sql_text = load_sql_strip_go(sql_path)
    if not sql_text:
        print(f"[ERRO] SQL vazio: {sql_path}")
        return

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

        # SQL deve trazer: posto, total
        required = {"posto", "total"}
        if not required.issubset(df.columns):
            print(f"[ERRO] SQL de matrículas deve retornar: posto, total")
            print(f"Colunas recebidas: {df.columns.tolist()}")
            return

        if df.empty:
            final[posto] = 0
            continue

        total_post = int(df["total"].sum())
        final[posto] = total_post

        print(f"[{json_out}] Posto {posto}: total = {total_post}")

    save_json(json_out, final)
    print(f"[OK] Gerado: {json_out}")


# --------------------------------------------------------
# EXPORTAÇÃO — VIDAS
# --------------------------------------------------------

def export_vidas(sql_path: str, json_out: str, conns: dict, postos: list):
    print(f"\n=== EXPORTANDO {json_out} ===")

    sql_text = load_sql_strip_go(sql_path)
    if not sql_text:
        print(f"[ERRO] SQL vazio: {sql_path}")
        return

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

        # SQL deve trazer: posto, faixa_etaria, total
        required = {"posto", "faixa_etaria", "total"}
        if not required.issubset(df.columns):
            print(f"[ERRO] SQL de vidas deve retornar: posto, faixa_etaria, total")
            print(f"Colunas recebidas: {df.columns.tolist()}")
            return

        if df.empty:
            final[posto] = {}
            continue

        mapa = (
            df.set_index("faixa_etaria")["total"]
              .astype(int)
              .to_dict()
        )

        final[posto] = mapa
        print(f"[{json_out}] Posto {posto}: {len(mapa)} faixas.")

    save_json(json_out, final)
    print(f"[OK] Gerado: {json_out}")


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
    conns  = build_conns_from_env(postos)

    if not conns:
        print("ERRO: Nenhuma conexão configurada no .env")
        return

    print("========== EXPORT CADASTRO TOTALIZADO ==========")
    print(f"Postos: {list(conns.keys())}")

    export_matriculas(
        sql_path=SQL_POR_MATRICULA,
        json_out=os.path.join(JSON_DIR, "pormatricula.json"),
        conns=conns,
        postos=postos,
    )

    export_vidas(
        sql_path=SQL_POR_VIDA,
        json_out=os.path.join(JSON_DIR, "porvida.json"),
        conns=conns,
        postos=postos,
    )

    print("\n✔ Concluído.")


if __name__ == "__main__":
    run()
