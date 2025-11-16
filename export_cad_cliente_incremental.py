# export_cadastro_incremental.py
# Pipeline incremental para cadastros:
#   - sql_pormatricula.sql  -> json_cadastro/pormatricula.json
#   - sql_porvida.sql       -> json_cadastro/porvida.json
#
# Estratégia:
#   - Para cada posto:
#       - ler JSON atual
#       - achar max(idcliente) daquele posto
#       - buscar somente idcliente > max
#       - append no JSON e salvar
#
import os
import json
import argparse
from datetime import datetime, timezone  # para carimbo de data/hora

import pandas as pd
from sqlalchemy import text

from export_governanca import (
    build_conns_from_env,
    ensure_dir,
    make_engine,
    POSTOS,
    load_sql_strip_go,  # reaproveitado do export_governanca
)

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SQL_DIR    = os.path.join(BASE_DIR, "SQL_cadastro")
JSON_DIR   = os.path.join(BASE_DIR, "json_cadastro")

SQL_POR_MATRICULA = os.path.join(SQL_DIR, "sql_pormatricula.sql")
SQL_POR_VIDA      = os.path.join(SQL_DIR, "sql_porvida.sql")


def load_json(path: str):
    """Carrega JSON existente (dict posto -> lista de registros + meta)."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception:
            # fallback hard se o arquivo estiver corrompido
            return {}
    if not isinstance(data, dict):
        return {}
    return data


def _normalize_for_json(obj):
    """
    Converte objetos não-serializáveis (Timestamp, datetime64, etc.)
    em tipos nativos serializáveis em JSON.
    """
    # dict
    if isinstance(obj, dict):
        return {k: _normalize_for_json(v) for k, v in obj.items()}

    # lista
    if isinstance(obj, list):
        return [_normalize_for_json(v) for v in obj]

    # pandas.Timestamp
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    # numpy.datetime64, se aparecer
    try:
        import numpy as np  # type: ignore
        if isinstance(obj, np.datetime64):
            return pd.to_datetime(obj).isoformat()
    except ImportError:
        pass

    return obj


def save_json(path: str, data):
    ensure_dir(os.path.dirname(path))
    safe_data = _normalize_for_json(data)

    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(safe_data, f, ensure_ascii=False, indent=2)

    # troca atômica (na prática, "commit" do arquivo)
    os.replace(tmp_path, path)


def get_last_idcliente(data: dict, posto: str) -> int:
    """Retorna o maior idcliente já persistido para o posto."""
    registros = data.get(posto, [])
    if not registros:
        return 0
    max_id = 0
    for r in registros:
        try:
            val = int(r.get("idcliente", 0) or 0)
        except Exception:
            val = 0
        if val > max_id:
            max_id = val
    return max_id


def fetch_incremental(engine, sql_text: str, last_id: int) -> pd.DataFrame:
    """Executa o SELECT incremental com last_idcliente como parâmetro."""
    with engine.connect() as con:
        df = pd.read_sql_query(text(sql_text), con, params={"last_id": last_id})
    return df


def process_dataset(nome: str, sql_path: str, json_name: str, conns: dict,
                    postos, reset: bool = False):
    """
    nome: rótulo para logs (pormatricula / porvida)
    sql_path: caminho do arquivo .sql
    json_name: nome do arquivo JSON final
    conns: dict posto -> conn_str
    postos: lista de postos a processar
    reset: se True, ignora incremental (last_id = 0 sempre)
    """
    print(f"\n=== DATASET {nome} ===")

    sql_text = load_sql_strip_go(sql_path)
    if not sql_text:
        print(f"[ERRO] SQL vazio em {sql_path}")
        return

    json_path = os.path.join(JSON_DIR, json_name)
    data = {} if reset else load_json(json_path)

    # loop de postos
    for posto, conn_str in conns.items():
        if posto not in postos:
            continue

        print(f"[{nome}] Posto {posto} -> conectando...")
        try:
            engine = make_engine(conn_str)
        except Exception as e:
            print(f"[{nome}] Posto {posto} ERRO engine: {e}")
            continue

        last_id = 0 if reset else get_last_idcliente(data, posto)
        print(f"[{nome}] Posto {posto} last_idcliente atual = {last_id}")

        try:
            df = fetch_incremental(engine, sql_text, last_id)
        except Exception as e:
            print(f"[{nome}] Posto {posto} ERRO exec: {e}")
            continue

        if df.empty:
            print(f"[{nome}] Posto {posto} sem novos registros.")
            continue

        # garante posto no payload
        df.insert(0, "posto", posto)

        # Conversão segura para JSON: datas em ISO, tipos nativos
        registros_json = json.loads(
            df.to_json(orient="records", date_format="iso")
        )

        data.setdefault(posto, []).extend(registros_json)
        print(
            f"[{nome}] Posto {posto} +{len(registros_json)} registros "
            f"(total agora = {len(data[posto])})"
        )

    # carimbo de data/hora do último incremento deste dataset
    data["_last_increment"] = datetime.now(timezone.utc).astimezone().isoformat()

    # persistência única ao final do dataset
    save_json(json_path, data)
    print(f"[{nome}] JSON atualizado -> {json_path}")


def parse_args():
    p = argparse.ArgumentParser(
        description="Export incremental de cadastros (pormatricula / porvida)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--postos",
        default="".join(POSTOS),
        help="Subset de postos. Ex.: ANX"
    )
    p.add_argument(
        "--reset",
        action="store_true",
        help="Ignora incremental: recarrega tudo (last_id=0 para todos)."
    )
    return p.parse_args()


def run():
    args = parse_args()
    ensure_dir(SQL_DIR)
    ensure_dir(JSON_DIR)

    postos = [c for c in args.postos if c.isalpha()]
    conns = build_conns_from_env(postos)
    if not conns:
        print("ERRO: .env sem DB_HOST_*/DB_BASE_* configurados para os postos informados.")
        return

    print("========== EXPORT CADASTRO INCREMENTAL ==========")
    print(f"- Postos: {list(conns.keys())}")
    print(f"- SQL dir: {SQL_DIR}")
    print(f"- JSON dir: {JSON_DIR}")
    print(f"- Reset={bool(args.reset)}")

    # Dataset 1: por matrícula
    process_dataset(
        nome="pormatricula",
        sql_path=SQL_POR_MATRICULA,
        json_name="pormatricula.json",
        conns=conns,
        postos=postos,
        reset=bool(args.reset),
    )

    # Dataset 2: por vida
    process_dataset(
        nome="porvida",
        sql_path=SQL_POR_VIDA,
        json_name="porvida.json",
        conns=conns,
        postos=postos,
        reset=bool(args.reset),
    )

    print("\n✅ Concluído export_cadastro_incremental.py.")


if __name__ == "__main__":
    run()
