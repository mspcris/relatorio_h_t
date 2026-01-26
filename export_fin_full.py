# export_fin_full.py
# Pipeline separado para fin_receita_full e fin_despesa_full

import os, re, glob, json, argparse
import pandas as pd
from datetime import date, datetime  # <-- inclui datetime

# Reuso de utilitários do pipeline legado
from export_governanca import (
    build_conns_from_env,
    run_query,
    month_bounds,
    month_iter,
    ym_to_date,
    current_month_bounds,
    EARLIEST_ALLOWED,
    POSTOS,
    ensure_dir,
    make_engine,
)

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
SQL_FULL_DIR    = os.path.join(BASE_DIR, "sql_full")          # Ajustado: minúsculo
DADOS_FULL_DIR  = os.path.join(BASE_DIR, "dados_fin_full")    # Garantir minúsculo
JSON_FULL_DIR   = os.path.join(BASE_DIR, "json_fin_full")     # Garantir minúsculo


def load_sql_strip_go(path: str) -> str:
    txt = open(path, "r", encoding="utf-8", errors="ignore").read()
    txt = re.sub(r"(?im)^\s*go\s*$", "", txt)
    return txt.strip()


def collect_full_sql_files():
    """
    Carrega todos os SQLs em sql_full/*.sql
    """
    files = sorted(glob.glob(os.path.join(SQL_FULL_DIR, "*.sql")))
    items = []

    for f in files:
        sql_txt = load_sql_strip_go(f)
        if not sql_txt:
            continue

        key = os.path.splitext(os.path.basename(f))[0]
        items.append({
            "path": f,
            "sql": sql_txt,
            "key": key
        })

    return items



def target_csv_full_path(posto: str, ym: str, key: str) -> str:
    return os.path.join(DADOS_FULL_DIR, f"{posto}_{ym}_{key}.csv")


def build_json_full(key: str):
    """
    Gera um JSON único por tipo:
      json_fin_full/fin_receita_full.json
      json_fin_full/fin_despesa_full.json

    Estrutura final:

    {
      "meta": {
        "dados_gerados_em": "08/12/2025, 16:19",
        "gerado_em": "08/12/2025, 16:19",
        "export_timestamp": "2025-12-08T16:19:00",
        "origem": "fin_receita_full"
      },
      "meses": ["2024-01", "2024-02", ...],
      "postos": ["A", "B", ...],
      "dados": {
        "2024-01": {
          "A": { "linhas": [ {<colunas do select>}, ... ] },
          "B": { "linhas": [ ... ] }
        },
        ...
      }
    }
    """
    ensure_dir(JSON_FULL_DIR)
    pattern = re.compile(
        rf"^(?P<posto>[A-Z])_(?P<ym>\d{{4}}-\d{{2}})_{re.escape(key)}\.csv$",
        re.I,
    )

    dfs = []
    for fn in os.listdir(DADOS_FULL_DIR):
        m = pattern.match(fn)
        if not m:
            continue
        posto = m.group("posto")
        ym = m.group("ym")
        path = os.path.join(DADOS_FULL_DIR, fn)

        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"[WARN] Falha ao ler {path}: {e}")
            continue

        # Enriquecer com posto e mês (colunas auxiliares)
        if df.empty:
            continue

        df.insert(0, "posto", posto)
        df.insert(1, "mes", ym)
        dfs.append(df)


    if not dfs:
        print(f"[INFO] Nenhum CSV encontrado para {key}; JSON não gerado.")
        return

    big = pd.concat(dfs, ignore_index=True)

    # ---------------- META ----------------
    agora_br = datetime.now().strftime("%d/%m/%Y, %H:%M")
    agora_iso = datetime.now().isoformat(timespec="seconds")

    meta = {
        # compatível com vários front-ends
        "dados_gerados_em": agora_br,
        "gerado_em": agora_br,
        "export_timestamp": agora_iso,
        "origem": key,  # ex: "fin_receita_full"
    }

    # ---------------- LISTAS ----------------
    lista_meses = sorted(big["mes"].dropna().unique().tolist())
    lista_postos = sorted(big["posto"].dropna().unique().tolist())

    # ---------------- DADOS AGRUPADOS ----------------
    dados = {}
    for _, row in big.iterrows():
        mes = row["mes"]
        posto = row["posto"]

        payload = row.to_dict()
        # remove chaves auxiliares (ficam só os campos do SELECT)
        payload.pop("mes", None)
        payload.pop("posto", None)

        slot = dados.setdefault(mes, {}).setdefault(
            posto,
            {
                "linhas": [],
                "valor_total": 0.0,
                "qtd": 0,
            }
        )

        # JSON-safe: NaN → None
        for k, v in payload.items():
            if pd.isna(v):
                payload[k] = None

        slot["linhas"].append(payload)

        # agrega somente se houver valor numérico
        val = payload.get("valorpago")
        if isinstance(val, (int, float)):
            slot["valor_total"] += float(val)

        slot["qtd"] += 1


    saida = {
        "meta": meta,
        "meses": lista_meses,
        "postos": lista_postos,
        "dados": dados,
    }

    out_path = os.path.join(JSON_FULL_DIR, f"{key}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(saida, f, ensure_ascii=False, indent=2)

    print(
        f"[JSON] Gerado -> {os.path.relpath(out_path, BASE_DIR)}  linhas={len(big)}"
    )


def parse_args():
    p = argparse.ArgumentParser(
        description="Export full: fin_receita_full / fin_despesa_full",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--from", dest="from_ym", default=None, help="Início YYYY-MM. Default=2024-01.")
    p.add_argument("--to", dest="to_ym", default=None, help="Fim exclusivo YYYY-MM. Default=início do mês corrente.")
    p.add_argument("--only-month", dest="only_month", default=None, help="Executa só um mês YYYY-MM.")
    p.add_argument("--postos", default="".join(POSTOS), help="Subset de postos. Ex: ANX.")
    p.add_argument("--force", action="store_true", help="Sobrescreve CSVs existentes.")
    p.add_argument("--dry-run", action="store_true", help="Executa sem gravar CSV/JSON.")
    return p.parse_args()


def run():
    args = parse_args()
    print("========== EXPORT FIN FULL ==========")

    ensure_dir(DADOS_FULL_DIR)
    ensure_dir(JSON_FULL_DIR)

    ini_cur, fim_cur, ym_current = current_month_bounds()

    if args.only_month:
        start = ym_to_date(args.only_month)
        end_exclusive = month_bounds(start)[1]
        periodo_desc = f"{args.only_month}"
    else:
        start = ym_to_date(args.from_ym) if args.from_ym else EARLIEST_ALLOWED
        end_exclusive = ym_to_date(args.to_ym) if args.to_ym else fim_cur
        periodo_desc = f"{start} .. < {end_exclusive}"

    postos = [c for c in args.postos if c.isalpha()]
    conns = build_conns_from_env(postos)
    if not conns:
        print("ERRO: .env sem DB_HOST_*/DB_BASE_* configurados para os postos informados.")
        return

    sqls = collect_full_sql_files()
    keys = sorted({s["key"] for s in sqls})
    if not sqls:
        print(f"ERRO: Sem SQLs full em {SQL_FULL_DIR}.")
        return

    print(f"- Período: {periodo_desc}")
    print(f"- Mês corrente: {ym_current}")
    print(f"- Postos: {list(conns.keys())}")
    print(f"- SQLs full: {[os.path.basename(s['path']) for s in sqls]}")
    print(f"- Force={args.force}  DryRun={args.dry_run}")

    # Execução
    for month_start in month_iter(start, end_exclusive):
        ini, fim, ym = month_bounds(month_start)
        print(f"\n-- Mês {ym} ----------------------------")
        for posto, odbc_str in conns.items():
            print(f"   [{posto}] conectando...")
            try:
                engine = make_engine(odbc_str)
            except Exception as e:
                print(f"   [{posto}] ERRO engine: {e}")
                continue

            for entry in sqls:
                key = entry["key"]  # fin_receita_full ou fin_despesa_full
                sql_txt = entry["sql"]
                out_path = target_csv_full_path(posto, ym, key)

                if not args.force and os.path.exists(out_path):
                    print(f"   [{posto}] SKIP {os.path.basename(out_path)} (existe)")
                    continue

                print(f"   [{posto}] RUN {os.path.basename(entry['path'])} -> {os.path.basename(out_path)}")
                if args.dry_run:
                    continue

                try:
                    df = run_query(engine, sql_txt, ini, fim)
                except Exception as e:
                    print(f"   [{posto}] ERRO exec: {e}")
                    continue

                try:
                    df.to_csv(out_path, index=False, encoding="utf-8-sig")
                    print(f"   [{posto}] OK linhas={len(df)}")
                except Exception as e:
                    print(f"   [{posto}] ERRO salvar: {e}")

    if args.dry_run:
        print("\n[JSON] SKIP (dry-run)")
        return

    # JSONs separados, conforme solicitado
    print("\n[JSON] Consolidação FULL (automática)")
    for key in keys:
        print(f"[JSON] Consolidação {key}")
        build_json_full(key)


    print("\nConcluído.")


if __name__ == "__main__":
    run()
