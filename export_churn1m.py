# export_churn1m.py — KPI "Churn de um mês" (kpi_churn_1m.html)
#
# Pedido do Cristiano (2026-11-05):
#   "100 clientes fizeram o plano em janeiro. 90 pagaram fevereiro, então
#    churn1m de 10%."
#
# TURMA  = quem fez o plano no mês — a REGRA ÚNICA de venda (export_vendas.py,
#          linhas_do_csv, conta = 1). Nada de segunda contagem de venda aqui:
#          a turma sai dos mesmos CSVs que alimentam o KPI Vendas e o KPI Metas.
# FICOU  = pagou a mensalidade cuja REFERÊNCIA (Fin_Receita.DataMensalidade) é
#          o mês seguinte ao da venda.
#
# O "dia de corte" (padrão 15) NÃO é aplicado aqui. O ETL grava, por cliente,
# vencimento e data de pagamento da mensalidade do mês seguinte, e a página
# aplica o corte na hora — por isso trocar o dia na tela não exige rodar nada.
#
# Só SELECT nos SQL Servers dos postos. Datas vão como parâmetro tipado (date),
# em TABELA (Fin_Receita), nunca em view.
#
# Saída: json_vendas/churn1m.json (o sync_www já copia a pasta json_vendas).
#   {"_meta": {...}, "cols": [...], "turmas": {"2026-08": {"A": {"rows": [...]}}}}
#
# Posto que falha mantém o que já estava no JSON e sai em _meta.erros — falha
# de posto nunca vira "ninguém pagou".

import os
import sys
import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

import pandas as pd
from sqlalchemy import text

from export_governanca import (
    build_conns_from_env,
    ensure_dir,
    make_engine,
    POSTOS,
    ym_to_date,
    month_bounds,
    month_iter,
)
from export_vendas import linhas_do_csv, target_csv_path
from etl_meta import ETLMeta

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
JSON_VENDAS_DIR = os.path.join(BASE_DIR, "json_vendas")
OUT_PATH        = os.path.join(JSON_VENDAS_DIR, "churn1m.json")

INICIO_PADRAO   = "2025-01"   # primeira turma exportada (--from muda)
MESES_REFAZ     = 4           # turmas recentes relidas a cada execução (mês corrente
                              # e os 3 anteriores): é onde a mensalidade do mês
                              # seguinte ainda está sendo paga. Mais antigo que isso
                              # só muda com --force.
LOTE_IDS        = 900         # ids por IN (...)

# Colunas de cada linha — a página lê por nome (cols), não por posição fixa.
COLS = ["matricula", "nome", "admissao", "plano", "vendedor", "venda",
        "venc", "pag", "sit", "valor"]

# sit: P = paga · A = em aberto · B = abonada · C = cancelada
#      S = sem mensalidade lançada para a referência do mês seguinte
_ORDEM_SIT = {"P": 0, "A": 1, "B": 2, "C": 3}

# Sem dois-pontos fora dos parâmetros (o text() do SQLAlchemy lê como bind).
SQL_MENSALIDADES = """
SELECT r.idCliente, r.DataVencimento, r.DataPagamento, r.DataCancelamento,
       r.DataAbono, r.Valor
FROM Fin_Receita r
JOIN Fin_ContaTipo ct ON ct.idContaTipo = r.idContaTipo
WHERE ct.Tipo = 'Mensalidade'
  AND r.idCliente IN ({ids})
  AND r.DataMensalidade >= :ini AND r.DataMensalidade < :fim
"""

SQL_NOMES_POSTOS = "SELECT Codigo, Descricao FROM cad_endereco WHERE LEN(Codigo) = 1"


def _d(v) -> str:
    """Data (sem hora) em ISO, ou '' — para a página comparar como texto."""
    if v is None:
        return ""
    try:
        return v.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _vendedor(l: dict) -> str:
    """Corretor BROKER → quem vendeu é o subcorretor (regra do KPI Vendas)."""
    corr, sub = l.get("corretor") or "", l.get("subcorretor") or ""
    if "BROKER" in corr.upper() and sub:
        return sub
    return corr or sub


def turma_do_mes(posto: str, ym: str):
    """Quem fez o plano no mês, pela regra única (conta = 1).
    Devolve (lista de (idCliente, linha classificada), desativados de fora)
    ou None se o CSV de vendas do posto/mês não existe."""
    path = target_csv_path(posto, ym)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, dtype={"Matricula": str})
    except pd.errors.EmptyDataError:
        return [], 0
    recs = df.to_dict("records")
    turma, fora_desativados = [], 0
    for rec, l in zip(recs, linhas_do_csv(df, posto)):
        if l["conta"]:
            try:
                turma.append((int(float(rec.get("idCliente"))), l))
            except (TypeError, ValueError):
                continue
        elif l["mens_pagas"] > 0 and l["taxa_pagas"] > 0 and l["desativado"]:
            # completou o plano e foi desativado depois: fora da regra única de
            # venda, logo fora da turma. A página mostra quantos são.
            fora_desativados += 1
    return turma, fora_desativados


def mensalidades_do_mes_seguinte(con, ids: list, ini: date, fim: date) -> dict:
    """idCliente -> (sit, venc, pag, valor) da mensalidade de referência [ini, fim).
    Cliente com mais de uma receita na referência fica com a melhor delas:
    paga (a de pagamento mais cedo) > aberta > abonada > cancelada."""
    melhor = {}
    for i in range(0, len(ids), LOTE_IDS):
        lote = ",".join(str(int(x)) for x in ids[i:i + LOTE_IDS])
        res = con.execute(text(SQL_MENSALIDADES.format(ids=lote)), {"ini": ini, "fim": fim})
        for idc, venc, pag, canc, abono, valor in res.fetchall():
            if pag is not None:
                sit = "P"
            elif canc is not None:
                sit = "C"
            elif abono is not None:
                sit = "B"
            else:
                sit = "A"
            chave = (_ORDEM_SIT[sit], _d(pag) or "9999")
            atual = melhor.get(idc)
            if atual is None or chave < atual[0]:
                melhor[idc] = (chave, (sit, _d(venc), _d(pag) if sit == "P" else "",
                                       None if valor is None else round(float(valor), 2)))
    return {k: v[1] for k, v in melhor.items()}


def processa_posto(posto: str, odbc: str, meses: list, verbose=True) -> dict:
    """Roda os meses pedidos de um posto. Devolve
    {"ok": {ym: entrada}, "erro": msg|None, "nomes": {codigo: nome}|None}."""
    out = {"ok": {}, "erro": None, "nomes": None}
    try:
        engine = make_engine(odbc)
    except Exception as e:
        out["erro"] = f"engine: {e}"
        return out

    for ym in meses:
        t = turma_do_mes(posto, ym)
        if t is None:
            continue   # sem CSV de vendas: mês ainda não exportado para este posto
        turma, fora = t
        _, fim_venda, _ = month_bounds(ym_to_date(ym))
        ini_ref, fim_ref, ym_ref = month_bounds(fim_venda)

        achadas = {}
        if turma:
            ids = [idc for idc, _ in turma]
            for tentativa in (1, 2):
                try:
                    with engine.connect() as con:
                        achadas = mensalidades_do_mes_seguinte(con, ids, ini_ref, fim_ref)
                        if out["nomes"] is None:
                            try:
                                out["nomes"] = {str(c).strip().upper(): str(d).strip()
                                                for c, d in con.execute(text(SQL_NOMES_POSTOS)).fetchall()
                                                if c and d}
                            except Exception:
                                pass
                    break
                except Exception as e:
                    if tentativa == 2:
                        out["erro"] = f"{ym}: {str(e)[:300]}"
                        if verbose:
                            print(f"   [{posto}] {ym} ERRO: {str(e)[:200]}")
                        return out   # link do posto caiu: não insiste nos outros meses
                    time.sleep(3)

        rows = []
        for idc, l in turma:
            sit, venc, pag, valor = achadas.get(idc, ("S", "", "", None))
            rows.append([l["matricula"], l["nome"], l["admissao"], l["plano"], _vendedor(l),
                         (l["data_completo"] or "")[:10], venc, pag, sit, valor])
        rows.sort(key=lambda r: (r[5], r[1]))
        out["ok"][ym] = {"ref": ym_ref, "rows": rows, "fora_desativados": fora,
                         "lido_em": datetime.now().isoformat(timespec="seconds")}
        if verbose:
            pagas = sum(1 for r in rows if r[8] == "P")
            sem = sum(1 for r in rows if r[8] == "S")
            print(f"   [{posto}] venda {ym} → ref {ym_ref}: turma={len(rows)} pagas={pagas} "
                  f"sem_lancamento={sem} desativados_fora={fora}")
    return out


def meses_a_refazer(n: int) -> set:
    t = date.today()
    y, m = t.year, t.month
    out = set()
    for _ in range(n):
        out.add(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def carregar_anterior() -> dict:
    try:
        with open(OUT_PATH, encoding="utf-8") as f:
            j = json.load(f)
        if j.get("cols") == COLS and isinstance(j.get("turmas"), dict):
            return j
    except Exception:
        pass
    return {"_meta": {}, "cols": COLS, "turmas": {}}


def parse_args():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--from", dest="from_ym", default=INICIO_PADRAO, help="primeira turma (mês da venda)")
    p.add_argument("--only-month", dest="only_month", default=None, help="só esta turma (YYYY-MM)")
    p.add_argument("--meses", type=int, default=MESES_REFAZ, help="turmas recentes relidas a cada execução")
    p.add_argument("--postos", default="".join(POSTOS))
    p.add_argument("--force", action="store_true", help="relê todas as turmas, não só as recentes")
    p.add_argument("--dry-run", action="store_true", help="consulta e mostra, sem gravar nada")
    return p.parse_args()


def run():
    args = parse_args()
    print(f"========== EXPORT CHURN 1M ({datetime.now().isoformat(timespec='seconds')}) ==========")

    ensure_dir(JSON_VENDAS_DIR)
    meta = ETLMeta("export_churn1m", "json_vendas")

    hoje = date.today()
    ini = ym_to_date(args.from_ym)
    fim_excl = month_bounds(hoje)[1]
    todos = [month_bounds(d)[2] for d in month_iter(ini, fim_excl)]
    if args.only_month:
        todos = [args.only_month]

    anterior = carregar_anterior()
    turmas = anterior["turmas"]
    recentes = meses_a_refazer(args.meses)

    postos = [c for c in args.postos.upper() if c.isalpha()]
    conns = build_conns_from_env(postos)

    plano = {}
    for p in conns:
        plano[p] = [ym for ym in todos
                    if args.force or args.only_month or ym in recentes
                    or p not in (turmas.get(ym) or {})]
    total = sum(len(v) for v in plano.values())
    print(f"Postos: {''.join(conns)} · turmas {todos[0]}..{todos[-1]} · consultas previstas: {total}")

    with ThreadPoolExecutor(max_workers=max(1, len(conns))) as ex:
        futuros = {p: ex.submit(processa_posto, p, conns[p], plano[p]) for p in conns}
        resultados = {p: f.result() for p, f in futuros.items()}

    erros, nomes = {}, None
    for p, r in resultados.items():
        for ym, entrada in r["ok"].items():
            turmas.setdefault(ym, {})[p] = entrada
        if r["erro"]:
            erros[p] = r["erro"]
            meta.error(p, r["erro"])
        else:
            meta.ok(p)
        if nomes is None and r["nomes"]:
            nomes = r["nomes"]

    nomes_postos = {p: n for p, n in (nomes or {}).items() if p in POSTOS} \
        or (anterior.get("_meta") or {}).get("postos_nomes") or {}

    payload = {
        "_meta": {
            "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "dados_ate": hoje.isoformat(),
            "regra": ("turma = quem fez o plano no mês (regra única de venda, KPI Vendas); "
                      "ficou = pagou a mensalidade com referência no mês seguinte; "
                      "o dia de corte é aplicado na página, sobre a data de pagamento"),
            "postos_nomes": nomes_postos,
            "erros": erros,
            "sit": {"P": "paga", "A": "em aberto", "B": "abonada", "C": "cancelada",
                    "S": "sem mensalidade lançada para a referência"},
        },
        "cols": COLS,
        "turmas": {ym: turmas[ym] for ym in sorted(turmas)},
    }

    if erros:
        print("POSTOS COM ERRO (ficou o dado anterior): " + ", ".join(f"{p} ({m[:80]})" for p, m in erros.items()))

    if args.dry_run:
        print("[dry-run] nada gravado.")
        return

    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    os.replace(tmp, OUT_PATH)
    meta.save()
    n = sum(len(e["rows"]) for t in payload["turmas"].values() for e in t.values())
    print(f"✔ {os.path.relpath(OUT_PATH, BASE_DIR)} — {len(payload['turmas'])} turmas, {n} clientes, "
          f"{os.path.getsize(OUT_PATH) / 1e6:.1f} MB")


if __name__ == "__main__":
    run()
