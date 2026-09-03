#!/usr/bin/env python3
"""
export_farmacia.py — ETL "Farmácia · Saídas e Consumo por posto".

Pergunta que responde (Cristiano, 2026-09-03): "quanto saiu da farmácia de
cada posto nos últimos X meses, para quem, de onde veio o que entrou, e quanto
tem em estoque agora — para eu decidir quanto mandar".

Quatro leituras por posto, todas nas tabelas base, só SELECT:

  sql/farmacia_saidas.sql    Est_Saida/Est_SaidaItem → o que a farmácia
                             ENTREGOU (enfermagem, laboratório, recepção...)
                             ou MANDOU (outro posto, descarte). É a "saída".
  sql/farmacia_entradas.sql  Est_Entrada/Est_EntradaItem → o que ENTROU e de
                             quem (fornecedor, outro posto, acerto).
  sql/farmacia_consumo.sql   Cad_LancamentoServico classe MEDICAMENTO* → o
                             que foi LANÇADO AO PACIENTE (consumo real).
  sql/farmacia_estoque.sql   Cad_Produto + lotes → foto do estoque de hoje.

Saída: um JSON por posto (json_consolidado/farmacia_<P>.json) + um índice
(farmacia_index.json). A página abre um posto de cada vez — arquivo pequeno,
carga rápida; o índice diz quais postos têm dado e quando foi gerado.

Decisões que não são óbvias no código:

  • idProduto NÃO é o mesmo entre postos (id 25 é Benzetacil em Campinho e
    Diazepam em Realengo). Todo cruzamento saída × estoque × entrada é DENTRO
    do mesmo posto, por idProduto. Comparar com o estoque do posto remetente é
    por nome normalizado, feito na página, com a qualidade do casamento à
    vista.
  • Saída com Gravado NULL (Numero 0) é rascunho — a vw_Est_Saida do CAMIM
    não conta. Fica fora do consumo, mas o total vai em `rascunho` para a
    página avisar quanto ficou de fora (~5 % em Anchieta).
  • QuantidadeEnfermaria do cadastro NÃO é estoque (só acumula envios; o
    Cristiano confirmou que a enfermaria não é controlada). Vai como
    referência, nunca como saldo.
  • Posto que falhou fica com o JSON ANTERIOR intacto e marcado no índice
    (mesma regra do medico_custo: falha de posto não pode virar "consumo
    zero" — e consumo zero é exatamente o que faria alguém mandar remédio
    de menos).
"""
from __future__ import annotations

import argparse
import decimal
import json
import math
import os
import re
import sys
import time
import traceback
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# .env do próprio diretório; se rodar de outro lugar (dry-run em /tmp na VM),
# cai no .env de produção.
for _env_path in (os.path.join(BASE_DIR, ".env"), "/opt/relatorio_h_t/.env"):
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
        break
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, "/opt/relatorio_h_t")
try:
    from etl_meta import ETLMeta
except Exception:  # noqa: BLE001
    ETLMeta = None

SQL_DIR = os.path.join(BASE_DIR, "sql")
OUT_DIR_PADRAO = os.path.join(BASE_DIR, "json_consolidado")

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS = list("ANXYBRPCDGIMJ")
_BRT = timezone(timedelta(hours=-3))

# 12 meses fechados + o corrente. Env para mudar sem mexer em código.
MESES_PADRAO = int(os.getenv("FARMACIA_MESES", "13") or 13)
WORKERS = int(os.getenv("FARMACIA_WORKERS", "5") or 5)
TIMEOUT_CONN = int(os.getenv("FARMACIA_TIMEOUT", "30") or 30)

# Fornecedor que na verdade é ajuste de inventário, não entrada de mercadoria.
_ACERTO_RE = re.compile(r"ACERTO|DEVOLU|RECONTAGEM|INVENT|TESTE", re.I)


# ── helpers ──────────────────────────────────────────────────────────────────
def _env(k: str, d: str = "") -> str:
    v = os.getenv(k, d)
    return v.strip() if isinstance(v, str) else v


def _num(v):
    """Decimal/NaN/None → float|None. NaN mata o JSON.parse do browser."""
    if v is None:
        return None
    if isinstance(v, decimal.Decimal):
        v = float(v)
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v):
    n = _num(v)
    return int(n) if n is not None else None


def _str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _data(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return None


def _r2(v):
    n = _num(v)
    return round(n, 2) if n is not None else None


def normalizar(s: str | None) -> str:
    """Sem acento, maiúsculo, só letras/dígitos separados por 1 espaço.
    Mesma regra do `normalizar()` da página — mudar nos dois."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Za-z0-9]+", " ", s).upper().strip()
    return re.sub(r"\s+", " ", s)


def _conn(posto: str):
    host, base = _env(f"DB_HOST_{posto}"), _env(f"DB_BASE_{posto}")
    if not host or not base:
        return None
    cs = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{_env(f'DB_PORT_{posto}', '1433')};DATABASE={base};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        f"UID={_env(f'DB_USER_{posto}')};PWD={_env(f'DB_PASSWORD_{posto}')}"
    )
    return create_engine("mssql+pyodbc:///?odbc_connect=" + quote_plus(cs),
                         pool_pre_ping=True,
                         connect_args={"timeout": TIMEOUT_CONN})


def _sql(nome: str) -> str:
    with open(os.path.join(SQL_DIR, nome), encoding="utf-8") as f:
        return f.read()


def _rows(con, sql: str, **params) -> list[dict]:
    res = con.execute(text(sql), params)
    cols = list(res.keys())
    return [dict(zip(cols, t)) for t in res.fetchall()]


def inicio_janela(meses: int, hoje: date | None = None) -> date:
    hoje = hoje or datetime.now(_BRT).date()
    y, m = hoje.year, hoje.month - (meses - 1)
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1)


# ── cad_endereco: nomes dos postos (NUNCA à mão — regra do CLAUDE.md) ────────
def carregar_enderecos(con) -> dict[int, dict]:
    linhas = _rows(con,
        "SELECT idEndereco, LTRIM(RTRIM(Codigo)) AS codigo, LTRIM(RTRIM(Descricao)) AS nome, "
        "LTRIM(RTRIM(Bairro)) AS bairro, ISNULL(Desativado,0) AS desativado, "
        "ISNULL(AtendimentoAtivoPosto,1) AS atende "
        "FROM cad_endereco WITH (NOLOCK) WHERE Codigo IS NOT NULL")
    return {int(r["idEndereco"]): {"codigo": r["codigo"], "nome": r["nome"] or r["codigo"],
                                   "bairro": r["bairro"],
                                   "ativo": not r["desativado"] and bool(r["atende"])}
            for r in linhas if r["idEndereco"] is not None}


def _classificar_saida(r: dict, posto: str, id_proprio: int | None) -> tuple[str, str]:
    """→ (tipo, destino_legivel).
    interna  = ficou no posto (enfermagem, laboratório, recepção...)
    posto    = transferência para OUTRO posto da rede
    descarte = vencido/inutilizado
    outro    = Laboratório Camim, Resgate, Operadora, Interclínicas..."""
    cod = (r.get("destino_codigo") or "").strip()
    nome = (r.get("destino") or "").strip()
    idend = _int(r.get("id_endereco"))
    if "DESCARTE" in normalizar(nome):
        return "descarte", nome or "Descarte"
    if idend is None or idend == -1 or idend == id_proprio or cod == posto:
        return "interna", nome or "(sem endereço)"
    if cod in POSTOS:
        return "posto", nome
    # cad_endereco também cadastra DEPARTAMENTOS do próprio posto com código
    # numérico ('13' TOMOGRAFIA, '14' RAIOX CAMPINHO, '15' LIMPEZA (Anchieta),
    # '16' ENDOSCOPIA/COLONOSCOPIA, '22' MANUTENÇÃO, '23' TI). Saída para eles
    # ficou no posto — é interna, e o nome do departamento vira o "setor".
    if cod.isdigit():
        return "interna", nome or f"endereço {idend}"
    return "outro", nome or f"endereço {idend}"


def _classificar_entrada(fornecedor: str, nomes_postos: dict[str, str], posto: str) -> tuple[str, str | None]:
    """→ (tipo, codigo_posto|None). Fornecedor 'ANCHIETA' / 'CAMPINHO' /
    'CAMIM CAMPO GRANDE' é transferência de posto. Casa por nome normalizado,
    preferindo o nome mais longo (senão 'CAMPO GRANDE' engole 'X CAMPO GRANDE').
    Qualquer coisa com ACERTO/DEVOLUÇÃO/RECONTAGEM é ajuste de inventário."""
    f = normalizar(fornecedor)
    if not f:
        return "fornecedor", None
    if _ACERTO_RE.search(f):
        return "acerto", None
    f_sem_camim = re.sub(r"^CAMIM\s+", "", f)
    melhor = None
    for cod, nome in sorted(nomes_postos.items(), key=lambda kv: -len(kv[1])):
        n = normalizar(nome)
        if not n:
            continue
        if f_sem_camim == n or f == n:
            melhor = cod
            break
    if melhor and melhor != posto:
        return "posto", melhor
    return "fornecedor", None


# ── coleta de um posto ───────────────────────────────────────────────────────
def coletar(posto: str, ini: date, sqls: dict) -> dict:
    t0 = time.time()
    eng = _conn(posto)
    if eng is None:
        raise RuntimeError("sem credenciais no .env")
    ini_dt = datetime(ini.year, ini.month, ini.day)   # datetime, nunca string
    with eng.connect() as con:
        enderecos = carregar_enderecos(con)
        nomes_postos = {e["codigo"]: e["nome"] for e in enderecos.values() if e["codigo"] in POSTOS}
        id_proprio = next((i for i, e in enderecos.items() if e["codigo"] == posto), None)

        estoque = _rows(con, sqls["estoque"])
        saidas = _rows(con, sqls["saidas"], ini=ini_dt)
        entradas = _rows(con, sqls["entradas"], ini=ini_dt)
        consumo = _rows(con, sqls["consumo"], ini=ini_dt)

    # produtos --------------------------------------------------------------
    produtos = []
    por_servico: dict[int, list[int]] = {}
    for r in estoque:
        pid = _int(r["id_produto"])
        if pid is None:
            continue
        sid = _int(r.get("id_servico"))
        if sid:
            por_servico.setdefault(sid, []).append(pid)
        produtos.append({
            "id": pid,
            "n": _str(r.get("produto")) or f"produto {pid}",
            "g": _str(r.get("grupo")),
            "d": 1 if r.get("desativado") else 0,
            "min": _r2(r.get("qtd_minima")),
            "e": _r2(r.get("estoque_farmacia")),
            "et": _r2(r.get("estoque_total")),
            "ev": _r2(r.get("estoque_vencido")),
            "v90": _r2(r.get("qtd_vence_90d")),
            "pv": _data(r.get("proximo_vencimento")),
            "sid": sid,
            "sn": _str(r.get("servico")),
            "sub": _str(r.get("substancia")),
            "nc": _str(r.get("nome_comercial")),
            "qe": _r2(r.get("qtd_enfermaria_sistema")),
            "ue": {"d": _data(r.get("ultima_entrada")),
                   "f": _str(r.get("ultima_entrada_fornecedor")),
                   "v": _r2(r.get("ultima_entrada_valor_unit")),
                   "q": _r2(r.get("ultima_entrada_qtd"))}
                  if r.get("ultima_entrada") else None,
        })
    ids_produto = {p["id"] for p in produtos}

    # saídas ----------------------------------------------------------------
    linhas_saida, rascunho = [], {"itens": 0, "qtd": 0.0, "por_ym": {}}
    destinos_outro: dict[str, float] = {}
    ultima_saida = None
    for r in saidas:
        pid = _int(r["id_produto"])
        if pid is None:
            continue
        ym = _int(r["ym"])
        qtd = _num(r["qtd"]) or 0.0
        if not r.get("gravado"):
            rascunho["itens"] += _int(r["itens"]) or 0
            rascunho["qtd"] += qtd
            rascunho["por_ym"][str(ym)] = rascunho["por_ym"].get(str(ym), 0.0) + qtd
            continue
        tipo, destino = _classificar_saida(r, posto, id_proprio)
        if tipo == "outro":
            destinos_outro[destino] = destinos_outro.get(destino, 0.0) + qtd
        if pid not in ids_produto:
            # produto apagado do cadastro mas com saída na janela
            produtos.append({"id": pid, "n": f"(produto {pid} fora do cadastro)", "g": None, "d": 1,
                             "min": None, "e": 0, "et": 0, "ev": 0, "v90": 0, "pv": None,
                             "sid": None, "sn": None, "sub": None, "nc": None, "qe": None, "ue": None})
            ids_produto.add(pid)
        u = _data(r.get("ultima"))
        if u and (ultima_saida is None or u > ultima_saida):
            ultima_saida = u
        setor = _str(r.get("setor")) or (destino if (tipo == "interna" and (r.get("destino_codigo") or "").strip().isdigit()) else "(sem setor)")
        linhas_saida.append([pid, ym, tipo, destino, setor,
                             _int(r["itens"]), _int(r["saidas"]), round(qtd, 2)])
    rascunho["qtd"] = round(rascunho["qtd"], 2)
    rascunho["por_ym"] = {k: round(v, 2) for k, v in rascunho["por_ym"].items()}

    # entradas --------------------------------------------------------------
    linhas_entrada = []
    origem_posto_qtd: dict[str, float] = {}
    ultima_entrada = None
    for r in entradas:
        pid = _int(r["id_produto"])
        if pid is None or pid not in ids_produto:
            continue
        tipo, cod = _classificar_entrada(r.get("fornecedor") or "", nomes_postos, posto)
        qtd = _num(r["qtd"]) or 0.0
        if tipo == "posto" and cod:
            origem_posto_qtd[cod] = origem_posto_qtd.get(cod, 0.0) + qtd
        u = _data(r.get("ultima"))
        if u and (ultima_entrada is None or u > ultima_entrada):
            ultima_entrada = u
        linhas_entrada.append([pid, _int(r["ym"]), tipo, cod or (_str(r.get("fornecedor")) or "(sem fornecedor)"),
                               _int(r["itens"]), _int(r["entradas"]), round(qtd, 2), _r2(r.get("valor"))])

    # consumo (lançado ao paciente) -----------------------------------------
    linhas_consumo, servicos, classes = [], {}, set()
    ultimo_consumo = None
    for r in consumo:
        sid = _int(r["id_servico"])
        if sid is None:
            continue
        servicos[str(sid)] = _str(r.get("servico")) or f"serviço {sid}"
        cl = _str(r.get("classe")) or "MEDICAMENTO"
        classes.add(cl)
        u = _data(r.get("ultima"))
        if u and (ultimo_consumo is None or u > ultimo_consumo):
            ultimo_consumo = u
        linhas_consumo.append([sid, _int(r["ym"]), cl, _int(r["lancamentos"]), _int(r["atendimentos"]),
                               _int(r["clientes"]), _r2(r["qtd"]), _r2(r.get("qtd_plano"))])

    # serviço compartilhado por N produtos (SONDA FOLEY 14/16/18/20/22/24 →
    # um só idServico): a página avisa que o consumo é do grupo, não do item.
    for p in produtos:
        p["sids"] = len(por_servico.get(p["sid"], [])) if p.get("sid") else 0

    remetente = max(origem_posto_qtd.items(), key=lambda kv: kv[1])[0] if origem_posto_qtd else None

    resumo = {
        "produtos": len(produtos),
        "produtos_ativos": sum(1 for p in produtos if not p["d"]),
        "saidas_itens": sum(l[5] or 0 for l in linhas_saida),
        "saidas_qtd": round(sum(l[7] or 0 for l in linhas_saida), 2),
        "saidas_qtd_interna": round(sum(l[7] or 0 for l in linhas_saida if l[2] == "interna"), 2),
        "saidas_qtd_posto": round(sum(l[7] or 0 for l in linhas_saida if l[2] == "posto"), 2),
        "entradas_qtd": round(sum(l[6] or 0 for l in linhas_entrada), 2),
        "consumo_qtd": round(sum(l[6] or 0 for l in linhas_consumo), 2),
        "ultima_saida": ultima_saida,
        "ultima_entrada": ultima_entrada,
        "ultimo_consumo": ultimo_consumo,
        "estoque_itens_com_saldo": sum(1 for p in produtos if (p["e"] or 0) > 0),
        "origens_posto": {k: round(v, 2) for k, v in sorted(origem_posto_qtd.items(), key=lambda kv: -kv[1])},
        "destinos_outro": {k: round(v, 2) for k, v in sorted(destinos_outro.items(), key=lambda kv: -kv[1])},
        "tempo_s": round(time.time() - t0, 1),
    }
    return {
        "posto": posto,
        "posto_nome": nomes_postos.get(posto) or posto,
        "postos_nomes": nomes_postos,
        "gerado_em": datetime.now(_BRT).isoformat(timespec="seconds"),
        "janela": {"ini": ini.isoformat(), "fim": datetime.now(_BRT).date().isoformat()},
        "produtos": produtos,
        "saidas": linhas_saida,
        "rascunho": rascunho,
        "entradas": linhas_entrada,
        "consumo": linhas_consumo,
        "servicos": servicos,
        "classes": sorted(classes),
        "remetente_sugerido": remetente,
        "resumo": resumo,
    }


# ── escrita ──────────────────────────────────────────────────────────────────
def _dump(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    os.replace(tmp, path)


def _ler_indice_anterior(out_dir: str) -> dict:
    try:
        with open(os.path.join(out_dir, "farmacia_index.json"), encoding="utf-8") as f:
            return {p["codigo"]: p for p in json.load(f).get("postos", [])}
    except Exception:  # noqa: BLE001
        return {}


def main() -> int:
    ap = argparse.ArgumentParser(description="ETL Farmácia · Saídas e Consumo")
    ap.add_argument("--postos", help="ex.: R,A (padrão: todos)")
    ap.add_argument("--meses", type=int, default=MESES_PADRAO, help=f"janela em meses (padrão {MESES_PADRAO})")
    ap.add_argument("--out", default=OUT_DIR_PADRAO, help="diretório de saída")
    ap.add_argument("--dry-run", action="store_true", help="lê tudo e imprime o resumo; não grava JSON nem meta")
    ap.add_argument("--workers", type=int, default=WORKERS)
    args = ap.parse_args()

    postos = [p.strip().upper() for p in args.postos.split(",")] if args.postos else POSTOS
    ini = inicio_janela(args.meses)
    sqls = {k: _sql(f"farmacia_{k}.sql") for k in ("saidas", "entradas", "consumo", "estoque")}
    print(f"{datetime.now(_BRT):%Y-%m-%d %H:%M:%S} início — postos={''.join(postos)} janela={ini}→hoje "
          f"({args.meses} meses) dry_run={args.dry_run} out={args.out}")

    meta = ETLMeta("export_farmacia", args.out) if (ETLMeta and not args.dry_run) else None
    anterior = _ler_indice_anterior(args.out)
    resultados: dict[str, dict] = {}
    erros: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(coletar, p, ini, sqls): p for p in postos}
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                resultados[p] = fut.result()
                r = resultados[p]["resumo"]
                print(f"  {p} {resultados[p]['posto_nome']:<16} ok {r['tempo_s']:>5}s | produtos={r['produtos']:>4} "
                      f"saídas={r['saidas_itens']:>6} itens / {r['saidas_qtd']:>12,.0f} un | "
                      f"entradas={r['entradas_qtd']:>10,.0f} un | consumo={r['consumo_qtd']:>8,.0f} | "
                      f"remetente={resultados[p]['remetente_sugerido'] or '-'} | última saída={r['ultima_saida']}")
                if meta:
                    meta.ok(p, produtos=r["produtos"], saidas_itens=r["saidas_itens"])
            except Exception as e:  # noqa: BLE001
                msg = str(e).splitlines()[0][:300] if str(e) else repr(e)
                erros[p] = msg
                print(f"  {p} ERRO: {msg}")
                traceback.print_exc()
                if meta:
                    meta.error(p, msg)

    if args.dry_run:
        for p, d in resultados.items():
            print(f"\n== {p} {d['posto_nome']} — amostra ==")
            print("  origens (posto):", d["resumo"]["origens_posto"])
            print("  destinos 'outro':", d["resumo"]["destinos_outro"])
            print("  rascunho fora da conta:", d["rascunho"]["itens"], "itens /", d["rascunho"]["qtd"], "un")
            print("  classes de consumo:", d["classes"])
            soma: dict[int, float] = {}
            for l in d["saidas"]:
                if l[2] == "interna":
                    soma[l[0]] = soma.get(l[0], 0.0) + (l[7] or 0)
            nomes = {x["id"]: x["n"] for x in d["produtos"]}
            for pid, q in sorted(soma.items(), key=lambda kv: -kv[1])[:5]:
                print(f"   top saída interna: {nomes.get(pid)} {q:,.0f}")
            print("  tamanho JSON ≈", len(json.dumps(d, ensure_ascii=False, separators=(',', ':'))) // 1024, "KB")
        print(f"\nDRY-RUN: nada gravado. ok={len(resultados)} erro={len(erros)}")
        return 1 if erros and not resultados else 0

    os.makedirs(args.out, exist_ok=True)
    for p, d in resultados.items():
        _dump(os.path.join(args.out, f"farmacia_{p}.json"), d)

    # índice: posto com erro mantém o registro anterior (JSON dele não foi
    # sobrescrito) e ganha a marca de erro — a página mostra "dado de <data>".
    lista = []
    for p in POSTOS:
        if p in resultados:
            d = resultados[p]
            r = d["resumo"]
            lista.append({"codigo": p, "nome": d["posto_nome"], "ok": True, "erro": None,
                          "gerado_em": d["gerado_em"], "produtos": r["produtos"],
                          "saidas_itens": r["saidas_itens"], "saidas_qtd": r["saidas_qtd"],
                          "consumo_qtd": r["consumo_qtd"], "entradas_qtd": r["entradas_qtd"],
                          "ultima_saida": r["ultima_saida"], "ultima_entrada": r["ultima_entrada"],
                          "ultimo_consumo": r["ultimo_consumo"],
                          "remetente_sugerido": d["remetente_sugerido"], "tempo_s": r["tempo_s"]})
        else:
            ant = dict(anterior.get(p) or {"codigo": p, "nome": p, "gerado_em": None})
            ant.update({"ok": False, "erro": erros.get(p) or ("não solicitado" if p not in postos else "sem resultado")})
            lista.append(ant)
    _dump(os.path.join(args.out, "farmacia_index.json"), {
        "gerado_em": datetime.now(_BRT).isoformat(timespec="seconds"),
        "janela": {"ini": ini.isoformat(), "meses": args.meses},
        "postos": lista,
    })
    if meta:
        try:
            meta.save()
        except Exception as e:  # noqa: BLE001
            print(f"  (meta não gravado: {e})")
    print(f"{datetime.now(_BRT):%Y-%m-%d %H:%M:%S} fim — ok={len(resultados)} erro={len(erros)} → {args.out}")
    return 0 if resultados else 1


if __name__ == "__main__":
    sys.exit(main())
