#!/usr/bin/env python3
"""
importar_faturas_contabo.py — Lê os PDFs de fatura da Contabo e lança em Custos de TI.

A Contabo NÃO tem endpoint de faturamento na API (verificado em 2026-08-02: a
API cobre só infraestrutura — instâncias, storage, firewall, DNS). O caminho é
o PDF, que felizmente tem layout estável e traz o custo LINHA A LINHA por VPS.

Cada linha da fatura vira um lançamento ligado à conta daquele VPS (casada pelo
IP), então o dashboard mostra quanto cada máquina custa — não um total cego.

Chave de duplicidade: `contabo::<nº da fatura>::<ip>`. Reimportar a mesma pasta
não duplica nada.

Uso:
    python importar_faturas_contabo.py PASTA --dry-run   # não grava; só confere
    python importar_faturas_contabo.py PASTA             # grava

Depende de `pdftotext` (poppler-utils), que já está na máquina do Cristiano.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()
load_dotenv("/opt/relatorio_h_t/.env")

RE_FATURA = re.compile(r"Invoice:\s+(\d+)")
RE_DATA = re.compile(r"Date:\s+(\d{2})\.(\d{2})\.(\d{4})")
RE_TOTAL = re.compile(r"Cumulative gross\s+([$€])\s*(-?[\d,]+\.\d{2})")
# "154.38.172.227 - API IA Cristiano   $30.00   03.07.2026 - 03.08.2026   $30.00"
# O " - nome" é OPCIONAL: VPS sem nome de exibição sai só com o IP, e foi isso
# que fez a primeira versão perder linhas inteiras (US$ 8,40 sumiram da 65).
RE_ITEM = re.compile(
    r"^(\d{1,3}(?:\.\d{1,3}){3})(?:\s*-\s*(.+?))?\s{2,}[$€]\s*(-?[\d,]+\.\d{2})\s+"
    r"\d{2}\.\d{2}\.\d{4}\s*-\s*\d{2}\.\d{2}\.\d{4}\s+[$€]\s*(-?[\d,]+\.\d{2})\s*$")
# Adicional pendurado no VPS da linha anterior. Não é só "Location:" — também
# aparece "NVMe Storage Extension [VPS S]" e afins, com o mesmo formato. Casar
# só "Location:" fazia sumir US$ 2,30/mês de várias faturas.
RE_ADICIONAL = re.compile(
    r"^(?!\d{1,3}(?:\.\d{1,3}){3})(.+?)\s{2,}[$€]\s*(-?[\d,]+\.\d{2})\s+"
    r"\d{2}\.\d{2}\.\d{4}\s*-\s*\d{2}\.\d{2}\.\d{4}\s+[$€]\s*(-?[\d,]+\.\d{2})\s*$")
# "Reward for Survey Completion            29.07.2025          $-5.00"
# Seção "One-time fees and credit entries": crédito e taxa avulsa, sem IP.
RE_AVULSO = re.compile(
    r"^(?!Location:)(.+?)\s{2,}\d{2}\.\d{2}\.\d{4}\s+[$€]\s*(-?[\d,]+\.\d{2})\s*$")
RE_SEC_AVULSA = re.compile(r"One-time fees", re.I)
RE_SEC_RECORRENTE = re.compile(r"Recurring fees", re.I)


def _num(s: str) -> float:
    return float(s.replace(",", "").replace("$-", "-").replace("-", "-"))


# O pdftotext deixa caracteres de controle C1 (U+0080 e vizinhos) colados nos
# valores de algumas faturas. Eles não são espaço para o Python, então o \s*$
# do regex não fechava e a linha inteira era descartada em silêncio.
RE_CONTROLE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\u00ad\u200b\ufeff]")


def ler_pdf(caminho: str) -> str:
    bruto = subprocess.run(["pdftotext", "-layout", caminho, "-"],
                           capture_output=True, text=True, timeout=60).stdout
    return RE_CONTROLE.sub("", bruto)


def parse_fatura(caminho: str) -> dict:
    """Extrai número, data, moeda, total e as linhas por IP de um PDF da Contabo."""
    txt = ler_pdf(caminho)
    mf, md, mt = RE_FATURA.search(txt), RE_DATA.search(txt), RE_TOTAL.search(txt)
    if not (mf and md):
        return {"arquivo": os.path.basename(caminho), "ok": False,
                "erro": "não achei número ou data da fatura"}

    dia, mes, ano = md.groups()
    itens: dict[str, dict] = {}
    avulsos: list[dict] = []
    ultimo_ip = None
    secao = None
    for bruta in txt.splitlines():
        linha = bruta.strip()
        if RE_SEC_AVULSA.search(linha):
            secao, ultimo_ip = "avulso", None
            continue
        if RE_SEC_RECORRENTE.search(linha):
            secao, ultimo_ip = "recorrente", None
            continue
        if linha.startswith(("Subtotal", "Cumulative", "+0% VAT")):
            ultimo_ip = None
            continue

        m = RE_ITEM.match(linha)
        if m:
            ip, nome, _preco, valor = m.groups()
            it = itens.setdefault(ip, {"ip": ip, "nome": (nome or "").strip(),
                                       "base": 0.0, "local": 0.0})
            if nome and not it["nome"]:
                it["nome"] = nome.strip()
            it["base"] = round(it["base"] + _num(valor), 2)
            ultimo_ip = ip
            continue

        if ultimo_ip and secao != "avulso":
            m = RE_ADICIONAL.match(linha)
            if m:
                itens[ultimo_ip]["local"] = round(
                    itens[ultimo_ip]["local"] + _num(m.group(3)), 2)
                itens[ultimo_ip].setdefault("adicionais", []).append(m.group(1).strip())
                continue

        # crédito / taxa avulsa: só na seção própria, para não capturar
        # cabeçalho nem linha solta da seção recorrente
        if secao == "avulso":
            m = RE_AVULSO.match(linha)
            if m and not m.group(1).lower().startswith(("subscription", "description")):
                avulsos.append({"descricao": m.group(1).strip(),
                                "valor": _num(m.group(2))})

    for it in itens.values():
        it["valor"] = round(it["base"] + it["local"], 2)
    soma_vps = round(sum(i["valor"] for i in itens.values()), 2)
    soma_avulsa = round(sum(a["valor"] for a in avulsos), 2)
    soma = round(soma_vps + soma_avulsa, 2)
    total = _num(mt.group(2)) if mt else None
    moeda = "EUR" if (mt and mt.group(1) == "€") else "USD"

    return {
        "arquivo": os.path.basename(caminho), "ok": True,
        "fatura": mf.group(1), "data": f"{ano}-{mes}-{dia}",
        "competencia": f"{ano}-{mes}", "moeda": moeda,
        "itens": list(itens.values()), "avulsos": avulsos,
        "soma_itens": soma, "soma_vps": soma_vps, "soma_avulsa": soma_avulsa,
        "total_pdf": total,
        # a soma das linhas TEM que bater com o "Cumulative gross" — se não bater,
        # o parser perdeu alguma linha e o lançamento sairia menor que a fatura
        "confere": total is not None and abs(soma - total) < 0.01,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Importa faturas PDF da Contabo")
    ap.add_argument("pasta", help="pasta com os PDFs")
    ap.add_argument("--dry-run", action="store_true", help="não grava nada")
    ap.add_argument("--centro", default="infra", help="key do centro de custo")
    args = ap.parse_args()

    pdfs = sorted(f for f in os.listdir(args.pasta) if f.lower().endswith(".pdf"))
    if not pdfs:
        print("nenhum PDF na pasta.")
        return 1

    faturas = [parse_fatura(os.path.join(args.pasta, f)) for f in pdfs]
    ruins = [f for f in faturas if not f["ok"]]
    divergentes = [f for f in faturas if f["ok"] and not f["confere"]]

    print(f"PDFs lidos: {len(faturas)}")
    print(f"  ilegíveis: {len(ruins)}")
    print(f"  soma das linhas != total do PDF: {len(divergentes)}")
    for f in ruins + divergentes:
        print(f"    {f['arquivo']}: {f.get('erro') or ''} "
              f"soma={f.get('soma_itens')} total={f.get('total_pdf')}")

    boas = [f for f in faturas if f["ok"] and f["confere"]]
    total_geral = round(sum(f["total_pdf"] for f in boas), 2)
    comps = sorted(f["competencia"] for f in boas)
    moedas = {f["moeda"] for f in boas}
    print(f"\nfaturas válidas: {len(boas)} · {comps[0]} a {comps[-1]} · "
          f"moeda(s): {', '.join(moedas)} · total {total_geral:,.2f}")

    ips = defaultdict(float)
    for f in boas:
        for i in f["itens"]:
            ips[i["ip"]] += i["valor"]
    print(f"IPs distintos nas faturas: {len(ips)}")

    if args.dry_run:
        print("\n--dry-run: nada foi gravado.")
        return 0

    import custos_ti
    import custos_ti_db as db

    s = db.TiSession()
    try:
        centro = custos_ti.get_centro(s, args.centro)
        if not centro:
            print(f"centro '{args.centro}' não existe"); return 1
        visa = next((f for f in custos_ti.listar_formas(s) if f.ultimos4 == "6852"), None)

        # casa a conta pelo IP guardado na observação
        por_ip = {}
        for c in custos_ti.listar_contas(s, centro.id):
            for parte in (c.obs or "").split("·"):
                if parte.strip().startswith("IP "):
                    por_ip[parte.strip()[3:].strip()] = c

        novos, pulados, sem_conta = 0, 0, defaultdict(float)
        for f in boas:
            for i in f["itens"]:
                ext = f"contabo::{f['fatura']}::{i['ip']}"
                if s.query(db.Lancamento).filter(
                        db.Lancamento.origem == "manual",
                        db.Lancamento.external_id == ext).first():
                    pulados += 1
                    continue
                conta = por_ip.get(i["ip"])
                if conta is None:
                    # VPS que não existe mais no painel — o custo é real e não
                    # pode sumir; entra sem conta, identificado pelo IP.
                    sem_conta[f"{i['ip']} - {i['nome']}"] += i["valor"]
                custos_ti.salvar_lancamento(s, {
                    "centro_id": centro.id,
                    "conta_id": conta.id if conta else None,
                    "competencia": f["competencia"],
                    "descricao": f"Contabo — {i['nome']} ({i['ip']})",
                    "fornecedor": "Contabo GmbH",
                    "forma_pagamento_id": visa.id if visa else None,
                    "valor": i["valor"], "moeda": f["moeda"], "status": "pago",
                    "data_pagamento": f["data"], "origem": "manual",
                    "external_id": ext,
                    "obs": f"Fatura {f['fatura']} · base {i['base']:.2f}"
                           + (f" + localização {i['local']:.2f}" if i["local"] else "")},
                    email="cristiano@camim.com.br")
                novos += 1
        print(f"\nlançados: {novos} | já existiam: {pulados}")
        if sem_conta:
            print(f"IPs sem conta cadastrada (VPS já cancelada) — {len(sem_conta)}:")
            for k, v in sorted(sem_conta.items(), key=lambda kv: -kv[1]):
                print(f"    {k:<46} {v:>9,.2f}")
    finally:
        s.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
