#!/usr/bin/env python3
"""contas_pdf.py — lê o PDF da conta fixa e SUGERE o valor. Nunca decide.

Por que existe: as contas que chegam em auditoria@ vêm 100% como PDF anexo, e
medido em 2026-08-06, 7 dos 10 PDFs de julho NÃO TÊM TEXTO NENHUM dentro — são
imagem escaneada. Sem OCR não há o que ler.

REGRA — o que sai daqui é SUGESTÃO, não lançamento. O OCR erra: na fatura do
MongoDB ele leu "Amount Due $235." e comeu os centavos, e os outros números que
achou na página (6.41, 25.46) não eram o total. Um valor desses entrando
sozinho no painel de custos é número errado que ninguém revisa. Por isso todo
PDF cai na fila de auditoria com o valor pré-preenchido e o PDF do lado, e só
vira lançamento quando uma pessoa confirma.

Cada sugestão volta com o TRECHO de onde o número saiu — a regra do projeto é
que número na tela diz de onde veio (mesmo motivo do bloco explicacao() do
medico_custo). Sem o trecho, a sugestão é um palpite sem prova.

Dependências (já instaladas na vps154 em 2026-08-06):
  pypdf (pip)  ·  poppler-utils: pdftotext/pdftoppm  ·  tesseract-ocr + -por
"""
from __future__ import annotations

import os
import re
import subprocess
import tempfile

# Tenta o texto embutido primeiro: é exato e de graça. Só cai no OCR quando o
# PDF é imagem. Abaixo deste tamanho o "texto" é lixo de metadado, não conteúdo.
MIN_TEXTO_UTIL = 80
PAGINAS_OCR = 3          # fatura põe o total na 1ª; 3 é folga para nota fiscal
DPI_OCR = 300            # abaixo disso o tesseract erra vírgula de centavo

# Ordem = prioridade. O primeiro grupo é o rótulo que a nota usa para o total
# que se paga; os últimos são fallback ruim, aceito só porque com trecho na
# tela a pessoa enxerga que veio de um rótulo fraco.
ANCORAS = [
    (100, r"valor\s+total\s+do\s+servi[çc]o"),
    (100, r"valor\s+total\s+da\s+nfs-?\s*e"),
    (95,  r"valor\s+total\s+com\s+impostos"),
    (95,  r"total\s+a\s+pagar"),
    (90,  r"amount\s+due"),
    (90,  r"total\s+due"),
    (85,  r"valor\s+l[íi]quido\s+da\s+nfs-?\s*e"),
    (80,  r"valor\s+total\s+da\s+fatura"),
    (75,  r"total\s+geral"),
    (60,  r"\bsubtotal\b"),
    (55,  r"\btotal\b"),
]

# 1.234,56 (br)  ·  1,234.56 (us)  ·  1234,56  ·  1234.56
RE_VALOR = re.compile(
    r"(?P<sim>R\$|US\$|USD|BRL|EUR|€|\$)?\s*"
    r"(?P<num>\d{1,3}(?:\.\d{3})+,\d{2}"
    r"|\d{1,3}(?:,\d{3})+\.\d{2}"
    r"|\d+,\d{2}"
    r"|\d+\.\d{2})"
)

# Específicos antes do "$" pelado, senão "R$" casa o "$" e vira dólar.
SIMBOLOS = [("r$", "BRL"), ("brl", "BRL"), ("us$", "USD"), ("usd", "USD"),
            ("€", "EUR"), ("eur", "EUR"), ("$", "USD")]


def _num(txt: str) -> float | None:
    """'1.234,56' e '1,234.56' viram 1234.56. A última vírgula/ponto é o decimal."""
    n = txt.replace(" ", "")
    if "," in n and "." in n:
        n = n.replace(".", "").replace(",", ".") if n.rfind(",") > n.rfind(".") \
            else n.replace(",", "")
    elif "," in n:
        n = n.replace(",", ".")
    try:
        return float(n)
    except ValueError:
        return None


def extrair_texto(raw: bytes) -> tuple[str, str]:
    """(texto, como) — 'texto' se o PDF tinha texto, 'ocr' se precisou olhar a
    imagem, 'vazio' se não saiu nada. Nunca levanta: PDF corrompido vira ''."""
    with tempfile.TemporaryDirectory() as td:
        pdf = os.path.join(td, "conta.pdf")
        with open(pdf, "wb") as f:
            f.write(raw)
        try:
            t = subprocess.run(["pdftotext", "-layout", pdf, "-"],
                               capture_output=True, timeout=90).stdout.decode("utf-8", "replace")
        except Exception:
            t = ""
        if len(t.strip()) >= MIN_TEXTO_UTIL:
            return t, "texto"
        try:
            subprocess.run(["pdftoppm", "-r", str(DPI_OCR), "-png", "-f", "1",
                            "-l", str(PAGINAS_OCR), pdf, os.path.join(td, "pg")],
                           capture_output=True, timeout=300)
            partes = []
            for nome in sorted(os.listdir(td)):
                if nome.startswith("pg") and nome.endswith(".png"):
                    r = subprocess.run(["tesseract", os.path.join(td, nome), "stdout",
                                        "-l", "por+eng"], capture_output=True, timeout=300)
                    partes.append(r.stdout.decode("utf-8", "replace"))
            ocr = "\n".join(partes)
            return (ocr, "ocr") if ocr.strip() else ("", "vazio")
        except Exception:
            return (t, "texto") if t.strip() else ("", "vazio")


def _moeda(pedaco: str, padrao: str = "BRL") -> str:
    baixo = pedaco.lower()
    return next((cod for sim, cod in SIMBOLOS if sim in baixo), padrao)


def sugerir_valor(texto: str) -> dict:
    """Acha o total mais provável. Devolve valor, moeda, trecho e a âncora usada.

    Procura o rótulo e pega o primeiro valor que aparece DEPOIS dele — inclusive
    na linha seguinte, porque nota fiscal em colunas põe o rótulo numa linha e o
    número na de baixo. Ganha a âncora de maior prioridade; empatou, o maior
    valor (o total é maior que o ISS e o desconto que ficam ao lado)."""
    if not texto or not texto.strip():
        return {"valor": None, "moeda": None, "trecho": None, "ancora": None}

    linhas = texto.splitlines()
    melhor = None
    for peso, padrao in ANCORAS:
        for m in re.finditer(padrao, texto, re.I):
            # janela: do rótulo até ~180 chars à frente (pega a linha de baixo)
            janela = texto[m.end():m.end() + 180]
            achado = RE_VALOR.search(janela)
            if not achado:
                continue
            valor = _num(achado.group("num"))
            if valor is None or valor <= 0:
                continue
            # O trecho vai do rótulo ATÉ o fim da linha onde o número está, para
            # que a prova SEMPRE contenha o número sugerido. Foi assim que a
            # fatura do MongoDB se entregou: rótulo "Amount Due $23." (OCR comeu
            # os centavos) e sugestão 25,46 — dois números que não se falam. Se a
            # prova não mostrasse os dois juntos, o erro passaria batido.
            ini = texto.rfind("\n", 0, m.start()) + 1
            pos_val = m.end() + achado.end()
            fim = texto.find("\n", pos_val)
            trecho = " ".join(x.strip() for x in
                              texto[ini:fim if fim > 0 else len(texto)].splitlines()
                              if x.strip())
            cand = {"valor": valor,
                    "moeda": _moeda(achado.group("sim") or janela[:40] or trecho),
                    "trecho": trecho[:280], "ancora": padrao, "_peso": peso}
            if melhor is None or (peso, valor) > (melhor["_peso"], melhor["valor"]):
                melhor = cand
        if melhor is not None:                        # não desce para âncora pior
            break
    if melhor is None:
        return {"valor": None, "moeda": None, "trecho": None, "ancora": None}
    melhor.pop("_peso", None)
    _ = linhas
    return melhor


def ler(raw: bytes) -> dict:
    """PDF -> {texto, como, valor, moeda, trecho, ancora}. Sugestão, não verdade."""
    texto, como = extrair_texto(raw)
    out = sugerir_valor(texto)
    out["texto"] = texto
    out["como"] = como
    return out


def anexo_pdf(msg):
    """(nome, tipo, bytes) do 1º PDF anexo da mensagem, ou (None, None, None).

    Só o primeiro: nas contas do Leonardo é sempre um PDF por e-mail. Se um dia
    vierem dois, o segundo aparece em `anexos` (a lista de nomes) e a pessoa vê
    na fila que faltou algo — melhor que escolher em silêncio."""
    from email.header import decode_header, make_header
    for parte in msg.walk():
        nome = parte.get_filename()
        if not nome:
            continue
        try:
            nome = str(make_header(decode_header(nome)))
        except Exception:
            pass
        tipo = (parte.get_content_type() or "").lower()
        if nome.lower().endswith(".pdf") or tipo == "application/pdf":
            try:
                return nome, tipo or "application/pdf", (parte.get_payload(decode=True) or b"")
            except Exception:
                return nome, tipo or "application/pdf", b""
    return None, None, None
