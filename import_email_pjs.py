#!/usr/bin/env python3
"""
import_email_pjs.py — Robô da caixa do alias prestadores@camim.com.br.

A partir de 01/09/2026 todo boleto de prestador PJ chega EXCLUSIVAMENTE em
prestadores@camim.com.br, que é um ALIAS que cai na caixa PESSOAL do Cristiano
(cristiano@camim.com.br). Este robô alimenta a fila da página Controle de PJs
(tabelas pj_email + pj_arquivo no RDS). NADA vira boleto sozinho — todo item
espera confirmação humana na página (mesma regra do custos_ti: valor nunca
entra calado).

REGRAS DE OURO — a caixa é PESSOAL, não é caixa de robô:
  * SOMENTE LEITURA no Gmail: select(readonly=True) + BODY.PEEK — o robô NUNCA
    marca como lido, NUNCA move, NUNCA aplica/retira marcador. (No caso do
    auditoria@ o robô usa UNSEEN e "ler na mão rouba o e-mail do robô"; aqui é
    o INVERSO: o Cristiano lê a própria caixa o dia todo, então o UNSEEN seria
    inútil — quem manda é o dedupe por Message-ID contra pj_email.)
  * A seleção é pelo MARCADOR do Gmail (PJ_IMAP_LABEL, default
    "0000000 - pagamento") que o filtro do Cristiano aplica a tudo que chega
    para o alias. Marcador vira pasta no IMAP. Se a pasta não existir, cai no
    fallback: busca X-GM-RAW deliveredto:<alias> na caixa toda (ainda readonly).
  * Reprocessar a caixa inteira é seguro por construção: Message-ID é UNIQUE
    em pj_email — nada duplica.

Uso:
    python import_email_pjs.py            # --probe implícito: só imprime
    python import_email_pjs.py --run      # grava na fila (pj_email/pj_arquivo)

O --run fica escrito na linha do cron, não dentro do .sh — quem abre o crontab
tem que enxergar qual linha escreve (mesma regra do import_email_custos_ti).

Env (.env do /opt/relatorio_h_t/): PJ_IMAP_HOST (default imap.gmail.com),
PJ_IMAP_USER (default cristiano@camim.com.br), PJ_IMAP_PASSWORD (app password),
PJ_ALIAS (default prestadores@camim.com.br), PJ_IMAP_LABEL, PJ_IMAP_DIAS (45).
"""
from __future__ import annotations

import email
import email.utils
import hashlib
import imaplib
import os
import re
import sys
from datetime import date, timedelta
from email.header import decode_header, make_header

from dotenv import load_dotenv

load_dotenv()
load_dotenv("/opt/relatorio_h_t/.env")

HOST = os.getenv("PJ_IMAP_HOST", "imap.gmail.com")
USER = os.getenv("PJ_IMAP_USER", "cristiano@camim.com.br")
PASSWORD = (os.getenv("PJ_IMAP_PASSWORD") or "").replace(" ", "")
ALIAS = os.getenv("PJ_ALIAS", "prestadores@camim.com.br").lower()
LABEL = os.getenv("PJ_IMAP_LABEL", "0000000 - pagamento")
DIAS = int(os.getenv("PJ_IMAP_DIAS", "45"))

_EXT_OK = re.compile(r"\.(pdf|xml|png|jpe?g|zip|docx?)$", re.I)
_RE_BOLETO = re.compile(r"boleto|fatura|cobran|invoice|bloqueto", re.I)
_RE_NF = re.compile(r"\bnfs?e?\b|nota[\s_-]*fiscal|danfe", re.I)


def _dec(s) -> str:
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:  # noqa: BLE001
        return str(s)


def _corpo_trecho(msg) -> str:
    for part in msg.walk():
        if part.get_content_type() == "text/plain" and not part.get_filename():
            try:
                txt = part.get_payload(decode=True).decode(
                    part.get_content_charset() or "utf-8", "replace")
                return re.sub(r"\s+", " ", txt).strip()[:800]
            except Exception:  # noqa: BLE001
                continue
    return ""


def _anexos(msg) -> list[tuple[str, str, bytes]]:
    """[(nome, mime, bytes)] — PDFs sempre; imagem só >=8KB (corta logo de
    assinatura). Extensões fora da lista são ignoradas e citadas no log."""
    out = []
    for part in msg.walk():
        nome = _dec(part.get_filename())
        if not nome:
            continue
        dados = part.get_payload(decode=True) or b""
        mime = (part.get_content_type() or "application/octet-stream").lower()
        if not _EXT_OK.search(nome):
            print(f"    · anexo ignorado (extensão): {nome}")
            continue
        if mime.startswith("image/") and len(dados) < 8 * 1024:
            print(f"    · anexo ignorado (imagem pequena, deve ser logo): {nome}")
            continue
        if not dados:
            continue
        out.append((nome[:260], mime[:100], dados))
    return out


def _tipo_sugerido(nome: str) -> str:
    if _RE_NF.search(nome):
        return "nf"
    if _RE_BOLETO.search(nome):
        return "boleto"
    return "outro"


def _para_o_alias(msg) -> bool:
    campos = " ".join(filter(None, (
        msg.get("To"), msg.get("Cc"), msg.get("Delivered-To"),
        msg.get("X-Original-To"), msg.get("X-Forwarded-To"))))
    return ALIAS in campos.lower()


def _reconhecer_empresa(sess, remetente: str, assunto: str, nomes_anexos: str):
    """Sugere a empresa: remetente casa com email_remetente (e-mail ou domínio),
    senão nome da empresa por fronteira de palavra SÓ em assunto+nome de anexo
    (o miolo do e-mail cita outros produtos — lição do custos_ti)."""
    import controle_pjs_db as pjdb
    rem = (remetente or "").lower()
    alvo_texto = f"{assunto} {nomes_anexos}".lower()
    empresas = sess.query(pjdb.PjEmpresa).filter_by(ativo=True).all()
    melhor = None
    for e in empresas:
        for tok in (e.email_remetente or "").lower().replace(";", ",").split(","):
            tok = tok.strip()
            if tok and tok in rem:
                return e
    for e in empresas:
        nome = (e.nome or "").strip().lower()
        if len(nome) >= 3 and re.search(rf"\b{re.escape(nome)}\b", alvo_texto):
            if melhor is None or len(nome) > len(melhor.nome):
                melhor = e  # nome mais longo ganha
    return melhor


def _abrir_caixa(m: imaplib.IMAP4_SSL) -> tuple[str, bool]:
    """Seleciona (READONLY) a pasta do marcador; sem ela, INBOX + busca raw.
    Devolve (descricao, usando_label)."""
    alvo = None
    typ, folders = m.list()
    for f in folders or []:
        s = f.decode("utf-8", "replace")
        # nome da pasta é o último campo entre aspas
        nome = s.split(' "/" ')[-1].strip().strip('"')
        if nome.lower() == LABEL.lower():
            alvo = nome
            break
    if alvo:
        typ, _ = m.select(f'"{alvo}"', readonly=True)
        if typ == "OK":
            return f"marcador '{alvo}'", True
    m.select("INBOX", readonly=True)
    return "INBOX (fallback X-GM-RAW deliveredto)", False


def run(gravar: bool) -> int:
    if not PASSWORD:
        print("ERRO: PJ_IMAP_PASSWORD não configurado no .env")
        return 2

    import controle_pjs_db as pjdb
    if gravar and pjdb.PjSession is None:
        print("ERRO: PG_RDS_* não configurado — sem banco não há onde gravar")
        return 2
    sess = pjdb.PjSession() if pjdb.PjSession is not None else None

    m = imaplib.IMAP4_SSL(HOST)
    m.login(USER, PASSWORD)
    caixa, com_label = _abrir_caixa(m)
    desde = (date.today() - timedelta(days=DIAS)).strftime("%d-%b-%Y")
    if com_label:
        typ, data = m.search(None, "SINCE", desde)
    else:
        typ, data = m.search(None, "SINCE", desde,
                             "X-GM-RAW", f'"deliveredto:{ALIAS}"')
    ids = (data[0] or b"").split()
    print(f"caixa: {caixa} · janela: {DIAS}d · candidatos: {len(ids)} · "
          f"modo: {'RUN' if gravar else 'PROBE (nada será gravado)'}")

    ja_vistos = set()
    if sess is not None:
        ja_vistos = {mid for (mid,) in sess.query(pjdb.PjEmail.message_id)}

    novos = pulados = fora_alias = 0
    for i in ids:
        # PEEK em tudo — NUNCA BODY[] sem .PEEK, senão o Gmail marca \Seen
        typ, raw = m.fetch(i, "(BODY.PEEK[])")
        if typ != "OK" or not raw or raw[0] is None:
            continue
        msg = email.message_from_bytes(raw[0][1])
        message_id = (msg.get("Message-ID") or "").strip()
        if not message_id:
            message_id = "<sintetico-%s>" % hashlib.sha1(
                f"{msg.get('From')}{msg.get('Date')}{msg.get('Subject')}"
                .encode()).hexdigest()[:24]
        if message_id in ja_vistos:
            pulados += 1
            continue
        if not com_label and not _para_o_alias(msg):
            fora_alias += 1
            continue

        assunto = _dec(msg.get("Subject"))
        remetente = _dec(msg.get("From"))
        try:
            data_email = email.utils.parsedate_to_datetime(msg.get("Date"))
            data_email = data_email.replace(tzinfo=None)
        except Exception:  # noqa: BLE001
            data_email = None
        anexos = _anexos(msg)
        aviso_alias = "" if (_para_o_alias(msg) or not com_label) \
            else "  [alias não aparece nos cabeçalhos — entrou pelo marcador]"
        print(f"  + {assunto[:70]!r} de {remetente[:50]} · {len(anexos)} anexo(s){aviso_alias}")

        novos += 1
        if not gravar or sess is None:
            continue
        emp = _reconhecer_empresa(sess, remetente, assunto,
                                  " ".join(n for n, _, _ in anexos))
        em = pjdb.PjEmail(
            message_id=message_id[:300], assunto=assunto[:500] or None,
            remetente=remetente[:300] or None, data_email=data_email,
            corpo_trecho=_corpo_trecho(msg) or None,
            empresa_id=emp.id if emp else None,
        )
        sess.add(em)
        sess.flush()
        for nome, mime, dados in anexos:
            sess.add(pjdb.PjArquivo(
                email_id=em.id, tipo=_tipo_sugerido(nome), nome=nome,
                mime=mime, tamanho=len(dados), conteudo=dados,
                enviado_por="robo_email"))
        sess.commit()
        ja_vistos.add(message_id)
        if emp:
            print(f"    · reconhecida: {emp.nome} (#{emp.id})")

    m.logout()
    if sess is not None:
        sess.close()
    print(f"fim: novos={novos} ja_na_fila={pulados} fora_do_alias={fora_alias}")
    return 0


if __name__ == "__main__":
    sys.exit(run(gravar="--run" in sys.argv))
