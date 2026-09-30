#!/usr/bin/env python3
"""import_email_custos_ti.py — coletor de contas fixas por e-mail para o Custos de TI.

O responsável encaminha os e-mails de conta fixa para auditoria@camim.com.br.
Hoje ninguém lê essa caixa: o custos_ti só recebe lançamento manual e Meta.
Este script fecha esse buraco — mas em ETAPAS, porque escrever em produção às
cegas com parsing de e-mail arbitrário é como se erra feio.

  --probe   (padrão)  SÓ LÊ. Conecta IMAP em modo readonly, lista os e-mails
                      recentes (de/assunto/data) e NÃO marca nada como lido,
                      NÃO grava nada. É o teste "o e-mail chega?".
  --run               Lê os não-lidos, extrai fornecedor/valor/vencimento do
                      formato de teste, grava em ti_lancamento (origem 'email')
                      com dedupe pelo Message-ID, e marca como lido.
                      >>> exige a migração da origem 'email' e é habilitado
                          só depois que o --probe provar que a leitura funciona.

Credenciais (no .env, mesmas do alarme por padrão):
  IMAP_HOST      (default imap.gmail.com)
  IMAP_PORT      (default 993)
  IMAP_USER      (default ALARM_EMAIL_USER)
  IMAP_PASSWORD  (default ALARM_EMAIL_PASSWORD)

Gmail: precisa de IMAP LIGADO nas configurações e de um APP PASSWORD (a senha
normal não autentica com 2FA). Um app password vale para IMAP e SMTP.
"""
from __future__ import annotations

import argparse
import email
import imaplib
import os
import sys
from email.header import decode_header, make_header

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass


def _cfg() -> dict:
    return {
        "host": os.getenv("IMAP_HOST", "imap.gmail.com"),
        "port": int(os.getenv("IMAP_PORT", "993")),
        "user": os.getenv("IMAP_USER") or os.getenv("ALARM_EMAIL_USER", ""),
        "pwd":  os.getenv("IMAP_PASSWORD") or os.getenv("ALARM_EMAIL_PASSWORD", ""),
        "box":  os.getenv("IMAP_MAILBOX", "INBOX"),
        # Só estes dois remetentes mandam conta (decisão do Cristiano). O que
        # vier de outro endereço NÃO vira lançamento — trava contra lançar
        # e-mail que não é conta. Vazio = ainda não configurado (não grava nada).
        "remetentes": _lista(os.getenv("CONTAS_REMETENTES", "")),
    }


def _lista(s: str) -> set[str]:
    return {x.strip().lower() for x in (s or "").replace(";", ",").split(",") if x.strip()}


def _endereco(cru: str) -> str:
    """Só o e-mail de 'Fulano <a@b.com>'."""
    import email.utils
    return (email.utils.parseaddr(cru or "")[1] or "").lower()


def _dec(s: str | None) -> str:
    try:
        return str(make_header(decode_header(s or "")))
    except Exception:
        return s or ""


def _conectar(cfg: dict, readonly: bool) -> imaplib.IMAP4_SSL:
    if not cfg["user"] or not cfg["pwd"]:
        sys.exit("IMAP_USER/IMAP_PASSWORD (ou ALARM_EMAIL_*) não configurados no .env")
    try:
        M = imaplib.IMAP4_SSL(cfg["host"], cfg["port"], timeout=30)
        M.login(cfg["user"], cfg["pwd"])
    except imaplib.IMAP4.error as e:
        sys.exit(f"Login IMAP recusado por {cfg['host']} ({cfg['user']}): {e}\n"
                 "→ confira se o IMAP está LIGADO no Gmail e se a senha é um app password.")
    except OSError as e:
        sys.exit(f"Não consegui falar com {cfg['host']}:{cfg['port']} — {e}")
    M.select(cfg["box"], readonly=readonly)
    return M


def probe(n: int) -> None:
    """Só leitura: prova que o e-mail chega, sem efeito colateral."""
    cfg = _cfg()
    print(f"IMAP {cfg['host']}:{cfg['port']} · conta {cfg['user']} · caixa {cfg['box']} (readonly)")
    print("remetentes de conta: " + (", ".join(sorted(cfg["remetentes"])) or "(nenhum configurado ainda)"))
    M = _conectar(cfg, readonly=True)
    typ, data = M.search(None, "ALL")
    ids = data[0].split() if data and data[0] else []
    print(f"total na caixa: {len(ids)}")
    for i in ids[-n:]:
        typ, d = M.fetch(i, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])")
        if not d or not d[0]:
            continue
        msg = email.message_from_bytes(d[0][1])
        de = _endereco(msg.get("From"))
        # ✓ = é de um remetente de conta (viraria lançamento no --run)
        marca = "✓" if de in cfg["remetentes"] else " "
        print(f"  {marca} {_dec(msg.get('Date'))[:28]:28} | "
              f"{de[:30]:30} | {_dec(msg.get('Subject'))[:40]}")
    M.logout()
    print("\nSó leitura — nada foi marcado nem gravado.  (✓ = remetente de conta)")


# ── parser da conta ──────────────────────────────────────────────────────────
# Formato de TESTE (o que pedir para você mandar): linhas chave: valor no corpo.
#   Fornecedor: Contabo
#   Valor: 45,90
#   Moeda: EUR              (opcional; default BRL; entende R$ / US$ / €)
#   Vencimento: 10/08/2026  (opcional; define a competência)
#   Centro: infra           (opcional; default CONTAS_CENTRO_DEFAULT)
#   Descrição: VPS mensal   (opcional; default = assunto do e-mail)
# Conta de fornecedor real tem outro layout — quando chegar a primeira, ajusto o
# parser sobre um exemplo de verdade em vez de adivinhar.
import re
from datetime import datetime

# Ordem = prioridade. Os específicos ANTES do "$" pelado, senão "R$" casa o "$"
# de USD e vira dólar (bug pego no teste). O primeiro que casar vence.
SIMBOLO_MOEDA = [("r$", "BRL"), ("brl", "BRL"), ("us$", "USD"), ("usd", "USD"),
                 ("€", "EUR"), ("eur", "EUR"), ("$", "USD")]


def _corpo(msg) -> str:
    """Texto plano do e-mail (ignora anexos e HTML na versão de teste)."""
    if msg.is_multipart():
        for parte in msg.walk():
            if parte.get_content_type() == "text/plain" and "attachment" not in str(
                    parte.get("Content-Disposition") or ""):
                try:
                    return parte.get_payload(decode=True).decode(
                        parte.get_content_charset() or "utf-8", "replace")
                except Exception:
                    continue
        return ""
    try:
        return msg.get_payload(decode=True).decode(
            msg.get_content_charset() or "utf-8", "replace")
    except Exception:
        return msg.get_payload() or ""


def _campo(texto: str, *nomes: str) -> str | None:
    for nome in nomes:
        m = re.search(rf"^\s*{nome}\s*[:\-]\s*(.+?)\s*$", texto, re.I | re.M)
        if m:
            return m.group(1).strip()
    return None


def _dinheiro(txt: str | None) -> tuple[float | None, str | None]:
    """Devolve (valor, moeda_detectada_pelo_símbolo)."""
    if not txt:
        return None, None
    baixo = txt.lower()
    moeda = next((cod for sim, cod in SIMBOLO_MOEDA if sim in baixo), None)
    m = re.search(r"(\d[\d.\s]*[,.]?\d*)", txt)
    if not m:
        return None, moeda
    n = m.group(1).replace(" ", "")
    # 1.234,56 (br) vs 1234.56 (us): a última vírgula/ponto é o decimal.
    if "," in n and "." in n:
        n = n.replace(".", "").replace(",", ".") if n.rfind(",") > n.rfind(".") \
            else n.replace(",", "")
    elif "," in n:
        n = n.replace(",", ".")
    try:
        return float(n), moeda
    except ValueError:
        return None, moeda


def _data(txt: str | None):
    if not txt:
        return None
    m = re.search(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2,4})", txt)
    if not m:
        return None
    d, mth, y = (int(x) for x in m.groups())
    if y < 100:
        y += 2000
    try:
        return datetime(y, mth, d).date()
    except ValueError:
        return None


def _centro_id(sess, termo: str | None, *, usar_padrao: bool = True):
    """Casa o 'Centro:' do e-mail com um centro por key ou nome; senão o padrão.

    `usar_padrao=False` responde None quando o e-mail não diz o centro — é o que
    permite distinguir "o e-mail escolheu infra" de "ninguém escolheu nada".
    Sem essa distinção o padrão ganha da conta reconhecida e a despesa da conta
    do centro 5 vai parar no centro 3.
    """
    import custos_ti_db as db
    centros = sess.query(db.CentroCusto).all()
    padrao = os.getenv("CONTAS_CENTRO_DEFAULT", "infra") if usar_padrao else ""
    alvo = (termo or padrao).strip().lower()
    if not alvo:
        return None
    for c in centros:
        if (c.key or "").lower() == alvo or (c.nome or "").lower() == alvo:
            return c.id
    for c in centros:                       # contém, como último recurso
        if alvo in (c.nome or "").lower():
            return c.id
    return None


def _anexos(msg) -> str | None:
    nomes = [p.get_filename() for p in msg.walk()
             if p.get_filename() and "attachment" in str(p.get("Content-Disposition") or "")]
    return ", ".join(n for n in nomes if n) or None


def _ids_dos_remetentes(M, remetentes: set[str]) -> list:
    """IDs dos não-lidos DOS REMETENTES DE CONTA, perguntando ao servidor.

    Antes isto baixava os 361 não-lidos e jogava 351 fora no Python: a caixa
    auditoria@ é COMPARTILHADA e o CRM despeja cópia de tudo nela. Perguntar
    'UNSEEN FROM fulano' devolve só as ~10 que interessam.

    Isto é filtro de desempenho, NÃO é a trava. A conferência de remetente
    continua no laço do run() — defesa em profundidade, porque 'FROM' no IMAP
    casa por substring do cabeçalho e um dia pode trazer o que não devia."""
    vistos, ids = set(), []
    for quem in sorted(remetentes):
        typ, data = M.search(None, "UNSEEN", "FROM", f'"{quem}"')
        for i in (data[0].split() if data and data[0] else []):
            if i not in vistos:
                vistos.add(i)
                ids.append(i)
    return ids


def _parse_email(sess, msg) -> dict:
    """E-mail -> (dados do lançamento se reconhecido) + contexto cru sempre.

    `_reconhecido` decide o destino: True vira lançamento; False vai para a
    caixa de Auditoria (piscando na página) com o motivo. Uma conta nova, de
    layout que o parser não entende, cai em Auditoria em vez de virar um
    lançamento errado — nada não reconhecido entra no custo silenciosamente."""
    assunto = _dec(msg.get("Subject")) or "(sem assunto)"
    corpo = _corpo(msg)
    remetente = _endereco(msg.get("From"))
    valor, moeda_sim = _dinheiro(_campo(corpo, "valor", "total", "preco", "preço"))
    moeda = (_campo(corpo, "moeda", "currency") or moeda_sim or "BRL").upper()
    if moeda not in ("BRL", "USD", "EUR"):
        moeda = dict(SIMBOLO_MOEDA).get(moeda.lower(), "BRL")
    venc = _data(_campo(corpo, "vencimento", "venc", "data"))
    # Só o que o e-mail DISSE. O padrão é aplicado no fim, depois da conta.
    centro = _centro_id(sess, _campo(corpo, "centro", "centro de custo"),
                        usar_padrao=False)

    # ── o PDF ────────────────────────────────────────────────────────────────
    # A conta real chega SÓ como PDF anexo — o corpo do e-mail do Leonardo diz o
    # nome do arquivo e a competência, nunca o valor. E 7 dos 10 PDFs de julho
    # não têm texto dentro (imagem escaneada), então quem lê é o OCR.
    import contas_pdf
    pdf_nome, pdf_tipo, pdf_raw = contas_pdf.anexo_pdf(msg)
    leitura = contas_pdf.ler(pdf_raw) if pdf_raw else {}

    # ── de qual conta CADASTRADA é este e-mail ───────────────────────────────
    # Sem amarrar o e-mail ao cadastro da conta, toda fatura vira despesa solta
    # e não há chave para perceber que a mesma conta já entrou no mês.
    # Só o assunto e o nome do arquivo entram na busca: são o que identifica a
    # fatura. O texto do PDF cita outros produtos do fornecedor e faria a conta
    # errada ganhar (ver reconhecer_conta).
    import custos_ti
    rec = custos_ti.reconhecer_conta(sess, " · ".join(filter(None, [assunto, pdf_nome])))
    conta = rec["conta"]
    # Ordem: centro escrito no e-mail > centro da conta reconhecida > padrão.
    # A conta sabe a que centro pertence; o padrão não sabe nada.
    if centro is None and conta is not None:
        centro = conta.centro_id
    if centro is None:
        centro = _centro_id(sess, None)
    # Fornecedor reconhecido vale mesmo sem conta definida (fatura da Contabo
    # cobre 16 VPS): é ele que sustenta a busca por repetição.
    fornecedor = _campo(corpo, "fornecedor", "de", "credor") or rec["fornecedor"]
    # A competência que importa é a da CONTA, e ela está no assunto
    # ("Julho/2026") — o corpo destes e-mails não traz vencimento. Sem isto a
    # competência sai vazia e a busca por repetição não busca nada.
    competencia = (venc.strftime("%Y-%m") if venc
                   else _competencia_do_assunto(assunto, _data_hora(msg.get("Date"))))

    mid = (msg.get("Message-ID") or "").strip("<> ").strip() or None
    motivo = None
    decisao = None
    if valor is not None and centro is None:
        motivo = "não consegui definir o centro de custo"
    elif valor is None:
        # REGRA — valor vindo de PDF NUNCA vira lançamento sozinho.
        # O OCR erra e erra calado: na fatura do MongoDB ele leu "Amount Due
        # $23." (comeu os centavos) e o número mais próximo na página era de
        # outra tabela. Um valor desses entra no painel e ninguém revisa. Então
        # PDF sempre para na fila, com a sugestão preenchida e o documento do
        # lado, e só vira lançamento quando uma pessoa confirma na tela.
        # EXCEÇÃO (2026-09-30, decisão do Cristiano): fatura que já foi lançada
        # nos 3 meses anteriores entra sozinha, copiando centro, conta, forma e
        # situação de lá (custos_ti.decidir_fatura). Só a primeira vez de cada
        # conta — ou a que parece repetida — ainda para na fila.
        if pdf_raw and leitura.get("valor") is not None:
            decisao = custos_ti.decidir_fatura(
                sess, assunto=assunto, anexo_nome=pdf_nome, competencia=competencia,
                valor=leitura.get("valor"), moeda=leitura.get("moeda"), message_id=mid)
            motivo = decisao["motivo"]
        elif pdf_raw:
            motivo = (f"PDF anexo, mas não consegui ler o valor "
                      f"({leitura.get('como') or 'sem texto'})")
        else:
            motivo = "não consegui ler o valor da conta"

    # ── trava de duplicidade DO ROBÔ ─────────────────────────────────────────
    # O dedupe por Message-ID só pega o mesmo e-mail chegando duas vezes. A
    # mesma FATURA reenviada, encaminhada por outra pessoa, ou já lançada à mão
    # antes de o robô passar, tem outro Message-ID e entraria de novo. Aqui o
    # e-mail redondo para na fila em vez de duplicar sozinho de madrugada —
    # guard no consumidor, não só na tela (lição de 2026-05-06).
    # Fatura AGREGADA: sem conta definida e o fornecedor tem várias contas já
    # lançadas neste mês (a da Contabo cobre as 17 VPS, R$ 1.387,98 em
    # julho/2026 — exatamente o total da nota). Lançar contaria o mesmo gasto
    # duas vezes. Vai para a fila, onde existe o botão de anexar a nota sem
    # criar despesa.
    semelhantes = []
    if decisao is None and motivo is None and conta is None:
        det = custos_ti.detalhamento_fornecedor(
            sess, competencia=competencia, fornecedor=fornecedor)
        if det:
            motivo = (f"as contas de {det['fornecedor']} já estão lançadas uma a "
                      f"uma em {competencia} ({det['lancamentos']} despesas, "
                      f"R$ {det['total_brl']:.2f}) — esta fatura parece ser o "
                      f"total delas")[:200]
    if decisao is None and motivo is None:
        semelhantes = custos_ti.lancamentos_semelhantes(
            sess, competencia=competencia,
            conta_id=(conta.id if conta else None),
            fornecedor=fornecedor)
        if semelhantes:
            motivo = (f"já existe despesa desta conta em {competencia} "
                      f"({semelhantes[0]['motivo']}) — confira se não é a mesma")
    reconhecido = motivo is None

    return {
        "origem": "email",
        "status": "previsto",           # conta a pagar, não pagamento feito
        "centro_id": centro,
        "conta_id": (conta.id if conta else None),
        "descricao": (_campo(corpo, "descricao", "descrição") or assunto)[:240],
        "fornecedor": fornecedor,
        "valor": valor,
        "moeda": moeda,
        "competencia": competencia,
        "external_id": mid,
        "obs": f"De: {remetente}",
        # contexto cru — usado tanto no lançamento quanto na Auditoria
        "_reconhecido": reconhecido,
        "_decisao": decisao,
        "_motivo": motivo,
        "_conta_nome": (conta.nome if conta else None),
        "_semelhantes": semelhantes,
        "_remetente": remetente,
        "_assunto": assunto,
        "_corpo": corpo,
        "_anexos": _anexos(msg),
        "_recebido_em": _data_hora(msg.get("Date")),
        # o documento e a leitura dele — o que a tela de auditoria mostra
        "_pdf_nome": pdf_nome,
        "_pdf_tipo": pdf_tipo,
        "_pdf_bytes": pdf_raw,
        "_pdf_texto": (leitura.get("texto") or None),
        "_pdf_como": (leitura.get("como") or None),
        "_valor_sugerido": leitura.get("valor"),
        "_moeda_sugerida": leitura.get("moeda"),
        "_trecho_valor": leitura.get("trecho"),
    }


_MESES_PT = ("janeiro", "fevereiro", "marco", "abril", "maio", "junho",
             "julho", "agosto", "setembro", "outubro", "novembro", "dezembro")
_SEM_ACENTO = str.maketrans("áàâãäéèêëíìîïóòôõöúùûüçñ", "aaaaaeeeeiiiiooooouuuucn")


def _competencia_do_assunto(assunto: str, recebido) -> str | None:
    """"Fatura Contabo - Julho/2026" -> "2026-07".

    Mesma leitura que a tela de auditoria faz (`competenciaDoAssunto`): a
    competência da conta é a que está no ASSUNTO, não a do dia em que o e-mail
    chegou. Uma fatura de julho encaminhada em agosto é de julho.
    """
    t = (assunto or "").lower().translate(_SEM_ACENTO)
    for i, mes in enumerate(_MESES_PT, start=1):
        if re.search(rf"{mes}\s*[/\- ]\s*(20\d{{2}})", t):
            ano = re.search(rf"{mes}\s*[/\- ]\s*(20\d{{2}})", t).group(1)
            return f"{ano}-{i:02d}"
    m = re.search(r"\b(0?[1-9]|1[0-2])\s*/\s*(20\d{2})\b", t)
    if m:
        return f"{m.group(2)}-{int(m.group(1)):02d}"
    # Último recurso: o mês em que o e-mail chegou. Vale para a busca por
    # repetição; quem confirma na tela ainda pode corrigir.
    return recebido.strftime("%Y-%m") if recebido else None


def _data_hora(cru: str | None):
    import email.utils
    try:
        dt = email.utils.parsedate_to_datetime(cru) if cru else None
        return dt.replace(tzinfo=None) if dt else None
    except Exception:
        return None


# ── gravação ─────────────────────────────────────────────────────────────────
def run(dry: bool) -> None:
    import custos_ti_db as db
    import custos_ti
    cfg = _cfg()
    if not cfg["remetentes"]:
        sys.exit("CONTAS_REMETENTES vazio no .env — sem remetentes, nada é lançado.")
    print(f"IMAP {cfg['host']} · {cfg['user']} · remetentes: {', '.join(sorted(cfg['remetentes']))}"
          + ("   [DRY-RUN: não grava]" if dry else ""))

    M = _conectar(cfg, readonly=dry)      # dry não marca como lido
    ids = _ids_dos_remetentes(M, cfg["remetentes"])
    print(f"não-lidos dos remetentes de conta: {len(ids)}")

    sess = db.TiSession()
    lancados = auditoria = ignorados = duplicados = anexados = 0
    for i in ids:
        typ, d = M.fetch(i, "(RFC822)")
        if not d or not d[0]:
            continue
        msg = email.message_from_bytes(d[0][1])
        de = _endereco(msg.get("From"))
        if de not in cfg["remetentes"]:      # A TRAVA: só os dois remetentes
            ignorados += 1
            continue
        dados = _parse_email(sess, msg)
        mid = dados["external_id"]
        if not dry and mid and _ja_visto(sess, db, mid):
            duplicados += 1
            M.store(i, "+FLAGS", "\\Seen")
            continue

        dec = dados["_decisao"]
        acao = dec["acao"] if dec else ("lancar" if dados["_reconhecido"] else "fila")
        if dry:
            print(f"  {'+' if acao != 'fila' else '?'} {de} · "
                  f"{dados['_assunto'][:34]:34} → {acao.upper()} ({dados['_motivo'] or 'valor no corpo'})")
            print(f"      conta cadastrada: {dados['_conta_nome'] or '(não reconheci)'}")
            if dec and dec["acao"] == "lancar":
                d = dec["dados"]
                print(f"      copia #{d['_precedente_id']} ({d['_precedente_comp']}): centro "
                      f"{d['centro_id']} · conta {d['conta_id']} · forma "
                      f"{d['forma_pagamento_id']} · {d['status']}")
            for s in dados["_semelhantes"]:
                print(f"      JÁ EXISTE #{s['id']} {s['competencia']} "
                      f"{s['moeda']} {s['valor']:,.2f} · {s['descricao'][:40]} "
                      f"({s['motivo']})")
            if dados["_pdf_nome"]:
                sug = (f"{dados['_moeda_sugerida']} {dados['_valor_sugerido']:,.2f}"
                       if dados["_valor_sugerido"] else "(não achei valor)")
                print(f"      PDF {dados['_pdf_nome'][:44]} [{dados['_pdf_como']}] "
                      f"sugere {sug}")
                if dados["_trecho_valor"]:
                    print(f"      prova: {dados['_trecho_valor'][:100]}")
            lancados += acao == "lancar"; auditoria += acao == "fila"
            anexados += acao == "anexar"
            continue
        try:
            if dec and dec["acao"] == "lancar":
                lanc = custos_ti.salvar_lancamento(
                    sess, _dados_lancamento(dec, dados["_remetente"], dados["_pdf_como"],
                                            dados["_trecho_valor"]),
                    email="import_email")
                # O item fica na aba "Lançadas" com o PDF: é ele que põe o
                # clipe na despesa e deixa conferir depois.
                sess.add(_item(db, dados, status="lancado", lancamento_id=lanc["id"]))
                lancados += 1
            elif dec and dec["acao"] == "anexar":
                sess.add(_item(db, dados, status="anexado",
                               rateio_fornecedor=dec["dados"]["fornecedor"],
                               rateio_competencia=dados["competencia"]))
                anexados += 1
            elif dados["_reconhecido"]:
                custos_ti.salvar_lancamento(sess, dados, email="import_email")
                lancados += 1
            else:
                sess.add(_item(db, dados))
                auditoria += 1
            sess.commit()
            M.store(i, "+FLAGS", "\\Seen")   # só marca lido depois de gravar
        except Exception as e:
            sess.rollback()
            if "uq_ti_" in str(e):           # corrida de dedupe: já entrou
                duplicados += 1
                M.store(i, "+FLAGS", "\\Seen")
            else:
                print(f"  ERRO ao gravar de {de}: {str(e)[:120]}")
    sess.close(); M.logout()
    print(f"\nlançados: {lancados} · anexados (nota agregada): {anexados} · "
          f"para auditoria (piscando): {auditoria} · "
          f"duplicados: {duplicados} · ignorados (outro remetente): {ignorados}")


def _item(db, dados: dict, **extra):
    """O e-mail como item da fila — com o PDF e o que o robô leu dele."""
    return db.EmailAuditoria(
        message_id=dados["external_id"], remetente=dados["_remetente"],
        assunto=dados["_assunto"], recebido_em=dados["_recebido_em"],
        corpo=dados["_corpo"], anexos=dados["_anexos"], motivo=(dados["_motivo"] or "")[:200],
        # o documento vai inteiro para o banco: é o que a pessoa
        # abre na tela para conferir o número antes de confirmar
        anexo_nome=dados["_pdf_nome"], anexo_tipo=dados["_pdf_tipo"],
        anexo_bytes=dados["_pdf_bytes"] or None,
        texto_extraido=dados["_pdf_texto"], extraido_como=dados["_pdf_como"],
        valor_sugerido=dados["_valor_sugerido"],
        moeda_sugerida=dados["_moeda_sugerida"],
        trecho_valor=dados["_trecho_valor"], **extra)


def _dados_lancamento(dec: dict, remetente: str, como: str | None,
                      trecho: str | None) -> dict:
    """Lançamento sem auditoria: a obs diz de onde veio cada coisa, para que
    corrigir seja fácil — é a contrapartida de não conferir antes."""
    d = {k: v for k, v in dec["dados"].items() if not k.startswith("_")}
    d["obs"] = (f"Lançado pelo robô sem auditoria (e-mail de {remetente}). "
                f"Centro, conta, forma e situação copiados de "
                f"#{dec['dados']['_precedente_id']} ({dec['dados']['_precedente_comp']}). "
                f"Valor lido do PDF por {como or '?'}"
                + (f": “{trecho[:160]}”" if trecho else "")
                + ". Se algo estiver errado, edite esta despesa.")
    return d


def reprocessar_fila(dry: bool) -> None:
    """Passa a regra dos 3 meses nos itens PENDENTES da fila (os que chegaram
    antes dela existir). O que não tem precedente continua esperando."""
    import custos_ti_db as db
    import custos_ti
    sess = db.TiSession()
    itens = (sess.query(db.EmailAuditoria)
             .filter(db.EmailAuditoria.status == "pendente")
             .order_by(db.EmailAuditoria.id).all())
    print(f"pendentes na fila: {len(itens)}" + ("   [DRY-RUN: não grava]" if dry else ""))
    cont = {"lancar": 0, "anexar": 0, "fila": 0}
    for it in itens:
        comp = _competencia_do_assunto(it.assunto, it.recebido_em)
        dec = custos_ti.decidir_fatura(
            sess, assunto=it.assunto, anexo_nome=it.anexo_nome, competencia=comp,
            valor=(float(it.valor_sugerido) if it.valor_sugerido is not None else None),
            moeda=it.moeda_sugerida, message_id=it.message_id)
        cont[dec["acao"]] += 1
        print(f"  #{it.id} {(it.assunto or '')[:44]:44} {comp} → {dec['acao'].upper()}: {dec['motivo']}")
        if dec["acao"] == "lancar":
            d = dec["dados"]
            print(f"      {d['moeda']} {d['valor']:,.2f} · centro {d['centro_id']} · conta "
                  f"{d['conta_id']} · forma {d['forma_pagamento_id']} · {d['status']}")
        if dry:
            continue
        try:
            if dec["acao"] == "lancar":
                lanc = custos_ti.salvar_lancamento(
                    sess, _dados_lancamento(dec, it.remetente, it.extraido_como,
                                            it.trecho_valor),
                    email="import_email")
                it.status, it.lancamento_id = "lancado", lanc["id"]
            elif dec["acao"] == "anexar":
                it.status = "anexado"
                it.rateio_fornecedor = dec["dados"]["fornecedor"]
                it.rateio_competencia = comp
            it.motivo = dec["motivo"][:200]
            sess.commit()
        except Exception as e:  # noqa: BLE001
            sess.rollback()
            print(f"      ERRO: {str(e)[:160]}")
    sess.close()
    print(f"\nlançados: {cont['lancar']} · anexados: {cont['anexar']} · "
          f"continuam na fila: {cont['fila']}")


def _ja_visto(sess, db, mid: str) -> bool:
    """Mesmo Message-ID já virou lançamento OU já está na auditoria.

    Procura o Message-ID em QUALQUER origem, não só em origem='email': quando a
    auditoria gruda este e-mail numa despesa que já tinha sido lançada à mão, o
    identificador fica numa linha origem='manual'. Filtrar por origem aqui faria
    o robô não enxergar essa linha e trazer a mesma conta de volta na próxima
    execução — exatamente a duplicidade que a tela acabou de evitar."""
    achou_lanc = sess.query(db.Lancamento.id).filter(
        db.Lancamento.external_id == mid).first()
    achou_aud = sess.query(db.EmailAuditoria.id).filter(
        db.EmailAuditoria.message_id == mid).first()
    return bool(achou_lanc or achou_aud)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--probe", action="store_true", help="só leitura, listar recentes (padrão)")
    ap.add_argument("--dry-run", action="store_true", help="lê os não-lidos e MOSTRA o que lançaria, sem gravar")
    ap.add_argument("--run", action="store_true", help="lê os não-lidos e GRAVA no custos_ti")
    ap.add_argument("--fila", action="store_true", help="aplica a regra dos 3 meses aos pendentes da fila (com --run grava; sem, só mostra)")
    ap.add_argument("-n", type=int, default=10, help="quantos e-mails recentes listar no probe")
    args = ap.parse_args()
    if args.fila:
        reprocessar_fila(dry=not args.run)
    elif args.run:
        run(dry=False)
    elif args.dry_run:
        run(dry=True)
    else:
        probe(args.n)


if __name__ == "__main__":
    main()
