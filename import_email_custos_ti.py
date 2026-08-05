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


def _centro_id(sess, termo: str | None):
    """Casa o 'Centro:' do e-mail com um centro por key ou nome; senão o padrão."""
    import custos_ti_db as db
    centros = sess.query(db.CentroCusto).all()
    alvo = (termo or os.getenv("CONTAS_CENTRO_DEFAULT", "infra")).strip().lower()
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
    centro = _centro_id(sess, _campo(corpo, "centro", "centro de custo"))

    motivo = None
    if valor is None:
        motivo = "não consegui ler o valor da conta"
    elif centro is None:
        motivo = "não consegui definir o centro de custo"
    reconhecido = motivo is None

    return {
        "origem": "email",
        "status": "previsto",           # conta a pagar, não pagamento feito
        "centro_id": centro,
        "descricao": (_campo(corpo, "descricao", "descrição") or assunto)[:240],
        "fornecedor": _campo(corpo, "fornecedor", "de", "credor"),
        "valor": valor,
        "moeda": moeda,
        "competencia": venc.strftime("%Y-%m") if venc else None,
        "external_id": (msg.get("Message-ID") or "").strip("<> ").strip() or None,
        "obs": f"De: {remetente}",
        # contexto cru — usado tanto no lançamento quanto na Auditoria
        "_reconhecido": reconhecido,
        "_motivo": motivo,
        "_remetente": remetente,
        "_assunto": assunto,
        "_corpo": corpo,
        "_anexos": _anexos(msg),
        "_recebido_em": _data_hora(msg.get("Date")),
    }


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
    typ, data = M.search(None, "UNSEEN")
    ids = data[0].split() if data and data[0] else []
    print(f"não-lidos: {len(ids)}")

    sess = db.TiSession()
    lancados = auditoria = ignorados = duplicados = 0
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

        destino = "LANÇAMENTO" if dados["_reconhecido"] else f"AUDITORIA ({dados['_motivo']})"
        if dry:
            print(f"  {'+' if dados['_reconhecido'] else '?'} {de} · "
                  f"{dados['_assunto'][:34]:34} → {destino}")
            lancados += dados["_reconhecido"]; auditoria += not dados["_reconhecido"]
            continue
        try:
            if dados["_reconhecido"]:
                custos_ti.salvar_lancamento(sess, dados, email="import_email")
                lancados += 1
            else:
                sess.add(db.EmailAuditoria(
                    message_id=mid, remetente=dados["_remetente"],
                    assunto=dados["_assunto"], recebido_em=dados["_recebido_em"],
                    corpo=dados["_corpo"], anexos=dados["_anexos"], motivo=dados["_motivo"]))
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
    print(f"\nlançados: {lancados} · para auditoria (piscando): {auditoria} · "
          f"duplicados: {duplicados} · ignorados (outro remetente): {ignorados}")


def _ja_visto(sess, db, mid: str) -> bool:
    """Mesmo Message-ID já virou lançamento OU já está na auditoria."""
    from sqlalchemy import or_
    achou_lanc = sess.query(db.Lancamento.id).filter(
        db.Lancamento.origem == "email", db.Lancamento.external_id == mid).first()
    achou_aud = sess.query(db.EmailAuditoria.id).filter(
        db.EmailAuditoria.message_id == mid).first()
    return bool(achou_lanc or achou_aud)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--probe", action="store_true", help="só leitura, listar recentes (padrão)")
    ap.add_argument("--dry-run", action="store_true", help="lê os não-lidos e MOSTRA o que lançaria, sem gravar")
    ap.add_argument("--run", action="store_true", help="lê os não-lidos e GRAVA no custos_ti")
    ap.add_argument("-n", type=int, default=10, help="quantos e-mails recentes listar no probe")
    args = ap.parse_args()
    if args.run:
        run(dry=False)
    elif args.dry_run:
        run(dry=True)
    else:
        probe(args.n)


if __name__ == "__main__":
    main()
