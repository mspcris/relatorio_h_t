"""
controle_pjs_routes.py — Blueprint Flask do módulo "Controle de PJs".

ACESSO (regra própria, MAIS restrita que o resto do sistema):
  entra SOMENTE (a) o dono — CONTROLE_PJS_OWNER, default cristiano@camim.com.br
  — ou (b) usuário ativo com a permissão explícita `controle_pjs` em
  user_page_permissions. is_admin NÃO entra. all_pages NÃO entra.
  O bit só é concedido/removido pelo dono, na própria página
  (POST /api/controle-pjs/acesso); o modal do /admin nem lista essa chave e o
  admin_editar preserva a linha ao regravar as demais (PAGINAS_PROTEGIDAS).

Endpoints (todos sob o guard acima):
  GET    /api/controle-pjs/resumo?mes=YYYY-MM        → visão do mês + matriz 12m
  GET    /api/controle-pjs/postos                    → letras+nomes (alarmes_db)
  GET    /api/controle-pjs/empresas/<id>             → detalhe (boletos+arquivos)
  POST   /api/controle-pjs/empresas                  → criar
  POST   /api/controle-pjs/empresas/<id>             → editar / desativar
  POST   /api/controle-pjs/boletos                   → criar (multipart c/ anexos)
  POST   /api/controle-pjs/boletos/<id>              → editar
  DELETE /api/controle-pjs/boletos/<id>              → apagar (anexos juntos)
  POST   /api/controle-pjs/arquivos                  → anexar (multipart)
  GET    /api/controle-pjs/arquivos/<id>             → baixar/exibir o arquivo
  DELETE /api/controle-pjs/arquivos/<id>             → apagar anexo
  GET    /api/controle-pjs/emails?status=pendente    → fila do alias prestadores@
  POST   /api/controle-pjs/emails/<id>/lancar        → e-mail → boleto (confirmação humana)
  POST   /api/controle-pjs/emails/<id>/descartar     → não é boleto (guarda tudo)
  POST   /api/controle-pjs/emails/<id>/reabrir
  GET    /api/controle-pjs/acesso                    → (dono) usuários + bit
  POST   /api/controle-pjs/acesso                    → (dono) concede/remove bit

Nenhum endpoint envia mensagem, cobra ou escreve no SQL Server da CAMIM.
Os únicos writes são no Postgres RDS (tabelas pj_*) e — no /acesso — na
user_page_permissions do camim_auth.db.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

from flask import Blueprint, Response, g, jsonify, request

import controle_pjs_db as pjdb

log = logging.getLogger(__name__)

controle_pjs_bp = Blueprint("controle_pjs_api", __name__)

_MAX_ARQUIVO = 30 * 1024 * 1024  # 30 MB por arquivo — "ocupação livre", com teto
_MIMES_OK = {
    "application/pdf", "image/png", "image/jpeg", "application/xml", "text/xml",
    "application/octet-stream", "application/zip",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


# ── Guard ────────────────────────────────────────────────────────────────────

def acesso_info() -> dict:
    """Quem é o usuário logado e se pode entrar AQUI (regra própria da página).

    Devolve {email, autorizado, dono, is_admin, postos}. Usado pelos endpoints
    e pelo app.py na rota /controle_pjs. is_admin/all_pages NÃO contam.
    """
    from auth_routes import decode_user
    from auth_db import SessionLocal, get_user_by_email as _gue

    email, postos = decode_user()
    info = {"email": email, "autorizado": False, "dono": False,
            "is_admin": False, "postos": postos or []}
    if not email:
        return info
    db = SessionLocal()
    try:
        u = _gue(db, email)
        if not u or not getattr(u, "ativo", True):
            return info
        info["is_admin"] = bool(getattr(u, "is_admin", False))
        if email.lower() == pjdb.OWNER_EMAIL:
            info["autorizado"] = True
            info["dono"] = True
        elif pjdb.PAGE_KEY in (u.lista_paginas() or []):
            info["autorizado"] = True
        return info
    finally:
        db.close()


def _require():
    """E-mail do usuário se autorizado; senão None."""
    info = acesso_info()
    return info["email"] if info["autorizado"] else None


def _deny():
    return jsonify({"ok": False, "error": "acesso restrito"}), 403


def _sess():
    if pjdb.PjSession is None:
        raise RuntimeError("PG_RDS_* não configurado")
    if "pj_sess" not in g:
        g.pj_sess = pjdb.PjSession()
    return g.pj_sess


@controle_pjs_bp.teardown_app_request
def _teardown(exc):
    s = g.pop("pj_sess", None)
    if s is not None:
        try:
            if exc:
                s.rollback()
            s.close()
        except Exception:  # noqa: BLE001
            pass


# ── Helpers ──────────────────────────────────────────────────────────────────

def _mes_atual() -> str:
    return date.today().strftime("%Y-%m")


def _ultimos_meses(n: int, fim: str | None = None) -> list[str]:
    y, m = map(int, (fim or _mes_atual()).split("-"))
    out = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return list(reversed(out))


def _parse_valor(v) -> float | None:
    """Aceita 1234.56, '1234.56' e '1.234,56'."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return round(float(v), 2)
    s = str(v).strip().replace("R$", "").strip()
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def _parse_data(v) -> date | None:
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(v).strip(), fmt).date()
        except ValueError:
            continue
    return None


def _postos_csv(v) -> str:
    """Normaliza lista/CSV de letras de posto — só A-Z, únicas, ordenadas."""
    if isinstance(v, str):
        v = v.split(",")
    letras = sorted({str(p).strip().upper() for p in (v or []) if str(p).strip()})
    letras = [p for p in letras if len(p) == 1 and p.isalpha()]
    return ",".join(letras)


def _salvar_upload(sess, fs, *, tipo, empresa_id=None, boleto_id=None,
                   email_id=None, competencia=None, por=None):
    """FileStorage → PjArquivo. Levanta ValueError com mensagem amigável."""
    dados = fs.read()
    if not dados:
        raise ValueError(f"arquivo '{fs.filename}' veio vazio")
    if len(dados) > _MAX_ARQUIVO:
        raise ValueError(f"arquivo '{fs.filename}' passa de 30 MB")
    mime = (fs.mimetype or "application/octet-stream").lower()
    if mime not in _MIMES_OK and not mime.startswith(("image/", "application/pdf")):
        raise ValueError(f"tipo de arquivo não aceito: {mime}")
    arq = pjdb.PjArquivo(
        empresa_id=empresa_id, boleto_id=boleto_id, email_id=email_id,
        tipo=tipo if tipo in ("contrato", "boleto", "nf", "outro") else "outro",
        competencia=competencia if pjdb.competencia_ok(competencia or "") else None,
        nome=(fs.filename or "arquivo")[:260], mime=mime[:100],
        tamanho=len(dados), conteudo=dados, enviado_por=por,
    )
    sess.add(arq)
    sess.flush()
    return arq


# ── Postos ───────────────────────────────────────────────────────────────────

@controle_pjs_bp.get("/api/controle-pjs/postos")
def api_postos():
    if not _require():
        return _deny()
    # Nomes SEMPRE do mapa canônico — nunca deduzir letra→bairro (regra do projeto).
    try:
        from alarmes_db import POSTOS_NOMES
        return jsonify([{"letra": k, "nome": v} for k, v in sorted(POSTOS_NOMES.items())])
    except Exception:  # noqa: BLE001
        return jsonify([{"letra": c, "nome": c} for c in "ABCDGIJMNPRXY"])


# ── Resumo ───────────────────────────────────────────────────────────────────

@controle_pjs_bp.get("/api/controle-pjs/resumo")
def api_resumo():
    if not _require():
        return _deny()
    mes = request.args.get("mes") or _mes_atual()
    if not pjdb.competencia_ok(mes):
        return jsonify({"ok": False, "error": "mes inválido"}), 400
    meses = _ultimos_meses(12, fim=mes)

    sess = _sess()
    from sqlalchemy import func
    empresas = sess.query(pjdb.PjEmpresa).order_by(pjdb.PjEmpresa.nome).all()

    somas = dict()  # (empresa_id, competencia) -> {total, n}
    for eid, comp, total, n in (
        sess.query(pjdb.PjBoleto.empresa_id, pjdb.PjBoleto.competencia,
                   func.sum(pjdb.PjBoleto.valor), func.count(pjdb.PjBoleto.id))
        .filter(pjdb.PjBoleto.competencia.in_(meses))
        .group_by(pjdb.PjBoleto.empresa_id, pjdb.PjBoleto.competencia)
    ):
        somas[(eid, comp)] = {"total": float(total or 0), "n": int(n)}

    # postos e status dos boletos do mês escolhido
    boletos_mes = {}
    for b in sess.query(pjdb.PjBoleto).filter(pjdb.PjBoleto.competencia == mes):
        d = boletos_mes.setdefault(b.empresa_id, {"postos": set(), "pagos": 0, "n": 0})
        d["n"] += 1
        d["pagos"] += 1 if b.status == "pago" else 0
        for p in (b.postos or "").split(","):
            if p:
                d["postos"].add(p)

    # contrato anexado por empresa + NF do mês (via boleto do mês ou NF solta)
    tem_contrato = {eid for (eid,) in sess.query(pjdb.PjArquivo.empresa_id)
                    .filter(pjdb.PjArquivo.tipo == "contrato",
                            pjdb.PjArquivo.empresa_id.isnot(None)).distinct()}
    nf_solta = {eid for (eid,) in sess.query(pjdb.PjArquivo.empresa_id)
                .filter(pjdb.PjArquivo.tipo == "nf",
                        pjdb.PjArquivo.competencia == mes,
                        pjdb.PjArquivo.empresa_id.isnot(None)).distinct()}
    nf_no_boleto = {eid for (eid,) in
                    sess.query(pjdb.PjBoleto.empresa_id)
                    .join(pjdb.PjArquivo, pjdb.PjArquivo.boleto_id == pjdb.PjBoleto.id)
                    .filter(pjdb.PjBoleto.competencia == mes,
                            pjdb.PjArquivo.tipo == "nf").distinct()}

    hoje = date.today()
    linhas = []
    for e in empresas:
        if tem_contrato and e.id in tem_contrato:
            contrato = "vencido" if (e.contrato_fim and e.contrato_fim < hoje) else "ok"
        else:
            contrato = "sem_contrato"
        bm = boletos_mes.get(e.id, {"postos": set(), "pagos": 0, "n": 0})
        linha = e.to_dict()
        linha.update({
            "contrato": contrato,
            "boletos_mes_n": bm["n"],
            "boletos_mes_pagos": bm["pagos"],
            "boletos_mes_total": somas.get((e.id, mes), {}).get("total", 0.0),
            "nf_mes": e.id in nf_solta or e.id in nf_no_boleto,
            "postos_mes": sorted(bm["postos"]),
            "por_mes": {m: somas.get((e.id, m), {}).get("total", 0.0) for m in meses},
        })
        linhas.append(linha)

    totais_mes = {m: round(sum(l["por_mes"][m] for l in linhas), 2) for m in meses}
    fila = sess.query(pjdb.PjEmail).filter_by(status="pendente").count()

    return jsonify({
        "ok": True, "mes": mes, "meses": meses, "empresas": linhas,
        "totais_por_mes": totais_mes, "total_mes": totais_mes.get(mes, 0.0),
        "fila_pendentes": fila,
    })


# ── Empresas ─────────────────────────────────────────────────────────────────

def _aplica_empresa(e: "pjdb.PjEmpresa", d: dict) -> str | None:
    if "nome" in d:
        nome = (d.get("nome") or "").strip()
        if not nome:
            return "nome é obrigatório"
        e.nome = nome[:200]
    for campo, tam in (("cnpj", 20), ("email_remetente", 400),
                       ("contato", 200), ("telefone", 40)):
        if campo in d:
            val = (d.get(campo) or "").strip()
            setattr(e, campo, val[:tam] if val else None)
    if "observacao" in d:
        e.observacao = (d.get("observacao") or "").strip() or None
    if "contrato_inicio" in d:
        e.contrato_inicio = _parse_data(d.get("contrato_inicio"))
    if "contrato_fim" in d:
        e.contrato_fim = _parse_data(d.get("contrato_fim"))
    if "ativo" in d:
        e.ativo = bool(d.get("ativo"))
    return None


@controle_pjs_bp.post("/api/controle-pjs/empresas")
def api_empresa_criar():
    email = _require()
    if not email:
        return _deny()
    d = request.get_json(silent=True) or {}
    sess = _sess()
    e = pjdb.PjEmpresa(nome="?")
    erro = _aplica_empresa(e, d)
    if erro:
        return jsonify({"ok": False, "error": erro}), 400
    ja = sess.query(pjdb.PjEmpresa).filter(
        pjdb.PjEmpresa.nome.ilike(e.nome)).first()
    if ja:
        return jsonify({"ok": False, "error": f"já existe a empresa '{ja.nome}' (#{ja.id})"}), 409
    sess.add(e)
    sess.commit()
    return jsonify({"ok": True, "empresa": e.to_dict()})


@controle_pjs_bp.post("/api/controle-pjs/empresas/<int:eid>")
def api_empresa_editar(eid):
    if not _require():
        return _deny()
    d = request.get_json(silent=True) or {}
    sess = _sess()
    e = sess.get(pjdb.PjEmpresa, eid)
    if not e:
        return jsonify({"ok": False, "error": "empresa não encontrada"}), 404
    erro = _aplica_empresa(e, d)
    if erro:
        return jsonify({"ok": False, "error": erro}), 400
    sess.commit()
    return jsonify({"ok": True, "empresa": e.to_dict()})


@controle_pjs_bp.get("/api/controle-pjs/empresas/<int:eid>")
def api_empresa_detalhe(eid):
    if not _require():
        return _deny()
    sess = _sess()
    e = sess.get(pjdb.PjEmpresa, eid)
    if not e:
        return jsonify({"ok": False, "error": "empresa não encontrada"}), 404
    boletos = (sess.query(pjdb.PjBoleto).filter_by(empresa_id=eid)
               .order_by(pjdb.PjBoleto.competencia.desc(), pjdb.PjBoleto.id.desc())
               .limit(200).all())
    arquivos_empresa = (sess.query(pjdb.PjArquivo)
                        .filter(pjdb.PjArquivo.empresa_id == eid,
                                pjdb.PjArquivo.boleto_id.is_(None))
                        .order_by(pjdb.PjArquivo.id.desc()).all())
    return jsonify({
        "ok": True,
        "empresa": e.to_dict(),
        "boletos": [b.to_dict() for b in boletos],
        "arquivos": [a.to_dict() for a in arquivos_empresa],
    })


# ── Boletos ──────────────────────────────────────────────────────────────────

@controle_pjs_bp.post("/api/controle-pjs/boletos")
def api_boleto_criar():
    email = _require()
    if not email:
        return _deny()
    eh_form = bool(request.content_type and "multipart" in request.content_type)
    d = request.form if eh_form else (request.get_json(silent=True) or {})
    sess = _sess()

    try:
        eid = int(d.get("empresa_id") or 0)
    except (TypeError, ValueError):
        eid = 0
    e = sess.get(pjdb.PjEmpresa, eid) if eid else None
    if not e:
        return jsonify({"ok": False, "error": "empresa inválida"}), 400
    comp = (d.get("competencia") or "").strip()
    if not pjdb.competencia_ok(comp):
        return jsonify({"ok": False, "error": "competência inválida (use AAAA-MM)"}), 400
    valor = _parse_valor(d.get("valor"))
    if valor is None or valor <= 0:
        return jsonify({"ok": False, "error": "valor inválido"}), 400

    b = pjdb.PjBoleto(
        empresa_id=e.id, competencia=comp, valor=valor,
        vencimento=_parse_data(d.get("vencimento")),
        postos=_postos_csv(d.get("postos")),
        descricao=((d.get("descricao") or "").strip() or None),
        status=d.get("status") if d.get("status") in ("recebido", "conferido", "pago") else "recebido",
        pago_em=_parse_data(d.get("pago_em")),
        origem="manual", criado_por=email,
    )
    sess.add(b)
    sess.flush()
    try:
        for campo, tipo in (("arquivo_boleto", "boleto"), ("arquivo_nf", "nf")):
            fs = request.files.get(campo)
            if fs and fs.filename:
                _salvar_upload(sess, fs, tipo=tipo, boleto_id=b.id,
                               competencia=comp, por=email)
    except ValueError as ve:
        sess.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400
    sess.commit()
    return jsonify({"ok": True, "boleto": b.to_dict()})


@controle_pjs_bp.post("/api/controle-pjs/boletos/<int:bid>")
def api_boleto_editar(bid):
    if not _require():
        return _deny()
    d = request.get_json(silent=True) or {}
    sess = _sess()
    b = sess.get(pjdb.PjBoleto, bid)
    if not b:
        return jsonify({"ok": False, "error": "boleto não encontrado"}), 404
    if "competencia" in d:
        if not pjdb.competencia_ok((d.get("competencia") or "").strip()):
            return jsonify({"ok": False, "error": "competência inválida"}), 400
        b.competencia = d["competencia"].strip()
    if "valor" in d:
        v = _parse_valor(d.get("valor"))
        if v is None or v <= 0:
            return jsonify({"ok": False, "error": "valor inválido"}), 400
        b.valor = v
    if "vencimento" in d:
        b.vencimento = _parse_data(d.get("vencimento"))
    if "postos" in d:
        b.postos = _postos_csv(d.get("postos"))
    if "descricao" in d:
        b.descricao = (d.get("descricao") or "").strip() or None
    if "status" in d and d["status"] in ("recebido", "conferido", "pago"):
        b.status = d["status"]
        if b.status != "pago":
            b.pago_em = None
    if "pago_em" in d:
        b.pago_em = _parse_data(d.get("pago_em"))
    sess.commit()
    return jsonify({"ok": True, "boleto": b.to_dict()})


@controle_pjs_bp.delete("/api/controle-pjs/boletos/<int:bid>")
def api_boleto_apagar(bid):
    if not _require():
        return _deny()
    sess = _sess()
    b = sess.get(pjdb.PjBoleto, bid)
    if not b:
        return jsonify({"ok": False, "error": "boleto não encontrado"}), 404
    # e-mail que apontava pra ele volta pra fila (o histórico do e-mail fica)
    for em in sess.query(pjdb.PjEmail).filter_by(boleto_id=bid):
        em.boleto_id = None
        em.status = "pendente"
    for a in sess.query(pjdb.PjArquivo).filter_by(boleto_id=bid):
        if a.email_id:            # anexo veio de e-mail: preserva na fila
            a.boleto_id = None
        else:
            sess.delete(a)
    sess.delete(b)
    sess.commit()
    return jsonify({"ok": True})


# ── Arquivos ─────────────────────────────────────────────────────────────────

@controle_pjs_bp.post("/api/controle-pjs/arquivos")
def api_arquivo_upload():
    email = _require()
    if not email:
        return _deny()
    sess = _sess()
    f = request.form
    eid = f.get("empresa_id", type=int)
    bid = f.get("boleto_id", type=int)
    if not eid and not bid:
        return jsonify({"ok": False, "error": "informe empresa_id ou boleto_id"}), 400
    if eid and not sess.get(pjdb.PjEmpresa, eid):
        return jsonify({"ok": False, "error": "empresa não encontrada"}), 404
    if bid and not sess.get(pjdb.PjBoleto, bid):
        return jsonify({"ok": False, "error": "boleto não encontrado"}), 404
    arquivos = [fs for fs in request.files.getlist("arquivo") if fs and fs.filename]
    if not arquivos:
        return jsonify({"ok": False, "error": "nenhum arquivo enviado"}), 400
    criados = []
    try:
        for fs in arquivos:
            criados.append(_salvar_upload(
                sess, fs, tipo=f.get("tipo") or "outro",
                empresa_id=eid or None, boleto_id=bid or None,
                competencia=f.get("competencia"), por=email))
    except ValueError as ve:
        sess.rollback()
        return jsonify({"ok": False, "error": str(ve)}), 400
    sess.commit()
    return jsonify({"ok": True, "arquivos": [a.to_dict() for a in criados]})


@controle_pjs_bp.get("/api/controle-pjs/arquivos/<int:aid>")
def api_arquivo_baixar(aid):
    if not _require():
        return _deny()
    sess = _sess()
    a = sess.get(pjdb.PjArquivo, aid)
    if not a:
        return jsonify({"ok": False, "error": "arquivo não encontrado"}), 404
    nome = (a.nome or f"arquivo_{aid}").replace('"', "")
    return Response(bytes(a.conteudo),
                    mimetype=a.mime or "application/octet-stream",
                    headers={"Content-Disposition": f'inline; filename="{nome}"'})


@controle_pjs_bp.delete("/api/controle-pjs/arquivos/<int:aid>")
def api_arquivo_apagar(aid):
    if not _require():
        return _deny()
    sess = _sess()
    a = sess.get(pjdb.PjArquivo, aid)
    if not a:
        return jsonify({"ok": False, "error": "arquivo não encontrado"}), 404
    if a.email_id:
        # anexo que veio de e-mail é prova de recebimento: sai do boleto/empresa
        # mas continua preso ao e-mail (mesma filosofia do "descartar não apaga").
        a.boleto_id = None
        a.empresa_id = None
    else:
        sess.delete(a)
    sess.commit()
    return jsonify({"ok": True})


# ── Fila de e-mails (prestadores@) ───────────────────────────────────────────

@controle_pjs_bp.get("/api/controle-pjs/emails")
def api_emails():
    if not _require():
        return _deny()
    status = request.args.get("status") or "pendente"
    sess = _sess()
    q = sess.query(pjdb.PjEmail)
    if status != "todos":
        q = q.filter_by(status=status)
    itens = q.order_by(pjdb.PjEmail.id.desc()).limit(120).all()
    out = []
    for em in itens:
        d = em.to_dict()
        d["anexos"] = [a.to_dict() for a in
                       sess.query(pjdb.PjArquivo).filter_by(email_id=em.id)
                       .order_by(pjdb.PjArquivo.id)]
        out.append(d)
    return jsonify({"ok": True, "itens": out})


@controle_pjs_bp.post("/api/controle-pjs/emails/<int:mid>/lancar")
def api_email_lancar(mid):
    email = _require()
    if not email:
        return _deny()
    d = request.get_json(silent=True) or {}
    sess = _sess()
    em = sess.get(pjdb.PjEmail, mid)
    if not em:
        return jsonify({"ok": False, "error": "item não encontrado"}), 404
    if em.status == "lancado" and em.boleto_id:
        return jsonify({"ok": False, "error": "este e-mail já virou o boleto "
                        f"#{em.boleto_id}"}), 409
    e = sess.get(pjdb.PjEmpresa, int(d.get("empresa_id") or 0))
    if not e:
        return jsonify({"ok": False, "error": "escolha a empresa"}), 400
    comp = (d.get("competencia") or "").strip()
    if not pjdb.competencia_ok(comp):
        return jsonify({"ok": False, "error": "competência inválida"}), 400
    valor = _parse_valor(d.get("valor"))
    if valor is None or valor <= 0:
        return jsonify({"ok": False, "error": "valor inválido"}), 400

    b = pjdb.PjBoleto(
        empresa_id=e.id, competencia=comp, valor=valor,
        vencimento=_parse_data(d.get("vencimento")),
        postos=_postos_csv(d.get("postos")),
        descricao=((d.get("descricao") or "").strip() or em.assunto or None),
        origem="email", criado_por=email,
    )
    sess.add(b)
    sess.flush()
    # anexos do e-mail passam a pertencer ao boleto (email_id fica: procedência)
    tipos = {int(x.get("id")): x.get("tipo") for x in (d.get("arquivos") or [])
             if x.get("id")}
    for a in sess.query(pjdb.PjArquivo).filter_by(email_id=em.id):
        a.boleto_id = b.id
        a.competencia = comp
        if tipos.get(a.id) in ("boleto", "nf", "contrato", "outro"):
            a.tipo = tipos[a.id]
    em.status = "lancado"
    em.empresa_id = e.id
    em.boleto_id = b.id
    sess.commit()
    return jsonify({"ok": True, "boleto": b.to_dict()})


@controle_pjs_bp.post("/api/controle-pjs/emails/<int:mid>/descartar")
def api_email_descartar(mid):
    if not _require():
        return _deny()
    sess = _sess()
    em = sess.get(pjdb.PjEmail, mid)
    if not em:
        return jsonify({"ok": False, "error": "item não encontrado"}), 404
    # Descartar NÃO apaga: e-mail e anexos ficam guardados (é a prova de que
    # a mensagem chegou) — mesma regra do custos_ti_auditoria.
    em.status = "descartado"
    sess.commit()
    return jsonify({"ok": True})


@controle_pjs_bp.post("/api/controle-pjs/emails/<int:mid>/reabrir")
def api_email_reabrir(mid):
    if not _require():
        return _deny()
    sess = _sess()
    em = sess.get(pjdb.PjEmail, mid)
    if not em:
        return jsonify({"ok": False, "error": "item não encontrado"}), 404
    if em.status == "lancado":
        return jsonify({"ok": False, "error": "já virou boleto — apague o boleto "
                        "para devolver o e-mail à fila"}), 409
    em.status = "pendente"
    sess.commit()
    return jsonify({"ok": True})


# ── Acesso (só o dono) ───────────────────────────────────────────────────────

@controle_pjs_bp.get("/api/controle-pjs/acesso")
def api_acesso_listar():
    info = acesso_info()
    if not info["dono"]:
        return _deny()
    from auth_db import SessionLocal, User, UserPagePermission
    db = SessionLocal()
    try:
        users = db.query(User).order_by(User.nome).all()
        com_bit = {p.user_id for p in db.query(UserPagePermission)
                   .filter_by(page_key=pjdb.PAGE_KEY)}
        return jsonify({"ok": True, "usuarios": [{
            "id": u.id, "nome": u.nome, "email": u.email,
            "ativo": bool(u.ativo),
            "dono": (u.email or "").lower() == pjdb.OWNER_EMAIL,
            "tem_acesso": u.id in com_bit or (u.email or "").lower() == pjdb.OWNER_EMAIL,
        } for u in users]})
    finally:
        db.close()


@controle_pjs_bp.post("/api/controle-pjs/acesso")
def api_acesso_toggle():
    info = acesso_info()
    if not info["dono"]:
        return _deny()
    d = request.get_json(silent=True) or {}
    uid = int(d.get("user_id") or 0)
    permitir = bool(d.get("permitir"))
    from auth_db import SessionLocal, User, UserPagePermission
    db = SessionLocal()
    try:
        u = db.get(User, uid)
        if not u:
            return jsonify({"ok": False, "error": "usuário não encontrado"}), 404
        if (u.email or "").lower() == pjdb.OWNER_EMAIL:
            return jsonify({"ok": False, "error": "o dono sempre tem acesso"}), 400
        ja = db.query(UserPagePermission).filter_by(
            user_id=uid, page_key=pjdb.PAGE_KEY).first()
        if permitir and not ja:
            db.add(UserPagePermission(user_id=uid, page_key=pjdb.PAGE_KEY))
        elif not permitir and ja:
            db.delete(ja)
        db.commit()
        log.info("controle_pjs acesso: %s %s para user #%s (%s)",
                 info["email"], "CONCEDEU" if permitir else "REMOVEU", uid, u.email)
        return jsonify({"ok": True})
    finally:
        db.close()
