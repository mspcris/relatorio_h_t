"""Fila de emissão de NF ao vivo — kpi_notas_rps.html (pedido do Cristiano, 28/09/2026).

"Existe uma fila e poucas são emitidas por minuto. Preciso saber que está andando."

A fila é o que a vw_Fin_Nota4pendente mostra: Fin_Nota com EmitirOuCancelar = 1.
O robô pega uma por vez, e o ritmo sai do próprio banco, sem guardar histórico:

- Fin_Nota.DataOrdemEmissao — quando a nota ENTROU na fila.
- Fin_Nota.DataErroEmissao  — apesar do nome, é a hora em que o robô PROCESSOU a
  nota (com ou sem sucesso). Medido em G e N, 28/09 18h: das 272 processadas em 2 h,
  214 tinham retorno da prefeitura com número de NF e status 1 (normal). Ex.: RPS
  319910 → NF 31922, emitida 18:15:01, DataErroEmissao 18:15:32.
- Fin_Nota.idNotaFiscalRetorno — preenchido quando a prefeitura devolveu a NF, mas
  NÃO em todo posto: em A e C, 100% das processadas na última hora vieram sem ele e
  os postos emitem normalmente. Por isso `com_retorno_60` vai no JSON e a tela NÃO
  mostra "sem retorno" — o aviso acusaria erro onde não há.
- DataEmissao NÃO serve para ritmo: na maioria das notas vem só com a data (00:00).

Só SELECT com NOLOCK. Posto que não responde vai em `erros` — nunca vira fila zero.
"""
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, jsonify, request

from medico_novo_routes import _check_admin, _conn_for_posto

logger = logging.getLogger(__name__)

notas_fila_bp = Blueprint("notas_fila_bp", __name__)

CACHE_S = 30
_cache: dict = {}
_cache_lock = threading.Lock()

# Código de emissão (CASE da vw_Fin_Nota4pendente) → rótulo. A letra do próprio
# posto é a clínica e ganha a razão social do sis_empresa.
ROTULOS = {"S": "SD-M Operadora", "O": "CAMIM Operadora", "L": "Laboratório", "Z": "Resgate"}

SQL_FILA = """
SET NOCOUNT ON;
SELECT v.CFemiteNota AS cf,
       COUNT(*) AS qtd,
       SUM(v.ValorNotaFiscal) AS valor,
       SUM(CASE WHEN v.Cancelar = 1 THEN 1 ELSE 0 END) AS cancelar,
       MIN(n.DataOrdemEmissao) AS mais_antiga
  FROM vw_Fin_Nota4pendente v WITH (NOLOCK)
  JOIN Fin_Nota n WITH (NOLOCK) ON n.idNota = v.idNota
 WHERE v.Desativado = 0
 GROUP BY v.CFemiteNota
"""

SQL_RITMO = """
SET NOCOUNT ON;
SELECT
  SUM(CASE WHEN DataErroEmissao >= DATEADD(minute, -15, GETDATE()) THEN 1 ELSE 0 END) AS proc_15,
  COUNT(*) AS proc_60,
  SUM(CASE WHEN idNotaFiscalRetorno IS NOT NULL THEN 1 ELSE 0 END) AS com_retorno_60,
  (SELECT MAX(DataErroEmissao) FROM Fin_Nota WITH (NOLOCK)) AS ultima,
  (SELECT TOP 1 RazaoSocial FROM Sis_Empresa WITH (NOLOCK) WHERE idEmpresa = 1) AS razao,
  GETDATE() AS agora
  FROM Fin_Nota WITH (NOLOCK)
 WHERE DataErroEmissao >= DATEADD(minute, -60, GETDATE())
"""


def _iso(d):
    return d.strftime("%Y-%m-%dT%H:%M:%S") if d else None


def _ler_posto(p: str) -> dict:
    t0 = time.time()
    con = _conn_for_posto(p)
    try:
        cur = con.cursor()
        cur.execute(SQL_RITMO)
        r = cur.fetchone()
        proc_15, proc_60, com_ret, ultima, razao, agora = r
        cur.execute(SQL_FILA)
        empresas = []
        for cf, qtd, valor, cancelar, antiga in cur.fetchall():
            cf = (cf or "").strip()
            rotulo = ROTULOS.get(cf) or (f"Clínica — {razao}" if cf == p and razao else f"Código {cf or '?'}")
            empresas.append({
                "cf": cf, "rotulo": rotulo, "qtd": int(qtd or 0), "valor": float(valor or 0),
                "cancelar": int(cancelar or 0), "mais_antiga": _iso(antiga),
            })
        empresas.sort(key=lambda e: -e["qtd"])
        return {
            "posto": p,
            "fila": sum(e["qtd"] for e in empresas),
            "valor": round(sum(e["valor"] for e in empresas), 2),
            "empresas": empresas,
            "proc_15": int(proc_15 or 0),
            "proc_60": int(proc_60 or 0),
            "com_retorno_60": int(com_ret or 0),
            "ultima": _iso(ultima),
            "agora": _iso(agora),
            "ms": int((time.time() - t0) * 1000),
        }
    finally:
        con.close()


@notas_fila_bp.route("/api/notas_rps/fila", methods=["GET"])
def api_fila():
    email, postos_acl, _login = _check_admin()
    if not email:
        return jsonify({"ok": False, "erro": "não autenticado"}), 401
    acl = {x.upper() for x in (postos_acl or [])}
    pedidos = [x.strip().upper() for x in (request.args.get("postos") or "").split(",") if x.strip()]
    postos = sorted(p for p in (pedidos or acl) if p in acl and len(p) == 1)
    if not postos:
        return jsonify({"ok": False, "erro": "nenhum posto liberado"}), 403

    chave = ",".join(postos)
    with _cache_lock:
        hit = _cache.get(chave)
        if hit and time.time() - hit[0] < CACHE_S:
            return jsonify(hit[1])

    res, erros = [], {}
    with ThreadPoolExecutor(max_workers=min(13, len(postos))) as ex:
        futs = {ex.submit(_ler_posto, p): p for p in postos}
        for f in as_completed(futs):
            p = futs[f]
            try:
                res.append(f.result())
            except Exception as e:  # posto fora do ar: aparece como sem leitura, nunca como fila zero
                logger.warning("notas_fila: posto %s falhou: %s", p, e)
                erros[p] = str(e)[:160]
    res.sort(key=lambda r: r["posto"])
    payload = {"ok": True, "postos": res, "erros": erros, "gerado_em": time.strftime("%Y-%m-%dT%H:%M:%S")}
    with _cache_lock:
        _cache[chave] = (time.time(), payload)
    return jsonify(payload)
