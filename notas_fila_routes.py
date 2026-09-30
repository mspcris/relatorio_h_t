"""Fila de emissão de NF ao vivo — kpi_notas_rps.html (pedido do Cristiano, 28/09/2026).

"Existe uma fila e poucas são emitidas por minuto. Preciso saber que está andando."

A fila é o que a vw_Fin_Nota4pendente mostra: Fin_Nota com EmitirOuCancelar = 1.
O robô pega uma por vez, e o ritmo sai do próprio banco, sem guardar histórico:

- Fin_Nota.DataOrdemEmissao — quando a nota ENTROU na fila.
- Fin_Nota.DataErroEmissao  — hora em que o robô TENTOU a nota (com ou sem
  sucesso). NÃO é "emitida": em Y (30/09) o robô tentou 4 notas na última hora
  e a última NF confirmada era de 22/09; em A, um lote de notas de agosto posto
  na fila às 18h21 foi "processado" em 0,2 s cada, sem nota nenhuma. Contar isso
  como ritmo fez a tela dizer "Andando" num posto parado há 8 dias.
- NOTA CONFIRMADA = retorno da prefeitura (Fin_NotaFiscalRetorno, pela data de
  emissão v06) OU NFS-e nacional com chave (Fin_Nota.xChaveNFSe) sem retorno.
  É isso que conta ritmo, previsão e o selo Andando/Lenta/Parada.
- idNotaFiscalRetorno NÃO serve: o RPS se repete entre anos e o ERP liga a nota
  a retorno de 2025 (RPS 40065 de Y → retorno de 07/06/2025).
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
DECLARE @agora datetime = GETDATE();
DECLARE @teto  datetime = DATEADD(minute, 5, @agora);
SELECT
  (SELECT COUNT(*) FROM Fin_Nota WITH (NOLOCK) WHERE DataErroEmissao >= DATEADD(minute, -15, @agora)) AS tent_15,
  (SELECT COUNT(*) FROM Fin_Nota WITH (NOLOCK) WHERE DataErroEmissao >= DATEADD(minute, -60, @agora)) AS tent_60,
  (SELECT COUNT(*) FROM Fin_NotaFiscalRetorno WITH (NOLOCK)
    WHERE v06_Data_Emissao_Nota_Fiscal >= DATEADD(minute, -15, @agora) AND v06_Data_Emissao_Nota_Fiscal <= @teto) AS ret_15,
  (SELECT COUNT(*) FROM Fin_NotaFiscalRetorno WITH (NOLOCK)
    WHERE v06_Data_Emissao_Nota_Fiscal >= DATEADD(minute, -60, @agora) AND v06_Data_Emissao_Nota_Fiscal <= @teto) AS ret_60,
  (SELECT COUNT(*) FROM Fin_Nota n WITH (NOLOCK)
    WHERE ISNULL(n.xChaveNFSe, '') <> '' AND n.DataErroEmissao >= DATEADD(minute, -15, @agora)
      AND NOT EXISTS (SELECT 1 FROM Fin_NotaFiscalRetorno r WITH (NOLOCK)
                       WHERE r.V09_Numero_RPS = n.RPS
                         AND r.v06_Data_Emissao_Nota_Fiscal >= DATEADD(day, -1, n.DataErroEmissao))) AS nac_15,
  (SELECT COUNT(*) FROM Fin_Nota n WITH (NOLOCK)
    WHERE ISNULL(n.xChaveNFSe, '') <> '' AND n.DataErroEmissao >= DATEADD(minute, -60, @agora)
      AND NOT EXISTS (SELECT 1 FROM Fin_NotaFiscalRetorno r WITH (NOLOCK)
                       WHERE r.V09_Numero_RPS = n.RPS
                         AND r.v06_Data_Emissao_Nota_Fiscal >= DATEADD(day, -1, n.DataErroEmissao))) AS nac_60,
  (SELECT MAX(DataErroEmissao) FROM Fin_Nota WITH (NOLOCK)) AS ultima_tentativa,
  (SELECT MAX(v06_Data_Emissao_Nota_Fiscal) FROM Fin_NotaFiscalRetorno WITH (NOLOCK)
    WHERE v06_Data_Emissao_Nota_Fiscal <= @teto) AS ultima_ret,
  (SELECT MAX(DataErroEmissao) FROM Fin_Nota WITH (NOLOCK) WHERE ISNULL(xChaveNFSe, '') <> '') AS ultima_nac,
  (SELECT TOP 1 RazaoSocial FROM Sis_Empresa WITH (NOLOCK) WHERE idEmpresa = 1) AS razao,
  @agora AS agora
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
        (tent_15, tent_60, ret_15, ret_60, nac_15, nac_60,
         ult_tent, ult_ret, ult_nac, razao, agora) = r
        ult_emit = max([d for d in (ult_ret, ult_nac) if d], default=None)
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
            "tent_15": int(tent_15 or 0),
            "tent_60": int(tent_60 or 0),
            "emit_15": int(ret_15 or 0) + int(nac_15 or 0),
            "emit_60": int(ret_60 or 0) + int(nac_60 or 0),
            "ultima_tentativa": _iso(ult_tent),
            "ultima_emitida": _iso(ult_emit),
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
