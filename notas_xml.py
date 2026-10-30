"""
notas_xml.py — XML das notas fiscais, lido AO VIVO do posto e entregue um a um
ou em ZIP. Usado por contabilidade_notas_routes.py (/contabilidade_notas) e por
notas_xml_routes.py (aba Ver Registros do KPI Notas x RPS).

De onde vem o XML (DBA Carneiro, 30/10/2026):
  * Fin_NotaFiscalRetorno.xmlCompleto — nota emitida pelo sistema, carioca ou
    nacional. É a fonte usada aqui: medido em 30/10/2026, TODAS as notas das
    views de set e out/2026 nos 13 postos casam com um retorno nacional com XML
    (~10,7 KB cada). Nova Iguaçu ainda tem 1.062 retornos não nacionais em 2026
    sem XML, mas nenhum deles é de nota das views de set/out.
  * Fin_NotaFiscalTodasEmitidas.XMLRecebido — robô que buscava TODAS as notas do
    certificado, com ou sem receita, podendo repetir entre postos. Última
    gravação em 31/03/2025 (A): está fora do período das telas (2026+) e NÃO é
    lida aqui. Se um dia for preciso XML de 2023-2025, é por ela.

Chave da nota = (posto, idNotaFiscalRetorno). O id vem do JSON do ETL
(id_retorno, gravado por sql_contab_notas/notas.sql e
sql_notas_rps/notas_individuais.sql); quem chama VALIDA o id contra o JSON do
mês antes de pedir o XML, para a URL não servir de leitura livre da tabela.

O ZIP é gerado numa thread de fundo com progresso (um mês da rede inteira são
~17 mil notas / ~180 MB de XML lidos pelos links dos postos — nenhum request
síncrono aguenta: o nginx corta em 60 s). O arquivo fica em NOTAS_XML_DIR por
2 h e some sozinho. O ZIP leva um LEIA-ME.txt com o que entrou e o que faltou.

Somente SELECT no SQL Server.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)

TTL_S = 2 * 3600            # quanto tempo o ZIP pronto fica no disco
MAX_JOBS_ATIVOS = 2         # gerações simultâneas (cada uma lê os postos em paralelo)
LOTE = 200                  # ids por SELECT ... IN (...)
POSTOS_PARALELOS = 4
TAMANHO_MIN_XML = 50        # abaixo disso o campo é lixo/vazio, não XML

_DIRS = [os.getenv("NOTAS_XML_DIR", ""), "/opt/camim-auth/notas_xml_tmp",
         os.path.join(tempfile.gettempdir(), "notas_xml_tmp")]

_jobs: dict[str, "Job"] = {}
_lock = threading.Lock()


def _dir() -> str:
    for d in _DIRS:
        if not d:
            continue
        try:
            os.makedirs(d, exist_ok=True)
            teste = os.path.join(d, ".w")
            with open(teste, "w") as f:
                f.write("1")
            os.remove(teste)
            return d
        except OSError:
            continue
    raise RuntimeError("nenhum diretório gravável para o ZIP (NOTAS_XML_DIR)")


# ── leitura no posto ──────────────────────────────────────────────────────────
def _conn(posto: str):
    from medico_novo_routes import _conn_for_posto   # tardio: evita import circular com app.py
    return _conn_for_posto(posto)


def _bytes(v) -> bytes | None:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        b = bytes(v)
    else:
        b = str(v).encode("utf-8")
    b = b.lstrip(b"\xef\xbb\xbf \r\n\t")
    return b if len(b) > TAMANHO_MIN_XML else None


def ler_xml(posto: str, id_retorno: int) -> bytes | None:
    """O XML de UMA nota, ou None se o retorno não tem XML."""
    con = _conn(posto)
    try:
        cur = con.cursor()
        cur.execute("SELECT xmlCompleto FROM Fin_NotaFiscalRetorno WITH (NOLOCK) WHERE idNotaFiscalRetorno = ?",
                    int(id_retorno))
        r = cur.fetchone()
        return _bytes(r[0]) if r else None
    finally:
        con.close()


def ler_xmls(posto: str, ids: list[int], con=None) -> dict[int, bytes]:
    """Os XMLs de vários ids do mesmo posto, em lotes. Id sem XML não entra no dict."""
    proprio = con is None
    if proprio:
        con = _conn(posto)
    out: dict[int, bytes] = {}
    try:
        cur = con.cursor()
        ids = [int(i) for i in ids]
        for i in range(0, len(ids), LOTE):
            lote = ids[i:i + LOTE]
            marcas = ",".join("?" * len(lote))
            cur.execute("SELECT idNotaFiscalRetorno, xmlCompleto FROM Fin_NotaFiscalRetorno WITH (NOLOCK) "
                        f"WHERE idNotaFiscalRetorno IN ({marcas})", *lote)
            for rid, xml in cur.fetchall():
                b = _bytes(xml)
                if b:
                    out[int(rid)] = b
    finally:
        if proprio:
            con.close()
    return out


_LER_XMLS_REAL = ler_xmls   # para o job saber quando abrir a conexão do posto (os testes trocam o leitor)


# ── nomes ─────────────────────────────────────────────────────────────────────
def slug(s: str, n: int = 40) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")[:n]


def nome_xml(nota: dict) -> str:
    """NF_<numero>_<posto>[_CANCELADA].xml — o número da nota é o que a contabilidade procura."""
    numero = str(nota.get("NotaFiscal") or "0")
    posto = str(nota.get("posto") or "").upper()
    canc = str(nota.get("Cancelada") or "").strip().lower() == "sim"
    return f"NF_{numero}_{posto}{'_CANCELADA' if canc else ''}.xml"


# ── job de ZIP ────────────────────────────────────────────────────────────────
class Job:
    def __init__(self, dono: str, nome_zip: str, itens: list[dict], cabecalho: str):
        self.id = uuid.uuid4().hex
        self.dono = dono
        self.nome_zip = nome_zip
        self.itens = itens                 # [{posto, id, nome, rotulo}]
        self.cabecalho = cabecalho
        self.total = len(itens)
        self.feitos = 0
        self.gravados = 0
        self.faltando: list[str] = []      # rótulos das notas cujo retorno veio sem XML
        self.erros: dict[str, str] = {}    # posto -> erro (posto fora do ar: o ZIP sai sem ele e DIZ)
        self.estado = "andando"            # andando | pronto | erro
        self.erro = ""
        self.criado_em = time.time()
        self.pronto_em: float | None = None
        self.arquivo = ""
        self.tamanho = 0

    def dict(self) -> dict:
        return {
            "job": self.id, "estado": self.estado, "erro": self.erro, "nome": self.nome_zip,
            "total": self.total, "feitos": self.feitos, "gravados": self.gravados,
            "faltando": len(self.faltando), "faltando_lista": self.faltando[:200],
            "erros": self.erros, "tamanho": self.tamanho,
            "pronto": self.estado == "pronto",
        }


def _limpar() -> None:
    agora = time.time()
    with _lock:
        velhos = [j for j in _jobs.values() if agora - j.criado_em > TTL_S]
        for j in velhos:
            _jobs.pop(j.id, None)
    for j in velhos:
        try:
            if j.arquivo and os.path.exists(j.arquivo):
                os.remove(j.arquivo)
        except OSError:
            pass
    # arquivos órfãos de um restart do serviço
    try:
        d = _dir()
        for f in os.listdir(d):
            p = os.path.join(d, f)
            if f.endswith((".zip", ".tmp")) and agora - os.path.getmtime(p) > TTL_S:
                os.remove(p)
    except Exception:
        pass


def _posto_no_zip(job: Job, zf: zipfile.ZipFile, zlock: threading.Lock, posto: str, itens: list[dict],
                  leitor) -> None:
    """Lê os XMLs de um posto em lotes e grava no ZIP. Posto que falha vai em
    job.erros e as notas dele ficam de fora — o LEIA-ME diz quantas."""
    con = None
    try:
        if leitor is _LER_XMLS_REAL:
            con = _conn(posto)
        for i in range(0, len(itens), LOTE):
            lote = itens[i:i + LOTE]
            xmls = leitor(posto, [it["id"] for it in lote], con)
            with zlock:
                for it in lote:
                    b = xmls.get(int(it["id"]))
                    if b:
                        zf.writestr(it["nome"], b)
                        job.gravados += 1
                    else:
                        job.faltando.append(it.get("rotulo") or it["nome"])
                    job.feitos += 1
    except Exception as e:
        log.warning("notas_xml: posto %s falhou no ZIP: %s", posto, e)
        job.erros[posto] = str(e)[:200]
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _rodar(job: Job, leitor) -> None:
    try:
        d = _dir()
        tmp = os.path.join(d, job.id + ".tmp")
        por_posto: dict[str, list[dict]] = {}
        for it in job.itens:
            por_posto.setdefault(it["posto"], []).append(it)
        zlock = threading.Lock()
        with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            with ThreadPoolExecutor(max_workers=min(POSTOS_PARALELOS, max(1, len(por_posto)))) as ex:
                futs = [ex.submit(_posto_no_zip, job, zf, zlock, p, its, leitor) for p, its in por_posto.items()]
                for f in as_completed(futs):
                    f.result()
            # feitos: postos em erro deixam o contador abaixo do total — fecha aqui
            job.feitos = job.total
            with zlock:
                zf.writestr("LEIA-ME.txt", _leia_me(job))
        final = os.path.join(d, job.id + ".zip")
        os.replace(tmp, final)
        job.arquivo = final
        job.tamanho = os.path.getsize(final)
        job.estado = "pronto"
        job.pronto_em = time.time()
    except Exception as e:
        log.exception("notas_xml: ZIP %s falhou", job.id)
        job.estado = "erro"
        job.erro = str(e)[:300]


def _leia_me(job: Job) -> str:
    linhas = [job.cabecalho.rstrip(), "",
              f"Notas pedidas: {job.total}",
              f"XMLs gravados neste ZIP: {job.gravados}",
              f"Notas sem XML no sistema: {len(job.faltando)}"]
    if job.erros:
        n_erro = sum(1 for it in job.itens if it["posto"] in job.erros)
        linhas.append(f"Notas que NAO entraram porque o posto nao respondeu: {n_erro}")
        for p, e in sorted(job.erros.items()):
            linhas.append(f"  - posto {p}: {e}")
        linhas.append("  Gere o ZIP de novo mais tarde para pegar essas notas.")
    if job.faltando:
        linhas += ["", "Sem XML no sistema (o retorno da prefeitura veio sem o XML):"]
        linhas += [f"  - {r}" for r in job.faltando]
    linhas += ["", "Gerado em " + time.strftime("%d/%m/%Y %H:%M:%S")]
    return "\r\n".join(linhas) + "\r\n"


def iniciar_zip(dono: str, nome_zip: str, itens: list[dict], cabecalho: str = "", leitor=None) -> Job:
    """Dispara a geração. `itens` = [{posto, id, nome, rotulo}] já validados pelo chamador.
    `leitor(posto, ids, con=None) -> {id: bytes}` só existe para os testes."""
    if not itens:
        raise ValueError("nenhuma nota com XML para baixar")
    _limpar()
    with _lock:
        ativos = sum(1 for j in _jobs.values() if j.estado == "andando")
        if ativos >= MAX_JOBS_ATIVOS:
            raise RuntimeError("já há duas gerações de ZIP em andamento; espere uma terminar")
        job = Job(dono, nome_zip, itens, cabecalho)
        _jobs[job.id] = job
    t = threading.Thread(target=_rodar, args=(job, leitor or ler_xmls), name=f"notas-xml-{job.id[:8]}", daemon=True)
    t.start()
    return job


def obter(job_id: str, dono: str) -> Job | None:
    with _lock:
        j = _jobs.get(job_id)
    if not j or j.dono != dono:
        return None
    return j
