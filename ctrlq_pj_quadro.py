"""ctrlq_pj_quadro.py — quadro de médicos ainda NÃO PJ, por posto e competência.

A aba "Sem contrato PJ" do `ctrlq_relatorio.html` faz essa conta no navegador do
gerente. Aqui ela é feita no servidor, para quem não é navegador: desde
11/09/2026 o `avisos_gerenciais` mostra na home de cada gestor quantos médicos
do posto dele seguem sem contrato PJ e quantos já foram justificados no mês —
a cobrança saiu do cartão do Tarefas (que o Cristiano apagou) e virou indicador.

Duas fontes, as MESMAS da página:
  * quem está sem PJ → `json_ctrlq_relatorio/CTRLQ_RELATORIO_CONSOLIDADO.json`
    (foto de ontem, gerada pelo `ctrlq_export_relatorio.py`);
  * quem justificou   → `Cad_EspecialidadeJustificativaPJ` no SQL Server de cada
    posto, lida ao vivo (`ctrlq_pj_routes._ler_posto`).

As regras de canonização abaixo (medicina alternativa fora, dedup por CRM dentro
do posto, justificativa casada por idMedico e, sem id, por CRM) são cópia fiel
do JS da página. Se um lado mudar, o outro muda junto — senão o gestor vê 9 na
home do avisos e 8 no KPI, e a cobrança perde a autoridade.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from datetime import date

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARQUIVO = "CTRLQ_RELATORIO_CONSOLIDADO.json"
# O Flask roda de /opt/camim-auth, o ETL escreve em /opt/relatorio_h_t: caminho
# relativo ao módulo não serve. Procura na ordem e usa o primeiro que existir.
CANDIDATOS = [
    os.getenv("CTRLQ_JSON_DIR", "").strip(),
    "/opt/relatorio_h_t/json_ctrlq_relatorio",
    os.path.join(BASE_DIR, "json_ctrlq_relatorio"),
    "/var/www/json_ctrlq_relatorio",
]

# ── Medicina alternativa: com o bit desmarcado ficam SÓ médicos (chamado
# 7ER-HUJ-L3A5). Espelha MED_ALT_LIST / MED_ALT_PATTERNS do ctrlq_relatorio.html.
MED_ALT_LISTA = {
    "ACUPUNTURISTA", "ENDOSCOPISTA", "DENSITOMETRISTA", "TERAPEUTA OCUPACIONAL ON LINE",
    "FONIATRA", "PSICOTERAPEUTA", "ENFERMEIRO", "AGENDAMENTO DE EXAMES", "FISIOTERAPEUTA",
    "FISIOTERAPEUTA ACUPUNTURISTA", "TERAPEUTA OCUPACIONAL", "NUTRICIONISTA", "FONOAUDIÓLOGO",
    "PSICÓLOGO", "PSICÓLOGO ON LINE", "TERAPEUTA", "NEUROPSICÓLOGO", "PSICANALISTA",
    "TÉCNICO EM RADIOLOGIA", "TÉCNICO EM TOMOGRAFIA", "ELETROENCEFALOGRAMA",
    "EMAGRECIMENTO ESTÉTICA", "ELETROCARDIOGRAMA", "MAPA", "ESPIROMETRIA", "HOLTER 24 HRS",
    "NUTRICIONISTA ON LINE", "PSICOPEDAGOGO", "NEUROPSICOPEDAGOGO", "PSICOMOTRICIDADE CLÍNICA",
    "PILATES", "NEUROPSICOPEDAGOGO ABA", "PSICOPEDAGOGO ABA", "ECOCARDIOGRAMA",
    "ULTRA-SONOGRAFISTA",
}
MED_ALT_PADROES = [re.compile(p) for p in (
    # terapias / não médicos
    r"PSIC", r"FONOAUDI", r"FISIOTERAP", r"TERAPIA|TERAPEUTA", r"NUTRICION|NUTRICAO",
    r"ACUPUNT", r"ENFERM", r"PILATES", r"EMAGRECIMENTO", r"BIOLOG", r"LABORATOR", r"MEDICAMENTO",
    # exames / equipamento — o "médico" é o aparelho ou a sala
    r"ELETRO(CARDIO|ENCEFALO|NEURO)", r"ECOCARDIOGRAMA|ECO DOPPLER", r"HOLTER", r"\bMAPA\b",
    r"RAIO ?X|RADIOLOG|MAMOGRAFIA|DENSITOMETR", r"ULTRA.?SON", r"ENDOSCOP|COLONOSCOP",
    r"ESPIROMETRIA|RESSONANCIA|TOMOGRAF", r"\bTESTE\b", r"AGENDAMENTO", r"TECNICO",
)]

CAMPO_NOME = ("medico", "nome", "MEDICO", "NOME", "Medico", "Nome")
CAMPO_CRM = ("crm", "CRM", "Crm")
CAMPO_ESPECIALIDADE = ("especialidade", "ESPECIALIDADE", "Especialidade")
CAMPO_PJ = ("pessoajuridica", "PessoaJuridica", "PESSOAJURIDICA", "pessoa_juridica", "PESSOA_JURIDICA")

_cache: dict = {"mtime": None, "dados": None}


class QuadroIndisponivel(Exception):
    """JSON do CTRL-Q ausente ou ilegível — sem ele não há de quem cobrar."""


# ── Canonização (espelho do JS) ──────────────────────────────────────────────

def sem_acento(texto: str) -> str:
    return unicodedata.normalize("NFD", str(texto or "")).encode("ascii", "ignore").decode()


def eh_medicina_alternativa(especialidade: str) -> bool:
    esp = str(especialidade or "").upper().strip()
    if not esp:
        return False
    if esp in MED_ALT_LISTA:
        return True
    norm = sem_acento(esp).upper()
    return any(rx.search(norm) for rx in MED_ALT_PADROES)


def _pick(linha: dict, campos) -> str:
    for c in campos:
        v = linha.get(c)
        if v is not None and v != "":
            return v
    return ""


def _b01(v) -> bool:
    """1/true/sim → True; o resto → False (mesma tolerância do b01 do JS)."""
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "sim", "yes"):
        return True
    if s in ("0", "false", "não", "nao", "no", ""):
        return False
    try:
        return float(s) == 1
    except ValueError:
        return False


def canonizar(linha: dict, posto: str) -> dict:
    crm = str(_pick(linha, CAMPO_CRM) or "").strip()
    return dict(
        nome=str(_pick(linha, CAMPO_NOME) or "").strip(),
        crm=crm,
        especialidade=str(_pick(linha, CAMPO_ESPECIALIDADE) or "").strip(),
        pj=_b01(_pick(linha, CAMPO_PJ)),
        posto=posto,
        idmedico=_inteiro(linha.get("idmedico") or linha.get("idMedico") or linha.get("IDMEDICO")),
        idespecialidade=_inteiro(linha.get("idespecialidade") or linha.get("idEspecialidade")),
    )


def _inteiro(v):
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n or None


def chave_dedup(d: dict) -> str:
    """Um médico por posto: pelo CRM; sem CRM, por nome+especialidade."""
    posto = d["posto"].strip().lower()
    crm = re.sub(r"\s+", "", d["crm"])
    if crm:
        return f"crm:{crm}|p:{posto}"
    nome, esp = d["nome"].lower(), d["especialidade"].lower()
    return f"n:{nome}|e:{esp}|p:{posto}" if (nome or esp) else ""


def medicos_do_mes(dados: dict, posto: str, competencia: str) -> list[dict]:
    """Médicos (sem medicina alternativa, sem repetição) do posto na competência."""
    linhas = (dados.get("dados", {}).get(competencia, {}).get(posto) or {}).get("linhas") or []
    vistos, saida = set(), []
    for linha in linhas:
        d = canonizar(linha, posto)
        if eh_medicina_alternativa(d["especialidade"]):
            continue
        chave = chave_dedup(d)
        if not chave or chave in vistos:
            continue
        vistos.add(chave)
        saida.append(d)
    return saida


# ── JSON do CTRL-Q ───────────────────────────────────────────────────────────

def caminho_consolidado() -> str:
    """Primeiro candidato que tem o arquivo (resolvido a cada leitura: o ETL
    pode criar a pasta depois que o Flask subiu)."""
    for pasta in CANDIDATOS:
        if pasta and os.path.isfile(os.path.join(pasta, ARQUIVO)):
            return os.path.join(pasta, ARQUIVO)
    raise QuadroIndisponivel(f"{ARQUIVO} não encontrado em {', '.join(c for c in CANDIDATOS if c)}")


def carregar_consolidado() -> dict:
    """Lê o consolidado do disco (releitura só quando o ETL reescreve o arquivo)."""
    caminho = caminho_consolidado()
    try:
        mtime = (caminho, os.path.getmtime(caminho))
    except OSError as e:
        raise QuadroIndisponivel(f"{ARQUIVO} ilegível: {e}") from e
    if _cache["mtime"] != mtime:
        try:
            with open(caminho, encoding="utf-8") as f:
                _cache["dados"] = json.load(f)
        except (OSError, ValueError) as e:
            raise QuadroIndisponivel(f"{ARQUIVO} ilegível: {e}") from e
        _cache["mtime"] = mtime
    return _cache["dados"]


def competencias(quantidade: int, hoje: date | None = None) -> list[str]:
    """As `quantidade` últimas competências, da mais antiga para o mês corrente."""
    hoje = hoje or date.today()
    ano, mes = hoje.year, hoje.month
    saida = []
    for _ in range(max(1, quantidade)):
        saida.append(f"{ano:04d}-{mes:02d}")
        mes -= 1
        if mes == 0:
            ano, mes = ano - 1, 12
    return list(reversed(saida))


# ── Justificativas ───────────────────────────────────────────────────────────

def indexar_justificativas(itens: list[dict]) -> tuple[dict, dict]:
    """(por idMedico, por CRM) — mais recente primeiro, como o pjIndexar do JS."""
    por_med, por_crm = {}, {}
    for it in itens:
        if it.get("id_medico") is not None:
            por_med.setdefault(it["id_medico"], []).append(it)
        crm = re.sub(r"\s+", "", str(it.get("crm") or ""))
        if crm:
            por_crm.setdefault(crm, []).append(it)
    def recente(j):
        return (str(j.get("data_hora") or ""), j.get("id") or 0)
    for lista in (*por_med.values(), *por_crm.values()):
        lista.sort(key=recente, reverse=True)
    return por_med, por_crm


def justificativas_de(d: dict, por_med: dict, por_crm: dict) -> list[dict]:
    """Justificativas do médico: por idMedico e, no JSON antigo sem id, por CRM."""
    if d["idmedico"] and d["idmedico"] in por_med:
        return por_med[d["idmedico"]]
    crm = re.sub(r"\s+", "", d["crm"])
    return por_crm.get(crm, []) if crm else []


def _resumir(j: dict) -> dict:
    """A última justificativa, enxuta — o avisos só mostra quem escreveu e quando."""
    return dict(
        texto=j.get("justificativa") or "",
        data_hora=j.get("data_hora"),
        competencia=j.get("competencia"),
        usuario=(j.get("usuario_nome") or j.get("usuario") or "").strip(),
    )


def quadro_do_posto(dados: dict, posto: str, meses: list[str], itens: list[dict]) -> dict:
    """Por competência: quantos sem PJ, quantos justificados naquele mês e quem falta."""
    por_med, por_crm = indexar_justificativas(itens)
    saida = {}
    for mes in meses:
        medicos = [d for d in medicos_do_mes(dados, posto, mes) if not d["pj"]]
        linhas, justificados = [], 0
        for d in medicos:
            justs = justificativas_de(d, por_med, por_crm)
            no_mes = next((j for j in justs if j.get("competencia") == mes), None)
            if no_mes:
                justificados += 1
            linhas.append(dict(
                nome=d["nome"], crm=d["crm"], especialidade=d["especialidade"],
                id_medico=d["idmedico"], justificado=bool(no_mes),
                justificativa=_resumir(no_mes or justs[0]) if (no_mes or justs) else None,
            ))
        linhas.sort(key=lambda x: (x["justificado"], x["nome"]))
        saida[mes] = dict(
            sem_pj=len(medicos), justificados=justificados, pendentes=len(medicos) - justificados,
            sem_dados=posto not in (dados.get("dados", {}).get(mes) or {}), medicos=linhas,
        )
    return saida
