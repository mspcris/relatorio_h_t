"""
painel_financeiro.py — motor do KPI "Painel Financeiro" (por enquanto: IMPOSTOS).

Pergunta do Cristiano (2026-09-03): "quero ver o imposto A, INSS por exemplo,
os últimos 36 meses daquele posto, a média, e todos os registros se eu
clicar. Cada imposto tem sua faixa de % de alarme — amarela e vermelha; a
vermelha manda e-mail para o Leonardo. Só quero ver na tela quem não tem %
cadastrada e quem está fora da faixa."

Fonte: RDS Postgres `fin_despesa` (carga a cada 2 h de vw_Fin_Despesa +
Fin_Despesa; mês = data_pagamento_auto, a mesma base do kpi_receita_despesa).
Nada aqui toca o SQL Server da CAMIM.

Como um lançamento vira "imposto": o plano de contas NÃO é igual entre postos
(plano 'IMPOSTOS CLÍNICA' + 'IMPOSTOS FUNCIONÁRIOS' num grupo, 'IMPOSTO' /
'IMPOSTOS' no outro) e o mesmo tributo tem várias grafias de tipo ('GPS - INSS'
e 'INSS'; 'GRF - FGTS', 'FGTS' e 'GUIA DO RECOLHIMENTO DO FGTS'; 'DAS - SIMPLES
NACIONAL', 'DAS' e 'SIMPLES NACIONAL'). Por isso a classificação é por RADICAL
sobre o tipo normalizado (mesma lição do filtro de especialidades), dentro
dos planos de imposto — e a página mostra quais tipos originais caíram em
cada grupo, para o Cristiano conferir.

Regra de avaliação (decidida por mim em 2026-09-03, "temperatura 0,9"):
  • mês avaliado   = último mês FECHADO (o corrente é parcial: o DAS do mês
                     ainda não foi pago no dia 5). O mês corrente aparece
                     como informação.
  • base           = média dos últimos N meses (padrão 12; 24 e 36 também
                     calculadas) contando SÓ meses com pagamento — imposto
                     anual parcelado (IPTU) tem meses zerados que não são
                     "queda".
  • desvio %       = (valor do mês − média base) ÷ média base.
  • mês sem pagamento num imposto que é mensal (pago em ≥ 8 dos 12 meses
                     anteriores) = desvio −100 % → cai na faixa e a página
                     diz "não pago". Imposto que não é mensal e não teve
                     pagamento no mês não é avaliado (status 'sem_mes').
  • faixa          = amarelo se |desvio| ≥ amarelo_pct; vermelho se |desvio|
                     ≥ vermelho_pct. Nos dois sentidos: pagar 60 % a menos é
                     tão estranho quanto 60 % a mais.
  • sem faixa      = status 'sem_faixa' — aparece SEMPRE, até alguém
                     cadastrar. Com faixa e dentro dela → 'normal', some da
                     tela (há um botão "mostrar todos").
"""
from __future__ import annotations

import os
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone
from statistics import mean

import psycopg2
import psycopg2.extras

_BRT = timezone(timedelta(hours=-3))
POSTOS = list("ABCDGIJMNPRXY")
EMAIL_ALERTA_PADRAO = os.getenv("IMPOSTOS_EMAIL_ALERTA", "leonardo@camim.com.br")
MESES_JANELA = 36

# ── classificação ────────────────────────────────────────────────────────────
# (chave, rótulo, regex sobre o TIPO normalizado). Ordem importa: a primeira
# que casar ganha. 'OUTROS' pega o resto do que está nos planos de imposto.
IMPOSTOS = [
    ("DAS",     "DAS · Simples Nacional",        r"^DAS\b|SIMPLES"),
    ("DARF",    "DARF · Receita Federal",         r"^DARF|RECEITA FEDERAL"),
    ("INSS",    "INSS · GPS",                     r"INSS|^GPS\b"),
    ("FGTS",    "FGTS · GRF",                     r"FGTS|^GRF\b"),
    ("GRRF",    "GRRF · FGTS rescisório",         r"^GRRF|RESCIS"),
    ("DARM",    "DARM · Prefeitura do Rio",       r"^DARM"),
    ("ISS",     "ISS · ISSQN",                    r"^ISS"),
    ("IPTU",    "IPTU",                           r"^IPTU"),
    ("IPVA",    "IPVA",                           r"^IPVA"),
    ("ESOCIAL", "e-Social",                       r"E ?SOCIAL"),
    ("TAXAS",   "Taxas e licenças",               r"TAXA|DATI|LICEN|INCENDIO|CRF|CRM\b|ALVAR|BOMBEIRO|SANITAR|LETREIRO|CONDOMIN"),
    ("OUTROS",  "Outros lançados como imposto",   r"."),
]
# Planos que são "de imposto" (normalizados, casamento por 'IMPOSTO' dentro do
# nome) + tipos de imposto que alguns postos lançam em DESPESAS FIXAS.
_RE_PLANO_IMPOSTO = re.compile(r"IMPOSTO")
_RE_TIPO_FIXAS = re.compile(r"^(DARF|FGTS|IPTU|ISS|DAS\b|INSS|GPS\b|GRF\b|GRRF|DARM)")


def normalizar(s) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Za-z0-9]+", " ", s).upper().strip()
    return re.sub(r"\s+", " ", s)


def classificar(plano, tipo) -> str | None:
    """→ chave do imposto ou None (não é imposto)."""
    pl, tp = normalizar(plano), normalizar(tipo)
    if _RE_PLANO_IMPOSTO.search(pl):
        pass
    elif pl == "DESPESAS FIXAS" and _RE_TIPO_FIXAS.search(tp):
        pass
    else:
        return None
    for chave, _rot, rx in IMPOSTOS:
        if re.search(rx, tp):
            return chave
    return "OUTROS"


ROTULOS = {c: r for c, r, _ in IMPOSTOS}


# ── banco ────────────────────────────────────────────────────────────────────
def pg_conn():
    return psycopg2.connect(
        host=os.environ["PG_RDS_HOST"],
        port=int(os.environ.get("PG_RDS_PORT", "9432")),
        dbname=os.environ.get("PG_RDS_DB", "relatorio_h_t"),
        user=os.environ["PG_RDS_USER"],
        password=os.environ["PG_RDS_PASSWORD"],
        sslmode=os.environ.get("PG_RDS_SSLMODE", "require"),
        connect_timeout=15,
    )


_SCHEMA_OK = False

def ensure_schema(pg) -> None:
    """Idempotente. Só ESTE projeto escreve nessas tabelas (mesmo RDS do custos_ti)."""
    global _SCHEMA_OK
    if _SCHEMA_OK:
        return
    with pg.cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS fin_imposto_faixa (
                imposto        text PRIMARY KEY,
                amarelo_pct    numeric(6,2) NOT NULL,
                vermelho_pct   numeric(6,2) NOT NULL,
                base_meses     int NOT NULL DEFAULT 12,
                email          text,
                observacao     text,
                atualizado_por text,
                atualizado_em  timestamptz NOT NULL DEFAULT now()
            )""")
        c.execute("""
            CREATE TABLE IF NOT EXISTS fin_imposto_alerta_envio (
                id          bigserial PRIMARY KEY,
                posto       text NOT NULL,
                imposto     text NOT NULL,
                mes_ref     text NOT NULL,
                status      text NOT NULL,
                valor       numeric(14,2),
                media       numeric(14,2),
                desvio_pct  numeric(8,2),
                email       text,
                enviado_em  timestamptz NOT NULL DEFAULT now(),
                UNIQUE (posto, imposto, mes_ref, status)
            )""")
    pg.commit()
    _SCHEMA_OK = True


def carregar_faixas(pg) -> dict:
    with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        c.execute("SELECT * FROM fin_imposto_faixa")
        out = {}
        for r in c.fetchall():
            r = dict(r)
            for k in ("amarelo_pct", "vermelho_pct"):
                r[k] = float(r[k]) if r[k] is not None else None
            r["atualizado_em"] = r["atualizado_em"].isoformat() if r.get("atualizado_em") else None
            out[r["imposto"]] = r
        return out


def salvar_faixa(pg, imposto: str, amarelo: float, vermelho: float, base_meses: int,
                 email: str | None, observacao: str | None, quem: str) -> dict:
    if imposto not in ROTULOS:
        raise ValueError(f"imposto desconhecido: {imposto}")
    if not (0 < amarelo < vermelho <= 1000):
        raise ValueError("faixas: 0 < amarelo < vermelho (em %)")
    if base_meses not in (12, 24, 36):
        raise ValueError("base_meses deve ser 12, 24 ou 36")
    with pg.cursor() as c:
        c.execute("""
            INSERT INTO fin_imposto_faixa (imposto, amarelo_pct, vermelho_pct, base_meses, email, observacao, atualizado_por, atualizado_em)
            VALUES (%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT (imposto) DO UPDATE SET amarelo_pct=EXCLUDED.amarelo_pct, vermelho_pct=EXCLUDED.vermelho_pct,
                base_meses=EXCLUDED.base_meses, email=EXCLUDED.email, observacao=EXCLUDED.observacao,
                atualizado_por=EXCLUDED.atualizado_por, atualizado_em=now()
        """, (imposto, amarelo, vermelho, base_meses, (email or "").strip() or None, (observacao or "").strip() or None, quem))
    pg.commit()
    return carregar_faixas(pg).get(imposto)


def apagar_faixa(pg, imposto: str) -> None:
    with pg.cursor() as c:
        c.execute("DELETE FROM fin_imposto_faixa WHERE imposto = %s", (imposto,))
    pg.commit()


# ── séries ───────────────────────────────────────────────────────────────────
def _ym(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _ym_add(ym: str, n: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7]) + n
    while m <= 0:
        m += 12; y -= 1
    while m > 12:
        m -= 12; y += 1
    return f"{y:04d}-{m:02d}"


def lista_meses(fim_ym: str, n: int) -> list[str]:
    return [_ym_add(fim_ym, -i) for i in range(n - 1, -1, -1)]


def carregar_series(pg, postos: list[str] | None = None, meses: int = MESES_JANELA) -> dict:
    """→ {posto: {imposto: {ym: {"valor":..,"n":..,"tipos":{tipo: valor}}}}}, e os
    tipos originais por imposto (para a tela mostrar o agrupamento)."""
    hoje = datetime.now(_BRT).date()
    ini = date(hoje.year, hoje.month, 1)
    for _ in range(meses):        # meses de janela + o corrente
        ini = (ini - timedelta(days=1)).replace(day=1)
    sql = """
        SELECT posto, COALESCE(plano,'') AS plano, COALESCE(tipo,'') AS tipo,
               to_char(data_pagamento_auto, 'YYYY-MM') AS ym,
               SUM(valor_pago) AS valor, COUNT(*) AS n
        FROM fin_despesa
        WHERE data_pagamento_auto >= %s
          AND data_cancelamento IS NULL
          AND (UPPER(COALESCE(plano,'')) LIKE '%%IMPOSTO%%' OR UPPER(TRIM(COALESCE(plano,''))) = 'DESPESAS FIXAS')
    """
    params: list = [ini]
    if postos:
        sql += " AND posto = ANY(%s)"
        params.append(postos)
    sql += " GROUP BY 1,2,3,4"
    series: dict = {}
    tipos_por_imposto: dict = {}
    with pg.cursor() as c:
        c.execute(sql, params)
        for posto, plano, tipo, ym, valor, n in c.fetchall():
            chave = classificar(plano, tipo)
            if not chave:
                continue
            v = float(valor or 0)
            m = series.setdefault(posto, {}).setdefault(chave, {}).setdefault(ym, {"valor": 0.0, "n": 0, "tipos": {}})
            m["valor"] += v; m["n"] += int(n or 0)
            m["tipos"][tipo.strip()] = m["tipos"].get(tipo.strip(), 0.0) + v
            t = tipos_por_imposto.setdefault(chave, {}).setdefault(tipo.strip(), {"valor": 0.0, "n": 0, "postos": set()})
            t["valor"] += v; t["n"] += int(n or 0); t["postos"].add(posto)
    for ch in tipos_por_imposto.values():
        for t in ch.values():
            t["postos"] = sorted(t["postos"]); t["valor"] = round(t["valor"], 2)
    return {"series": series, "tipos": tipos_por_imposto, "ini": ini.isoformat()}


def avaliar(series: dict, faixas: dict, hoje: date | None = None) -> list[dict]:
    """Uma linha por posto × imposto com médias, mês avaliado, desvio e status."""
    hoje = hoje or datetime.now(_BRT).date()
    ym_atual = _ym(hoje)
    ym_ref = _ym_add(ym_atual, -1)                 # último mês fechado
    meses36 = lista_meses(ym_ref, 36)
    out = []
    for posto, por_imp in series.items():
        for imposto, por_ym in por_imp.items():
            serie = [{"ym": ym, "valor": round(por_ym.get(ym, {}).get("valor", 0.0), 2),
                      "n": por_ym.get(ym, {}).get("n", 0)} for ym in meses36]
            def media(n):
                vals = [x["valor"] for x in serie[-n:] if x["n"] > 0]
                return round(mean(vals), 2) if vals else None
            def media_cheia(n):
                vals = [x["valor"] for x in serie[-n:]]
                return round(mean(vals), 2) if vals else None
            m12, m24, m36 = media(12), media(24), media(36)
            meses_com_pgto_12 = sum(1 for x in serie[-12:] if x["n"] > 0)
            mensal = meses_com_pgto_12 >= 8
            ref = por_ym.get(ym_ref, {"valor": 0.0, "n": 0})
            atual = por_ym.get(ym_atual, {"valor": 0.0, "n": 0})
            faixa = faixas.get(imposto)
            base_n = int(faixa["base_meses"]) if faixa else 12
            # a média base NÃO inclui o mês avaliado
            vals_base = [x["valor"] for x in serie[:-1][-base_n:] if x["n"] > 0]
            media_base = round(mean(vals_base), 2) if vals_base else None
            valor_ref = round(ref["valor"], 2)
            if ref["n"] == 0 and not mensal:
                desvio = None; status = "sem_mes"
            elif media_base is None:
                desvio = None; status = "sem_base"
            else:
                desvio = round((valor_ref - media_base) / media_base * 100.0, 1) if media_base else None
                if not faixa:
                    status = "sem_faixa"
                elif desvio is None:
                    status = "sem_base"
                elif abs(desvio) >= float(faixa["vermelho_pct"]):
                    status = "vermelho"
                elif abs(desvio) >= float(faixa["amarelo_pct"]):
                    status = "amarelo"
                else:
                    status = "normal"
            ultimo = max((ym for ym, m in por_ym.items() if m["n"] > 0), default=None)
            out.append({
                "posto": posto, "imposto": imposto, "rotulo": ROTULOS.get(imposto, imposto),
                "mes_ref": ym_ref, "valor_ref": valor_ref, "n_ref": ref["n"], "nao_pago": ref["n"] == 0 and mensal,
                "mes_atual": ym_atual, "valor_atual": round(atual["valor"], 2), "n_atual": atual["n"],
                "media12": m12, "media24": m24, "media36": m36,
                "media12_cheia": media_cheia(12), "media_base": media_base, "base_meses": base_n,
                "mensal": mensal, "meses_com_pgto_12": meses_com_pgto_12,
                "desvio_pct": desvio, "status": status,
                "faixa": {"amarelo": faixa["amarelo_pct"], "vermelho": faixa["vermelho_pct"], "email": faixa.get("email"),
                          "atualizado_por": faixa.get("atualizado_por"), "atualizado_em": faixa.get("atualizado_em")} if faixa else None,
                "ultimo_pagamento_ym": ultimo,
                "serie": serie,
                "tipos_ref": por_ym.get(ym_ref, {}).get("tipos", {}),
            })
    ordem = {"vermelho": 0, "amarelo": 1, "sem_faixa": 2, "sem_base": 3, "normal": 4, "sem_mes": 5}
    out.sort(key=lambda r: (r["posto"], ordem.get(r["status"], 9), -(r["media12"] or 0)))
    return out


def painel(postos: list[str] | None = None) -> dict:
    pg = pg_conn()
    try:
        ensure_schema(pg)
        faixas = carregar_faixas(pg)
        dados = carregar_series(pg, postos)
        linhas = avaliar(dados["series"], faixas)
        return {
            "gerado_em": datetime.now(_BRT).isoformat(timespec="seconds"),
            "janela_ini": dados["ini"],
            "impostos": [{"chave": c, "rotulo": r} for c, r, _ in IMPOSTOS],
            "tipos_por_imposto": dados["tipos"],
            "faixas": faixas,
            "email_padrao": EMAIL_ALERTA_PADRAO,
            "linhas": linhas,
        }
    finally:
        pg.close()


def registros(posto: str, imposto: str, ym: str | None, limite: int = 500) -> list[dict]:
    """Lançamentos (linha a linha) de um posto × imposto, opcionalmente num mês."""
    pg = pg_conn()
    try:
        sql = """
            SELECT posto, id_despesa, valor_pago, valor_devido, data_pagamento, data_pagamento_auto, data_prestacao,
                   data_vencimento, data_cancelamento, descricao, comentario, tipo, plano, plano_principal,
                   fornecedor, conta, forma, usuario, usuario_inclusao, imported_at
            FROM fin_despesa
            WHERE posto = %s AND data_pagamento_auto >= NOW() - INTERVAL '37 months'
              AND (UPPER(COALESCE(plano,'')) LIKE '%%IMPOSTO%%' OR UPPER(TRIM(COALESCE(plano,''))) = 'DESPESAS FIXAS')
        """
        params: list = [posto]
        if ym:
            sql += " AND to_char(data_pagamento_auto,'YYYY-MM') = %s"
            params.append(ym)
        sql += " ORDER BY data_pagamento_auto DESC, id_despesa DESC"
        out = []
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute(sql, params)
            for r in c.fetchall():
                if classificar(r["plano"], r["tipo"]) != imposto:
                    continue
                r = dict(r)
                for k, v in list(r.items()):
                    if hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
                    elif v is not None and not isinstance(v, (str, int, float, bool)):
                        r[k] = float(v)
                out.append(r)
                if len(out) >= limite:
                    break
        return out
    finally:
        pg.close()
