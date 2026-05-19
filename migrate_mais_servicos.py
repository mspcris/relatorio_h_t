"""
migrate_mais_servicos.py — Extrai metadados visuais (icone, cor, descrição)
dos 27 cards de mais_servicos.html e popula esses campos na tabela
public.servicos do RDS Postgres.

Roda uma vez (idempotente). Depois deste script, o mais_servicos.html pode
ser convertido para template Jinja2 que itera os serviços a partir do DB
sem perder o visual atual.

Uso:
    python3 migrate_mais_servicos.py             # aplica
    python3 migrate_mais_servicos.py --dry-run   # só mostra o que faria
"""
import argparse
import re
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from servicos_db import PgSession, Servico


HTML_PATH = Path(__file__).parent / "mais_servicos.html"


# Regex que captura cada bloco de card no row-normais:
#   <div class="col-md-4 mb-4">
#       …
#       <i class="fas fa-XXX fa-2x …" style="color:#XXXXXX">
#       …
#       <h5 class="card-title">TITULO</h5>
#       <p class="card-text flex-grow-1">DESCRICAO</p>
#       …
#       <a href="HREF" …
#   </div></div></div>
#
# Padrão é regular. CAMIM Analytics tem `{% if %}` (Jinja já presente) com 2 hrefs;
# vamos pegar o primeiro href real (chatgpt.com…).
CARD_RE = re.compile(
    r'<div class="col-md-4 mb-4">.*?'
    r'<i class="(?P<fa_prefix>fas|fab|far) fa-(?P<icon>[a-z0-9-]+)\s+fa-2x.*?style="color:(?P<cor>#?[0-9A-Fa-f]{6,7}|[A-Za-z][A-Za-z0-9]*)".*?'
    r'<h5 class="card-title">(?P<titulo>[^<]+)</h5>\s*'
    r'<p class="card-text[^"]*">(?P<descricao>.*?)</p>.*?'
    r'<a\s+href="(?P<href>[^"]+)"',
    re.DOTALL,
)


# Mapeamento explícito href encontrado no HTML → key da tabela.
# (alguns hrefs nos cards são templates Flask sem .html, outros com .html — todas as variações
# levam à mesma key.)
HREF_TO_KEY = {
    # Internas com .html
    "agenda_dia.html":              "agenda_dia",
    "preagendamento.html":          "preagendamento",
    "higienizacao.html":            "higienizacao",
    "medico_falta.html":            "medico_falta",
    "ctrlq_desbloqueio.html":       "ctrlq_desbloqueio",
    "medico_novo.html":             "medico_novo",
    "qualidade_agenda.html":        "qualidade_agenda",
    "tef_dashboard.html":           "tef",
    "tef.html":                     "tef",
    "leads_analytics.html":         "leads_analytics",
    "k_whatsapp_como_funciona.html":     "k_whatsapp_explicado",
    "k_adicional_relatorio_pcs.html":    "k_relatorio_pcs",
    "k_adicional_NBS-IBS-CBS.html":      "k_nbs_ibs_cbs",
    "email_clientes_dashboard.html":     "email_clientes",
    "wpp_dashboard.html":           "wpp_dashboard",
    "chat_dashboard.html":          "chat_dashboard",
    "chat_avaliacoes.html":         "chat_avaliacoes",
    # Internas (rotas Flask) sem .html — usadas nos cards do mais_servicos.html
    "/preagendamento":              "preagendamento",
    "/higienizacao":                "higienizacao",
    "/medico_falta":                "medico_falta",
    "/ctrlq_desbloqueio":           "ctrlq_desbloqueio",
    "/medico_novo":                 "medico_novo",
    "/qualidade_agenda":            "qualidade_agenda",
    "/tef":                         "tef",
    "/email_clientes":              "email_clientes",
    "/chat_avaliacoes":             "chat_avaliacoes",
    "/agenda_dia":                  "agenda_dia",
    "/mais_servicos":               "mais_servicos",
    "/leiame":                      "leiame",
    # Externas
    "https://camila.camim.com.br/":     "camila_funcionarios",
    "https://atendimento.camilaia.camim.com.br/crm":  "camila_crm",
    "https://central.camim.com.br/":    "central",
    "https://chat.camim.com.br/":       "chat_externo",
    "https://cobranca.camim.com.br/":   "cobranca",
    "https://crm.camim.com.br/":        "crm",
    "https://iot.propagacaodigital.com.br/":  "iot_monitor",
    "https://iot.propagacaodigital.com.br":   "iot_monitor",
    "https://camila5.ia.camim.com.br/login?next=/": "push_cobranca",
    "https://avisos.camim.com.br/":     "quadro_avisos_postos",
    "https://avisos.camim.com.br/avisos": "monitor_avisos",
    "https://tarefas.camim.com.br/":    "tarefas",
    "https://broker.camim.com.br/":     "broker",
    "https://corretores.camim.com.br/": "corretores",
    "https://camila1.ia.camim.com.br/": "wpp_campanhas",
}


def _key_for_href(href: str) -> str | None:
    """Resolve href → key. Aceita match exato ou prefixo do chatgpt.com (CAMIM Analytics)."""
    if href in HREF_TO_KEY:
        return HREF_TO_KEY[href]
    if href.startswith("https://chatgpt.com/"):
        return "gpt_kpi_manus"
    # variações: sem '/' final, sem prefixo '/'
    if href.startswith("/") and href[1:] in HREF_TO_KEY:
        return HREF_TO_KEY[href[1:]]
    if href.endswith("/") and href[:-1] in HREF_TO_KEY:
        return HREF_TO_KEY[href[:-1]]
    return None


def _normalize_cor(cor: str) -> str:
    cor = cor.strip()
    if cor.startswith("#"):
        return cor.upper()
    return cor  # nome de cor (raro)


def _normalize_icone(prefix: str, icone: str) -> str:
    """Retorna 'fas fa-XXX' / 'fab fa-XXX' / 'far fa-XXX' (FontAwesome)."""
    return f"{prefix} fa-{icone}"


def parse_cards(html: str) -> list[dict]:
    cards = []
    seen_hrefs = set()
    for m in CARD_RE.finditer(html):
        href = m.group("href").strip()
        # Caso CAMIM Analytics ({% if %}): pegamos o primeiro href real.
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)
        cards.append({
            "fa_prefix": m.group("fa_prefix").strip(),
            "icon":      m.group("icon").strip(),
            "cor":       _normalize_cor(m.group("cor")),
            "titulo":    m.group("titulo").strip(),
            "descricao": m.group("descricao").strip(),
            "href":      href,
        })
    return cards


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    html = HTML_PATH.read_text(encoding="utf-8")
    cards = parse_cards(html)
    print(f"[migrate_mais_servicos] {len(cards)} cards encontrados em mais_servicos.html")

    db = PgSession()
    try:
        atualizados = 0
        sem_match  = []
        ja_iguais  = 0

        for card in cards:
            key = _key_for_href(card["href"])
            if not key:
                sem_match.append((card["titulo"], card["href"]))
                continue

            servico = db.query(Servico).filter_by(key=key).first()
            if not servico:
                sem_match.append((card["titulo"], f"key={key} não encontrada na tabela"))
                continue

            icone_novo = _normalize_icone(card["fa_prefix"], card["icon"])
            cor_nova   = card["cor"]
            desc_nova  = card["descricao"]

            mudou = (servico.icone != icone_novo or
                     servico.cor   != cor_nova   or
                     (servico.descricao or "") != desc_nova)

            if not mudou:
                ja_iguais += 1
                continue

            print(f"  {'[DRY] ' if args.dry_run else ''}{key:25} icone={icone_novo:22} cor={cor_nova} → {card['titulo']}")
            if not args.dry_run:
                servico.icone = icone_novo
                servico.cor   = cor_nova
                if not servico.descricao:  # preserva descrição já editada (item 8 já tem)
                    servico.descricao = desc_nova
                else:
                    # Se for o card do gpt_kpi_manus (seed já preencheu descricao), mantém.
                    if key != "gpt_kpi_manus":
                        servico.descricao = desc_nova
            atualizados += 1

        if not args.dry_run:
            db.commit()
        print(f"\n[migrate_mais_servicos] {'DRY-RUN' if args.dry_run else 'OK'} — atualizados: {atualizados}, já iguais: {ja_iguais}, sem match: {len(sem_match)}")
        if sem_match:
            print("\n  Cards sem match (revisar manualmente):")
            for titulo, info in sem_match:
                print(f"    - {titulo:35} → {info}")
            sys.exit(2 if not args.dry_run else 0)
    except Exception as e:
        db.rollback()
        print(f"[migrate_mais_servicos] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
