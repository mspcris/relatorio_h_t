"""
seed_servicos.py — Popula public.servicos no RDS Postgres a partir dos ~50
itens hoje hardcoded em auth_routes.PAGINAS_DISPONIVEIS.

Idempotente: usa UPSERT em (key) para que rodar múltiplas vezes não duplique
nem sobrescreva colunas editadas posteriormente pelo admin (label, lock,
ordem, descricao, icone, cor preservam o valor atual quando já existem).

Uso:
    cd /opt/relatorio_h_t/  (ou local com .env carregado)
    python3 seed_servicos.py             # cria tabela + insere/atualiza linhas
    python3 seed_servicos.py --dry-run   # só mostra o que faria, sem escrever
    python3 seed_servicos.py --force     # sobrescreve TODAS as colunas, mesmo
                                          # se a linha já existe (use só p/ reset)

Não migra UserPagePermission — os page_keys são preservados, então as
permissões já gravadas em SQLite continuam apontando para os mesmos serviços.
"""
import argparse
import os
import sys
from pathlib import Path

# Garante que o .env local seja carregado se rodado fora de produção
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from sqlalchemy.dialects.postgresql import insert as pg_insert

from servicos_db import PgSession, Servico, init_pg_db


# ── Catálogo a popular ────────────────────────────────────────────────────────
#
# Mesma lista do auth_routes.PAGINAS_DISPONIVEIS, mantendo cada `key`.
# Adicionados campos:
#   - ordem  : posição dentro do grupo (10, 20, 30 … pra permitir inserts no meio)
#   - lock   : NULL para kpi; default "verde" para mais/extras (admin classifica depois)
#              Níveis: verde (IDCamim basta) / prata (cadastro extra) / dourado (admin/diretor)
#              / vermelho (oculto na intranet.camim.com.br).
#
# Renames e movimentações da nova lista serão aplicados pelo seed só quando
# rodado com --force (ou via UI do admin depois). Aqui ficam os valores
# CRUS DE HOJE, sem aplicar ainda as 11 melhorias.

SERVICOS_SEED: list[dict] = [
    # ── KPIs (sem cadeado) ────────────────────────────────────────────────────
    {"key": "alimentacao",                "label": "KPI Custo Alimentação",      "group_name": "kpi", "href": "/kpi_alimentacao.html",                "ordem":  10},
    {"key": "medicos",                    "label": "KPI Custo Médico",           "group_name": "kpi", "href": "/kpi_medicos.html",                    "ordem":  20},
    {"key": "ctrlq_relatorio",            "label": "KPI Médicos (Qualidade)",    "group_name": "kpi", "href": "/ctrlq_relatorio.html",                "ordem":  30},
    {"key": "kpi_v2",                     "label": "KPI Mensalidades",           "group_name": "kpi", "href": "/kpi_v2.html",                         "ordem":  40},
    {"key": "kpi_vendas",                 "label": "KPI Vendas",                 "group_name": "kpi", "href": "/kpi_vendas.html",                     "ordem":  50},
    {"key": "clientes",                   "label": "KPI Clientes",               "group_name": "kpi", "href": "/kpi_clientes.html",                   "ordem":  60},
    {"key": "kpi_prescricao",             "label": "KPI Prescrições",            "group_name": "kpi", "href": "/KPI_prescricao.html",                 "ordem":  70},
    {"key": "kpi_fidelizacao",            "label": "KPI Fidelização Churn",      "group_name": "kpi", "href": "/kpi_fidelizacao_cliente.html",        "ordem":  80},
    {"key": "kpi_consultas",              "label": "KPI Consultas (Status)",     "group_name": "kpi", "href": "/kpi_consultas_status.html",           "ordem":  90},
    {"key": "kpi_egide",                  "label": "KPI Égide Saúde",            "group_name": "kpi", "href": "/kpi_egide.html",                      "ordem": 100},
    {"key": "kpi_notas_rps",              "label": "KPI Notas x RPS",            "group_name": "kpi", "href": "/kpi_notas_rps.html",                  "ordem": 110},
    {"key": "kpi_metas",                  "label": "KPI Metas (Mens/Vendas)",    "group_name": "kpi", "href": "/kpi_metas.html",                      "ordem": 120},
    {"key": "kpi_governo",                "label": "KPI Índices Oficiais",       "group_name": "kpi", "href": "/kpi_governo.html",                    "ordem": 130},
    {"key": "kpi_liberty",                "label": "KPI CAMIM Liberty",          "group_name": "kpi", "href": "/kpi_liberty.html",                    "ordem": 140},
    {"key": "kpi_receita_despesa",        "label": "KPI Receitas x Despesas",    "group_name": "kpi", "href": "/kpi_receita_despesa.html",            "ordem": 150},
    {"key": "kpi_receita_despesa_rateio", "label": "KPI R x D com Rateio",       "group_name": "kpi", "href": "/kpi_receita_despesa_rateio.html",     "ordem": 160},
    {"key": "growth",                     "label": "Growth Dashboard",           "group_name": "kpi", "href": "/growth.html",                         "ordem": 170},
    {"key": "email_clientes",             "label": "Email de Cobrança",          "group_name": "kpi", "href": "/email_clientes",                      "ordem": 180},
    {"key": "leiame",                     "label": "Leia-me (Painel Antigo)",    "group_name": "kpi", "href": "/leiame",                              "ordem": 190},
    # CHAT Avaliações e Mais Serviços ficam ao FINAL do grupo KPI (item 10 da lista)
    {"key": "chat_avaliacoes",            "label": "CHAT Avaliações",            "group_name": "kpi", "href": "/chat_avaliacoes",                     "ordem": 900},
    {"key": "mais_servicos",              "label": "Mais Serviços",              "group_name": "kpi", "href": "/mais_servicos",                       "ordem": 910},

    # ── Mais Serviços (cadeado default "verde"; admin classifica depois) ──────
    {"key": "k_whatsapp_explicado",       "label": "WhatsApp - Explicando a Cobrança",         "group_name": "mais", "href": "/k_whatsapp_como_funciona.html",                "lock": "verde", "ordem":  10},
    {"key": "cobranca",                   "label": "Cobrança",                                 "group_name": "mais", "href": "https://cobranca.camim.com.br/",                "lock": "verde", "ordem":  20},
    {"key": "chat_externo",               "label": "Chat",                                     "group_name": "mais", "href": "https://chat.camim.com.br/",                    "lock": "verde", "ordem":  30},
    {"key": "broker",                     "label": "Vendas Efetivar - Broker",                 "group_name": "mais", "href": "https://broker.camim.com.br/",                  "lock": "verde", "ordem":  40},
    {"key": "corretores",                 "label": "Vendas Leads Corretores",                  "group_name": "mais", "href": "https://corretores.camim.com.br/",              "lock": "verde", "ordem":  50},
    {"key": "leads_analytics",            "label": "Vendas - Leads Analytics",                 "group_name": "mais", "href": "/leads_analytics.html",                         "lock": "verde", "ordem":  60},
    {"key": "tarefas",                    "label": "Tarefas - Nosso Trello",                   "group_name": "mais", "href": "https://tarefas.camim.com.br/",                 "lock": "verde", "ordem":  70},
    {"key": "push_cobranca",              "label": "Push de Cobrança IA",                      "group_name": "mais", "href": "https://camila5.ia.camim.com.br/login?next=/",  "lock": "verde", "ordem":  80},
    {"key": "wpp_campanhas",              "label": "WhatsApp Campanhas",                       "group_name": "mais", "href": "https://camila1.ia.camim.com.br/",              "lock": "verde", "ordem":  90},
    {"key": "camila_crm",                 "label": "Camila dos Clientes",                      "group_name": "mais", "href": "https://atendimento.camilaia.camim.com.br/crm", "lock": "verde", "ordem": 100},
    {"key": "crm",                        "label": "CRM",                                      "group_name": "mais", "href": "https://crm.camim.com.br/",                     "lock": "verde", "ordem": 110},
    {"key": "central",                    "label": "Central",                                  "group_name": "mais", "href": "https://central.camim.com.br/",                 "lock": "verde", "ordem": 120},
    {"key": "agenda_dia",                 "label": "Agenda do Dia (F3)",                       "group_name": "mais", "href": "/agenda_dia",                                   "lock": "verde", "ordem": 130},
    {"key": "preagendamento",             "label": "Dashboard Pré-Agendamento",                "group_name": "mais", "href": "/preagendamento",                               "lock": "verde", "ordem": 140},
    {"key": "iot_monitor",                "label": "Monitor IoT (Ar Condicionado)",            "group_name": "mais", "href": "https://iot.propagacaodigital.com.br/",         "lock": "verde", "ordem": 150},
    {"key": "camila_funcionarios",        "label": "Camila dos Funcionários",                  "group_name": "mais", "href": "https://camila.camim.com.br/",                  "lock": "verde", "ordem": 160},
    {"key": "medico_novo",                "label": "Médico - Inclusão Agenda Temporária",      "group_name": "mais", "href": "/medico_novo",                                  "lock": "verde", "ordem": 170},
    {"key": "medico_falta",               "label": "Médico - Cadastrar Falta + WhatsApp",      "group_name": "mais", "href": "/medico_falta",                                 "lock": "verde", "ordem": 180},
    {"key": "tef",                        "label": "TEF Recorrente",                           "group_name": "mais", "href": "/tef",                                          "lock": "verde", "ordem": 190},
    {"key": "chat_dashboard",             "label": "Dashboard Chat (Camila.ai)",               "group_name": "mais", "href": "/chat_dashboard.html",                          "lock": "verde", "ordem": 200},
    {"key": "wpp_dashboard",              "label": "Dashboard WhatsApp (Meta)",                "group_name": "mais", "href": "/wpp_dashboard.html",                           "lock": "verde", "ordem": 210},
    {"key": "ctrlq_desbloqueio",          "label": "Médico - Desbloqueio de Agenda — CTRL-Q",  "group_name": "mais", "href": "/ctrlq_desbloqueio",                            "lock": "verde", "ordem": 220},
    {"key": "qualidade_agenda",           "label": "Qualidade da Agenda Médica",               "group_name": "mais", "href": "/qualidade_agenda",                             "lock": "verde", "ordem": 230},
    {"key": "higienizacao",               "label": "Higienização",                             "group_name": "mais", "href": "/higienizacao",                                 "lock": "verde", "ordem": 240},
    {"key": "monitor_avisos",             "label": "Monitor de Avisos",                        "group_name": "mais", "href": "https://avisos.camim.com.br/avisos",            "lock": "verde", "ordem": 250},
    {"key": "quadro_avisos_postos",       "label": "MURAL - Quadro de Avisos",                 "group_name": "mais", "href": "https://avisos.camim.com.br/",                  "lock": "verde", "ordem": 260},
    # CAMIM Analytics migrado de has_openai_account → page_key, com lock=ouro
    {"key": "gpt_kpi_manus",              "label": "ChatGPT dos KPI's / Manus",                "group_name": "mais", "href": "https://chatgpt.com/g/g-67be0c9b8b748191988b4e2bd49b09d2-camim-analytics", "lock": "dourado", "ordem": 270, "descricao": "GPT customizado da OpenAI com acesso às APIs dos KPIs — pergunte qualquer dado em linguagem natural. Acesso restrito a usuários com conta na OpenAI da CAMIM."},

    # ── Extras (Planejamento PCs + Notas Fiscais NBS/IBS/CBS) ─────────────────
    {"key": "k_relatorio_pcs",            "label": "Planejamento PC's",          "group_name": "extras", "href": "/k_adicional_relatorio_pcs.html",   "lock": "verde", "ordem":  10},
    {"key": "k_nbs_ibs_cbs",              "label": "Notas Fiscais NBS/IBS/CBS",  "group_name": "extras", "href": "/k_adicional_NBS-IBS-CBS.html",     "lock": "verde", "ordem":  20},
]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Não escreve no banco, só mostra o que faria")
    ap.add_argument("--force",   action="store_true", help="Sobrescreve TODAS as colunas (label, ordem, lock, etc.) mesmo se já existir")
    args = ap.parse_args()

    print(f"[seed_servicos] {'DRY-RUN' if args.dry_run else 'EXECUTANDO'} — {len(SERVICOS_SEED)} itens")

    if not args.dry_run:
        print("[seed_servicos] Criando tabela public.servicos se não existir…")
        init_pg_db()

    db = PgSession()
    try:
        for row in SERVICOS_SEED:
            # ON CONFLICT em (key):
            #   - sem --force: insere se não existe; se já existe, NÃO sobrescreve nada
            #     (a `label` editada pelo admin ou ajuste posterior é preservada).
            #   - com --force: sobrescreve label, href, group_name, lock, ordem, descricao.
            stmt = pg_insert(Servico).values(**row)
            if args.force:
                update_cols = {
                    "label":      stmt.excluded.label,
                    "href":       stmt.excluded.href,
                    "group_name": stmt.excluded.group_name,
                    "lock":       stmt.excluded.lock,
                    "ordem":      stmt.excluded.ordem,
                    "descricao":  stmt.excluded.descricao,
                }
                stmt = stmt.on_conflict_do_update(index_elements=["key"], set_=update_cols)
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=["key"])

            if args.dry_run:
                print(f"  [{row['group_name']:6}] {row['key']:30} → {row['label']}")
            else:
                db.execute(stmt)

        if not args.dry_run:
            db.commit()

        total = db.query(Servico).count()
        por_grupo = {g: db.query(Servico).filter_by(group_name=g).count() for g in ("kpi", "mais", "extras")}
        print(f"[seed_servicos] OK — total na tabela: {total}  (kpi={por_grupo['kpi']}, mais={por_grupo['mais']}, extras={por_grupo['extras']})")
    except Exception as e:
        db.rollback()
        print(f"[seed_servicos] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
