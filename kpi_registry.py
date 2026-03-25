"""
KPI Registry — Catálogo centralizado de todos os KPIs
Permite descoberta automática e integração com sistemas externos (Manus, etc)
"""

KPI_MANIFEST = {
    "version": "1.0.0",
    "last_updated": "2026-03-25",
    "base_url": "https://teste-ia.camim.com.br",
    "kpis": [
        {
            "id": "fidelizacao",
            "title": "Fidelização de Clientes",
            "description": "Visualize a retenção de clientes por período de admissão, com análise de 1, 3, 6 e 12 meses. Dados agregáveis por posto ou grupo de postos.",
            "url": "/kpi_fidelizacao_cliente",
            "route": "kpi_fidelizacao_cliente.html",
            "category": "Clientes",
            "icon": "fa-handshake",
            "keywords": ["fidelizacao", "retencao", "clientes", "admissao", "adesao", "retenção"],
            "filters": {
                "month": {
                    "type": "month",
                    "required": True,
                    "description": "Mês de referência (adesão/admissão)",
                    "format": "YYYY-MM",
                    "default_offset": -13,
                    "default_description": "13 meses para trás"
                },
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": "Postos a analisar (pode ser múltiplo)",
                    "options": ["A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"],
                    "default": "ALL",
                    "groups": {
                        "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
                        "couto": ["C", "D", "J", "M", "P"]
                    }
                }
            },
            "data_source": "fidelizacao.db + json_consolidado/fidelizacao_cliente.json",
            "refresh_frequency": "diária (madrugada)",
            "retention_periods": [1, 3, 6, 12],
            "metrics": [
                {
                    "name": "total_admissoes",
                    "label": "Total de Admissões",
                    "type": "integer",
                    "description": "Número total de clientes admitidos no período selecionado"
                },
                {
                    "name": "retencao_1m",
                    "label": "Retenção 1 mês",
                    "type": "percentage",
                    "description": "% de clientes retidos após 1 mês da admissão"
                },
                {
                    "name": "retencao_3m",
                    "label": "Retenção 3 meses",
                    "type": "percentage",
                    "description": "% de clientes retidos após 3 meses da admissão"
                },
                {
                    "name": "retencao_6m",
                    "label": "Retenção 6 meses",
                    "type": "percentage",
                    "description": "% de clientes retidos após 6 meses da admissão"
                },
                {
                    "name": "retencao_12m",
                    "label": "Retenção 12 meses",
                    "type": "percentage",
                    "description": "% de clientes retidos após 12 meses da admissão"
                }
            ],
            "api_endpoints": {
                "data": "/json_consolidado/fidelizacao_cliente.json",
                "metadata": "/api/kpis/metadata/fidelizacao"
            },
            "examples": {
                "basic": "/kpi_fidelizacao_cliente?month=2025-02",
                "with_postos": "/kpi_fidelizacao_cliente?month=2025-02&postos=A,B,C",
                "group": "/kpi_fidelizacao_cliente?month=2025-02&postos=altamiro"
            },
            "manus_prompt_template": "Mostre a fidelização de clientes para {month}. Analize a retenção de {postos_desc} em 1, 3, 6 e 12 meses."
        },
        {
            "id": "vendas",
            "title": "KPI Vendas",
            "description": "Dashboard de vendas com análise de quantidade de vendas, receita total, ticket médio e performance por período. Suporta análise por posto ou grupos de postos.",
            "url": "/kpi_vendas",
            "route": "kpi_vendas.html",
            "category": "Financeiro",
            "icon": "fa-shopping-cart",
            "keywords": ["vendas", "receita", "ticket", "quantidade", "performance", "faturamento"],
            "filters": {
                "month_from": {
                    "type": "month",
                    "required": True,
                    "description": "Mês inicial do período de análise",
                    "format": "YYYY-MM"
                },
                "month_to": {
                    "type": "month",
                    "required": True,
                    "description": "Mês final do período de análise",
                    "format": "YYYY-MM"
                },
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": "Postos a analisar (pode ser múltiplo)",
                    "options": ["A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"],
                    "default": "ALL",
                    "groups": {
                        "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
                        "couto": ["C", "D", "J", "M", "P"]
                    }
                }
            },
            "metrics": [
                {
                    "name": "quantidade_vendas",
                    "label": "Quantidade de Vendas",
                    "type": "integer",
                    "description": "Número total de vendas realizadas no período"
                },
                {
                    "name": "receita_total",
                    "label": "Receita Total",
                    "type": "currency",
                    "description": "Receita total gerada pelas vendas (em reais)"
                },
                {
                    "name": "ticket_medio",
                    "label": "Ticket Médio",
                    "type": "currency",
                    "description": "Valor médio por venda (receita total / quantidade)"
                }
            ],
            "data_source": "SQL Server (fin_receita)",
            "refresh_frequency": "diária (madrugada)",
            "api_endpoints": {
                "data": "/json_consolidado/vendas.json",
                "metadata": "/api/kpis/metadata/vendas"
            },
            "examples": {
                "basic": "/kpi_vendas?month_from=2025-01&month_to=2025-03",
                "with_postos": "/kpi_vendas?month_from=2025-01&month_to=2025-03&postos=A,B,C",
                "group": "/kpi_vendas?month_from=2025-01&month_to=2025-03&postos=altamiro"
            },
            "manus_prompt_template": "Mostre as vendas de {month_from} a {month_to}. Analize a quantidade, receita total e ticket médio de {postos_desc}."
        },
        {
            "id": "mensalidades",
            "title": "KPI Mensalidades",
            "description": "Dashboard de análise de mensalidades e receita recorrente. Visualize quantidade de mensalidades, receita total, ticket médio mensal e evolução temporal. Dados agregáveis por posto ou grupo de postos.",
            "url": "/kpi_v2",
            "route": "kpi_v2.html",
            "category": "Financeiro",
            "icon": "fa-chart-line",
            "keywords": ["mensalidades", "receita", "recorrente", "performance", "assinatura"],
            "filters": {
                "month_from": {
                    "type": "month",
                    "required": True,
                    "description": "Mês inicial do período de análise",
                    "format": "YYYY-MM"
                },
                "month_to": {
                    "type": "month",
                    "required": True,
                    "description": "Mês final do período de análise",
                    "format": "YYYY-MM"
                },
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": "Postos a analisar (pode ser múltiplo)",
                    "options": ["A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"],
                    "default": "ALL",
                    "groups": {
                        "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
                        "couto": ["C", "D", "J", "M", "P"]
                    }
                }
            },
            "metrics": [
                {
                    "name": "quantidade_mensalidades",
                    "label": "Quantidade de Mensalidades",
                    "type": "integer",
                    "description": "Número total de mensalidades ativas no período"
                },
                {
                    "name": "receita_recorrente",
                    "label": "Receita Recorrente",
                    "type": "currency",
                    "description": "Receita total de mensalidades (em reais)"
                },
                {
                    "name": "ticket_medio_mensal",
                    "label": "Ticket Médio Mensal",
                    "type": "currency",
                    "description": "Valor médio por mensalidade (receita / quantidade)"
                }
            ],
            "data_source": "SQL Server (fin_receita com idcontatipo=5)",
            "refresh_frequency": "diária (madrugada)",
            "api_endpoints": {
                "data": "/json_consolidado/mensalidades.json",
                "metadata": "/api/kpis/metadata/mensalidades"
            },
            "examples": {
                "basic": "/kpi_v2?month_from=2025-01&month_to=2025-03",
                "with_postos": "/kpi_v2?month_from=2025-01&month_to=2025-03&postos=A,B,C",
                "group": "/kpi_v2?month_from=2025-01&month_to=2025-03&postos=couto"
            },
            "manus_prompt_template": "Mostre as mensalidades de {month_from} a {month_to}. Analize a quantidade, receita recorrente e ticket médio mensal de {postos_desc}."
        },
        {
            "id": "clientes",
            "title": "KPI Clientes",
            "description": "Dashboard consolidado de análise de base de clientes. Visualize total de clientes ativos, novos clientes, crescimento mensal e segmentação por grupos. Dados agregáveis por posto ou grupo de postos.",
            "url": "/clientes",
            "route": "clientes.html",
            "category": "Clientes",
            "icon": "fa-users",
            "keywords": ["clientes", "base", "crescimento", "atividade", "ativos", "novos"],
            "filters": {
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": "Postos a analisar (pode ser múltiplo)",
                    "options": ["A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"],
                    "default": "ALL",
                    "groups": {
                        "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
                        "couto": ["C", "D", "J", "M", "P"]
                    }
                }
            },
            "metrics": [
                {
                    "name": "total_clientes",
                    "label": "Total de Clientes",
                    "type": "integer",
                    "description": "Número total de clientes ativos na base"
                },
                {
                    "name": "clientes_novos",
                    "label": "Clientes Novos (Mês)",
                    "type": "integer",
                    "description": "Número de novos clientes admitidos no mês atual"
                },
                {
                    "name": "crescimento_mensal",
                    "label": "Crescimento Mensal",
                    "type": "percentage",
                    "description": "Taxa de crescimento da base em relação ao mês anterior"
                }
            ],
            "data_source": "SQL Server (Cad_Cliente)",
            "refresh_frequency": "diária (madrugada)",
            "api_endpoints": {
                "data": "/json_consolidado/clientes.json",
                "metadata": "/api/kpis/metadata/clientes"
            },
            "examples": {
                "basic": "/clientes",
                "with_postos": "/clientes?postos=A,B,C",
                "group": "/clientes?postos=altamiro"
            },
            "manus_prompt_template": "Mostre o dashboard de clientes. Analize a base de clientes, novos clientes e crescimento mensal de {postos_desc}."
        },
        {
            "id": "prescricoes",
            "title": "KPI Prescrições",
            "description": "Dashboard de análise de prescrições em aberto. Monitore prescrições vencidas, prescrições próximas do vencimento e risco de perda de acesso à saúde dos clientes. Dados agregáveis por posto ou grupo de postos.",
            "url": "/kpi_prescricao",
            "route": "KPI_prescricao.html",
            "category": "Operacional",
            "icon": "fa-prescription-bottle-alt",
            "keywords": ["prescricoes", "risco", "saude", "acesso", "vencidas", "aberto"],
            "filters": {
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": "Postos a analisar (pode ser múltiplo)",
                    "options": ["A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"],
                    "default": "ALL",
                    "groups": {
                        "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
                        "couto": ["C", "D", "J", "M", "P"]
                    }
                }
            },
            "metrics": [
                {
                    "name": "total_prescricoes",
                    "label": "Total de Prescrições",
                    "type": "integer",
                    "description": "Número total de prescrições em aberto"
                },
                {
                    "name": "prescricoes_vencidas",
                    "label": "Prescrições Vencidas",
                    "type": "integer",
                    "description": "Número de prescrições já vencidas"
                },
                {
                    "name": "clientes_risco",
                    "label": "Clientes em Risco",
                    "type": "integer",
                    "description": "Número de clientes com prescrições vencidas ou próximas do vencimento"
                }
            ],
            "data_source": "SQL Server (Prescrição + Cad_Cliente)",
            "refresh_frequency": "diária (madrugada)",
            "api_endpoints": {
                "data": "/json_consolidado/prescricoes.json",
                "metadata": "/api/kpis/metadata/prescricoes"
            },
            "examples": {
                "basic": "/kpi_prescricao",
                "with_postos": "/kpi_prescricao?postos=A,B,C",
                "group": "/kpi_prescricao?postos=couto"
            },
            "manus_prompt_template": "Mostre o status de prescrições. Analize prescrições vencidas, prescrições próximas do vencimento e clientes em risco de perda de acesso à saúde de {postos_desc}."
        },
        {
            "id": "consultas",
            "title": "KPI Consultas",
            "description": "Dashboard de monitoramento de consultas. Acompanhe consultas agendadas, realizadas, canceladas e suas distribuições por especialidade. Dados agregáveis por posto ou grupo de postos.",
            "url": "/kpi_consultas",
            "route": "kpi_consultas_status.html",
            "category": "Operacional",
            "icon": "fa-stethoscope",
            "keywords": ["consultas", "agendamento", "especialidade", "status", "realizadas", "canceladas"],
            "filters": {
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": "Postos a analisar (pode ser múltiplo)",
                    "options": ["A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"],
                    "default": "ALL",
                    "groups": {
                        "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
                        "couto": ["C", "D", "J", "M", "P"]
                    }
                }
            },
            "metrics": [
                {
                    "name": "consultas_agendadas",
                    "label": "Consultas Agendadas",
                    "type": "integer",
                    "description": "Número total de consultas agendadas"
                },
                {
                    "name": "consultas_realizadas",
                    "label": "Consultas Realizadas",
                    "type": "integer",
                    "description": "Número de consultas efetivamente realizadas"
                },
                {
                    "name": "consultas_canceladas",
                    "label": "Consultas Canceladas",
                    "type": "integer",
                    "description": "Número de consultas canceladas"
                },
                {
                    "name": "taxa_realizacao",
                    "label": "Taxa de Realização",
                    "type": "percentage",
                    "description": "Percentual de consultas realizadas (realizadas / agendadas)"
                }
            ],
            "data_source": "SQL Server (Consulta + Especialidade)",
            "refresh_frequency": "diária (madrugada)",
            "api_endpoints": {
                "data": "/json_consolidado/consultas.json",
                "metadata": "/api/kpis/metadata/consultas"
            },
            "examples": {
                "basic": "/kpi_consultas",
                "with_postos": "/kpi_consultas?postos=A,B,C",
                "group": "/kpi_consultas?postos=altamiro"
            },
            "manus_prompt_template": "Mostre o status de consultas. Analize consultas agendadas, realizadas, canceladas e taxa de realização por especialidade de {postos_desc}."
        }
    ]
}


def get_kpi_by_id(kpi_id: str) -> dict:
    """Retorna metadados de um KPI específico"""
    for kpi in KPI_MANIFEST["kpis"]:
        if kpi["id"] == kpi_id:
            return kpi
    return None


def get_kpis_by_category(category: str) -> list:
    """Retorna todos os KPIs de uma categoria"""
    return [kpi for kpi in KPI_MANIFEST["kpis"] if kpi.get("category") == category]


def search_kpis(query: str) -> list:
    """Busca KPIs por keywords ou title"""
    query_lower = query.lower()
    results = []
    for kpi in KPI_MANIFEST["kpis"]:
        if (query_lower in kpi["title"].lower() or
            query_lower in kpi["description"].lower() or
            any(query_lower in k.lower() for k in kpi.get("keywords", []))):
            results.append(kpi)
    return results


def get_manifest() -> dict:
    """Retorna o manifest completo"""
    return KPI_MANIFEST
