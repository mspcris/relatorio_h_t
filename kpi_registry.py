"""
KPI Registry — Catálogo centralizado de todos os KPIs
Permite descoberta automática e integração com sistemas externos (Manus, etc)
"""

KPI_MANIFEST = {
    "version": "1.2.0",
    "last_updated": "2026-04-19",
    "base_url": "https://kpi.camim.com.br",
    "kpis": [
        {
            "id": "receita_despesa",
            "title": "Receita x Despesa",
            "description": (
                "Dashboard financeiro consolidado de receita e despesa de todos os "
                "postos. Permite análise por período (mês único, intervalo, "
                "year-over-year), drilldown por plano principal / plano / tipo de "
                "conta / serviço / forma de pagamento, ranking de postos por "
                "variação, detecção de anomalias (média + desvio padrão) e "
                "composição. Exclui automaticamente movimentações de RETIRADA / "
                "CAMPINHO. Agregável por posto individual, grupo Altamiro, grupo "
                "Couto ou todos os postos."
            ),
            "url": "/kpi_receita_despesa.html",
            "route": "kpi_receita_despesa.html",
            "category": "Financeiro",
            "icon": "fa-scale-balanced",
            "priority": "alta",
            "keywords": [
                "receita", "despesa", "financeiro", "lucro", "margem",
                "custo", "gasto", "plano de contas", "plano principal",
                "tipo de conta", "forma de pagamento", "serviço", "rateio",
                "crescimento", "encolhimento", "year over year", "yoy",
                "mes anterior", "mom", "variacao", "anomalia", "alerta",
                "ranking", "drilldown", "composicao"
            ],
            "filters": {
                "grupo": {
                    "type": "single-select",
                    "required": True,
                    "description": (
                        "ANTES de responder, SEMPRE perguntar ao usuário: "
                        "'Todos os postos? Altamiro? Couto? Ou um posto "
                        "específico?' Só prosseguir após resposta."
                    ),
                    "options": ["todos", "altamiro", "couto", "especifico"],
                    "default": None,
                    "groups": {
                        "todos": ["A","B","C","D","G","I","J","M","N","P","R","X","Y"],
                        "altamiro": ["A","B","G","I","N","R","X","Y"],
                        "couto": ["C","D","J","M","P"]
                    }
                },
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": (
                        "Lista específica de postos (usar quando grupo=especifico "
                        "ou para sobrescrever o grupo). Aceita códigos A..Y "
                        "separados por vírgula."
                    ),
                    "options": ["A","B","C","D","G","I","J","M","N","P","R","X","Y"]
                },
                "mes": {
                    "type": "month",
                    "required": False,
                    "format": "YYYY-MM",
                    "description": "Mês único de análise (ex: 2026-03)."
                },
                "mes_ini": {
                    "type": "month",
                    "required": False,
                    "format": "YYYY-MM",
                    "description": "Início do intervalo (alternativa a mes)."
                },
                "mes_fim": {
                    "type": "month",
                    "required": False,
                    "format": "YYYY-MM",
                    "description": "Fim do intervalo (alternativa a mes)."
                },
                "dimensao_receita": {
                    "type": "single-select",
                    "required": False,
                    "options": ["tipo", "forma", "servico"],
                    "default": "tipo",
                    "description": (
                        "Dimensão do drilldown de RECEITA: "
                        "tipo (categoria da conta), forma (forma de pagamento), "
                        "servico (tipo de serviço lançado)."
                    )
                },
                "dimensao_despesa": {
                    "type": "single-select",
                    "required": False,
                    "options": ["plano_principal", "plano", "tipo"],
                    "default": "plano_principal",
                    "description": (
                        "Dimensão do drilldown de DESPESA: plano_principal "
                        "(grupo contábil macro), plano (subconta), tipo "
                        "(tipo específico da conta)."
                    )
                }
            },
            "metrics": [
                {"name": "receita_total", "label": "Receita Total", "type": "currency"},
                {"name": "despesa_total", "label": "Despesa Total", "type": "currency"},
                {"name": "saldo", "label": "Saldo (Receita − Despesa)", "type": "currency"},
                {"name": "margem", "label": "Margem %", "type": "percentage"},
                {"name": "var_mom_receita", "label": "Variação MoM Receita", "type": "percentage"},
                {"name": "var_mom_despesa", "label": "Variação MoM Despesa", "type": "percentage"},
                {"name": "var_yoy_receita", "label": "Variação YoY Receita", "type": "percentage"},
                {"name": "var_yoy_despesa", "label": "Variação YoY Despesa", "type": "percentage"}
            ],
            "data_source": (
                "SQL Server (Fin_Receita + Fin_Despesa + Fin_Plano + "
                "Fin_PlanoPrincipal + Fin_ContaTipo + Cad_ServicoTipo), "
                "consolidado em 6 JSONs em /json_consolidado/fin_*.json"
            ),
            "json_files": [
                "fin_receita_tipo.json",
                "fin_receita_forma.json",
                "fin_receita_lancamento.json",
                "fin_despesa_planodeprincipal.json",
                "fin_despesa_plano.json",
                "fin_despesa_tipo.json"
            ],
            "refresh_frequency": "diária (madrugada)",
            "retirada_rule": (
                "Exclui automaticamente linhas cujo PlanoPrincipal, plano ou "
                "tipo contenham 'RETIRADA' ou 'CAMPINHO' (mesma regra da página "
                "original). Usar /api/receita_despesa/resumo?incluir_retirada=true "
                "para trazê-las de volta."
            ),
            "api_endpoints": {
                "data_receita_tipo":    "/json_consolidado/fin_receita_tipo.json",
                "data_receita_forma":   "/json_consolidado/fin_receita_forma.json",
                "data_receita_lanc":    "/json_consolidado/fin_receita_lancamento.json",
                "data_despesa_pp":      "/json_consolidado/fin_despesa_planodeprincipal.json",
                "data_despesa_plano":   "/json_consolidado/fin_despesa_plano.json",
                "data_despesa_tipo":    "/json_consolidado/fin_despesa_tipo.json",
                "metadata":             "/api/kpis/metadata/receita_despesa",
                "contexto":             "/api/receita_despesa/contexto",
                "resumo":               "/api/receita_despesa/resumo",
                "serie":                "/api/receita_despesa/serie",
                "crescimento":          "/api/receita_despesa/crescimento",
                "ranking_postos":       "/api/receita_despesa/ranking_postos",
                "composicao":           "/api/receita_despesa/composicao",
                "composicao_multi":     "/api/receita_despesa/composicao_multi",
                "drilldown_variacao":   "/api/receita_despesa/drilldown_variacao",
                "drilldown_variacao_multi": "/api/receita_despesa/drilldown_variacao_multi",
                "posto_detalhe":        "/api/receita_despesa/posto_detalhe",
                "alertas":              "/api/receita_despesa/alertas",
                "analise_completa":     "/api/receita_despesa/analise_completa",
                "pergunta_assistida":   "/api/receita_despesa/pergunta_assistida"
            },
            "examples": {
                "contexto":
                    "/api/receita_despesa/contexto",
                "resumo_todos_mes":
                    "/api/receita_despesa/resumo?grupo=todos&mes=2026-03",
                "resumo_altamiro_intervalo":
                    "/api/receita_despesa/resumo?grupo=altamiro&mes_ini=2026-01&mes_fim=2026-03",
                "resumo_posto_especifico":
                    "/api/receita_despesa/resumo?postos=A&mes=2026-03",
                "serie_12m":
                    "/api/receita_despesa/serie?grupo=todos&mes_ini=2025-04&mes_fim=2026-03",
                "ranking_despesa_mom":
                    "/api/receita_despesa/ranking_postos?metrica=despesa&base=mes_anterior&mes=2026-03",
                "ranking_despesa_yoy":
                    "/api/receita_despesa/ranking_postos?metrica=despesa&base=ano_anterior&mes=2026-03",
                "composicao_despesa_pp":
                    "/api/receita_despesa/composicao?tipo=despesa&dimensao=plano_principal&grupo=todos&mes=2026-03",
                "composicao_receita_forma":
                    "/api/receita_despesa/composicao?tipo=receita&dimensao=forma&grupo=todos&mes=2026-03",
                "drilldown_despesa_posto":
                    "/api/receita_despesa/drilldown_variacao?tipo=despesa&postos=A&mes=2026-03&base=mes_anterior",
                "posto_detalhe":
                    "/api/receita_despesa/posto_detalhe?posto=A&mes=2026-03",
                "alertas":
                    "/api/receita_despesa/alertas?grupo=todos&mes=2026-03&janela=6",
                "analise_completa":
                    "/api/receita_despesa/analise_completa?grupo=altamiro&mes=2026-03",
                "pergunta_assistida":
                    "/api/receita_despesa/pergunta_assistida?q=qual+posto+subiu+o+custo+em+marco&grupo=todos&mes=2026-03"
            },
            "perguntas_que_responde": [
                "Qual posto aumentou o custo em março?",
                "Qual tipo de conta subiu neste posto?",
                "Estou crescendo ou encolhendo?",
                "O posto A está encolhendo?",
                "Altamiro cresceu mais que Couto no último trimestre?",
                "Qual é a maior despesa da rede este mês?",
                "Onde a margem está caindo?",
                "Comparando março/2026 com março/2025, o que piorou?"
            ],
            "instrucoes_para_agente": {
                "passo_1_perguntar_sempre": (
                    "ANTES de responder qualquer pergunta de receita/despesa, "
                    "SEMPRE perguntar: 'Você quer analisar todos os postos, "
                    "o grupo Altamiro, o grupo Couto, ou um posto específico?'"
                ),
                "passo_2_perguntar_se_necessario": (
                    "Se a pergunta do usuário não mencionar período, perguntar: "
                    "'Qual mês ou intervalo você quer analisar?'"
                ),
                "passo_3_endpoint_inicial": (
                    "Para pergunta livre em linguagem natural, chamar "
                    "/api/receita_despesa/pergunta_assistida?q=<pergunta>... "
                    "— ele roteia para o endpoint mais adequado."
                ),
                "passo_4_para_analise_profunda": (
                    "Chamar /api/receita_despesa/analise_completa — retorna um "
                    "pacote executivo pronto (resumo, ranking, composição, "
                    "variações MoM/YoY e alertas) em uma única chamada."
                )
            },
            "manus_prompt_template": (
                "Usuário perguntou: '{pergunta}'. PRIMEIRO confirme se a análise é "
                "para todos os postos, Altamiro, Couto ou posto específico. DEPOIS "
                "confirme o período (mês ou intervalo). ENTÃO chame "
                "/api/receita_despesa/pergunta_assistida ou /analise_completa com "
                "os filtros escolhidos."
            )
        },
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
        },
        {
            "id": "fin_despesas_raw",
            "title": "Despesas Financeiras (registro a registro)",
            "description": (
                "Acesso row-level aos pagamentos de despesa de todos os 13 postos. "
                "Replica incremental de vw_Fin_Despesa em Postgres RDS AWS, "
                "atualizada a cada 2 horas. Permite listar cada despesa individual "
                "com todos os campos (cliente, fornecedor, plano, conta, valor, "
                "datas, forma, corretor, medico, descricao, comentario, etc.) e "
                "tambem agregar por dimensao (conta, plano, fornecedor, mes, ...) "
                "para identificar qual conta especifica aumentou em um periodo. "
                "Filtros ETL fixos: [Valor pago] IS NOT NULL, idContaTipo <> 11, "
                "Data de pagamento >= 2020-01-01."
            ),
            "url": None,
            "route": None,
            "category": "Financeiro",
            "icon": "fa-receipt",
            "priority": "alta",
            "keywords": [
                "despesa", "pagamento", "conta", "fornecedor", "plano",
                "plano principal", "tipo de conta", "registro", "linha",
                "detalhamento", "drilldown", "row level", "registro a registro",
                "qual conta subiu", "qual conta aumentou", "qual fornecedor",
                "pagamentos individuais", "transacoes", "detalhe da despesa",
                "lancamento", "id_despesa"
            ],
            "filters": {
                "grupo": {
                    "type": "single-select",
                    "required": False,
                    "options": ["todos", "altamiro", "couto"],
                    "groups": {
                        "todos": ["A","B","C","D","G","I","J","M","N","P","R","X","Y"],
                        "altamiro": ["A","B","G","I","N","R","X","Y"],
                        "couto": ["C","D","J","M","P"]
                    }
                },
                "postos": {
                    "type": "multi-select",
                    "required": False,
                    "description": "CSV de postos (ex: 'A,B,C'). Sobrepoe grupo se ambos forem passados."
                },
                "data_ini": {"type": "date", "format": "YYYY-MM-DD"},
                "data_fim": {"type": "date", "format": "YYYY-MM-DD"},
                "mes":      {"type": "string", "format": "YYYY-MM", "description": "Atalho: data_pagamento naquele mes"},
                "cliente":  {"type": "like"},
                "tipo":     {"type": "like"},
                "plano":    {"type": "like"},
                "plano_principal": {"type": "like"},
                "fornecedor": {"type": "like"},
                "conta":    {"type": "like"},
                "corretor": {"type": "like"},
                "medico":   {"type": "like"},
                "forma":    {"type": "like"},
                "descricao": {"type": "like"},
                "min_valor": {"type": "float"},
                "max_valor": {"type": "float"},
                "limit":    {"type": "int", "min": 1, "max": 500, "default": 100, "only": "/despesas"},
                "order":    {
                    "type": "enum",
                    "options": ["data_pagamento_desc", "id_desc", "id_asc", "valor_desc"],
                    "default": "data_pagamento_desc",
                    "only": "/despesas"
                },
                "cursor":   {"type": "int", "description": "id_despesa do ultimo item da pagina anterior (usar junto com cursor_posto e order=id_desc|id_asc)"},
                "cursor_posto": {"type": "string", "description": "posto do ultimo item da pagina anterior"},
                "top":      {"type": "int", "min": 1, "max": 1000, "default": 100, "only": "/resumo"},
                "group_by": {
                    "type": "enum",
                    "options": ["conta", "plano", "plano_principal", "tipo", "fornecedor", "corretor", "medico", "forma", "posto", "mes", "cliente"],
                    "default": "conta",
                    "only": "/resumo"
                }
            },
            "data_source": "RDS AWS Postgres (relatorio_h_t.fin_despesa)",
            "refresh_frequency": "a cada 2 horas (ETL incremental por idDespesa)",
            "api_endpoints": {
                "listar":  "/api/fin/despesas",
                "resumo":  "/api/fin/despesas/resumo",
                "meta":    "/api/fin/despesas/meta",
                "metadata": "/api/kpis/metadata/fin_despesas_raw"
            },
            "examples": {
                "meta":
                    "/api/fin/despesas/meta",
                "ultimos_100_todos_postos":
                    "/api/fin/despesas?grupo=todos&order=data_pagamento_desc&limit=100",
                "qual_conta_subiu_em_marco":
                    "/api/fin/despesas/resumo?grupo=todos&mes=2026-03&group_by=conta&top=30",
                "comparar_conta_fevereiro_vs_marco":
                    "/api/fin/despesas/resumo?grupo=todos&mes=2026-03&group_by=conta — (e repetir com mes=2026-02 para comparar)",
                "fornecedores_que_mais_receberam_num_posto":
                    "/api/fin/despesas/resumo?postos=A&data_ini=2026-01-01&data_fim=2026-03-31&group_by=fornecedor&top=20",
                "plano_principal_posto_A_Q1":
                    "/api/fin/despesas/resumo?postos=A&data_ini=2026-01-01&data_fim=2026-03-31&group_by=plano_principal",
                "detalhes_de_um_fornecedor":
                    "/api/fin/despesas?fornecedor=NOME+DO+FORNECEDOR&order=data_pagamento_desc&limit=50",
                "pagamentos_acima_10k":
                    "/api/fin/despesas?min_valor=10000&order=valor_desc&limit=50",
                "filtro_por_conta_especifica":
                    "/api/fin/despesas?conta=ALUGUEL&order=data_pagamento_desc&limit=100"
            },
            "perguntas_que_responde": [
                "Qual conta subiu em marco/2026?",
                "Quais pagamentos especificos compoem o aumento dessa conta?",
                "Quem foi o fornecedor da maior despesa do mes?",
                "Me mostre todos os pagamentos ao fornecedor X no trimestre.",
                "Quais foram as 10 maiores despesas do posto A em marco?",
                "Quanto foi pago em aluguel ano passado?",
                "Ha alguma despesa acima de R$ 50 mil recente?",
                "Qual o detalhamento registro a registro do aumento em 'Material de escritorio'?"
            ],
            "instrucoes_para_agente": {
                "passo_1_identificar_dimensao": (
                    "Para pergunta do tipo 'qual X subiu', use /api/fin/despesas/resumo "
                    "com group_by apropriado (conta, plano, fornecedor, ...) e compare "
                    "dois meses (mes=YYYY-MM) lado a lado."
                ),
                "passo_2_drilldown_para_registros": (
                    "Depois de identificar a conta/fornecedor/plano que aumentou, "
                    "chamar /api/fin/despesas com os mesmos filtros (conta=..., "
                    "fornecedor=..., mes=YYYY-MM) para listar os pagamentos "
                    "individuais que explicam a variacao."
                ),
                "passo_3_paginacao": (
                    "Se has_more=true no retorno, continuar com order=id_desc + "
                    "cursor=<id_despesa> + cursor_posto=<posto> para paginar."
                ),
                "regra_de_postos": (
                    "Se o usuario nao especificar, perguntar: 'Todos os postos, "
                    "Altamiro, Couto ou posto especifico?'"
                )
            },
            "manus_prompt_template": (
                "Usuario perguntou: '{pergunta}'. PRIMEIRO chame "
                "/api/fin/despesas/resumo com group_by adequado para identificar a "
                "dimensao que variou. DEPOIS chame /api/fin/despesas com os filtros "
                "daquela dimensao para trazer os pagamentos registro a registro."
            )
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
