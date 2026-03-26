# 🚀 Teste de Performance - Fidelização

## Comando para Testar na VM

```bash
cd /opt/relatorio_h_t && source .venv/bin/activate && \
python3 export_fidelizacao.py --postos Y --from 2025-01 --to 2025-04
```

**O que faz:**
- Apenas o posto Y (menor carga)
- 3 meses (jan, fev, mar 2025) = 3 chamadas
- Saída detalhada com timings
- Log salvo em `logs/export_fidelizacao_*.log`

---

## Problema Identificado

A abordagem atual (iterar todos os meses × todos os postos) é cara:
- **60+ meses × 13 postos = 780+ chamadas à procedure por dia**
- Cada chamada: conexão + procedure + fetch + store
- Pode levar horas todos os dias
- Impacto direto no banco de dados

---

## Opções Estratégicas

### **Opção A: Snapshot de Dados**
Copiar tabelas inteiras do banco para local

**Pros:**
- Procedure roda uma única vez
- Depois tudo é local/rápido
- Sem impacto ao banco depois

**Cons:**
- Tabelas grandes (Cad_Cliente, fin_receita)
- Complexo transpor lógica da procedure para SQL local
- Inconsistência se banco atualizar

---

### **Opção B: Cache Inteligente** ⭐ (RECOMENDADO)
Guardar resultado da procedure + apenas atualizar últimos meses

**Pros:**
- Reduz chamadas: 780 → ~26/dia (2 meses × 13 postos)
- Simples de implementar
- Histórico completo no banco
- Tempo de cron: <2 minutos

**Cons:**
- Ainda faz vários calls/dia

---

### **Opção C: On-Demand com Worker**
Frontend com botão "Atualizar" + background worker

**Pros:**
- Zero impacto ao cron
- Usuário controla quando atualizar
- Mostra progresso em tempo real

**Cons:**
- Precisa infrastructure (Celery/RQ)
- Complexo

---

### **Opção D: Hybrid (MELHOR CUSTO-BENEFÍCIO)**
Cron atualiza **APENAS mês atual + anterior**, histórico é carregado uma vez

**Pros:**
- ✅ Tempo de cron: <1 minuto/dia
- ✅ Dados históricos completos
- ✅ Sempre "hoje" atualizado
- ✅ Simples de implementar
- ✅ Sem impacto ao banco

**Cons:**
- Precisa de snapshot inicial (roda uma única vez)

---

## Próximos Passos

1. **Roda o teste** na VM
2. **Me passa o output** (tempo de cada chamada)
3. **Você escolhe a estratégia**
4. **Eu implemento**

Estimado que cada chamada leve:
- Conexão: 100-500ms
- Procedure: 500ms - 5s (depende de quanto dado retorna)
- Fetch: 100-500ms
- Store: 100-500ms

**Total por chamada: 1-10 segundos**

Se for 5 seg × 26 calls/dia = ~2 minutos (tolerável)
Se for 5 seg × 780 calls/dia = ~65 minutos (intolerável)
