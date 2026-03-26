# 🎨 Design System Implementation Guide

## O que foi implementado

✅ **CSS Design System** (`css/style_kpi.css`)
- Cores CAMIM: verde primário `#00dc64`, verde escuro `#006633`
- Inspirado na página de login (auth.camim.com.br)
- Apple-like: cards brancos, bordas suaves, sombras discretas, espaçamento generoso
- Variáveis CSS para cores, espaçamento, bordas e sombras
- Mobile responsivo: stacked layout em < 768px

✅ **Calendar Picker Module** (`js/calendar-picker.js`)
- Comportamento padronizado em TODAS as páginas
- Abre no clique do botão (NÃO no focus do input)
- Suporta 2 modos: `'month'` (MM/YYYY) e `'day'` (DD/MM/YYYY)
- Design visual integrado com o novo system
- Reutilizável em qualquer página

## Como usar (Para Codex)

### Passo 1: Importar o módulo

Adicione em `<head>` (logo após `<script src="/js/chat.js">...`):

```html
<script src="/js/calendar-picker.js?v=20260326"></script>
```

### Passo 2: Adaptar o HTML dos filtros

**Padrão NOVO para TODAS as páginas:**

```html
<div class="card">
  <div class="filter-bar">
    <!-- Bloco 1: From Date -->
    <div class="filter-group">
      <label class="filter-label">De</label>
      <div style="display: flex; gap: 6px;">
        <input id="from1" type="text" class="form-control form-control-sm"
               placeholder="MM/YYYY" maxlength="7" inputmode="numeric" autocomplete="off" />
        <button id="btnCalendarFrom1" class="btn btn-sm" title="Abrir calendário">📅</button>
      </div>
    </div>

    <!-- Bloco 2: To Date -->
    <div class="filter-group">
      <label class="filter-label">Até</label>
      <div style="display: flex; gap: 6px;">
        <input id="to1" type="text" class="form-control form-control-sm"
               placeholder="MM/YYYY" maxlength="7" inputmode="numeric" autocomplete="off" />
        <button id="btnCalendarTo1" class="btn btn-sm" title="Abrir calendário">📅</button>
      </div>
    </div>

    <!-- Bloco 3: Ações -->
    <div class="filter-actions">
      <button id="btnFiltrar1" class="btn btn-sm btn-primary">🔍 Filtrar</button>
      <button id="reset1" class="btn btn-sm btn-outline-secondary">Reset</button>
      <button id="btnMesAtual1" class="btn btn-sm btn-outline-info">Mês Atual</button>
    </div>

    <!-- Bloco 4: Postos (opcional, flex-grow-1) -->
    <div class="flex-grow-1">
      <label class="filter-label">Postos</label>
      <ul id="postoTabs1" class="nav nav-pills scroll-tabs"></ul>
    </div>
  </div>
</div>
```

### Passo 3: Inicializar no JavaScript

**Remover:**
- Qualquer event listener `'focus'` nos inputs
- A função `mostrarCalendario()` antiga (ou manter se usada em outro contexto)

**Adicionar** (ao final do `DOMContentLoaded` ou logo após inicializar dados):

```javascript
// Inicializar calendar pickers
initCalendarPicker('#from1', '#btnCalendarFrom1', 'month');
initCalendarPicker('#to1', '#btnCalendarTo1', 'month');
initCalendarPicker('#from2', '#btnCalendarFrom2', 'month');  // Se houver 2º bloco
initCalendarPicker('#to2', '#btnCalendarTo2', 'month');

// Manter os listeners de botões (Filtrar, Reset, Mês Atual)
// ... código existente ...
```

### Passo 4: Para páginas com filtro POR DIA (dia-a-dia)

Use `mode: 'day'`:

```javascript
initCalendarPicker('#fromD', '#btnCalendarFromD', 'day');
initCalendarPicker('#toD', '#btnCalendarToD', 'day');
```

HTML similar, mas com placeholder diferente:

```html
<input id="fromD" type="text" placeholder="DD/MM/YYYY" maxlength="10" ... />
```

## Páginas a atualizar (ordem de prioridade)

### ALTA PRIORIDADE (quebradas atualmente)

1. **kpi_v2.html** ← COMEÇAR AQUI (já tem import)
2. **kpi_alimentacao.html** (range duplo)
3. **kpi_medicos.html** (range duplo)
4. **kpi_vendas.html** (range duplo)
5. **ctrlq_relatorio.html** (range duplo)

### MÉDIA PRIORIDADE

6. **kpi_fidelizacao_cliente.html** (mês único)
7. **kpi_metas_vendas_mensalidades.html** (mês único)
8. **kpi_notas_rps.html** (dia-a-dia)
9. **kpi_receita_despesa.html** (range mês)
10. **kpi_receita_despesa_rateio.html** (range mês)
11. **kpi_liberty.html** (range mês)

### BAIXA PRIORIDADE (especiais)

12. **KPI_prescricao.html** (dia-a-dia, 2 inputs)
13. **kpi_consultas_status.html** (dia-a-dia, 2 inputs)
14. **kpi_governo.html** (jQuery, range mês)
15. **kpi_clientes.html** (SEM FILTRO DE DATA - NÃO ALTERAR)

## Checklist de verificação

Para cada página atualizada, verificar:

- [ ] Import do `calendar-picker.js` adicionado
- [ ] HTML dos filtros segue novo padrão (filter-group + buttons separados)
- [ ] `initCalendarPicker()` chamado para cada input
- [ ] Nenhum event listener `'focus'` nos inputs
- [ ] Botões Filtrar/Reset/Mês Atual funcionando
- [ ] Calendar abre ao clicar no botão 📅
- [ ] Input permite digitação manual (com máscara)
- [ ] Mobile responsivo (botões empilhados em <768px)
- [ ] Dados carregam após clicar "Filtrar"

## Testes Manuais

Após cada página:

1. **Desktop**
   - [ ] Clicar em 📅 → abre calendar
   - [ ] Mudar mês/ano → cores corretas
   - [ ] Clicar OK → valor preenchido
   - [ ] Digitar manualmente → máscara funciona
   - [ ] Clicar "Filtrar" → dados carregam
   - [ ] Clicar "Mês Atual" → preenchido com mês atual
   - [ ] Clicar "Reset" → volta ao padrão

2. **Mobile (375px width)**
   - [ ] Botões em uma linha (não quebrados)
   - [ ] Calendar modal legível
   - [ ] Scroll funcionando
   - [ ] Filtros legíveis

## Troubleshooting

**Calendar não abre:**
- Verificar se `calendar-picker.js` está importado
- Verificar se `initCalendarPicker()` está sendo chamado
- Inspecionar console para erros

**Valores não salvam:**
- Remover `addEventListener('change')` dos inputs
- Adicionar event listener ao botão "Filtrar" ao invés

**Mobile quebrado:**
- Verificar se `.filter-bar` tem `flex-wrap: wrap`
- Verificar `@media (max-width: 768px)` em `style_kpi.css`

## Design Visual

Cores padrão (em `style_kpi.css`):

```css
--camim-green-bright: #00dc64;
--camim-green-dark: #006633;
--apple-bg-primary: #f8f9fa;
--apple-bg-secondary: #ffffff;
--apple-border: #e0e0e0;
```

Nunca alterar cores direto no HTML - usar variáveis CSS!

---

**Versão:** 2026-03-26
**Autor:** Claude Code
**Status:** Pronto para implementação em todas as 15 páginas KPI
