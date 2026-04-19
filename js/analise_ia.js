/**
 * Análise de IA — painel com respostas pré-computadas para perguntas
 * recorrentes dos diretores. Não é LLM; é pandas/SQL com resposta direta.
 *
 * Uso (em kpi_receita_despesa.html):
 *   <script src="/js/analise_ia.js"></script>
 *   <script>
 *     AnaliseIA.init({ kpi: 'receita_despesa', anchor: '#btnETLStatus' });
 *   </script>
 */
(function () {
  'use strict';

  var POSTO_NAMES = {
    A:'Anchieta', B:'Bangu', C:'Campinho', D:'Del Castilho',
    G:'Campo Grande', I:'Nova Iguaçu', J:'Jacarepaguá', M:'Madureira',
    N:'Nilópolis', P:'Rio das Pedras', R:'Realengo', X:'Xerém',
    Y:'Campo Grande Y'
  };

  var GEAR_ICON =
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" fill="currentColor">' +
    '<path d="M19.14 12.94a7.14 7.14 0 0 0 .06-.94 7.14 7.14 0 0 0-.06-.94l2.03-1.58a.5.5 0 0 0 .12-.64l-1.92-3.32a.5.5 0 0 0-.61-.22l-2.39.96a7.02 7.02 0 0 0-1.62-.94l-.36-2.54A.5.5 0 0 0 13.9 2h-3.84a.5.5 0 0 0-.49.42l-.36 2.54a7.02 7.02 0 0 0-1.62.94l-2.39-.96a.5.5 0 0 0-.61.22L2.67 8.48a.5.5 0 0 0 .12.64l2.03 1.58A7.14 7.14 0 0 0 4.76 12c0 .32.02.63.06.94l-2.03 1.58a.5.5 0 0 0-.12.64l1.92 3.32a.5.5 0 0 0 .61.22l2.39-.96c.5.38 1.04.7 1.62.94l.36 2.54a.5.5 0 0 0 .49.42h3.84a.5.5 0 0 0 .49-.42l.36-2.54a7.02 7.02 0 0 0 1.62-.94l2.39.96a.5.5 0 0 0 .61-.22l1.92-3.32a.5.5 0 0 0-.12-.64l-2.03-1.58ZM12 15.5a3.5 3.5 0 1 1 0-7 3.5 3.5 0 0 1 0 7Z"/>' +
    '</svg>';

  var state = {
    kpi: 'receita_despesa',
    mounted: false,
  };

  /* ═══════════════════ UI ═══════════════════ */

  function injectStyles() {
    if (document.getElementById('analise-ia-styles')) return;
    var css = [
      '#btnAnaliseIA{display:inline-flex;align-items:center;gap:4px;font-size:.8rem;padding:2px 8px;border-radius:4px;margin-left:.5rem}',
      '#btnAnaliseIA svg{transition:transform .4s ease}',
      '#btnAnaliseIA:hover svg{transform:rotate(60deg)}',
      '#aiaOverlay{position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:9998;display:none;align-items:flex-start;justify-content:center;padding:20px;overflow-y:auto}',
      '#aiaOverlay.open{display:flex}',
      '#aiaPanel{background:#fff;color:#222;border-radius:10px;width:min(1200px,96vw);min-height:60vh;box-shadow:0 20px 50px rgba(0,0,0,.4);display:flex;flex-direction:column;overflow:hidden}',
      '#aiaHeader{padding:14px 18px;border-bottom:1px solid #e4e7eb;display:flex;align-items:center;gap:10px;background:#f8f9fb}',
      '#aiaHeader h3{margin:0;font-size:1.05rem;font-weight:700;flex:1}',
      '#aiaHeader .sub{font-size:.78rem;color:#6b7280}',
      '#aiaClose{width:30px;height:30px;border:1px solid #d1d5db;background:#fff;border-radius:6px;cursor:pointer;font-size:16px;line-height:1;color:#374151}',
      '#aiaClose:hover{background:#f3f4f6}',
      '#aiaBody{padding:14px 18px;flex:1;overflow-y:auto;background:#fff}',
      '.aia-controls{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;padding:10px 12px;background:#f8f9fb;border:1px solid #e4e7eb;border-radius:8px;margin-bottom:14px}',
      '.aia-controls label{font-size:.72rem;color:#6b7280;margin:0 0 2px 0;display:block;font-weight:600;text-transform:uppercase;letter-spacing:.5px}',
      '.aia-controls select,.aia-controls input[type=number]{font-size:.85rem;padding:4px 8px;border:1px solid #d1d5db;border-radius:5px;background:#fff;min-width:110px}',
      '.aia-controls .btn-go{background:#16a34a;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:.85rem;font-weight:600;cursor:pointer}',
      '.aia-controls .btn-go:hover{background:#15803d}',
      '.aia-controls .btn-go:disabled{background:#9ca3af;cursor:not-allowed}',
      '.aia-section-title{font-size:.95rem;font-weight:700;color:#111827;margin:6px 0 10px;padding-bottom:6px;border-bottom:2px solid #16a34a}',
      '.aia-meta{font-size:.78rem;color:#6b7280;margin-bottom:10px}',
      '.aia-posto-card{border:1px solid #e4e7eb;border-radius:8px;margin-bottom:12px;overflow:hidden;background:#fff}',
      '.aia-posto-head{padding:8px 12px;background:linear-gradient(90deg,#f8f9fb,#fff);border-bottom:1px solid #e4e7eb;display:flex;align-items:center;gap:10px;cursor:pointer;user-select:none}',
      '.aia-posto-head:hover{background:#f1f5f9}',
      '.aia-posto-letra{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:5px;background:#16324f;color:#fff;font-weight:700;font-size:.85rem}',
      '.aia-posto-nome{font-weight:600;color:#111827;font-size:.92rem;flex:1}',
      '.aia-posto-stats{font-size:.75rem;color:#6b7280}',
      '.aia-posto-caret{color:#9ca3af;transition:transform .2s}',
      '.aia-posto-card.collapsed .aia-posto-caret{transform:rotate(-90deg)}',
      '.aia-posto-card.collapsed .aia-posto-body{display:none}',
      '.aia-posto-body{padding:10px 12px;display:grid;grid-template-columns:1fr 1fr;gap:14px}',
      '@media(max-width:720px){.aia-posto-body{grid-template-columns:1fr}}',
      '.aia-mini-title{font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin:0 0 6px}',
      '.aia-mini-title.up{color:#b91c1c}',
      '.aia-mini-title.down{color:#047857}',
      '.aia-table{width:100%;border-collapse:collapse;font-size:.82rem}',
      '.aia-table th{text-align:left;padding:4px 6px;border-bottom:1px solid #e4e7eb;font-weight:600;color:#6b7280;font-size:.72rem;text-transform:uppercase}',
      '.aia-table th.num,.aia-table td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}',
      '.aia-table td{padding:4px 6px;border-bottom:1px solid #f3f4f6}',
      '.aia-table tr:last-child td{border-bottom:none}',
      '.aia-table .grupo{color:#111827;max-width:360px}',
      '.aia-table .grupo .tipo{font-weight:600;color:#111827;display:block;line-height:1.25;word-break:break-word}',
      '.aia-table .grupo .path{font-size:.68rem;color:#9ca3af;display:block;margin-top:1px;line-height:1.15;word-break:break-word}',
      '.aia-table .delta-up{color:#b91c1c;font-weight:600}',
      '.aia-table .delta-down{color:#047857;font-weight:600}',
      '.aia-empty{padding:10px;color:#9ca3af;font-size:.8rem;font-style:italic}',
      '.aia-loading{display:flex;align-items:center;gap:10px;padding:20px;color:#6b7280;font-size:.9rem}',
      '.aia-loading .spin{width:16px;height:16px;border:2px solid #e4e7eb;border-top-color:#16a34a;border-radius:50%;animation:aiaSpin .8s linear infinite}',
      '@keyframes aiaSpin{to{transform:rotate(360deg)}}',
      '.aia-error{padding:12px;background:#fee2e2;color:#991b1b;border-radius:6px;font-size:.85rem}',
    ].join('\n');
    var s = document.createElement('style');
    s.id = 'analise-ia-styles';
    s.textContent = css;
    document.head.appendChild(s);
  }

  function createButton() {
    var btn = document.createElement('button');
    btn.id = 'btnAnaliseIA';
    btn.className = 'btn btn-sm btn-outline-secondary';
    btn.title = 'Análise de IA';
    btn.innerHTML = GEAR_ICON + ' <span class="d-none d-md-inline">Análise de IA</span>';
    btn.addEventListener('click', openPanel);
    return btn;
  }

  function createOverlay() {
    var ov = document.createElement('div');
    ov.id = 'aiaOverlay';
    ov.innerHTML =
      '<div id="aiaPanel" role="dialog" aria-label="Análise de IA">' +
        '<div id="aiaHeader">' +
          '<h3>Análise de IA</h3>' +
          '<span class="sub">Respostas pré-computadas sobre os dados do KPI</span>' +
          '<button id="aiaClose" title="Fechar">✕</button>' +
        '</div>' +
        '<div id="aiaBody"></div>' +
      '</div>';
    ov.addEventListener('click', function (e) {
      if (e.target === ov) closePanel();
    });
    document.body.appendChild(ov);
    document.getElementById('aiaClose').addEventListener('click', closePanel);
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && ov.classList.contains('open')) closePanel();
    });
    return ov;
  }

  function openPanel() {
    document.getElementById('aiaOverlay').classList.add('open');
    renderBody();
  }

  function closePanel() {
    document.getElementById('aiaOverlay').classList.remove('open');
  }

  /* ═══════════════════ LEITURA DE FILTROS DA PÁGINA ═══════════════════ */

  function lerFiltrosPagina() {
    // postos ativos nas abas
    var activeLinks = document.querySelectorAll('#postoTabs .nav-link.active[data-posto]');
    var postos = [].map.call(activeLinks, function (a) { return a.dataset.posto; })
      .filter(function (p) { return p && p !== 'ALL'; });

    // Meses — selects "Análise de variação entre meses" (convenção invertida da página)
    // selVarMesComp visualmente = "Mês de referência" (mais recente)
    // selVarMesRef visualmente = "Comparar com" (base)
    var mesRef = (document.getElementById('selVarMesComp') || {}).value || '';
    var mesComp = (document.getElementById('selVarMesRef') || {}).value || '';

    var retirada = !!(document.getElementById('chkRetirada') || {}).checked;

    return {
      postos: postos,
      mesRef: mesRef,
      mesComp: mesComp,
      retirada: retirada,
    };
  }

  function coletarTodosMeses() {
    // Lista de meses disponível — pega do próprio select existente
    var sel = document.getElementById('selVarMesComp');
    if (!sel) return [];
    return [].map.call(sel.options, function (o) { return o.value; }).filter(Boolean);
  }

  /* ═══════════════════ FORMATAÇÃO ═══════════════════ */

  function fmtBRL(v) {
    var n = Number(v) || 0;
    return n.toLocaleString('pt-BR', {
      style: 'currency', currency: 'BRL',
      minimumFractionDigits: 2, maximumFractionDigits: 2,
    });
  }

  function fmtPct(v) {
    if (v === null || v === undefined || isNaN(v)) return '—';
    var n = Number(v);
    var sign = n > 0 ? '+' : '';
    return sign + n.toFixed(1).replace('.', ',') + '%';
  }

  function fmtMesLabel(m) {
    if (!m || m.length < 7) return m;
    var nomes = { '01':'jan','02':'fev','03':'mar','04':'abr','05':'mai','06':'jun',
                  '07':'jul','08':'ago','09':'set','10':'out','11':'nov','12':'dez' };
    return (nomes[m.slice(5, 7)] || m.slice(5, 7)) + '-' + m.slice(2, 4);
  }

  /* ═══════════════════ RENDERIZAÇÃO ═══════════════════ */

  function renderBody() {
    var body = document.getElementById('aiaBody');
    var f = lerFiltrosPagina();
    var meses = coletarTodosMeses();

    body.innerHTML =
      '<div class="aia-section-title">Top 10 variações por tipo — posto a posto</div>' +
      '<div class="aia-meta">Para cada posto, os 10 tipos de despesa que mais aumentaram e os 10 que mais reduziram entre os dois meses selecionados.</div>' +
      renderControls(f, meses) +
      '<div id="aiaResultado"></div>';

    document.getElementById('aiaGo').addEventListener('click', executar);

    // Já dispara a primeira análise automaticamente
    executar();
  }

  function renderControls(f, meses) {
    var optsMes = meses.map(function (m) {
      return '<option value="' + m + '">' + fmtMesLabel(m) + '</option>';
    }).join('');

    return (
      '<div class="aia-controls">' +
        '<div><label>Mês referência</label>' +
          '<select id="aiaMesRef">' + optsMes + '</select></div>' +
        '<div><label>Mês comparação</label>' +
          '<select id="aiaMesComp">' + optsMes + '</select></div>' +
        '<div><label>Top N</label>' +
          '<input id="aiaTop" type="number" min="1" max="50" value="10" style="width:70px"></div>' +
        '<div><label>Postos</label>' +
          '<div style="font-size:.85rem;color:#111827;padding:6px 0">' +
            (f.postos.length ? f.postos.join(', ') : 'todos') +
          '</div></div>' +
        '<div><label>Retirada</label>' +
          '<div style="font-size:.85rem;color:#111827;padding:6px 0">' +
            (f.retirada ? 'incluída' : 'excluída') +
          '</div></div>' +
        '<button id="aiaGo" class="btn-go">Atualizar</button>' +
      '</div>' +
      (function () {
        // Pré-seleciona os valores atuais após inserir no DOM
        setTimeout(function () {
          if (f.mesRef) document.getElementById('aiaMesRef').value = f.mesRef;
          if (f.mesComp) document.getElementById('aiaMesComp').value = f.mesComp;
        }, 0);
        return '';
      })()
    );
  }

  function executar() {
    var res = document.getElementById('aiaResultado');
    var f = lerFiltrosPagina();
    var mesRef = document.getElementById('aiaMesRef').value;
    var mesComp = document.getElementById('aiaMesComp').value;
    var top = parseInt(document.getElementById('aiaTop').value, 10) || 10;

    if (!mesRef || !mesComp) {
      res.innerHTML = '<div class="aia-error">Selecione os dois meses.</div>';
      return;
    }
    if (mesRef === mesComp) {
      res.innerHTML = '<div class="aia-error">Os meses precisam ser diferentes.</div>';
      return;
    }

    res.innerHTML =
      '<div class="aia-loading"><span class="spin"></span>' +
      'Calculando variações por posto (' + fmtMesLabel(mesRef) + ' × ' + fmtMesLabel(mesComp) + ')…</div>';

    // Monta parâmetro postos: se nada selecionado na página, manda "todos"
    var paramPostos = f.postos.length ? f.postos.join(',') : 'todos';

    var params = new URLSearchParams({
      tipo: 'despesa',
      dimensao: 'tipo',
      postos: paramPostos,
      mes_ref: mesRef,
      mes_comp: mesComp,
      top: String(top),
      retirada: f.retirada ? 'true' : 'false',
    });

    fetch('/api/receita_despesa/drilldown_variacao_multi?' + params.toString(), {
      credentials: 'same-origin',
      cache: 'no-store',
    })
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        if (!data.ok) throw new Error(data.error || 'resposta inválida');
        renderResultado(res, data, mesRef, mesComp, top);
      })
      .catch(function (err) {
        res.innerHTML = '<div class="aia-error">Falha ao consultar: ' + (err.message || err) + '</div>';
      });
  }

  function renderResultado(container, data, mesRef, mesComp, top) {
    var porPosto = data.por_posto || {};
    var letras = Object.keys(porPosto).sort();

    if (!letras.length) {
      container.innerHTML = '<div class="aia-empty">Nenhum posto retornado.</div>';
      return;
    }

    var lblRef = fmtMesLabel(mesRef);
    var lblComp = fmtMesLabel(mesComp);

    var html = letras.map(function (p) {
      return renderPostoCard(p, porPosto[p], lblRef, lblComp, top);
    }).join('');

    container.innerHTML = html;

    // wire do toggle
    container.querySelectorAll('.aia-posto-head').forEach(function (head) {
      head.addEventListener('click', function () {
        head.parentNode.classList.toggle('collapsed');
      });
    });
  }

  function renderPostoCard(letra, dados, lblRef, lblComp, top) {
    var nome = POSTO_NAMES[letra] || letra;

    if (!dados || !dados.ok) {
      return (
        '<div class="aia-posto-card">' +
          '<div class="aia-posto-head">' +
            '<span class="aia-posto-letra">' + letra + '</span>' +
            '<span class="aia-posto-nome">' + nome + '</span>' +
            '<span class="aia-posto-stats" style="color:#b91c1c">' +
              'sem dados: ' + ((dados && dados.motivo) || 'desconhecido') + '</span>' +
          '</div>' +
        '</div>'
      );
    }

    var aum = dados.top_aumentou || [];
    var red = dados.top_diminuiu || [];

    return (
      '<div class="aia-posto-card">' +
        '<div class="aia-posto-head">' +
          '<span class="aia-posto-letra">' + letra + '</span>' +
          '<span class="aia-posto-nome">' + nome + '</span>' +
          '<span class="aia-posto-stats">' +
            aum.length + ' aumentos • ' + red.length + ' reduções • ' +
            (dados.total_itens || 0) + ' itens no total' +
          '</span>' +
          '<span class="aia-posto-caret">▾</span>' +
        '</div>' +
        '<div class="aia-posto-body">' +
          '<div>' +
            '<h6 class="aia-mini-title up">▲ Top ' + top + ' aumentos</h6>' +
            renderTabelaItens(aum, lblRef, lblComp, 'up') +
          '</div>' +
          '<div>' +
            '<h6 class="aia-mini-title down">▼ Top ' + top + ' reduções</h6>' +
            renderTabelaItens(red, lblRef, lblComp, 'down') +
          '</div>' +
        '</div>' +
      '</div>'
    );
  }

  function renderTabelaItens(itens, lblRef, lblComp, dir) {
    if (!itens || !itens.length) {
      return '<div class="aia-empty">Sem itens.</div>';
    }

    var rows = itens.map(function (i) {
      var delta = Number(i.delta_abs) || 0;
      var cls = delta > 0 ? 'delta-up' : (delta < 0 ? 'delta-down' : '');
      var sinal = delta > 0 ? '+' : '';
      var partes = String(i.grupo || '').split(' / ');
      var tipo = partes.length > 1 ? partes[partes.length - 1] : (partes[0] || '');
      var path = partes.length > 1 ? partes.slice(0, -1).join(' › ') : '';
      var grupoHtml =
        '<span class="tipo">' + escapeHtml(tipo) + '</span>' +
        (path ? '<span class="path">' + escapeHtml(path) + '</span>' : '');
      return (
        '<tr>' +
          '<td class="grupo" title="' + escapeHtml(i.grupo || '') + '">' + grupoHtml + '</td>' +
          '<td class="num">' + fmtBRL(i.valor_mes_ref) + '</td>' +
          '<td class="num">' + fmtBRL(i.valor_mes_comp) + '</td>' +
          '<td class="num ' + cls + '">' + sinal + fmtBRL(delta) + '</td>' +
          '<td class="num ' + cls + '">' + fmtPct(i.delta_pct) + '</td>' +
        '</tr>'
      );
    }).join('');

    return (
      '<table class="aia-table">' +
        '<thead><tr>' +
          '<th>Tipo</th>' +
          '<th class="num">' + lblRef + '</th>' +
          '<th class="num">' + lblComp + '</th>' +
          '<th class="num">Δ R$</th>' +
          '<th class="num">Δ %</th>' +
        '</tr></thead>' +
        '<tbody>' + rows + '</tbody>' +
      '</table>'
    );
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /* ═══════════════════ INIT ═══════════════════ */

  function init(opts) {
    if (state.mounted) return;
    opts = opts || {};
    state.kpi = opts.kpi || 'receita_despesa';

    injectStyles();
    createOverlay();

    // Insere o botão após o âncora (ETL). Retry porque o botão ETL pode
    // ser criado depois do DOMContentLoaded.
    var anchor = opts.anchor || '#btnETLStatus';
    var tries = 0;
    var timer = setInterval(function () {
      tries++;
      var a = document.querySelector(anchor);
      if (a && !document.getElementById('btnAnaliseIA')) {
        var btn = createButton();
        a.parentNode.insertBefore(btn, a.nextSibling);
        state.mounted = true;
        clearInterval(timer);
      } else if (tries > 40) {
        // fallback: encaixa ao lado do ia-anchor
        var ia = document.getElementById('ia-anchor');
        if (ia && !document.getElementById('btnAnaliseIA')) {
          var btn2 = createButton();
          ia.parentNode.insertBefore(btn2, ia);
          state.mounted = true;
        }
        clearInterval(timer);
      }
    }, 200);
  }

  window.AnaliseIA = { init: init };
})();
