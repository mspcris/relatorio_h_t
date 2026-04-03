/**
 * ETL Status Widget — botão + painel toggle com status de atualização dos JSONs.
 *
 * Uso:
 *   <script src="/js/etl_status.js"></script>
 *   <script>
 *     ETLStatus.init({
 *       etlScript: 'export_governanca.py',
 *       sources: [
 *         { label: 'Consolidado Mensal', url: '/json_consolidado/consolidado_mensal.json' },
 *       ],
 *       anchor: '#ia-anchor',      // opcional, default '#ia-anchor'
 *       staleHours: 6,             // opcional, default 6
 *     });
 *   </script>
 */
(function () {
  'use strict';

  var STALE_DEFAULT = 6; // horas

  function fmtDate(d) {
    return d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
  }

  function fmtAge(ms) {
    var h = Math.floor(ms / 3600000);
    if (h >= 24) {
      var d = Math.floor(h / 24);
      return d + 'd ' + (h % 24) + 'h';
    }
    if (h > 0) return h + 'h ' + Math.floor((ms % 3600000) / 60000) + 'min';
    var m = Math.floor(ms / 60000);
    return m + 'min';
  }

  /* ── SVG inline do ícone ETL (database + engrenagem) ── */
  var ETL_ICON = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" fill="currentColor">' +
    '<ellipse cx="12" cy="6" rx="8" ry="3" opacity=".9"/>' +
    '<path d="M4 6v4c0 1.66 3.58 3 8 3s8-1.34 8-3V6" opacity=".7"/>' +
    '<path d="M4 10v4c0 1.66 3.58 3 8 3s8-1.34 8-3v-4" opacity=".5"/>' +
    '<circle cx="18" cy="18" r="5" fill="#fff" stroke="currentColor" stroke-width="1.5"/>' +
    '<path d="M18 15.5v1.2l.9.5m-.9-.2a1.5 1.5 0 1 0 1.5 1.5" fill="none" stroke="currentColor" stroke-width="1" stroke-linecap="round"/>' +
    '</svg>';

  function createButton() {
    var btn = document.createElement('button');
    btn.id = 'btnETLStatus';
    btn.className = 'btn btn-sm btn-outline-secondary ml-2';
    btn.title = 'Status do ETL';
    btn.style.cssText = 'display:inline-flex;align-items:center;gap:4px;font-size:.8rem;padding:2px 8px;border-radius:4px;';
    btn.innerHTML = ETL_ICON + ' <span class="d-none d-md-inline">ETL</span>';
    return btn;
  }

  function createPanel() {
    var panel = document.createElement('div');
    panel.id = 'etlStatusPanel';
    panel.style.cssText = 'display:none;font-size:.82rem;padding:8px 12px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:6px;margin-bottom:10px;';
    return panel;
  }

  function statusDot(ok) {
    return '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px;background:' +
      (ok ? '#28a745' : '#dc3545') + ';"></span>';
  }

  async function fetchStatus(sources) {
    return Promise.all(sources.map(async function (s) {
      try {
        var r = await fetch(s.url, { method: 'HEAD', cache: 'no-store' });
        if (!r.ok) return { label: s.label, date: null, ok: false };
        var lm = r.headers.get('Last-Modified');
        return { label: s.label, date: lm ? new Date(lm) : null, ok: true };
      } catch (e) {
        return { label: s.label, date: null, ok: false };
      }
    }));
  }

  function renderPanel(panel, results, etlScript, staleHours) {
    var now = Date.now();
    var staleMs = staleHours * 3600000;
    var html = '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' +
      '<strong style="font-size:.85rem;">Status ETL</strong>' +
      '<span style="color:#6b7280;font-size:.78rem;">(' + etlScript + ')</span></div>';
    html += '<div style="display:flex;flex-wrap:wrap;gap:8px 18px;">';
    for (var i = 0; i < results.length; i++) {
      var r = results[i];
      if (!r.date) {
        html += '<span>' + statusDot(false) + r.label + ': <span style="color:#dc3545;font-weight:600;">sem dados</span></span>';
      } else {
        var age = now - r.date.getTime();
        var isStale = age > staleMs;
        var color = isStale ? '#dc3545' : '#28a745';
        var weight = isStale ? 'font-weight:600;' : '';
        html += '<span>' + statusDot(!isStale) + r.label + ': ' +
          '<span style="color:' + color + ';' + weight + '">' +
          fmtDate(r.date) + ' (' + fmtAge(age) + ' atrás)' +
          (isStale ? ' — possível falha' : '') +
          '</span></span>';
      }
    }
    html += '</div>';
    panel.innerHTML = html;
  }

  window.ETLStatus = {
    init: function (cfg) {
      if (!cfg || !cfg.sources || !cfg.sources.length) return;

      var anchorSel = cfg.anchor || '#ia-anchor';
      var staleHours = cfg.staleHours || STALE_DEFAULT;
      var etlScript = cfg.etlScript || 'ETL';

      /* espera DOM pronto */
      function setup() {
        var anchor = document.querySelector(anchorSel);
        if (!anchor) return;

        /* ── Botão na navbar ── */
        var btn = createButton();
        anchor.parentNode.insertBefore(btn, anchor);

        /* ── Painel (inserido no início do <section class="content">) ── */
        var panel = createPanel();
        var content = document.querySelector('section.content') || document.querySelector('.content-wrapper') || anchor.closest('nav')?.nextElementSibling;
        if (content) {
          content.insertBefore(panel, content.firstChild);
        } else {
          anchor.closest('nav')?.parentNode.appendChild(panel);
        }

        var loaded = false;
        btn.addEventListener('click', async function () {
          if (panel.style.display === 'none') {
            panel.style.display = 'block';
            if (!loaded) {
              panel.innerHTML = '<span style="color:#6b7280;"><i class="fas fa-spinner fa-spin"></i> Verificando...</span>';
              var results = await fetchStatus(cfg.sources);
              renderPanel(panel, results, etlScript, staleHours);
              loaded = true;
            }
          } else {
            panel.style.display = 'none';
          }
        });
      }

      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', setup);
      } else {
        setup();
      }
    }
  };
})();
