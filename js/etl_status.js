/**
 * ETL Status Widget — botão + painel toggle com status POR POSTO.
 *
 * Uso:
 *   <script src="/js/etl_status.js"></script>
 *   <script>
 *     ETLStatus.init({
 *       metaUrl: '/json_consolidado/_etl_meta_export_governanca.json',
 *       anchor: '#ia-anchor',      // opcional, default '#ia-anchor'
 *       staleHours: 6,             // opcional, default 6
 *     });
 *   </script>
 *
 * Formato do JSON de metadata (gerado por etl_meta.py):
 *   { script, started_at, finished_at, postos: { A: {status,at,msg?}, ... } }
 */
(function () {
  'use strict';

  var STALE_DEFAULT = 6;

  /* ── Nomes legíveis dos postos ── */
  var POSTO_NAMES = {
    A:'Anchieta', B:'Boa Viagem', C:'Couto Pereira', D:'Derby',
    G:'Graças', I:'Ilha do Leite', J:'Jaqueira', M:'Madalena',
    N:'Nações Unidas', P:'Piedade', R:'Rosarinho', X:'Espinheiro',
    Y:'Tamarineira'
  };

  function fmtDate(d) {
    return d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
  }

  function fmtAge(ms) {
    var h = Math.floor(ms / 3600000);
    if (h >= 24) return Math.floor(h / 24) + 'd ' + (h % 24) + 'h';
    if (h > 0) return h + 'h ' + Math.floor((ms % 3600000) / 60000) + 'min';
    return Math.floor(ms / 60000) + 'min';
  }

  var ETL_ICON = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" fill="currentColor">' +
    '<ellipse cx="12" cy="6" rx="8" ry="3" opacity=".9"/>' +
    '<path d="M4 6v4c0 1.66 3.58 3 8 3s8-1.34 8-3V6" opacity=".7"/>' +
    '<path d="M4 10v4c0 1.66 3.58 3 8 3s8-1.34 8-3v-4" opacity=".5"/>' +
    '<circle cx="18" cy="18" r="5" fill="#fff" stroke="currentColor" stroke-width="1.5"/>' +
    '<path d="M18 15.5v1.2l.9.5m-.9-.2a1.5 1.5 0 1 0 1.5 1.5" fill="none" stroke="currentColor" stroke-width="1" stroke-linecap="round"/>' +
    '</svg>';

  function dot(ok) {
    return '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px;vertical-align:middle;background:' +
      (ok ? '#28a745' : '#dc3545') + ';"></span>';
  }

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

  function renderMeta(panel, data, staleHours) {
    var now = Date.now();
    var staleMs = staleHours * 3600000;
    var script = data.script || '?';
    var finished = data.finished_at ? new Date(data.finished_at) : null;
    var postos = data.postos || {};
    var keys = Object.keys(postos).sort();

    var countOk = 0, countErr = 0;
    keys.forEach(function (k) { postos[k].status === 'ok' ? countOk++ : countErr++; });

    /* ── Cabeçalho ── */
    var html = '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap;">' +
      '<strong style="font-size:.85rem;">Status ETL</strong>' +
      '<span style="color:#6b7280;font-size:.78rem;">(' + script + ')</span>';
    if (finished) {
      var age = now - finished.getTime();
      html += '<span style="font-size:.78rem;color:#6b7280;">Última execução: ' + fmtDate(finished) + ' (' + fmtAge(age) + ' atrás)</span>';
    }
    if (keys.length > 0) {
      html += '<span style="font-size:.78rem;">' + dot(countErr === 0) +
        '<b>' + countOk + '</b> ok' + (countErr > 0 ? ', <b style="color:#dc3545">' + countErr + '</b> erro' : '') +
        '</span>';
    }
    html += '</div>';

    /* ── Lista por posto ── */
    if (keys.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px 16px;">';
      keys.forEach(function (k) {
        var p = postos[k];
        var label = 'Posto ' + k + (POSTO_NAMES[k] ? ' (' + POSTO_NAMES[k] + ')' : '');
        if (p.status === 'ok') {
          var dt = p.at ? new Date(p.at) : null;
          var age = dt ? now - dt.getTime() : Infinity;
          var isStale = age > staleMs;
          var color = isStale ? '#dc3545' : '#28a745';
          var weight = isStale ? 'font-weight:600;' : '';
          html += '<span>' + dot(!isStale) + label + ': ' +
            '<span style="color:' + color + ';' + weight + '">' +
            (dt ? fmtDate(dt) + ' (' + fmtAge(age) + ')' : 'ok') +
            (isStale ? ' — desatualizado' : '') +
            '</span></span>';
        } else {
          var msg = p.msg ? ' — ' + p.msg.substring(0, 80) : '';
          html += '<span>' + dot(false) + label + ': ' +
            '<span style="color:#dc3545;font-weight:600;">ERRO' + msg + '</span></span>';
        }
      });
      html += '</div>';
    } else {
      html += '<span style="color:#6b7280;">Nenhum dado de postos encontrado.</span>';
    }

    panel.innerHTML = html;
  }

  function renderFallback(panel, msg) {
    panel.innerHTML = '<span style="color:#f0ad4e;">' + dot(false) + msg + '</span>';
  }

  window.ETLStatus = {
    init: function (cfg) {
      if (!cfg || !cfg.metaUrl) return;

      var anchorSel = cfg.anchor || '#ia-anchor';
      var staleHours = cfg.staleHours || STALE_DEFAULT;

      function setup() {
        var anchor = document.querySelector(anchorSel);
        if (!anchor) return;

        var btn = createButton();
        anchor.parentNode.insertBefore(btn, anchor);

        var panel = createPanel();
        var content = document.querySelector('section.content') ||
          document.querySelector('.content-wrapper') ||
          anchor.closest('nav')?.nextElementSibling;
        if (content) {
          content.insertBefore(panel, content.firstChild);
        } else {
          anchor.closest('nav')?.parentNode?.appendChild(panel);
        }

        var loaded = false;
        btn.addEventListener('click', async function () {
          if (panel.style.display === 'none') {
            panel.style.display = 'block';
            if (!loaded) {
              panel.innerHTML = '<span style="color:#6b7280;"><i class="fas fa-spinner fa-spin"></i> Verificando status do ETL...</span>';
              try {
                var r = await fetch(cfg.metaUrl, { cache: 'no-store' });
                if (!r.ok) throw new Error('HTTP ' + r.status);
                var data = await r.json();
                renderMeta(panel, data, staleHours);
              } catch (e) {
                renderFallback(panel, 'Metadata ETL não encontrado. O robô ainda não rodou ou o arquivo não foi publicado.');
              }
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
