/**
 * ETL Status Widget v2 — botao colorido + painel com diagnostico.
 *
 * Uso:
 *   <script src="/js/etl_status.js"></script>
 *   <script>
 *     ETLStatus.init({
 *       metaUrl: '/json_consolidado/_etl_meta_export_governanca.json',
 *       anchor: '#ia-anchor',
 *     });
 *   </script>
 *
 * Cores do botao (auto-fetch ao carregar) — baseado em CICLOS perdidos, nao tempo absoluto:
 *   VERDE    = dentro do ciclo atual (runAge <= 1x intervalo) e todos postos OK
 *   AMARELO  = 1 ciclo perdido (1x < runAge <= 2x intervalo) OU posto com erro/faltando
 *              na ultima execucao (1 falha conhecida)
 *   VERMELHO = 2+ ciclos perdidos (runAge > 2x intervalo) OU posto sem atualizar ha 2+ ciclos
 *   CINZA    = ETL sem agendamento no cron (manual, neutro)
 *
 * Exemplo: cron a cada 15 min — 0–15min verde, 15–30min amarelo, 30min+ vermelho.
 * Exemplo: cron 1x/dia      — 0–24h verde, 24–48h amarelo, 48h+ vermelho.
 *
 * O painel mostra:
 *   - script .py responsavel
 *   - wrapper .sh no cron
 *   - periodicidade (a cada 15 min, diario as 02:00, ...)
 *   - ultima execucao e proxima esperada
 *   - status por posto
 *
 * Registro local mapeia cada script -> metadados de cron (fonte: /etc/cron.d/relatorio_ht).
 * Atualize este arquivo quando adicionar/alterar entradas no cron.
 */
(function () {
  'use strict';

  /* ── Registro de ETLs ──────────────────────────────────────────────── */
  var REGISTRY = {
    'export_governanca':              { py: 'export_governanca.py',            sh: 'export_governanca.sh',             cron_desc: 'a cada 15 min',               interval_min: 15 },
    'export_receita_despesa':         { py: 'export_receita_despesa.py',       sh: 'export_receita_despesa.sh',        cron_desc: 'a cada 6 horas',              interval_min: 360 },
    'export_vendas':                  { py: 'export_vendas.py',                sh: 'export_cadastro_vendas.sh',        cron_desc: 'diario as 01:00',             interval_min: 1440 },
    'export_cad_cliente_incremental': { py: 'export_cad_cliente_incremental.py', sh: 'export_cadastro_vendas.sh',      cron_desc: 'diario as 01:00',             interval_min: 1440 },
    'export_metas':                   { py: 'export_metas.py',                 sh: 'export_metas.sh',                  cron_desc: 'a cada 6 horas',              interval_min: 360 },
    'export_fin_full_rateio':         { py: 'export_fin_full_rateio.py',       sh: 'export_fin_full_rateio.sh',        cron_desc: 'diario as 01:30',             interval_min: 1440 },
    'export_fidelizacao':             { py: 'export_fidelizacao.py',           sh: 'run_export_fidelizacao.sh',        cron_desc: 'diario as 02:30',             interval_min: 1440 },
    'export_consultas_mensal_json':   { py: 'export_consultas_mensal.py',      sh: 'export_consultas_mensal.sh',       cron_desc: 'diario as 02:00',             interval_min: 1440 },
    'export_growth':                  { py: 'export_growth.py',                sh: 'export_growth.sh',                 cron_desc: 'diario as 03:00',             interval_min: 1440 },
    'export_leads_analytics':         { py: 'export_leads_analytics_cache.py', sh: 'export_leads_analytics_cache.sh',  cron_desc: '2x/dia (03:30 e 12:30)',      interval_min: 540 },
    'export_liberty2':                { py: 'export_liberty2.py',              sh: '(direto no cron)',                 cron_desc: 'a cada 12h (02:00 e 14:00)',  interval_min: 720 },
    'ctrlq_export_relatorio':         { py: 'ctrlq_export_relatorio.py',       sh: 'export_ctrlq_relatorio.sh',        cron_desc: 'a cada 15 min',               interval_min: 15 },
    'etl_higienizacao_snapshot':      { py: 'etl_higienizacao_snapshot.py',    sh: 'export_higienizacao.sh',           cron_desc: 'de hora em hora',             interval_min: 60 },
    'export_agenda_dia':              { py: 'export_agenda_dia.py',            sh: '(direto no cron)',                 cron_desc: 'de hora em hora, 07h–17h',    interval_min: 60 },
    'export_qualidade_agenda':        { py: 'export_qualidade_agenda.py',      sh: 'export_qualidade_agenda.sh',       cron_desc: 'diario as 05:00',             interval_min: 1440 },
    'export_vagas':                   { py: 'export_vagas.py',                 sh: '(sem wrapper)',                    cron_desc: 'NAO AGENDADO — sem entrada no cron', interval_min: null },
    'export_notas_rps':               { py: 'export_notas_rps.py',             sh: 'export_notas_rps.sh',              cron_desc: 'de hora em hora',             interval_min: 60 },
    'indicadores_etl':                { py: 'indicadores_etl.py',              sh: '(direto no cron)',                 cron_desc: 'diario as 02:45',             interval_min: 1440 },
    'export_preagendamento':          { py: 'export_preagendamento.py',        sh: 'export_preagendamento.sh',         cron_desc: 'diario as 02:30',             interval_min: 1440 }
  };

  /* ── Nomes legiveis dos postos ── */
  var POSTO_NAMES = {
    A:'Anchieta', B:'Bangu', C:'Campinho', D:'Del Castilho',
    G:'Campo Grande', I:'Nova Iguacu', J:'Jacarepagua', M:'Madureira',
    N:'Nilopolis', P:'Rio das Pedras', R:'Realengo', X:'X Campo Grande',
    Y:'Y Campo Grande'
  };

  /* ── Paleta ── */
  var COLORS = {
    green:  { bg: '#d4edda', border: '#28a745', text: '#155724', dot: '#28a745' },
    yellow: { bg: '#fff3cd', border: '#ffc107', text: '#856404', dot: '#f0ad4e' },
    red:    { bg: '#f8d7da', border: '#dc3545', text: '#721c24', dot: '#dc3545' },
    gray:   { bg: '#e9ecef', border: '#adb5bd', text: '#495057', dot: '#6c757d' }
  };

  /* ── Utilitarios ── */
  function fmtDate(d) {
    return d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
  }
  function fmtAge(ms) {
    var abs = Math.abs(ms);
    var h = Math.floor(abs / 3600000);
    var m = Math.floor((abs % 3600000) / 60000);
    if (h >= 24) return Math.floor(h / 24) + 'd ' + (h % 24) + 'h';
    if (h > 0)   return h + 'h ' + m + 'min';
    return m + 'min';
  }
  function scriptFromMetaUrl(url) {
    var m = url.match(/_etl_meta_(.+)\.json/);
    return m ? m[1] : null;
  }
  function pdot(level) {
    var c = COLORS[level] || COLORS.gray;
    return '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px;vertical-align:middle;background:' + c.dot + ';"></span>';
  }

  /* ── Classificacao do status geral (por CICLOS perdidos) ──
   *
   * Regra unica para qualquer ETL, independente da periodicidade:
   *   runAge <= 1x intervalo  → VERDE  (dentro do ciclo atual)
   *   runAge >  1x intervalo  → AMARELO (1 ciclo perdido)
   *   runAge >  2x intervalo  → VERMELHO (2+ ciclos perdidos)
   *
   * O mesmo se aplica a cada posto individualmente (posto.at).
   * Posto com status='error' na ultima execucao ou ausente = AMARELO (1 falha pontual).
   * So vira VERMELHO se a falha persistir alem de 2 ciclos.
   */
  function classify(data, reg, expectedPostos) {
    var now = Date.now();
    var finished = data && data.finished_at ? new Date(data.finished_at) : null;
    var interval = reg && reg.interval_min ? reg.interval_min * 60000 : null;
    var postos = (data && data.postos) || {};
    var keys = expectedPostos && expectedPostos.length > 0 ? expectedPostos.slice() : Object.keys(postos);

    // Sem agendamento no cron -> neutro (cinza), nao alarme
    if (reg && reg.interval_min === null) {
      return { level: 'gray', summary: 'sem agendamento', reason: 'Script existe mas nao tem entrada no /etc/cron.d/relatorio_ht da VM (execucao manual).' };
    }

    // Sem registro de execucao
    if (!finished) {
      return { level: 'yellow', summary: 'sem registro', reason: 'ETL ainda nao registrou nenhuma execucao no meta.' };
    }

    var runAge = now - finished.getTime();

    // 2+ ciclos perdidos no ETL inteiro -> vermelho (regra principal)
    if (interval && runAge > 2 * interval) {
      var ciclosPerdidos = Math.floor(runAge / interval);
      return {
        level: 'red',
        summary: ciclosPerdidos + ' ciclos atrasado',
        reason: 'Ultima execucao ha ' + fmtAge(runAge) + ' — esperado a cada ' + fmtAge(interval) + ' (' + ciclosPerdidos + ' ciclos sem rodar).'
      };
    }

    // Agrega por posto (vermelho > amarelo)
    var countErr = 0, countStaleRed = 0, countStaleYellow = 0, countMissing = 0;
    keys.forEach(function (k) {
      var p = postos[k];
      if (!p) { countMissing++; return; }
      if (p.status !== 'ok') { countErr++; return; }
      if (!interval) return;
      var pAge = p.at ? now - new Date(p.at).getTime() : Infinity;
      if (pAge > 2 * interval) countStaleRed++;
      else if (pAge > interval) countStaleYellow++;
    });

    // Algum posto sem atualizar ha 2+ ciclos -> vermelho
    if (countStaleRed > 0) {
      return {
        level: 'red',
        summary: countStaleRed + ' posto(s) 2+ ciclos atras',
        reason: countStaleRed + ' posto(s) sem atualizacao ha mais de 2 ciclos.'
      };
    }

    // ETL inteiro com 1 ciclo perdido -> amarelo
    if (interval && runAge > interval) {
      return {
        level: 'yellow',
        summary: '1 ciclo atrasado',
        reason: 'Ultima execucao ha ' + fmtAge(runAge) + ' — esperado a cada ' + fmtAge(interval) + '.'
      };
    }

    // Falhas pontuais (1 ciclo) -> amarelo
    if (countErr > 0) {
      return {
        level: 'yellow',
        summary: countErr + ' erro(s)',
        reason: countErr + ' posto(s) falharam na ultima execucao (1 ciclo). Vermelho so se persistir alem de 2 ciclos.'
      };
    }
    if (countStaleYellow > 0) {
      return {
        level: 'yellow',
        summary: countStaleYellow + ' posto(s) 1 ciclo atras',
        reason: countStaleYellow + ' posto(s) sem atualizacao ha mais de 1 ciclo.'
      };
    }
    if (countMissing > 0 && keys.length > 0) {
      return {
        level: 'yellow',
        summary: countMissing + ' posto(s) faltando',
        reason: countMissing + ' posto(s) nao apareceram na ultima execucao do meta.'
      };
    }

    return { level: 'green', summary: 'ok', reason: 'Executou no ciclo atual e sem erros.' };
  }

  /* ── Ícone ── */
  var ETL_ICON = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true">' +
    '<ellipse cx="12" cy="6" rx="8" ry="3" opacity=".9"/>' +
    '<path d="M4 6v4c0 1.66 3.58 3 8 3s8-1.34 8-3V6" opacity=".7"/>' +
    '<path d="M4 10v4c0 1.66 3.58 3 8 3s8-1.34 8-3v-4" opacity=".5"/>' +
    '</svg>';

  /* ── DOM ── */
  function createButton() {
    var btn = document.createElement('button');
    btn.id = 'btnETLStatus';
    btn.type = 'button';
    btn.className = 'btn btn-sm ml-2';
    btn.style.cssText = 'display:inline-flex;align-items:center;gap:6px;font-size:.8rem;padding:3px 10px;border-radius:4px;border:1px solid #adb5bd;background:#e9ecef;color:#495057;';
    btn.innerHTML =
      '<span class="etl-dot" style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#6c757d;"></span>' +
      ETL_ICON +
      ' <span class="etl-label">ETL</span>';
    return btn;
  }
  function createPanel() {
    var panel = document.createElement('div');
    panel.id = 'etlStatusPanel';
    panel.style.cssText = 'display:none;font-size:.82rem;padding:12px 14px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:6px;margin-bottom:10px;';
    return panel;
  }

  function applyButtonColor(btn, level, summary) {
    var c = COLORS[level] || COLORS.gray;
    btn.style.backgroundColor = c.bg;
    btn.style.border = '1px solid ' + c.border;
    btn.style.color = c.text;
    var dot = btn.querySelector('.etl-dot');
    if (dot) dot.style.background = c.dot;
    var label = btn.querySelector('.etl-label');
    if (label) label.textContent = 'ETL' + (summary ? ' · ' + summary : '');
    btn.title = 'Status ETL: ' + (summary || '—') + ' (clique para detalhes)';
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c];
    });
  }

  function renderPanel(panel, data, reg, result, expectedPostos, metaUrl, labels) {
    labels = labels || {};
    var itemsLabel = labels.itemsLabel || 'Postos:';
    var itemPrefix = labels.itemPrefix || 'Posto ';
    var nameMap    = labels.nameMap    || POSTO_NAMES;
    var now = Date.now();
    var finished = data && data.finished_at ? new Date(data.finished_at) : null;
    var postos = (data && data.postos) || {};
    var interval = reg && reg.interval_min ? reg.interval_min * 60000 : null;
    var scriptName = (data && data.script) || (reg && reg.py ? reg.py.replace('.py', '') : (scriptFromMetaUrl(metaUrl) || '—'));
    var c = COLORS[result.level];

    var html = '';

    /* Cabecalho colorido */
    html += '<div style="padding:10px 12px;border-radius:4px;margin-bottom:12px;background:' + c.bg + ';border-left:4px solid ' + c.border + ';color:' + c.text + ';">' +
            '<div style="font-weight:700;font-size:.95rem;">' + escapeHtml(scriptName) + ' — ' + escapeHtml(result.summary.toUpperCase()) + '</div>' +
            '<div style="font-size:.78rem;margin-top:2px;">' + escapeHtml(result.reason) + '</div>' +
            '</div>';

    /* Linha tecnica (py / sh / cron / ultima / proxima) */
    html += '<div style="display:grid;grid-template-columns:max-content 1fr;gap:4px 14px;font-size:.78rem;margin-bottom:10px;">';
    if (reg) {
      html += '<span style="color:#6b7280;">Script .py:</span><code>' + escapeHtml(reg.py) + '</code>';
      html += '<span style="color:#6b7280;">Wrapper .sh:</span><code>' + escapeHtml(reg.sh) + '</code>';
      html += '<span style="color:#6b7280;">Agendamento:</span><span>' + escapeHtml(reg.cron_desc) + '</span>';
    } else {
      html += '<span style="color:#6b7280;">Script:</span><code>' + escapeHtml(scriptName) + '</code>';
      html += '<span style="color:#6b7280;">Agendamento:</span><span style="color:#856404;">(ETL nao registrado no widget — adicione em /js/etl_status.js)</span>';
    }
    if (finished) {
      var runAge = now - finished.getTime();
      html += '<span style="color:#6b7280;">Ultima execucao:</span><span>' + fmtDate(finished) + ' <span style="color:#6b7280;">(' + fmtAge(runAge) + ' atras)</span></span>';
      if (interval) {
        var nextRun = new Date(finished.getTime() + interval);
        var delta = nextRun.getTime() - now;
        var nextLabel = delta > 0 ? 'em ' + fmtAge(delta) : fmtAge(delta) + ' atras';
        var nextColor = delta > 0 ? '#6b7280' : '#856404';
        html += '<span style="color:#6b7280;">Proxima esperada:</span><span>' + fmtDate(nextRun) + ' <span style="color:' + nextColor + ';">(' + nextLabel + ')</span></span>';
      }
    } else {
      html += '<span style="color:#6b7280;">Ultima execucao:</span><span style="color:#856404;">sem registro</span>';
    }
    html += '</div>';

    /* Lista por posto (ou "etapas" para ETLs sem postos) */
    var keys;
    if (expectedPostos && expectedPostos.length > 0) keys = expectedPostos.slice().sort();
    else if (expectedPostos && expectedPostos.length === 0) keys = Object.keys(postos).sort();
    else keys = Object.keys(postos).sort();
    if (keys.length > 0) {
      html += '<div style="font-weight:600;margin-bottom:4px;font-size:.8rem;">' + escapeHtml(itemsLabel) + '</div>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px 16px;">';
      keys.forEach(function (k) {
        var p = postos[k];
        var label = itemPrefix + k + (nameMap[k] ? ' (' + nameMap[k] + ')' : '');
        if (!p) {
          html += '<span>' + pdot('gray') + escapeHtml(label) + ': <span style="color:#856404;">nao rodou na ultima execucao</span></span>';
          return;
        }
        if (p.status !== 'ok') {
          var msg = p.msg ? ' — ' + escapeHtml(p.msg.substring(0, 80)) : '';
          html += '<span>' + pdot('red') + escapeHtml(label) + ': <span style="color:#721c24;font-weight:600;">ERRO' + msg + '</span></span>';
          return;
        }
        var dt = p.at ? new Date(p.at) : null;
        var pAge = dt ? now - dt.getTime() : Infinity;
        var lvl = 'green';
        if (interval && pAge > 2 * interval) lvl = 'red';
        else if (interval && pAge > interval) lvl = 'yellow';
        html += '<span>' + pdot(lvl) + escapeHtml(label) + ': ' +
                '<span style="color:' + COLORS[lvl].text + ';">' +
                (dt ? fmtDate(dt) + ' (' + fmtAge(pAge) + ')' : 'ok') +
                '</span></span>';
      });
      html += '</div>';
    }

    panel.innerHTML = html;
  }

  /* ── API publica ── */
  window.ETLStatus = {
    init: function (cfg) {
      if (!cfg || !cfg.metaUrl) return;

      var anchorSel = cfg.anchor || '#ia-anchor';
      // expectedPostos explicitamente [] → ETL sem postos (ex.: indicadores_etl)
      var expectedPostos = (cfg.expectedPostos !== undefined) ? cfg.expectedPostos : Object.keys(POSTO_NAMES);
      var labels = cfg.labels || (expectedPostos && expectedPostos.length === 0 ? { itemsLabel: 'Etapas:', itemPrefix: 'Etapa ', nameMap: {} } : null);
      var scriptName = scriptFromMetaUrl(cfg.metaUrl);
      var reg = REGISTRY[scriptName] || null;

      function setup() {
        var anchor = document.querySelector(anchorSel);
        if (!anchor) return;

        var btn = createButton();
        anchor.parentNode.insertBefore(btn, anchor);

        var panel = createPanel();
        var content = document.querySelector('section.content') ||
                      document.querySelector('.content-wrapper') ||
                      (anchor.closest('nav') && anchor.closest('nav').nextElementSibling);
        if (content) {
          content.insertBefore(panel, content.firstChild);
        } else if (anchor.closest('nav') && anchor.closest('nav').parentNode) {
          anchor.closest('nav').parentNode.appendChild(panel);
        }

        /* Botao inicial: cinza "carregando" */
        applyButtonColor(btn, 'gray', 'carregando');

        var data = null;
        var result = null;

        /* Cron nao agendado: marca cinza (neutro), nao alarma. */
        if (reg && reg.interval_min === null) {
          result = { level: 'gray', summary: 'sem agendamento', reason: 'Script ' + reg.py + ' nao tem entrada no cron (execucao manual).' };
          applyButtonColor(btn, result.level, result.summary);
        } else {
          fetch(cfg.metaUrl, { cache: 'no-store' })
            .then(function (r) {
              if (!r.ok) throw new Error('HTTP ' + r.status);
              return r.json();
            })
            .then(function (d) {
              data = d;
              result = classify(d, reg, expectedPostos);
              applyButtonColor(btn, result.level, result.summary);
            })
            .catch(function () {
              result = { level: 'yellow', summary: 'meta ausente', reason: 'Arquivo de metadados do ETL nao foi encontrado — talvez o robo nunca tenha rodado com etl_meta.' };
              applyButtonColor(btn, 'yellow', 'meta ausente');
            });
        }

        btn.addEventListener('click', function () {
          if (panel.style.display === 'none') {
            panel.style.display = 'block';
            if (data) {
              renderPanel(panel, data, reg, result, expectedPostos, cfg.metaUrl, labels);
            } else if (result) {
              /* Sem data (ex: cron nao agendado, ou fetch falhou) */
              var c = COLORS[result.level];
              panel.innerHTML =
                '<div style="padding:10px 12px;border-radius:4px;background:' + c.bg + ';border-left:4px solid ' + c.border + ';color:' + c.text + ';">' +
                  '<strong>' + escapeHtml(result.summary.toUpperCase()) + '</strong><br>' +
                  '<span style="font-size:.78rem;">' + escapeHtml(result.reason) + '</span>' +
                '</div>';
              if (reg) {
                panel.innerHTML +=
                  '<div style="margin-top:10px;display:grid;grid-template-columns:max-content 1fr;gap:4px 14px;font-size:.78rem;">' +
                    '<span style="color:#6b7280;">Script .py:</span><code>' + escapeHtml(reg.py) + '</code>' +
                    '<span style="color:#6b7280;">Wrapper .sh:</span><code>' + escapeHtml(reg.sh) + '</code>' +
                    '<span style="color:#6b7280;">Agendamento:</span><span>' + escapeHtml(reg.cron_desc) + '</span>' +
                  '</div>';
              }
            } else {
              panel.innerHTML = '<span style="color:#6b7280;">Carregando…</span>';
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
