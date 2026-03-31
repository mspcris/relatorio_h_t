/* chat.js — IA CAMIM (v5 — P/M/G, provedor Groq/OpenAI/Anthropic, saudação personalizada)
 * - Botão "🤖 IA" inserido após o seletor anchor
 * - Painel fixo no canto inferior direito
 * - Tamanhos P / M / G selecionáveis no header
 * - Provedores: Groq (padrão) | OpenAI | Anthropic
 * - Saudação personalizada via /ia/saudacao
 * - Sem "IA Resumida" / "Resposta Objetiva" — resposta direta
 * - Exportar conversa em HTML (ícone ⬇)
 */

(function () {

  /* ─── Tamanhos disponíveis ─── */
  const SIZES = {
    P: { width: 'min(440px, 95vw)', maxHeight: '68vh' },
    M: { width: 'min(740px, 95vw)', maxHeight: '82vh' },
    G: { width: 'min(1080px, 95vw)', maxHeight: '92vh' }
  };

  /* ─── Provedores ─── */
  const PROVIDERS = {
    groq:      { label: 'Groq',      color: '#6366f1' },
    openai:    { label: 'OpenAI',    color: '#10a37f' },
    anthropic: { label: 'Anthropic', color: '#d4a017' }
  };

  /* ─── Ícones dos provedores ─── */
  const PROVIDER_ICONS = {
    groq: '<span style="font-size:15px;line-height:1">🤖</span>',
    openai: `<svg width="16" height="16" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path fill="#10a37f" d="M22.282 9.821a5.985 5.985 0 0 0-.516-4.91 6.046 6.046 0 0 0-6.51-2.9A6.065 6.065 0 0 0 4.981 4.18a5.985 5.985 0 0 0-3.998 2.9 6.046 6.046 0 0 0 .743 7.097 5.98 5.98 0 0 0 .51 4.911 6.051 6.051 0 0 0 6.515 2.9A5.985 5.985 0 0 0 13.26 24a6.056 6.056 0 0 0 5.772-4.206 5.99 5.99 0 0 0 3.997-2.9 6.056 6.056 0 0 0-.747-7.073zm-9.022 12.61a4.476 4.476 0 0 1-2.876-1.04l.141-.081 4.779-2.758a.795.795 0 0 0 .392-.681V9.134l2.02 1.168a.071.071 0 0 1 .038.052v5.583a4.504 4.504 0 0 1-4.494 4.494zM3.6 18.304a4.47 4.47 0 0 1-.535-3.014l.142.085 4.783 2.759a.771.771 0 0 0 .78 0l5.843-3.369v2.332a.08.08 0 0 1-.033.062L9.74 19.95a4.5 4.5 0 0 1-6.14-1.646zM2.34 7.896a4.475 4.475 0 0 1 2.366-1.973V11.6a.766.766 0 0 0 .388.676l5.815 3.355-2.02 1.168a.076.076 0 0 1-.071.008L4.25 14.633a4.504 4.504 0 0 1-1.91-6.737zm16.597 3.855l-5.843-3.387 2.02-1.164a.076.076 0 0 1 .071-.008l4.57 2.638a4.5 4.5 0 0 1-.696 8.118v-5.569a.795.795 0 0 0-.463-.628zm2.01-3.023l-.141-.085-4.774-2.782a.776.776 0 0 0-.785 0L9.409 9.23V6.897a.066.066 0 0 1 .028-.061l4.572-2.638a4.5 4.5 0 0 1 6.68 4.66zm-12.64 4.135l-2.02-1.164a.08.08 0 0 1-.038-.057V6.075a4.5 4.5 0 0 1 7.375-3.453l-.142.08-4.78 2.76a.795.795 0 0 0-.393.681zm1.097-2.365l2.602-1.5 2.607 1.5v2.999l-2.597 1.5-2.607-1.5z"/></svg>`,
    anthropic: `<svg width="16" height="16" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path fill="#d4a017" d="M13.827 3.52h-3.654L5.443 20h3.33l.862-2.538h4.73l.862 2.538h3.33L13.827 3.52zm-3.237 11.19 1.61-4.74 1.61 4.74H10.59z"/></svg>`
  };
  const PROVIDER_DEFAULT = 'groq';

  const ChatIA = {
    _CFG: { TIMEOUT_MS: 180000 },
    _state: { mounted: false, size: 'M', provider: PROVIDER_DEFAULT, opts: null },

    init(options) {
      const defaults = {
        apiUrl: '/api/ia/chat',
        mountAfterSelector: '#btnComparar',
        title: 'IA CAMIM',
        timeoutMs: 180000,
        getPayload: async ({ userQuery }) => ({ prompt: userQuery || 'ok' })
      };

      this._state.opts = Object.assign({}, defaults, options || {});

      this._injectStyles();
      this._injectUI();
      this._wireUI();
      this._state.mounted = true;

      this._ensureBtnTimer = setInterval(() => this._ensureLauncher(), 1500);
    },

    /* ═══════════════════ ESTILOS ═══════════════════ */
    _injectStyles() {
      if (document.getElementById('chatia-styles')) return;
      const css = `
#iaDeepPanel{font-size:.90rem;color:#eaeaea;display:flex;flex-direction:column}
#iaDeepPanel .iad-chat{display:flex;flex-direction:column;gap:10px}
#iaDeepPanel .iad-msg{max-width:84%;padding:10px 14px;border-radius:12px;line-height:1.5;white-space:pre-wrap;word-break:break-word}
#iaDeepPanel .iad-bot{background:#2a2a2a;border:1px solid #3a3a3a;align-self:flex-start}
#iaDeepPanel .iad-user{background:#16324f;border:1px solid #275a8a;align-self:flex-end}
#iaDeepPanel .iad-meta{font-size:11px;color:#6b7280;margin-top:3px}
#iaDeepPanel.card{background:#1e1e1e;color:#fff;border:1px solid #2a2a2a}
#iaDeepPanel .card-header,#iaDeepPanel .card-footer{background:#242424;color:#fff;border-color:#2a2a2a}
#iaDeepPanel .iad-body{background:#1e1e1e;overflow-y:auto;flex:1;padding:12px 14px}
#iaDeepPanel .btn.btn-outline-secondary{color:#ccc;border-color:#555}
#iaDeepPanel .btn.btn-outline-secondary:hover{background:#333}
#iaDeepPanel .btn.btn-outline-secondary.active{background:#444;color:#fff;font-weight:600}
#iaDeep.btn{margin-left:.5rem;font-weight:600}
.iad-content h5{font-size:1em;font-weight:600;margin:.25em 0}
.iad-content p{font-size:1em;margin:.3em 0}
.iad-loading{display:flex;align-items:center;gap:8px;padding:8px 12px}
.iad-spinner{width:16px;height:16px;border-radius:50%;border:2px solid rgba(255,255,255,.2);border-top-color:#fff;animation:iadSpin .8s linear infinite;display:inline-block}
@keyframes iadSpin{to{transform:rotate(360deg)}}
.iad-type{--reveal:0%;-webkit-mask-image:linear-gradient(90deg,#000 calc(var(--reveal)),transparent 0);mask-image:linear-gradient(90deg,#000 calc(var(--reveal)),transparent 0)}
.iad-type.caret::after{content:'';display:inline-block;width:2px;height:1em;vertical-align:baseline;background:#ddd;margin-left:2px;animation:iadBlink 1s step-end infinite}
@keyframes iadBlink{50%{opacity:0}}
.iad-prov-icon{display:inline-flex;align-items:center;justify-content:center;flex-shrink:0}
.iad-status-msg{opacity:.75;font-size:.82rem;transition:opacity .22s ease}
.iad-status-msg.iad-fade-out{opacity:0}
#iadProvBar{display:flex;gap:4px;padding:4px 12px 6px;background:#1a1a1a;border-bottom:1px solid #2a2a2a;flex-shrink:0}
#iadProvBar button{font-size:.75rem;padding:2px 10px;border-radius:20px;border:1px solid #444;background:transparent;color:#aaa;cursor:pointer;transition:all .15s}
#iadProvBar button.active{color:#fff;font-weight:600}
`.trim();
      const s = document.createElement('style');
      s.id = 'chatia-styles';
      s.textContent = css;
      document.head.appendChild(s);
    },

    /* ═══════════════════ BOTÃO LANÇADOR ═══════════════════ */
    _ensureLauncher() {
      const anchor = document.querySelector(this._state.opts.mountAfterSelector);
      if (!document.getElementById('iaDeep') && anchor && anchor.parentNode) {
        const btn = document.createElement('button');
        btn.id = 'iaDeep';
        btn.className = 'btn btn-sm';
        btn.style.cssText = `
          background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
          color: white;
          font-weight: 700;
          letter-spacing: 1px;
          font-size: 0.78rem;
          border: 1px solid rgba(100, 180, 255, 0.35);
          border-radius: 8px;
          padding: 5px 12px;
          box-shadow: 0 0 10px rgba(80, 140, 255, 0.25), 0 2px 6px rgba(0,0,0,0.3);
          transition: all 0.25s ease;
          text-transform: uppercase;
        `;
        btn.innerHTML = '<span style="background: linear-gradient(90deg,#a78bfa,#60a5fa); -webkit-background-clip:text; -webkit-text-fill-color:transparent; font-size:1rem; font-weight:900;">AI</span>';
        btn.title = 'Abrir assistente de IA';
        btn.onmouseenter = () => {
          btn.style.boxShadow = '0 0 18px rgba(100,160,255,0.5), 0 4px 12px rgba(0,0,0,0.4)';
          btn.style.transform = 'translateY(-2px)';
          btn.style.borderColor = 'rgba(140, 200, 255, 0.6)';
        };
        btn.onmouseleave = () => {
          btn.style.boxShadow = '0 0 10px rgba(80,140,255,0.25), 0 2px 6px rgba(0,0,0,0.3)';
          btn.style.transform = '';
          btn.style.borderColor = 'rgba(100, 180, 255, 0.35)';
        };

        anchor.parentNode.insertBefore(btn, anchor.nextSibling);

        btn.addEventListener('click', () => {
          const panel = this._panel();
          if (!panel) return;
          const open = (panel.style.display === 'none' || panel.style.display === '');
          panel.style.display = open ? 'flex' : 'none';
          this._toggleBtn(btn, open);
          if (open) {
            const inp = this._q('#iadInput');
            if (inp) inp.focus();
          }
        });
      }
    },

    /* ═══════════════════ PAINEL PRINCIPAL ═══════════════════ */
    _injectUI() {
      this._ensureLauncher();

      if (!document.getElementById('iaDeepPanel')) {
        const sz = SIZES.M;
        const wrap = document.createElement('div');
        wrap.id = 'iaDeepPanel';
        wrap.className = 'card';
        wrap.style.cssText = [
          'display:none',
          `width:${sz.width}`,
          `max-height:${sz.maxHeight}`,
          'position:fixed',
          'right:16px',
          'bottom:16px',
          'z-index:9999',
          'box-shadow:0 12px 32px rgba(0,0,0,.4)',
          'transition:width .2s, max-height .2s'
        ].join(';');

        wrap.innerHTML = `
<!-- HEADER -->
<div class="card-header d-flex align-items-center" style="gap:6px;padding:7px 10px;flex-shrink:0">
  <b style="flex:1;font-size:.88rem" id="iadTitle">${this._state.opts.title}</b>

  <!-- Tamanhos P/M/G -->
  <div class="btn-group btn-group-sm" title="Tamanho da janela">
    <button id="iadSzP" class="btn btn-sm btn-outline-secondary" title="Pequeno">P</button>
    <button id="iadSzM" class="btn btn-sm btn-outline-secondary active" title="Médio">M</button>
    <button id="iadSzG" class="btn btn-sm btn-outline-secondary" title="Grande">G</button>
  </div>

  <!-- Exportar -->
  <button id="iadExport" class="btn btn-sm btn-outline-secondary" title="Exportar conversa"
          style="width:28px;height:28px;padding:0;display:flex;align-items:center;justify-content:center;font-size:14px">⬇</button>

  <!-- Fechar -->
  <button id="iadClose" class="btn btn-sm btn-outline-secondary" title="Fechar"
          style="width:28px;height:28px;padding:0;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:15px">✕</button>
</div>

<!-- BARRA DE PROVEDORES -->
<div id="iadProvBar">
  <span style="font-size:.72rem;color:#666;align-self:center;margin-right:2px">Provedor:</span>
  <button id="iadProvGroq"      class="active" data-prov="groq">Groq</button>
  <button id="iadProvOpenai"    data-prov="openai">OpenAI</button>
  <button id="iadProvAnthropic" data-prov="anthropic">Anthropic</button>
</div>

<!-- CORPO DO CHAT -->
<div class="iad-body" id="iadBody">
  <div id="iadOut" class="iad-chat"></div>
</div>

<!-- FOOTER -->
<div class="card-footer" style="flex-shrink:0;padding:8px 10px">
  <div class="d-flex" style="gap:8px">
    <input id="iadInput" class="form-control form-control-sm"
           placeholder="Faça sua pergunta sobre os dados…" autocomplete="off">
    <button id="iadSend" class="btn btn-sm btn-primary" style="white-space:nowrap">Enviar</button>
  </div>
</div>
`;
        document.body.appendChild(wrap);
        this._loadSaudacao();
      }
    },

    /* ═══════════════════ EVENTOS ═══════════════════ */
    _wireUI() {
      /* Fechar */
      this._q('#iadClose').addEventListener('click', () => {
        this._panel().style.display = 'none';
        const btn = this._btn();
        if (btn) this._toggleBtn(btn, false);
      });

      /* Exportar */
      this._q('#iadExport').addEventListener('click', () => this._exportToHTML());

      /* Tamanhos */
      ['P', 'M', 'G'].forEach(sz => {
        const el = this._q(`#iadSz${sz}`);
        if (!el) return;
        el.addEventListener('click', () => {
          this._applySize(sz);
          ['P', 'M', 'G'].forEach(s => {
            const b = this._q(`#iadSz${s}`);
            if (b) b.classList.toggle('active', s === sz);
          });
        });
      });

      /* Provedores */
      this._q('#iadProvBar').addEventListener('click', e => {
        const btn = e.target.closest('[data-prov]');
        if (!btn) return;
        const prov = btn.dataset.prov;
        this._state.provider = prov;

        this._q('#iadProvBar').querySelectorAll('[data-prov]').forEach(b => {
          const active = b.dataset.prov === prov;
          b.classList.toggle('active', active);
          const cfg = PROVIDERS[b.dataset.prov];
          b.style.borderColor = active ? (cfg?.color || '#888') : '#444';
          b.style.color = active ? '#fff' : '#aaa';
          if (active) b.style.background = (cfg?.color || '#888') + '33';
          else b.style.background = 'transparent';
        });

        this._append('bot', `<span style="opacity:.7;font-size:.82rem">Provedor alterado para <b>${PROVIDERS[prov]?.label || prov}</b>. Nova pergunta usará este provedor.</span>`);
      });

      /* Ativa estilo inicial do provedor padrão */
      const defBtn = this._q(`[data-prov="${PROVIDER_DEFAULT}"]`);
      if (defBtn) {
        const cfg = PROVIDERS[PROVIDER_DEFAULT];
        defBtn.style.borderColor = cfg?.color || '#888';
        defBtn.style.color = '#fff';
        defBtn.style.background = (cfg?.color || '#888') + '33';
      }

      /* Enviar */
      const send  = this._q('#iadSend');
      const input = this._q('#iadInput');

      send.addEventListener('click', async () => {
        const q = (input.value || '').trim();
        if (!q) return;
        input.value = '';
        this._append('user', q);
        await this._sendToIA(q);
      });

      input.addEventListener('keydown', e => {
        if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send.click(); }
      });
    },

    _applySize(sz) {
      const panel = this._panel();
      if (!panel) return;
      const cfg = SIZES[sz] || SIZES.M;
      panel.style.width    = cfg.width;
      panel.style.maxHeight = cfg.maxHeight;
      this._state.size = sz;
    },

    /* ═══════════════════ SAUDAÇÃO ═══════════════════ */
    async _loadSaudacao() {
      let nome = null;
      let perguntas = [];

      try {
        const resp = await fetch('/api/ia/saudacao', { credentials: 'same-origin', cache: 'no-store' });
        if (resp.ok) {
          const data = await resp.json();
          nome = data.nome || null;
          if (nome) window.USER_NOME = nome;
          perguntas = data.perguntas_frequentes || [];
        }
      } catch (_) {}

      if (!nome) nome = window.USER_NOME || null;
      if (!nome) {
        const em = (window.USER_EMAIL || '').trim();
        if (em && /@/.test(em)) {
          const part = em.split('@')[0].replace(/[._\-\d]+/g, ' ').trim().split(' ')[0];
          nome = part ? part.charAt(0).toUpperCase() + part.slice(1) : null;
        }
      }
      if (!nome) nome = 'Você';

      this._append('bot', `Olá, ${nome}! Sou a IA da CAMIM. Pergunte sobre os dados desta página.`);

      if (perguntas.length > 0) {
        const out = this._outBox();
        const row = document.createElement('div');
        row.style.cssText = 'display:flex;flex-wrap:wrap;gap:6px;margin:6px 0 2px 0';
        perguntas.forEach(q => {
          const pill = document.createElement('button');
          pill.className = 'btn btn-sm btn-outline-secondary';
          pill.style.cssText = 'font-size:.76rem;padding:2px 10px;border-radius:20px;max-width:300px;white-space:normal;text-align:left';
          pill.textContent = q;
          pill.addEventListener('click', () => {
            row.remove();
            this._append('user', q);
            this._sendToIA(q);
          });
          row.appendChild(pill);
        });
        out.appendChild(row);
      }

      this._scrollBottom();
    },

    /* ═══════════════════ ENVIO PARA IA ═══════════════════ */
    async _sendToIA(userQuery) {
      this._dotsStart();
      try {
        const payload = await this._state.opts.getPayload({ userQuery });
        if (payload && typeof payload === 'object') {
          payload.pagina   = window.IA_PAGINA || window.location.pathname;
          payload.provider = this._state.provider;  // groq | openai | anthropic
        }
        const raw  = await this._callIA(payload, this._state.opts.timeoutMs);
        this._dotsStop();
        this._renderBotMessage(this._coerceText(raw));
      } catch (err) {
        this._dotsStop();
        this._append('bot', `<span style="color:#f39c12">Não foi possível responder — ${this._escapeHtml(err?.message || 'erro desconhecido.')}</span>`);
      }
    },

    /* ═══════════════════ RENDERIZAÇÃO BOT ═══════════════════ */
    _renderBotMessage(text) {
      const row = document.createElement('div');
      row.className = 'd-flex flex-column';

      const box = document.createElement('div');
      box.className = 'iad-msg iad-bot';

      const content = document.createElement('div');
      content.className = 'iad-content';

      const coerced = this._coerceText(text);
      const view = this._normalizeForView(coerced);

      if (view.mode === 'html') {
        // LLM retornou HTML — sanitiza e renderiza como HTML
        content.innerHTML = this._safeHtml(view.html);
      } else {
        const blocks = this._parseBlocks(view.text);
        if (!blocks.length) {
          const p = document.createElement('p');
          p.style.margin = '.3em 0';
          p.textContent = String(coerced || '').trim();
          content.appendChild(p);
        } else {
          blocks.forEach(b => {
            let el;
            if (b.type === 'h2') {
              el = document.createElement('h5');
              el.style.cssText = 'font-size:1em;font-weight:700;margin:.6em 0 .2em;color:#e0e0e0;border-bottom:1px solid #3a3a3a;padding-bottom:3px';
            } else if (b.type === 'h3') {
              el = document.createElement('h6');
              el.style.cssText = 'font-size:.92em;font-weight:600;margin:.45em 0 .15em;color:#ccc';
            } else if (b.type === 'li') {
              el = document.createElement('p');
              el.style.cssText = 'margin:.15em 0 .15em 1em';
              b.text = '• ' + b.text;
            } else if (b.type === 'li2') {
              el = document.createElement('p');
              el.style.cssText = 'margin:.1em 0 .1em 2em;opacity:.85';
              b.text = '◦ ' + b.text;
            } else {
              el = document.createElement('p');
              el.style.cssText = 'margin:.35em 0';
            }
            el.textContent = b.text;
            content.appendChild(el);
          });
        }
      }

      requestAnimationFrame(() => this._typeReveal(content));

      box.appendChild(content);

      const meta = document.createElement('div');
      meta.className = 'iad-meta';
      meta.textContent = 'IA CAMIM · ' + (PROVIDERS[this._state.provider]?.label || this._state.provider);

      row.appendChild(box);
      row.appendChild(meta);
      this._outBox().appendChild(row);
      this._scrollBottom();
    },

    /* ═══════════════════ PARSER MARKDOWN → BLOCOS ═══════════════════ */
    _parseBlocks(rawText) {
      let t = String(rawText || '').trim();
      t = t.replace(/\r\n/g, '\n').replace(/\\n/g, '\n').replace(/\\t/g, '  ');
      // Remove code fences (keep inner text)
      t = t.replace(/```\w*\n?([\s\S]*?)```/g, '$1');
      // Remove bold/italic markers
      t = t.replace(/\*\*(.*?)\*\*/g, '$1').replace(/__(.*?)__/g, '$1');
      t = t.replace(/ _(.*?)_ /g, ' $1 ');
      // Convert markdown table separator lines (|---|---|) to nothing
      t = t.replace(/^\|?[\s:]*[-|: ]{3,}[\s:]*\|?\s*$/gm, '');
      // Convert markdown table rows to readable format
      t = t.replace(/^\|(.+)\|\s*$/gm, (_, inner) =>
        inner.split('|').map(c => c.trim()).filter(Boolean).join('  —  ')
      );
      // Normalize whitespace
      t = t.replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();

      const blocks = [];
      let paraLines = [];

      const flushPara = () => {
        const txt = paraLines.join(' ').trim();
        if (txt) blocks.push({ type: 'p', text: txt });
        paraLines = [];
      };

      for (const raw of t.split('\n')) {
        const line = raw.trim();
        if (!line) { flushPara(); continue; }

        if (/^#{1,2} /.test(line)) {
          flushPara();
          blocks.push({ type: 'h2', text: line.replace(/^#+\s+/, '') });
        } else if (/^#{3,} /.test(line)) {
          flushPara();
          blocks.push({ type: 'h3', text: line.replace(/^#+\s+/, '') });
        } else if (/^\s{2,}[-*•] /.test(raw)) {
          flushPara();
          blocks.push({ type: 'li2', text: line.replace(/^[-*•]\s+/, '') });
        } else if (/^[-*•] /.test(line)) {
          flushPara();
          blocks.push({ type: 'li', text: line.replace(/^[-*•]\s+/, '') });
        } else {
          paraLines.push(line);
        }
      }
      flushPara();
      return blocks;
    },

    /* ═══════════════════ HELPERS UI ═══════════════════ */
    _panel()  { return document.getElementById('iaDeepPanel'); },
    _outBox() { return document.getElementById('iadOut'); },
    _btn()    { return document.getElementById('iaDeep'); },
    _q(sel)   { return document.querySelector(sel); },

    _scrollBottom() {
      const body = this._q('#iadBody');
      if (body) requestAnimationFrame(() => { body.scrollTop = body.scrollHeight; });
    },

    _toggleBtn(el, on) {
      if (!el) return;
      el.classList.toggle('btn-outline-primary', !on);
      el.classList.toggle('btn-info', on);
    },

    _append(role, html) {
      const row = document.createElement('div');
      row.className = 'd-flex flex-column';

      const b = document.createElement('div');
      b.className = 'iad-msg ' + (role === 'user' ? 'iad-user' : 'iad-bot');
      if (role === 'user') b.textContent = html;
      else b.innerHTML = html;

      const m = document.createElement('div');
      m.className = 'iad-meta';
      if (role === 'user') m.style.alignSelf = 'flex-end';
      m.textContent = role === 'user'
        ? (window.USER_NOME || 'Você')
        : 'IA CAMIM · ' + (PROVIDERS[this._state.provider]?.label || this._state.provider);

      row.appendChild(b);
      row.appendChild(m);
      this._outBox().appendChild(row);
      this._scrollBottom();
    },

    /* ═══════════════════ TYPEWRITER (rAF, char-by-char) ═══════════════════ */
    _typeReveal(container) {
      if (!container) return;
      const els = [...container.querySelectorAll('h1, h2, h3, h4, h5, h6, p, li, span, div:not(.iad-content)')];
      if (!els.length) return;

      // Salva texto completo e esvazia cada elemento
      const texts = els.map(el => {
        const t = el.textContent || '';
        el.textContent = '';
        return t;
      });

      const totalChars = texts.reduce((s, t) => s + t.length, 0) || 1;
      // Alvo ~5s para textos curtos, mais rápido para longos
      const msPerChar = Math.max(2, Math.min(18, 5000 / totalChars));

      let elIdx = 0, charIdx = 0, acc = 0, lastTs = 0;

      const frame = (ts) => {
        if (elIdx >= els.length) { this._scrollBottom(); return; }
        if (lastTs === 0) { lastTs = ts; requestAnimationFrame(frame); return; }

        acc += ts - lastTs;
        lastTs = ts;

        const toAdd = Math.min(Math.floor(acc / msPerChar), 40);
        if (toAdd > 0) {
          acc -= toAdd * msPerChar;
          for (let i = 0; i < toAdd; i++) {
            if (elIdx >= els.length) break;
            charIdx++;
            if (charIdx > texts[elIdx].length) {
              els[elIdx].textContent = texts[elIdx]; // finaliza elemento
              elIdx++;
              charIdx = 0;
            } else {
              els[elIdx].textContent = texts[elIdx].slice(0, charIdx);
            }
          }
          this._scrollBottom();
        }

        if (elIdx < els.length) requestAnimationFrame(frame);
      };

      requestAnimationFrame(frame);
    },

    /* ═══════════════════ NETWORK ═══════════════════ */
    async _callIA(payload, timeout) {
      const API = this._state.opts.apiUrl || '/ia/chat';
      const controller = new AbortController();
      const to = setTimeout(() => controller.abort(), timeout || this._CFG.TIMEOUT_MS);

      try {
        const r = await fetch(API, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin',
          body: JSON.stringify(typeof payload === 'string' ? { prompt: payload } : payload),
          signal: controller.signal
        });
        clearTimeout(to);

        if (!r.ok) {
          const txt = await r.text().catch(() => '');
          throw new Error(`HTTP ${r.status}${txt ? ' — ' + txt.slice(0, 200) : ''}`);
        }

        const resText = await r.text();
        let res = null;
        try { res = JSON.parse(resText); } catch {}
        if (!res) return resText;

        // Compatível com diferentes formatos de resposta
        if (res.content_mode === 'free_text') return res.text || '';
        if (res.content_mode === 'json') return (res.data && (res.data.html || res.data.livre || res.data.text)) || JSON.stringify(res.data);
        if (res.resposta) return res.resposta;  // formato OpenAI /api/ia/pergunta
        return typeof res === 'string' ? res : (res.html || res.livre || res.text || JSON.stringify(res));

      } catch (err) {
        clearTimeout(to);
        throw err;
      }
    },

    /* ═══════════════════ SPINNER ═══════════════════ */
    _dotsStart() {
      const msgs = [
        'Recebendo dados…',
        'Analisando KPI…',
        'Enviando dados para Pandas…',
        'Enviando para LLM…',
        'Recebendo Resposta…',
      ];
      // Insere "Gastando seus tokens :)" em posição aleatória
      msgs.splice(Math.floor(Math.random() * (msgs.length + 1)), 0, 'Gastando seus tokens :)');

      const icon = PROVIDER_ICONS[this._state.provider] || PROVIDER_ICONS.groq;

      const wrap = document.createElement('div');
      wrap.id = '_iadSpinner';
      wrap.className = 'iad-loading';
      wrap.style.gap = '8px';
      wrap.innerHTML = `<span class="iad-prov-icon">${icon}</span><span class="iad-spinner"></span><span id="_iadStatusMsg" class="iad-status-msg">${msgs[0]}</span>`;
      this._outBox().appendChild(wrap);
      this._scrollBottom();

      let idx = 1;
      this._dotsInterval = setInterval(() => {
        const el = document.getElementById('_iadStatusMsg');
        if (!el) return;
        el.classList.add('iad-fade-out');
        setTimeout(() => {
          if (!document.getElementById('_iadStatusMsg')) return;
          el.textContent = msgs[idx % msgs.length];
          el.classList.remove('iad-fade-out');
          idx++;
        }, 230);
      }, 1700);
    },

    _dotsStop() {
      if (this._dotsInterval) {
        clearInterval(this._dotsInterval);
        this._dotsInterval = null;
      }
      const el = document.getElementById('_iadSpinner');
      if (el) el.remove();
    },

    /* ═══════════════════ TEXTO ═══════════════════ */
    _coerceText(out) {
      const s = String(out ?? '').trim();
      if (s.startsWith('{') || s.startsWith('[')) {
        try {
          const obj = JSON.parse(s);
          const keys = ['html','livre','text','resposta','analysis','resumo','summary','content','answer'];
          for (const k of keys) {
            if (typeof obj[k] === 'string' && obj[k].trim()) return this._sanitizeText(obj[k]);
          }
          return this._sanitizeText(JSON.stringify(obj));
        } catch {}
      }
      return this._sanitizeText(s);
    },

    _sanitizeText(s) {
      let t = String(s || '');
      if (!t) return t;
      t = t.replace(/\r\n/g,'\n').replace(/\\n/g,'\n').replace(/\\t/g,'  ').replace(/\u00A0/g,' ');
      t = t.replace(/```([\s\S]*?)```/g,'$1');
      t = t.replace(/^\s*[+\-─═┌┐└┘┼┤├┬┴]+.*$/gm,' ');
      t = t.replace(/\*\*(.*?)\*\*/g,'$1').replace(/__([^_]+)__/g,'$1').replace(/_(.*?)_/g,'$1');
      t = t.replace(/^\s*\|(.+?)\|\s*$/gm,(_,inner) => inner.split('|').map(c=>c.trim()).filter(Boolean).join(' — '));
      t = t.replace(/[ \t]+\n/g,'\n').replace(/\n{3,}/g,'\n\n').trim();
      return t || String(s||'').trim();
    },

    _normalizeForView(rawText) {
      const cleaned = this._sanitizeText(rawText || '');
      if (/<\/?(p|h[1-6]|ul|ol|li|table|thead|tbody|tr|th|td|strong|em|b|i|div|span)[\s>]/i.test(cleaned)
          && !cleaned.startsWith('{') && !/```/.test(cleaned)) {
        return { mode: 'html', html: cleaned };
      }
      return { mode: 'markdown', text: cleaned };
    },

    _safeHtml(html) {
      const tmp = document.createElement('div');
      tmp.innerHTML = html;
      const allowed = new Set(['P','BR','B','STRONG','I','EM','UL','OL','LI','H1','H2','H3','H4','H5','H6','TABLE','THEAD','TBODY','TR','TH','TD','HR','SPAN','DIV','CODE','PRE']);
      const walk = node => {
        [...node.childNodes].forEach(c => {
          if (c.nodeType !== Node.ELEMENT_NODE) return;
          if (!allowed.has(c.tagName)) { while (c.firstChild) node.insertBefore(c.firstChild, c); node.removeChild(c); return; }
          [...c.attributes].forEach(a => { if (a.name.startsWith('on') || a.name === 'style') c.removeAttribute(a.name); });
          walk(c);
        });
      };
      walk(tmp);
      return tmp.innerHTML;
    },

    _escapeHtml(str) {
      return String(str||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    },

    /* ═══════════════════ EXPORTAR ═══════════════════ */
    _exportToHTML() {
      const out = this._outBox();
      if (!out) { alert('Sem conteúdo.'); return; }
      const cssNode = document.getElementById('chatia-styles');
      const titulo  = this._state.opts.title || 'Conversa IA';
      const usuario = window.USER_NOME || 'Usuário';
      const agora   = new Date();
      const html = `<!DOCTYPE html>
<html lang="pt-br"><head><meta charset="utf-8"><title>${this._escapeHtml(titulo)}</title>
<style>body{margin:0;padding:16px;background:#111827;color:#e5e7eb;font-family:system-ui,sans-serif;font-size:14px;line-height:1.5}
h1{font-size:1.1rem;margin:0 0 6px}hr{border:none;border-top:1px solid #374151;margin:10px 0}
${cssNode ? cssNode.textContent : ''}</style></head><body>
<h1>${this._escapeHtml(titulo)}</h1>
<div style="font-size:.8rem;color:#9ca3af;margin-bottom:8px">Exportado em ${agora.toLocaleString('pt-BR')} — ${this._escapeHtml(usuario)}</div>
<hr><div style="display:flex;flex-direction:column;gap:10px">${out.innerHTML}</div>
</body></html>`;
      const a = document.createElement('a');
      a.href = URL.createObjectURL(new Blob([html],{type:'text/html;charset=utf-8'}));
      a.download = `conversa_ia_${agora.toISOString().slice(0,10)}.html`;
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(a.href);
    }
  };

  window.ChatIA = ChatIA;

})();
