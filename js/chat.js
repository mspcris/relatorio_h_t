/* chat.js — Painel de IA reutilizável (v3: spinner + resumo por mensagem)
 * Objetivo:
 *  1) Sempre pedir o COMPLETO à IA.
 *  2) Se “IA Resumida” estiver ativo, gerar RESUMO do COMPLETO e exibir só o resumo.
 *  3) Cada bolha tem seu próprio toggle “Resumo [ ]”.
 *  4) Bolha do bot só aparece quando o conteúdo está pronto.
 *  5) Spinner redondo substitui “CARREGANDO”.
 */
(function () {
  const ChatIA = {
    _CFG: { TIMEOUT_MS: 180000 },
    _state: { mounted: false, brief: true, opts: null },

    init(options) {
      const defaults = {
        apiUrl: (window.IA_API_URL || '/ia/analisar'),
        mountAfterSelector: '#btnComparar',
        title: 'IA — Análise Completa & Chat',
        briefInstruction: 'Responda em até 3 linhas e 300 caracteres. Sem listas.',
        timeoutMs: 180000,
        briefDefault: true,
        getPayload: async ({ userQuery }) => ({ prompt: userQuery || 'ok' })
      };
      this._state.opts = Object.assign({}, defaults, options || {});
      this._state.brief = !!this._state.opts.briefDefault;

      this._injectStyles();
      this._injectUI();
      this._wireUI();
      this._state.mounted = true;

      this._ensureBtnTimer = setInterval(() => this._ensureLauncher(), 1500);
    },

    /* ===================== UI ===================== */
    _injectStyles() {
      if (document.getElementById('chatia-styles')) return;
      const css = `
#iaDeepPanel{font-size:.90rem;color:#eaeaea}
#iaDeepPanel .iad-chat{display:flex;flex-direction:column;gap:10px}
#iaDeepPanel .iad-msg{max-width:78%;padding:10px 12px;border-radius:12px;line-height:1.45;white-space:pre-wrap;word-break:break-word;text-align:justify}
#iaDeepPanel .iad-bot{background:#2a2a2a;border:1px solid #3a3a3a;align-self:flex-start}
#iaDeepPanel .iad-user{background:#16324f;border:1px solid #275a8a;align-self:flex-end}
#iaDeepPanel .iad-meta{font-size:12px;color:#9aa1a9;margin-top:2px}
#iaDeepPanel.card{background:#1e1e1e;color:#fff;border:1px solid #2a2a2a}
#iaDeepPanel .card-header,#iaDeepPanel .card-footer{background:#242424;color:#fff;border-color:#2a2a2a}
#iaDeepPanel .body{background:#1e1e1e;max-height:60vh;overflow:auto;padding-bottom:80px}
#iaDeepPanel .btn.btn-outline-secondary{color:#ddd;border-color:#5a5a5a}
#iaDeepPanel .btn.btn-outline-secondary:hover{background:#333}
#iaDeep.btn{margin-left:.5rem}
#iadBrief{display:inline-flex;align-items:center;gap:6px}
#iadBrief input{margin:0}
.iad-hint{font-style:italic;color:#cfcfcf}

/* toolbar da bolha */
.iad-toolbar{opacity:.9; display:flex; gap:8px; align-items:center; justify-content:space-between; margin-bottom:6px}
.iad-toolbar label{cursor:pointer; user-select:none}
.iad-toolbar .iad-view-hint{opacity:.8}

/* texto: título e parágrafo com a MESMA fonte para evitar “zoom” no resumo */
.iad-content h5{font-size:1em; font-weight:600; margin:.2em 0}
.iad-content p{font-size:1em; margin:.35em 0}

/* efeito type-reveal por CSS mask */
.iad-type{ --reveal:0%; -webkit-mask-image:linear-gradient(90deg,#000 calc(var(--reveal)),transparent 0); mask-image:linear-gradient(90deg,#000 calc(var(--reveal)),transparent 0); }
.iad-type.caret::after{content:''; display:inline-block; width:2px; height:1em; vertical-align:baseline; background:#ddd; margin-left:2px; animation:iadBlink 1s step-end infinite;}
@keyframes iadBlink{50%{opacity:0}}

/* Spinner redondo */
.iad-loading { display:flex; align-items:center; gap:8px }
.iad-spinner{
  width:20px; height:20px;
  border-radius:50%;
  border:3px solid rgba(255,255,255,.25);
  border-top-color:#fff;
  animation: iadSpin .8s linear infinite;
  display:inline-block; vertical-align:middle;
}
@keyframes iadSpin{ to { transform: rotate(360deg); } }
`.trim();
      const s = document.createElement('style');
      s.id = 'chatia-styles';
      s.textContent = css;
      document.head.appendChild(s);
    },

    _ensureLauncher() {
      const anchor = document.querySelector(this._state.opts.mountAfterSelector);
      if (!document.getElementById('iaDeep') && anchor && anchor.parentNode) {
        const btn = document.createElement('button');
        btn.id = 'iaDeep';
        btn.className = 'btn btn-sm btn-outline-primary';
        btn.textContent = 'IA Análise';
        btn.title = 'Relatório completo e chat';
        anchor.parentNode.insertBefore(btn, anchor.nextSibling);
        btn.addEventListener('click', () => {
          const panel = this._panel(); if (!panel) return;
          const open = (panel.style.display === 'none' || panel.style.display === '');
          panel.style.display = open ? 'block' : 'none';
          this._toggleBtn(btn, open);
          if (open) this._prefillInput(true);
        });
        btn.dataset.wired = '1';
      }
    },

    _injectUI() {
      this._ensureLauncher();
      if (!document.getElementById('iaDeepPanel')) {
        const wrap = document.createElement('div');
        wrap.id = 'iaDeepPanel';
        wrap.className = 'card';
        wrap.style.cssText = 'display:none;position:fixed;right:16px;bottom:16px;width:min(760px,95vw);max-height:85vh;z-index:9999;box-shadow:0 10px 24px rgba(0,0,0,.18)';
        wrap.innerHTML = `
<div class="card-header d-flex align-items-center justify-content-between">
  <b class="mb-0">${this._state.opts.title}</b>
  <div class="d-flex" style="gap:8px">
    <label id="iadBrief" class="btn btn-sm btn-outline-secondary mb-0" title="Padrão para PRÓXIMAS mensagens">
      <input id="iadBriefChk" type="checkbox"${this._state.brief ? ' checked' : ''}/>
      <span>IA Resumida (padrão)</span>
    </label>
    <button id="iadSave" class="btn btn-sm btn-outline-secondary">Salvar conversa</button>
    <button id="iadRefresh" class="btn btn-sm btn-outline-secondary">Regerar</button>
    <button id="iadClose" class="btn btn-sm btn-outline-secondary">Fechar</button>
  </div>
</div>
<div class="card-body body">
  <div id="iadOut" class="iad-chat ia-clean"></div>
</div>
<div class="card-footer">
  <div class="d-flex" style="gap:8px">
    <input id="iadInput" class="form-control form-control-sm" placeholder="Faça perguntas sobre os dados...">
    <button id="iadSend" class="btn btn-sm btn-primary">Iniciar</button>
  </div>
  <small class="text-muted">Mensagens usam o recorte atual. Texto corrido, sem tabelas ASCII.</small>
</div>`;
        document.body.appendChild(wrap);
        this._append('bot', 'Olá, eu sou a Camila.AI e vou te ajudar a analisar os dados desta página. Toque em Iniciar ou digite sua pergunta.');
        this._prefillInput();
      }
    },

    _prefillInput(initial) {
      const input = this._q('#iadInput');
      const send = this._q('#iadSend');
      if (!input || !send) return;
      if (initial) send.textContent = 'Iniciar';
    },

    _wireUI() {
      const panel = this._panel();
      const btn = this._btn();
      const briefLbl = this._q('#iadBrief');
      const briefChk = this._q('#iadBriefChk');
      const close = this._q('#iadClose');
      const regen = this._q('#iadRefresh');
      const saveBtn = this._q('#iadSave');
      const send = this._q('#iadSend');
      const input = this._q('#iadInput');

      if (btn && !btn.dataset.wired) {
        btn.addEventListener('click', () => {
          const open = (panel.style.display === 'none');
          panel.style.display = open ? 'block' : 'none';
          this._toggleBtn(btn, open);
          if (open) this._prefillInput(true);
        });
        btn.dataset.wired = '1';
      }

      close.addEventListener('click', () => { panel.style.display = 'none'; this._toggleBtn(btn, false); });
      regen.addEventListener('click', async () => { await this._sendToIA(null, { force: true }); });

      if (saveBtn) {
        saveBtn.addEventListener('click', () => this._exportToHTML());
      }

      // Global = padrão para próximas mensagens
      briefLbl.addEventListener('click', (e) => {
        if (e.target.id !== 'iadBriefChk') { e.preventDefault(); briefChk.checked = !briefChk.checked; }
        this._state.brief = !!briefChk.checked;
        const msg = this._state.brief
          ? 'Padrão atualizado: novas mensagens abrirão em RESUMO.'
          : 'Padrão atualizado: novas mensagens abrirão em COMPLETO.';
        this._append('bot', `<span class="iad-hint">${msg}</span>`);
      });

      send.addEventListener('click', async () => {
        let q = (input.value || '').trim();
        if (!q && send.textContent === 'Iniciar') { send.textContent = 'Enviar'; input.focus(); return; }
        if (!q) return;
        input.value = '';
        this._append('user', q);
        await this._sendToIA(q);
      });

      input.addEventListener('input', () => { if (send.textContent !== 'Enviar') send.textContent = 'Enviar'; });
      input.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send.click(); } });
    },

    _panel() { return document.getElementById('iaDeepPanel'); },
    _outBox() { return document.getElementById('iadOut'); },
    _btn() { return document.getElementById('iaDeep'); },
    _q(sel) { return document.querySelector(sel); },
    _toggleBtn(el, on) {
      if (!el) return;
      el.classList.toggle('btn-outline-primary', !on);
      el.classList.toggle('btn-primary', on);
      el.classList.toggle('active', on);
      el.setAttribute('aria-pressed', on ? 'true' : 'false');
    },

    /* ===================== Helpers visuais ===================== */
    _displayNameFromEmail() {
      const email = (window.USER_EMAIL || '').trim();
      if (!email || !/@/.test(email)) return 'Você';
      const first = email.split('@')[0];
      return first ? first.charAt(0).toUpperCase() + first.slice(1) : 'Você';
    },

    _escapeHtml(str) {
      return String(str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    },

    _append(role, html) {
      const row = document.createElement('div'); row.className = 'd-flex flex-column';
      const b = document.createElement('div'); b.className = 'iad-msg ' + (role === 'user' ? 'iad-user' : 'iad-bot');
      if (role === 'user') b.textContent = html; else b.innerHTML = html;
      const m = document.createElement('div'); m.className = 'iad-meta'; m.textContent = role === 'user' ? this._displayNameFromEmail() : 'Camila.AI';
      row.appendChild(b); row.appendChild(m); this._outBox().appendChild(row);
      const sc = this._panel().querySelector('.body'); sc.scrollTop = sc.scrollHeight;
    },

    /* ===================== Renderer da bolha do bot ===================== */
    _renderBotCard({ fullText, defaultBrief }) {
      const row = document.createElement('div'); row.className = 'd-flex flex-column';
      const box = document.createElement('div'); box.className = 'iad-msg iad-bot';
      const meta = document.createElement('div'); meta.className = 'iad-meta'; meta.textContent = 'Camila.AI';

      const toolbar = document.createElement('div');
      toolbar.className = 'iad-toolbar';
      toolbar.innerHTML = `
        <label class="small mb-0">
          <input type="checkbox" class="iad-brief-toggle"${defaultBrief ? ' checked' : ''}/>
          <span>Resumo</span>
        </label>
        <small class="muted iad-view-hint"></small>
      `;

      const content = document.createElement('div'); content.className = 'iad-content';

      box.dataset.raw = fullText || '';
      box.dataset.full = this._cleanGroq(fullText || '');
      box.dataset.summary = '';
      box.dataset.mode = defaultBrief ? 'summary' : 'full';

      box.appendChild(toolbar);
      box.appendChild(content);
      row.appendChild(box);
      row.appendChild(meta);
      this._outBox().appendChild(row);

      const toggle = toolbar.querySelector('.iad-brief-toggle');
      const hint = toolbar.querySelector('.iad-view-hint');

      toggle.addEventListener('change', async () => {
        const wantSummary = !!toggle.checked;
        if (wantSummary) {
          if (!box.dataset.summary) {
            try {
              const sum = await this._summarizeText(box.dataset.full || box.dataset.raw || '');
              this._attachSummaryToCard(box, sum);
            } catch { toggle.checked = false; }
          }
          if (!box.dataset.summary) {
            toggle.checked = false;
            this._renderCardContent(box, 'full');
            return;
          }
          this._renderCardContent(box, 'summary');
        } else {
          this._renderCardContent(box, 'full');
        }
      });

      hint.textContent = defaultBrief ? 'mostrando resumo' : 'mostrando completo';
      return box;
    },

    _attachSummaryToCard(box, summaryText) {
      if (!box) return;
      const cleaned = this._cleanGroq(summaryText || '');
      if (cleaned) box.dataset.summary = cleaned;
    },

    _renderCardContent(box, mode) {
      if (!box) return;
      const content = box.querySelector('.iad-content');
      const hint = box.querySelector('.iad-view-hint');
      const isSummary = (mode === 'summary');
      box.dataset.mode = isSummary ? 'summary' : 'full';

      let txt = isSummary ? (box.dataset.summary || '') : (box.dataset.full || '');

      if (!txt || !txt.trim()) {
        const raw = isSummary
          ? (box.dataset.full || box.dataset.raw || '')
          : (box.dataset.raw || '');
        txt = String(raw || '').trim();
        if (!txt) {
          txt = '[A IA retornou apenas tabela/código ou conteúdo vazio. Ajuste o prompt ou verifique o backend.]';
        }
      }

      if (isSummary && !txt) {
        mode = 'full';
        txt = box.dataset.full || box.dataset.raw || '';
        const tgl = box.querySelector('.iad-brief-toggle'); if (tgl) tgl.checked = false;
      }

      content.innerHTML = '';

      const norm = this._normalizeForView(txt);

      if (norm.mode === 'html') {
        content.innerHTML = this._safeHtml(norm.html);
        if (hint) hint.textContent = mode === 'summary'
          ? 'mostrando resumo (HTML)'
          : 'mostrando completo (HTML)';
      } else {
        const cleaned = norm.text;
        const blocks = cleaned.split(/\n{2,}/).map(s => s.trim()).filter(Boolean);
        blocks.forEach(b => {
          const isTitle = /^###\s*/.test(b);
          const text = b.replace(/^###\s*/, '');
          const el = document.createElement(isTitle ? 'h5' : 'p');
          if (isTitle) el.className = 'iad-h';
          el.textContent = text;
          content.appendChild(el);
        });

        if (hint) hint.textContent = mode === 'summary' ? 'mostrando resumo' : 'mostrando completo';
        requestAnimationFrame(() => this._typeReveal(content));
      }

      const sc = this._panel()?.querySelector('.body');
      if (sc) requestAnimationFrame(() => { sc.scrollTop = sc.scrollHeight; });
    },

    /* ===================== Envio ===================== */
    async _sendToIA(userQuery, { force = false } = {}) {
      this._dotsStart();
      try {
        const payload = await this._state.opts.getPayload({ userQuery });

        const raw = await this._callIA(payload, this._state.opts.timeoutMs);
        this._dotsStop();

        const fullText = this._coerceText(raw);
        const card = this._renderBotCard({ fullText, defaultBrief: this._state.brief });

        if (this._state.brief) {
          (async () => {
            try {
              const sum = await this._summarizeText(fullText);
              this._attachSummaryToCard(card, sum);
              if (card.dataset.summary) this._renderCardContent(card, 'summary');
              else this._renderCardContent(card, 'full');
            } catch { this._renderCardContent(card, 'full'); }
          })();
        } else {
          this._renderCardContent(card, 'full');
        }

      } catch (err) {
        this._dotsStop();
        const msg = `Não foi possível responder${err?.message ? ` — possível motivo: ${err.message}` : ''}.`;
        this._append('bot', `<span class="text-warning">${msg}</span>`);
      }
    },

    /* ===================== Summarização — sempre a partir do FULL ===================== */
    async _summarizeText(text) {
      const p = {
        prompt:
          `[RESUMO-ESTRITO]: ${this._state.opts.briefInstruction}\n` +
          `Formate em PT-BR com parágrafos curtos. Use "###" para subtítulos quando fizer sentido.\n\n` +
          `Texto-base:\n"""${text}"""`,
        prefs: { accept_format: 'free_text', temperature: 0.2, max_tokens: 600 }
      };
      this._dotsStart();
      try {
        const out = await this._callIA(p, Math.round(this._state.opts.timeoutMs * 0.8));
        return this._coerceText(out);
      } finally {
        this._dotsStop();
      }
    },

    /* ===================== Normalização de saída ===================== */
    _coerceText(out) {
      const s = String(out ?? '').trim();
      if (this._looksLikeJson(s)) {
        try {
          const obj = JSON.parse(s);
          const picked = this._pickFields(obj, [
            'html','livre','text','analysis','resumo','summary','conteudo','content','answer'
          ]);
          if (picked) return this._sanitizeText(picked);
          return this._sanitizeText(JSON.stringify(obj));
        } catch { /* segue */ }
      }
      return this._sanitizeText(s);
    },

    _looksLikeJson(s) { return s.startsWith('{') || s.startsWith('['); },

    _pickFields(obj, keys) {
      if (obj == null) return '';
      for (const k of keys) if (typeof obj?.[k] === 'string' && obj[k].trim()) return obj[k];
      const parts = [];
      Object.entries(obj).forEach(([k, v]) => { if (typeof v === 'string' && v.trim()) parts.push(v.trim()); });
      return parts.length ? parts.join('\n\n') : '';
    },

    _sanitizeText(s) {
      let t = String(s || '');
      const original = t;

      t = t.replace(/\r\n/g, '\n').replace(/\u00A0/g, ' ');

      t = t.replace(/```([\s\S]*?)```/g, '$1');

      t = t.replace(/^\s*\|[- :]+\|\s*$/gm, ' ');

      t = t.replace(/\*\*(.*?)\*\*/g, '$1');
      t = t.replace(/__([^_]+)__/g, '$1');
      t = t.replace(/_(.*?)_/g, '$1');

      t = t.replace(/^\s*\|(.+?)\|\s*$/gm, (m, inner) => {
        const cols = inner.split('|').map(c => c.trim()).filter(Boolean);
        if (!cols.length) return '';
        return cols.join(' — ');
      });

      t = t.replace(/[ \t]+\n/g, '\n');
      t = t.replace(/([.,;:!?])(?!\s|$)/g, '$1 ');
      t = t.replace(/\n{3,}/g, '\n\n').trim();

      if (!t || t.length < 5) return original.trim();
      return t;
    },

    /* ===================== Detecção/normalização HTML x markdown ===================== */
    _looksLikeHtmlFragment(s) {
      const txt = String(s || '').trim();
      if (!txt) return false;
      if (this._looksLikeJson(txt) || /```/.test(txt)) return false;
      return /<\/?(p|h[1-6]|ul|ol|li|table|thead|tbody|tr|th|td|strong|em|b|i|div|span)[\s>]/i.test(txt);
    },

    _safeHtml(html) {
      const tmp = document.createElement('div');
      tmp.innerHTML = html;

      const allowedTags = new Set([
        'P','BR','B','STRONG','I','EM',
        'UL','OL','LI',
        'H1','H2','H3','H4','H5','H6',
        'TABLE','THEAD','TBODY','TR','TH','TD',
        'HR','SPAN','DIV','CODE','PRE'
      ]);

      const walk = (node) => {
        const children = Array.from(node.childNodes);
        for (const c of children) {
          if (c.nodeType === Node.ELEMENT_NODE) {
            const tag = c.tagName.toUpperCase();
            if (!allowedTags.has(tag)) {
              while (c.firstChild) node.insertBefore(c.firstChild, c);
              node.removeChild(c);
              continue;
            }
            [...c.attributes].forEach(attr => {
              const name = attr.name.toLowerCase();
              if (name.startsWith('on') || name === 'style' || name === 'srcdoc') {
                c.removeAttribute(attr.name);
              }
            });
            walk(c);
          }
        }
      };
      walk(tmp);
      return tmp.innerHTML;
    },

    _normalizeForView(rawText) {
      const cleaned = this._cleanGroq(rawText || '');
      if (this._looksLikeHtmlFragment(cleaned)) {
        return { mode: 'html', html: cleaned };
      }
      return { mode: 'markdown', text: cleaned };
    },

    /* ===================== Efeito “digitar” via CSS mask ===================== */
    _typeReveal(container, baseMs = 25) {
      if (!container) return;
      const targets = container.querySelectorAll('h5, p');
      targets.forEach((el) => {
        el.classList.add('iad-type','caret');
        el.style.setProperty('--reveal','0%');
        const chars = (el.textContent || '').length || 1;
        const dur = Math.min(8000, Math.max(600, chars * baseMs));
        const anim = el.animate(
          [{ '--reveal': '0%' }, { '--reveal': '100%' }],
          { duration: dur, easing: `steps(${Math.min(chars, 2000)}, end)` }
        );
        anim.onfinish = () => {
          el.classList.remove('caret');
          el.style.setProperty('--reveal','100%');
        };
      });
    },

    /* ===================== Networking ===================== */
    async _callIA(payload, timeout) {
      const API = this._state.opts.apiUrl;
      const maxAttempts = 2;
      let lastError = null;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const controller = new AbortController();
        const to = setTimeout(
          () => controller.abort(),
          timeout || this._state.opts.timeoutMs || this._CFG.TIMEOUT_MS
        );

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
            const t = await r.text().catch(() => '');
            throw new Error(`HTTP ${r.status}${t ? ' — ' + t : ''}`);
          }

          let resText = await r.text(); let res = null;
          try { res = JSON.parse(resText); } catch { }

          if (!res) return resText;
          if (res && res.content_mode === 'free_text') return res.text || '';
          if (res && res.content_mode === 'json') {
            return (res.data && (res.data.html || res.data.livre || res.data.text)) || JSON.stringify(res.data);
          }
          return typeof res === 'string' ? res : (res.html || res.livre || JSON.stringify(res));

        } catch (err) {
          lastError = err;
          if (attempt === maxAttempts) throw lastError;
        }
      }

      throw lastError || new Error('Falha desconhecida na chamada de IA');
    },

    /* ===================== Spinner ===================== */
    _dotsStart() {
      const wrap = document.createElement('div');
      wrap.className = 'iad-msg iad-bot iad-loading';
      wrap.innerHTML = `<span class="iad-spinner" aria-hidden="true"></span>`;
      this._outBox().appendChild(wrap);

      const int = setInterval(() => {
        const sc = this._panel().querySelector('.body');
        sc.scrollTop = sc.scrollHeight;
      }, 260);

      this._dotsStop._int = int;
      this._dotsStop._el = wrap;
    },

    _dotsStop() {
      if (this._dotsStop._int) { clearInterval(this._dotsStop._int); this._dotsStop._int = null; }
      if (this._dotsStop._el) { this._dotsStop._el.remove(); this._dotsStop._el = null; }
    },

    /* ===================== Exportar conversa em HTML ===================== */
    _exportToHTML() {
      const out = document.getElementById('iadOut');
      if (!out) {
        alert('Não encontrei o conteúdo da conversa para salvar.');
        return;
      }

      const cssNode = document.getElementById('chatia-styles');
      const cssChat = cssNode ? cssNode.textContent : '';
      const titulo = this._state.opts.title || 'Conversa com a IA';
      const usuario = this._displayNameFromEmail();
      const agora = new Date();
      const dataStr = agora.toLocaleString('pt-BR');
      const nomeArquivo = `conversa_ia_${agora.toISOString().slice(0,10)}.html`;

      const htmlDoc = `
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>${this._escapeHtml(titulo)} — ${this._escapeHtml(usuario)}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{
      margin:0;
      padding:16px;
      background:#111827;
      color:#e5e7eb;
      font-family:system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
      font-size:14px;
      line-height:1.5;
    }
    h1{
      font-size:1.2rem;
      margin:0 0 4px 0;
    }
    .meta-header{
      font-size:.8rem;
      color:#9ca3af;
      margin-bottom:12px;
    }
    hr{
      border:none;
      border-top:1px solid #374151;
      margin:12px 0;
    }
    .card{
      background:#111827;
      border:1px solid #1f2933;
      border-radius:8px;
      padding:12px;
    }
    .body{
      max-height:none;
      overflow:visible;
      padding-bottom:0;
    }
    ${cssChat}
  </style>
</head>
<body>
  <h1>${this._escapeHtml(titulo)}</h1>
  <div class="meta-header">
    Exportado em ${this._escapeHtml(dataStr)} — Usuário: ${this._escapeHtml(usuario)}
  </div>
  <hr>
  <div id="iaDeepPanel" class="card">
    <div class="card-body body">
      <div id="iadOut" class="iad-chat ia-clean">
${out.innerHTML}
      </div>
    </div>
  </div>
</body>
</html>`.trim();

      const blob = new Blob([htmlDoc], { type: 'text/html;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = nomeArquivo;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(a.href);
    },

    /* ===================== Sanitização bruta vinda da IA ===================== */
    _cleanGroq(raw) {
      let s = String(raw || '');
      const original = s;

      s = s.replace(/\r\n/g, '\n').replace(/\\n/g, '\n').replace(/\\t/g, '  ');

      s = s.replace(/```([\s\S]*?)```/g, '$1');

      s = s.replace(/^\s*[+\-─═┌┐└┘┼┤├┬┴]+.*$/gm, ' ');

      s = s.replace(/\\begin\{aligned\}[\s\S]*?\\end\{aligned\}/g, ' ');
      s = s.replace(/\$\$[\s\S]*?\$\$/g, ' ');
      s = s.replace(/\\\[[\s\S]*?\\\]/g, ' ');
      s = s.replace(/\\\([\s\S]*?\\\)/g, ' ');

      s = s.replace(/[ \t]+\n/g, '\n');
      s = s.replace(/###\s*/g, '\n\n### ');
      s = s.replace(/\n{3,}/g, '\n\n');
      s = s.trim();

      if (!s) return original.trim();
      return s;
    }
  };

  window.ChatIA = ChatIA;
})();
