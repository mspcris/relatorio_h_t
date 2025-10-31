/* chat.js — Painel de IA reutilizável (v2)
 * Objetivo: enviar SEMPRE o snapshot completo por padrão
 *  - Chunking determinístico com síntese final
 *  - Alternável para modo "inteligente" que evita reenvio quando contexto não mudou
 * API: ChatIA.init({
 *   getPayload,                // async ({ userQuery }) => snapshot COMPLETO da página
 *   apiUrl,                    // POST endpoint
 *   mountAfterSelector,        // seletor para ancorar o botão
 *   title,                     // título do painel
 *   briefInstruction,          // instrução dura para resumo
 *   timeoutMs,                 // tempo base por requisição
 *   briefDefault,              // inicia com IA Resumida marcada
 *   alwaysFull                 // true => manda snapshot em TODA mensagem; false => heurística
 * })
 */
(function () {
  const ChatIA = {
      _CFG: {
    /* === Retry config === */
      MAX_RETRIES: 5,          // reenviar mais 5 vezes
      RETRY_DELAY_MS: 1200,     // intervalo entre tentativas  
    /* ===================== Config ===================== */
      ALWAYS_FULL: true,                  // default. pode ser sobrescrito por init({ alwaysFull })
      MAX_PAYLOAD_CHARS: 120_000,         // corte aproximado por chamada
      CHUNK_BLOCKS: 4,                    // blocos por requisição quando chunkar
      SYNTH_PARTS_LIMIT: 12,              // segurança
      REFRESH_FULL_AFTER_MIN: 5,          // minutos
    },

    /* ===================== Estado ===================== */
    _state: {
      mounted: false,
      brief: true,
      opts: null,
      retried: false,
      lastSnapshotHash: null,
      lastSnapshotAt: 0,
      forceFullNext: false,
    },

    /* ===================== Boot ===================== */
    init(options) {
      const defaults = {
        apiUrl: (window.IA_API_URL || '/ia/analisar'),
        mountAfterSelector: '#btnComparar',
        title: 'IA — Análise Completa & Chat',
        briefInstruction: 'Responda em 200 a 300 caracteres, no máximo 3 linhas. Seja direto, executivo e objetivo.',
        timeoutMs: 180000,
        briefDefault: true,
        alwaysFull: true, // preferível para manter contexto 100% determinístico
        getPayload: async ({ userQuery }) => ({ prompt: userQuery || 'ok' })
      };
      this._state.opts = Object.assign({}, defaults, options || {});
      this._state.brief = !!this._state.opts.briefDefault;
      this._CFG.ALWAYS_FULL = !!this._state.opts.alwaysFull;

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
.iad-loading .dot{opacity:.35}.iad-loading .dot.on{opacity:1}
#iadBrief{display:inline-flex;align-items:center;gap:6px}
#iadBrief input{margin:0}
.iad-hint{font-style:italic;color:#cfcfcf}
#iadMode{display:inline-flex;align-items:center;gap:6px}
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
    <label id="iadBrief" class="btn btn-sm btn-outline-secondary mb-0" title="Respostas curtas">
      <input id="iadBriefChk" type="checkbox"${this._state.brief ? ' checked' : ''}/>
      <span>IA Resumida</span>
    </label>
    <label id="iadMode" class="btn btn-sm btn-outline-secondary mb-0" title="Modo de envio">
      <input id="iadAlwaysFull" type="checkbox"${this._CFG.ALWAYS_FULL ? ' checked' : ''}/>
      <span>Enviar snapshot sempre</span>
    </label>
    <button id="iadPDF" class="btn btn-sm btn-outline-secondary">PDF</button>
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
        this._append('bot', 'Olá, eu sou a Camila.AI e vou te ajudar a analisar os dados que estão nesta página. Toque em Iniciar ou digite algo sobre os dados aqui constantes.');
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
      const modeLbl = this._q('#iadMode');
      const modeChk = this._q('#iadAlwaysFull');
      const close = this._q('#iadClose');
      const regen = this._q('#iadRefresh');
      const pdfBtn = this._q('#iadPDF');
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
      regen.addEventListener('click', async () => { this._state.forceFullNext = true; await this._sendToIA(null, { force: true }); });
      pdfBtn.addEventListener('click', () => this._exportToPDF());

      briefLbl.addEventListener('click', (e) => {
        if (e.target.id !== 'iadBriefChk') { e.preventDefault(); briefChk.checked = !briefChk.checked; }
        this._state.brief = !!briefChk.checked;
        const msg = this._state.brief ? 'Resposta fora do escopo. Marcando bit IA Resumida.' : 'Resposta fora do escopo. Desmarcando bit IA Resumida.';
        this._append('bot', `<span class="iad-hint">${msg}</span>`);
      });

      modeLbl.addEventListener('click', (e) => {
        if (e.target.id !== 'iadAlwaysFull') { e.preventDefault(); modeChk.checked = !modeChk.checked; }
        this._CFG.ALWAYS_FULL = !!modeChk.checked;
        const msg = this._CFG.ALWAYS_FULL ? 'Modo: enviar snapshot completo em toda mensagem.' : 'Modo: envio inteligente com reaproveitamento de contexto.';
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
    _toggleBtn(el, on) { if (!el) return; el.classList.toggle('btn-outline-primary', !on); el.classList.toggle('btn-primary', on); el.classList.toggle('active', on); el.setAttribute('aria-pressed', on ? 'true' : 'false'); },

    /* ===================== Helpers ===================== */
    _displayNameFromEmail() {
      const email = (window.USER_EMAIL || '').trim(); if (!email || !/@/.test(email)) return 'Você';
      const first = email.split('@')[0]; return first ? first.charAt(0).toUpperCase() + first.slice(1) : 'Você';
    },

    _sleep(ms) { return new Promise(r => setTimeout(r, ms)); },

    _append(role, html) {
      const row = document.createElement('div'); row.className = 'd-flex flex-column';
      const b = document.createElement('div'); b.className = 'iad-msg ' + (role === 'user' ? 'iad-user' : 'iad-bot');
      if (role === 'user') b.textContent = html; else b.innerHTML = html;
      const m = document.createElement('div'); m.className = 'iad-meta'; m.textContent = role === 'user' ? this._displayNameFromEmail() : 'Camila.AI';
      row.appendChild(b); row.appendChild(m); this._outBox().appendChild(row);
      const sc = this._panel().querySelector('.body'); sc.scrollTop = sc.scrollHeight;
    },

    _appendBotStreaming(raw) {
      const cleaned = this._cleanGroq(raw); if (!cleaned || !cleaned.trim()) return '';
      const blocks = cleaned.split(/\n{2,}/).map(s => s.trim()).filter(Boolean);
      const row = document.createElement('div'); row.className = 'd-flex flex-column';
      const box = document.createElement('div'); box.className = 'iad-msg iad-bot';
      const meta = document.createElement('div'); meta.className = 'iad-meta'; meta.textContent = 'Camila.AI';
      row.appendChild(box); row.appendChild(meta); this._outBox().appendChild(row);
      blocks.forEach(b => { const isTitle = /^###\s*/.test(b); const text = b.replace(/^###\s*/, ''); const el = document.createElement(isTitle ? 'h5' : 'p'); if (isTitle) el.className = 'iad-h'; el.textContent = text; box.appendChild(el); });
      const sc = this._panel().querySelector('.body'); requestAnimationFrame(() => { sc.scrollTop = sc.scrollHeight; });
      return cleaned;
    },

    _dotsStart() {
      const wrap = document.createElement('div'); wrap.className = 'iad-msg iad-bot iad-loading';
      wrap.innerHTML = `Aguardando <span class="iad-dots"><span class="dot on">.</span><span class="dot">.</span><span class="dot">.</span></span>`;
      this._outBox().appendChild(wrap); let t = 0;
      const int = setInterval(() => { wrap.querySelectorAll('.dot').forEach(x => x.classList.remove('on')); wrap.querySelectorAll('.dot')[t % 3].classList.add('on'); t++; const sc = this._panel().querySelector('.body'); sc.scrollTop = sc.scrollHeight; }, 260);
      this._dotsStop._int = int; this._dotsStop._el = wrap;
    },
    _dotsStop() { if (this._dotsStop._int) { clearInterval(this._dotsStop._int); this._dotsStop._int = null; } if (this._dotsStop._el) { this._dotsStop._el.remove(); this._dotsStop._el = null; } },

    /* ===================== Estrutura de envio ===================== */
    async _sendToIA(userQuery, { force = false } = {}) {
      this._dotsStart();
      try {
        const snapshot = await this._state.opts.getPayload({ userQuery });
        const now = Date.now();
        const hash = this._stableHash(snapshot);
        const ageMin = (now - this._state.lastSnapshotAt) / 60000;

        const mustFull = this._CFG.ALWAYS_FULL || this._state.forceFullNext || force || !this._state.lastSnapshotHash || hash !== this._state.lastSnapshotHash || ageMin >= this._CFG.REFRESH_FULL_AFTER_MIN;

        this._state.forceFullNext = false;
        this._state.lastSnapshotAt = now;
        this._state.lastSnapshotHash = hash;

        if (mustFull) {
          await this._sendPossiblyChunked(snapshot);
        } else {
          // modo inteligente: envia apenas prompt + referência do hash
          const minimal = {
            prompt: (snapshot.prompt || ''),
            qa: snapshot.qa || null,
            contexto: 'qa',
            meta: Object.assign({}, snapshot.meta || {}, { snapshot_hash: hash, page: (snapshot.meta && snapshot.meta.page) || 'kpi_v2' }),
            prefs: Object.assign({}, snapshot.prefs || {})
          };
          if (this._state.brief) this._mergeBriefInstruction(minimal, this._state.opts.briefInstruction, 340);
          const out = await this._callIA(minimal, this._state.opts.timeoutMs);
          this._appendBotStreaming(out);
        }
      } catch (err) {
        const msg = `Não foi possível responder${err?.message ? ` — possível motivo: ${err.message}` : ''}.`;
        this._append('bot', `<span class="text-warning">${msg}</span>`);
      } finally { this._dotsStop(); }
    },

    async _sendPossiblyChunked(fullPayload) {
      // aplica instrução de resumo se marcado
      const basePrefs = Object.assign({}, fullPayload.prefs || {});
      const full = this._state.brief ? this._mergeBriefInstruction(fullPayload, this._state.opts.briefInstruction, 340) : fullPayload;

      // calcula tamanho aproximado
      const approx = this._approxSize(full);
      const blocks = Array.isArray(full.blocks) ? full.blocks : [];

      if (approx <= this._CFG.MAX_PAYLOAD_CHARS || blocks.length <= this._CFG.CHUNK_BLOCKS) {
        const out = await this._callIA(full, this._state.opts.timeoutMs);
        this._appendBotStreaming(out);
        return;
      }

      // chunking por blocos
      const groups = [];
      for (let i = 0; i < blocks.length; i += this._CFG.CHUNK_BLOCKS) groups.push(blocks.slice(i, i + this._CFG.CHUNK_BLOCKS));

      const partialTexts = [];
      for (let i = 0; i < groups.length; i++) {
        const p = Object.assign({}, full, { blocks: groups[i] });
        p.prompt = `${full.prompt || ''}\n\n[Parte ${i + 1}/${groups.length}]`;
        p.prefs = Object.assign({}, basePrefs);
        if (this._state.brief) this._mergeBriefInstruction(p, this._state.opts.briefInstruction, 420);
        const out = await this._callIA(p, this._state.opts.timeoutMs * 1.5);
        const cleaned = this._cleanGroq(out);
        partialTexts.push(cleaned);
        this._append('bot', `<span class="iad-hint">Parcial ${i + 1}/${groups.length} recebida.</span>`);
        this._appendBotStreaming(cleaned);
      }

      // síntese final
      const synthPayload = {
        prompt: (this._state.brief
          ? `${this._state.opts.briefInstruction}\n\nResuma os trechos abaixo em UMA única resposta com foco executivo.`
          : 'Consolide os trechos abaixo em UMA resposta coerente, integrando tendências, riscos e oportunidades.'),
        contexto: 'sintese',
        parts: partialTexts.slice(0, this._CFG.SYNTH_PARTS_LIMIT)
      };
      const outS = await this._callIA(synthPayload, this._state.opts.timeoutMs * 2);
      this._append('bot', '<span class="iad-hint">Síntese final:</span>');
      this._appendBotStreaming(outS);
    },

    /* ===================== Utilidades ===================== */
    _stableHash(obj) {
      // hash simples e determinístico
      const s = typeof obj === 'string' ? obj : JSON.stringify(obj, Object.keys(obj).sort());
      let h = 2166136261 >>> 0; // FNV-1a 32-bit
      for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = (h * 16777619) >>> 0; }
      return ('0000000' + h.toString(16)).slice(-8);
    },

    _approxSize(obj) {
      try { return JSON.stringify(obj).length; } catch { return 0; }
    },

    _mergeBriefInstruction(payload, instruction, maxTokens) {
      const p = Object.assign({}, payload);
      const inst = `[RESUMO-ESTRITO]: ${instruction} Priorize síntese.`;
      if (typeof p.prompt === 'string') p.prompt = `${inst}\n\n${p.prompt}`; else p.prompt = inst;
      p.prefs = Object.assign({}, p.prefs, { max_tokens: Math.max(120, Math.min(700, maxTokens || 340)), temperature: 0.2 });
      return p;
    },

    /* ===================== Networking ===================== */
    async _callIA(payload, timeout) {
      const API = this._state.opts.apiUrl;
      const controller = new AbortController();
      const to = setTimeout(() => controller.abort(), timeout || this._state.opts.timeoutMs);
      let r; try {
        r = await fetch(API, { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', body: JSON.stringify(typeof payload === 'string' ? { prompt: payload } : payload), signal: controller.signal });
      } finally { clearTimeout(to); }
      if (!r.ok) { let t = ''; try { t = await r.text(); } catch { } throw new Error(`HTTP ${r.status}${t ? ' — ' + t : ''}`); }
      let resText = await r.text(); let res = null; try { res = JSON.parse(resText); } catch { }
      if (!res) return resText;
      if (res && res.content_mode === 'free_text') return res.text || '';
      if (res && res.content_mode === 'json') return (res.data && (res.data.html || res.data.livre)) || JSON.stringify(res.data);
      return typeof res === 'string' ? res : (res.html || res.livre || JSON.stringify(res));
    },

    _exportToPDF() {
      if (typeof html2pdf === 'undefined') { alert('html2pdf.js não carregado.'); return; }
      const node = document.getElementById('iadOut'); if (!node) return;
      const opt = { margin: [0, 0, 0, 0], filename: `IA-Conversa_${new Date().toISOString().slice(0, 10)}.pdf`, image: { type: 'jpeg', quality: 0.98 }, html2canvas: { scale: 2, useCORS: true, backgroundColor: null, windowWidth: node.scrollWidth, windowHeight: node.scrollHeight }, jsPDF: { unit: 'px', format: 'a4', orientation: 'portrait' }, pagebreak: { mode: ['css', 'legacy'] } };
      html2pdf().set(opt).from(node).save();
    },

    /* ===================== Sanitização ===================== */
    _cleanGroq(raw) {
      let s = String(raw || '');
      s = s.replace(/\r\n/g, '\n').replace(/\\n/g, '\n').replace(/\\t/g, '  ');
      s = s.replace(/```[\s\S]*?```/g, ' ');
      s = s.replace(/^\s*[|+\-─═┌┐└┘┼┤├┬┴]+.*$/gm, ' ');
      s = s.replace(/^\s*\|\s*(?:[^|\n]+\|)+\s*$/gm, ' ');
      s = s.replace(/^.*\|.*\|.*$/gm, ' ');
      s = s.replace(/\\begin\{aligned\}[\s\S]*?\\end\{aligned\}/g, ' ');
      s = s.replace(/\$\$[\s\S]*?\$\$/g, ' ');
      s = s.replace(/\\\[[\s\S]*?\\\]/g, ' ');
      s = s.replace(/\\\([\s\S]*?\\\)/g, ' ');
      s = s.replace(/[ \t]+\n/g, '\n');
      s = s.replace(/###\s*/g, '\n\n### ');
      s = s.replace(/\n{3,}/g, '\n\n');
      return s.trim();
    }
  };

  window.ChatIA = ChatIA;
})();
