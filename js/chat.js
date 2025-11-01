/* chat.js — Painel de IA reutilizável (v2)
 * Objetivo: enviar SEMPRE o snapshot completo por padrão
 *  - "IA Resumida": agora resume após receber a resposta completa
 *  - Parseia respostas JSON e evita mostrar { ... } no chat
 *  - Parágrafos e títulos para leitura fluida
 * API: ChatIA.init({
 *   getPayload, apiUrl, mountAfterSelector, title, briefInstruction,
 *   timeoutMs, briefDefault
 * })
 */
(function () {
  const ChatIA = {
    _CFG: {
      MAX_RETRIES: 5,
      RETRY_DELAY_MS: 1200,
      MAX_PAYLOAD_CHARS: 120_000,
      CHUNK_BLOCKS: 4,
      SYNTH_PARTS_LIMIT: 12,
      REFRESH_FULL_AFTER_MIN: 5
    },

    _state: {
      mounted: false,
      brief: true,
      opts: null,
      lastRaw: '',
      lastSnapshotHash: null,
      lastSnapshotAt: 0
    },

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
.iad-loading .dot{opacity:.35}.iad-loading .dot.on{opacity:1}
#iadBrief{display:inline-flex;align-items:center;gap:6px}
#iadBrief input{margin:0}
.iad-hint{font-style:italic;color:#cfcfcf}
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
      regen.addEventListener('click', async () => { await this._sendToIA(null, { force: true }); });
      pdfBtn.addEventListener('click', () => this._exportToPDF());

      briefLbl.addEventListener('click', (e) => {
        if (e.target.id !== 'iadBriefChk') { e.preventDefault(); briefChk.checked = !briefChk.checked; }
        this._state.brief = !!briefChk.checked;
        const msg = this._state.brief ? 'IA Resumida: respostas serão consolidadas e resumidas.' : 'IA Completa: respostas integrais.';
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

    /* ===================== Helpers ===================== */
    _displayNameFromEmail() {
      const email = (window.USER_EMAIL || '').trim();
      if (!email || !/@/.test(email)) return 'Você';
      const first = email.split('@')[0];
      return first ? first.charAt(0).toUpperCase() + first.slice(1) : 'Você';
    },

    _append(role, html) {
      const row = document.createElement('div'); row.className = 'd-flex flex-column';
      const b = document.createElement('div'); b.className = 'iad-msg ' + (role === 'user' ? 'iad-user' : 'iad-bot');
      if (role === 'user') b.textContent = html; else b.innerHTML = html;
      const m = document.createElement('div'); m.className = 'iad-meta'; m.textContent = role === 'user' ? this._displayNameFromEmail() : 'Camila.AI';
      row.appendChild(b); row.appendChild(m); this._outBox().appendChild(row);
      const sc = this._panel().querySelector('.body'); sc.scrollTop = sc.scrollHeight;
    },

    _appendBotStreaming(raw) {
      const cleaned = this._cleanGroq(raw);
      if (!cleaned || !cleaned.trim()) return '';

      const blocks = cleaned.split(/\n{2,}/).map(s => s.trim()).filter(Boolean);
      const row = document.createElement('div'); row.className = 'd-flex flex-column';
      const box = document.createElement('div'); box.className = 'iad-msg iad-bot';
      const meta = document.createElement('div'); meta.className = 'iad-meta'; meta.textContent = 'Camila.AI';
      row.appendChild(box); row.appendChild(meta); this._outBox().appendChild(row);








      
      blocks.forEach(b => {
        const isTitle = /^###\s*/.test(b);
        const text = b.replace(/^###\s*/, '');
        const el = document.createElement(isTitle ? 'h5' : 'p');
        if (isTitle) el.className = 'iad-h';
        el.textContent = text;
        box.appendChild(el);
      });

      // dispara o efeito de revelar após montar todo o conteúdo
      requestAnimationFrame(() => this._typeReveal(box));

      // ...cria 'box', adiciona blocos...
      // dispare a animação CSS após montar o conteúdo
      requestAnimationFrame(() => this._typeReveal(box));

      const sc = this._panel().querySelector('.body');
      requestAnimationFrame(() => { sc.scrollTop = sc.scrollHeight; });
      return cleaned;

    },





    // Efeito “digitar” por CSS mask (sem stream): anima a var CSS --reveal
_typeReveal(container, baseMs = 25) {
  if (!container) return;
  // aplica nos filhos (h5, p) da bolha do bot, para revelar bloco a bloco
  const targets = container.querySelectorAll('h5, p');
  targets.forEach((el) => {
    // seta estado inicial
    el.classList.add('iad-type', 'caret');
    el.style.setProperty('--reveal', '0%');

    // calcula duração em função do tamanho do texto
    const chars = (el.textContent || '').length || 1;
    const dur = Math.min(8000, Math.max(600, chars * baseMs));

    // anima a variável CSS --reveal de 0% a 100% em "steps"
    const anim = el.animate(
      [{ '--reveal': '0%' }, { '--reveal': '100%' }],
      { duration: dur, easing: `steps(${Math.min(chars, 2000)}, end)` }
    );

    anim.onfinish = () => {
      el.classList.remove('caret');
      el.style.setProperty('--reveal', '100%'); // mantém visível após a animação
    };
  });
},





    _dotsStart() {
      const wrap = document.createElement('div'); wrap.className = 'iad-msg iad-bot iad-loading';
      wrap.innerHTML = `Aguardando <span class="iad-dots"><span class="dot on">.</span><span class="dot">.</span><span class="dot">.</span></span>`;
      this._outBox().appendChild(wrap);
      let t = 0;
      const int = setInterval(() => {
        wrap.querySelectorAll('.dot').forEach(x => x.classList.remove('on'));
        wrap.querySelectorAll('.dot')[t % 3].classList.add('on'); t++;
        const sc = this._panel().querySelector('.body'); sc.scrollTop = sc.scrollHeight;
      }, 260);
      this._dotsStop._int = int; this._dotsStop._el = wrap;
    },
    _dotsStop() {
      if (this._dotsStop._int) { clearInterval(this._dotsStop._int); this._dotsStop._int = null; }
      if (this._dotsStop._el) { this._dotsStop._el.remove(); this._dotsStop._el = null; }
    },

    /* ===================== Envio ===================== */
    async _sendToIA(userQuery, { force = false } = {}) {
      this._dotsStart();
      try {
        // 1) sempre envia o snapshot completo
        const basePayload = await this._state.opts.getPayload({ userQuery });

        // 2) chamada principal
        const raw = await this._callIA(basePayload, this._state.opts.timeoutMs);
        this._dotsStop();

        // 3) normaliza texto. Se vier JSON, extrai campos úteis.
        const fullText = this._coerceText(raw);
        const shownFull = this._appendBotStreaming(fullText);

        // 4) se IA Resumida marcada, dispara um segundo round para resumir o texto já obtido
        if (this._state.brief) {
          const resumo = await this._summarizeText(fullText);
          this._append('bot', `<span class="iad-hint">Resumo gerado a partir da resposta integral.</span>`);
          this._appendBotStreaming(resumo);
        }

          function typeReveal(el, baseMs = 25){
            const chars = (el.textContent || '').length || 1;
            const dur = Math.min(8000, Math.max(600, chars * baseMs));
            el.classList.add('iad-type','caret');
            const anim = el.animate(
             [{ '--reveal': '0%' }, { '--reveal': '100%' }],
             { duration: dur, easing: `steps(${Math.min(chars, 2000)}, end)` }
           );
           anim.onfinish = () => el.classList.remove('caret');
          }

      } catch (err) {
        this._dotsStop();
        const msg = `Não foi possível responder${err?.message ? ` — possível motivo: ${err.message}` : ''}.`;
        this._append('bot', `<span class="text-warning">${msg}</span>`);
      }
    },

    async _summarizeText(text) {
      // prompt enxuto para sumarização e formatação em PT-BR
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
      // 1) se já veio string, tenta detectar JSON e extrair campos relevantes
      const s = String(out ?? '').trim();
      if (this._looksLikeJson(s)) {
        try {
          const obj = JSON.parse(s);
          const picked = this._pickFields(obj, [
            'html','livre','text','analysis','resumo','summary','conteudo','content','answer'
          ]);
          if (picked) return this._sanitizeText(picked);
          return this._sanitizeText(JSON.stringify(obj)); // fallback legível
        } catch { /* segue fluxo */ }
      }
      return this._sanitizeText(s);
    },
    _looksLikeJson(s) { return s.startsWith('{') || s.startsWith('['); },
    _pickFields(obj, keys) {
      if (obj == null) return '';
      for (const k of keys) if (typeof obj?.[k] === 'string' && obj[k].trim()) return obj[k];
      // merge básico de valores string em 1º nível
      const parts = [];
      Object.entries(obj).forEach(([k, v]) => { if (typeof v === 'string' && v.trim()) parts.push(v.trim()); });
      return parts.length ? parts.join('\n\n') : '';
    },
    _sanitizeText(s) {
      let t = String(s || '');
      t = t.replace(/\r\n/g, '\n').replace(/\u00A0/g, ' ');
      t = t.replace(/```[\s\S]*?```/g, ' ');
      t = t.replace(/^\s*\|.*\|\s*$/gm, ' ');
      t = t.replace(/[ \t]+\n/g, '\n');
      t = t.replace(/([.,;:!?])(?!\s|$)/g, '$1 ');
      t = t.replace(/\n{3,}/g, '\n\n').trim();
      return t;
    },

    /* ===================== Networking ===================== */
    // Efeito “digitar” sem stream: revela o texto completo usando máscara em steps
    _typeReveal(el, baseMs = 25) {
      if (!el) return;
      const chars = (el.textContent || '').length || 1;
      const dur = Math.min(8000, Math.max(600, chars * baseMs));
      el.classList.add('iad-type', 'caret');
      const anim = el.animate(
        [{ '--reveal': '0%' }, { '--reveal': '100%' }],
        { duration: dur, easing: `steps(${Math.min(chars, 2000)}, end)` }
      );
      anim.onfinish = () => el.classList.remove('caret');
    },
    
    
    
    
    async _callIA(payload, timeout) {
      const API = this._state.opts.apiUrl;
      const controller = new AbortController();
      const to = setTimeout(() => controller.abort(), timeout || this._state.opts.timeoutMs);
      let r; try {
        r = await fetch(API, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin',
          body: JSON.stringify(typeof payload === 'string' ? { prompt: payload } : payload),
          signal: controller.signal
        });
      } finally { clearTimeout(to); }
      if (!r.ok) { let t = ''; try { t = await r.text(); } catch { } throw new Error(`HTTP ${r.status}${t ? ' — ' + t : ''}`); }
      let resText = await r.text(); let res = null; try { res = JSON.parse(resText); } catch { }
      if (!res) return resText;
      if (res && res.content_mode === 'free_text') return res.text || '';
      if (res && res.content_mode === 'json') return (res.data && (res.data.html || res.data.livre || res.data.text)) || JSON.stringify(res.data);
      return typeof res === 'string' ? res : (res.html || res.livre || JSON.stringify(res));
    },

    _exportToPDF() {
      if (typeof html2pdf === 'undefined') { alert('html2pdf.js não carregado.'); return; }
      const node = document.getElementById('iadOut'); if (!node) return;
      const opt = { margin: [0, 0, 0, 0], filename: `IA-Conversa_${new Date().toISOString().slice(0, 10)}.pdf`,
        image: { type: 'jpeg', quality: 0.98 },
        html2canvas: { scale: 2, useCORS: true, backgroundColor: null, windowWidth: node.scrollWidth, windowHeight: node.scrollHeight },
        jsPDF: { unit: 'px', format: 'a4', orientation: 'portrait' },
        pagebreak: { mode: ['css', 'legacy'] } };
      html2pdf().set(opt).from(node).save();
    },

    /* ===================== Sanitização base p/ render ===================== */
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
