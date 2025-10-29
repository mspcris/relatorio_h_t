/* chat.js — Painel de IA reutilizável
 * API: ChatIA.init({
 *   getPayload,                // async ({ userQuery }) => payload COMPLETO com TODOS os dados da página
 *   apiUrl,                    // POST endpoint
 *   mountAfterSelector,        // seletor para ancorar o botão
 *   title,                     // título do painel
 *   briefInstruction,          // instrução dura para resumo
 *   timeoutMs,                 // tempo base
 *   briefDefault               // bool: inicia com IA Resumida marcada
 * })
 * Requer: window.fetch, html2pdf.js (opcional para PDF)
 */
(function () {
  const ChatIA = {
    _state: {
      mounted: false,
      brief: true,
      opts: null,
      retried: false,
    },

    init(options) {
      const defaults = {
        apiUrl: (window.IA_API_URL || '/ia/analisar'),
        mountAfterSelector: '#btnComparar',
        title: 'IA — Análise Completa & Chat',
        briefInstruction: 'Responda em 200 a 300 caracteres, no máximo 3 linhas. Seja direto, executivo e objetivo.',
        timeoutMs: 180000, // base maior
        briefDefault: true,
        getPayload: async ({ userQuery }) => ({ prompt: userQuery || 'ok' })
      };
      this._state.opts = Object.assign({}, defaults, options || {});
      this._state.brief = !!this._state.opts.briefDefault;

      this._injectStyles();
      this._injectUI();
      this._wireUI();
      this._state.mounted = true;

      // watchdog: garante o botão mesmo após re-render
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
#iaDeepPanel .iad-user{background:#16324f;border:1px solid #275a8a;align-self:flex-end} /* usuário à direita */
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

        // mensagem inicial simulando a IA
        this._append('bot', 'Olá, eu sou a Camila.AI e vou te ajudar a analisar os dados que estão nesta página. Toque em Iniciar ou digite algo sobre os dados aqui constantes.');
      }
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

      btn.addEventListener('click', () => {
        const open = (panel.style.display === 'none');
        panel.style.display = open ? 'block' : 'none';
        this._toggleBtn(btn, open);
        // NENHUM envio automático aqui
      });

      close.addEventListener('click', () => {
        panel.style.display = 'none';
        this._toggleBtn(btn, false);
      });

      regen.addEventListener('click', async () => {
        await this._sendToIA(null, { force: true });
      });

      pdfBtn.addEventListener('click', () => this._exportToPDF());

      // marcar/desmarcar precisa escrever no chat
      briefLbl.addEventListener('click', (e) => {
        if (e.target.id !== 'iadBriefChk') { e.preventDefault(); briefChk.checked = !briefChk.checked; }
        this._state.brief = !!briefChk.checked;
        const msg = this._state.brief
          ? 'Resposta fora do escopo. Marcando bit IA Resumida.'
          : 'Resposta fora do escopo. Desmarcando bit IA Resumida.';
        this._append('bot', `<span class="iad-hint">${msg}</span>`);
      });

      // Iniciar/Enviar
      send.addEventListener('click', async () => {
        let q = (input.value || '').trim();

        // primeiro clique sem texto: apenas alterna rótulo
        if (!q && send.textContent === 'Iniciar') {
          send.textContent = 'Enviar';
          input.focus();
          return;
        }

        if (!q) return;
        input.value = '';
        this._append('user', q);
        await this._sendToIA(q);
      });

      input.addEventListener('input', () => {
        if (send.textContent !== 'Enviar') send.textContent = 'Enviar';
      });

      input.addEventListener('keydown', e => {
        if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send.click(); }
      });
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
      const email = (window.USER_EMAIL || '').trim(); // defina window.USER_EMAIL no HTML após login
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
      const sc = this._panel().querySelector('.body');
      sc.scrollTop = sc.scrollHeight;
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

      const sc = this._panel().querySelector('.body');
      requestAnimationFrame(() => { sc.scrollTop = sc.scrollHeight; });
      return cleaned;
    },

    _dotsStart() {
      const wrap = document.createElement('div');
      wrap.className = 'iad-msg iad-bot iad-loading';
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

    /* ===================== Envio + Fallback ===================== */
    async _sendToIA(userQuery, { force = false } = {}) {
      let reason = '';
      this._state.retried = false;

      try {
        this._dotsStart();

        let payload = await this._state.opts.getPayload({ userQuery });

        // Dominância do RESUMO quando marcado
        if (this._state.brief) payload = this._mergeBriefInstruction(payload, this._state.opts.briefInstruction, 340);

        // “Regerar” sem prompt do usuário
        if (force && !userQuery && this._state.brief) {
          payload = this._mergeBriefInstruction(payload, this._state.opts.briefInstruction, 340);
        }

        // 1ª tentativa normal
        let out = await this._callIA(payload, this._state.opts.timeoutMs);
        this._dotsStop();
        let text = this._appendBotStreaming(out);

        // Se vazio, fallback agressivo
        if (!text || !text.trim()) {
          reason = 'resposta vazia';
          text = await this._fallback(payload, reason);
        }

        if (!text || !text.trim()) {
          // Último recurso: mensagem tratada
          const msg = `Não foi possível responder${reason ? ` — possível motivo: ${reason}` : ''}.`;
          this._append('bot', `<span class="text-warning">${msg}</span>`);
        }
      } catch (err) {
        this._dotsStop();
        const msg = `Não foi possível responder${err?.message ? ` — possível motivo: ${err.message}` : ''}.`;
        this._append('bot', `<span class="text-warning">${msg}</span>`);
      }
    },

    async _fallback(payload, reasonIn) {
      // 1) alterna bit IA Resumida e informa
      const briefChk = this._q('#iadBriefChk');
      let note = '';
      if (this._state.brief) {
        briefChk.checked = false; this._state.brief = false;
        note = 'Resposta fora do escopo. Desmarcando bit IA Resumida.';
      } else {
        briefChk.checked = true; this._state.brief = true;
        note = 'Resposta fora do escopo. Marcando bit IA Resumida.';
      }
      this._append('bot', `<span class="iad-hint">${note}</span>`);

      // 2) reenvia com timeout maior
      const p2 = this._state.brief
        ? this._mergeBriefInstruction(payload, this._state.opts.briefInstruction, 420)
        : payload;

      try {
        this._dotsStart();
        const out2 = await this._callIA(p2, this._state.opts.timeoutMs * 2);
        this._dotsStop();
        const text2 = this._appendBotStreaming(out2);
        if (text2 && text2.trim()) return text2;
      } catch (e) {
        this._dotsStop();
      }

      // 3) chunk por blocos se existir muito dado (divide payload.blocks)
      if (Array.isArray(payload.blocks) && payload.blocks.length > 4) {
        const groups = [];
        const chunkSize = 4;
        for (let i = 0; i < payload.blocks.length; i += chunkSize) {
          groups.push(payload.blocks.slice(i, i + chunkSize));
        }
        const parts = [];
        for (let i = 0; i < groups.length; i++) {
          const pi = Object.assign({}, payload, {
            blocks: groups[i],
            prompt: (payload.prompt || '') + `\n\n[Parte ${i + 1}/${groups.length}]`,
          });
          if (this._state.brief) this._mergeBriefInstruction(pi, this._state.opts.briefInstruction, 420);
          try {
            this._dotsStart();
            const oi = await this._callIA(pi, this._state.opts.timeoutMs * 2.5);
            this._dotsStop();
            const ti = this._cleanGroq(oi);
            if (ti && ti.trim()) parts.push(ti);
          } catch { this._dotsStop(); }
        }
        if (parts.length) {
          const synthPayload = {
            prompt: (this._state.brief
              ? `${this._state.opts.briefInstruction}\n\nResuma os trechos abaixo em UMA única resposta.`
              : 'Consolide os trechos abaixo em UMA resposta coerente:'),
            contexto: 'sintese',
            parts: parts.slice(0, 10) // segurança
          };
          try {
            this._dotsStart();
            const outS = await this._callIA(synthPayload, this._state.opts.timeoutMs * 2);
            this._dotsStop();
            const txt = this._appendBotStreaming(outS);
            if (txt && txt.trim()) return txt;
          } catch { this._dotsStop(); }
        }
      }

      return '';
    },

    _mergeBriefInstruction(payload, instruction, maxTokens) {
      const p = Object.assign({}, payload);
      const inst = `[RESUMO-ESTRITO]: ${instruction} Priorize síntese.`;
      if (typeof p.prompt === 'string') p.prompt = `${inst}\n\n${p.prompt}`;
      else p.prompt = inst;
      p.prefs = Object.assign({}, p.prefs, { max_tokens: Math.max(120, Math.min(700, maxTokens || 340)), temperature: 0.2 });
      return p;
    },

    /* ===================== Networking ===================== */
    async _callIA(payload, timeout) {
      const API = this._state.opts.apiUrl;
      const controller = new AbortController();
      const to = setTimeout(() => controller.abort(), timeout || this._state.opts.timeoutMs);
      let r;
      try {
        r = await fetch(API, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin',
          body: JSON.stringify(typeof payload === 'string' ? { prompt: payload } : payload),
          signal: controller.signal
        });
      } finally { clearTimeout(to); }
      if (!r.ok) {
        let t = ''; try { t = await r.text(); } catch { }
        // retorna texto de erro para diagnóstico do fallback
        throw new Error(`HTTP ${r.status}${t ? ' — ' + t : ''}`);
      }
      let resText = await r.text();
      let res = null;
      try { res = JSON.parse(resText); } catch { /* mantém texto */ }
      if (!res) return resText;
      if (res && res.content_mode === 'free_text') return res.text || '';
      if (res && res.content_mode === 'json') return (res.data && (res.data.html || res.data.livre)) || JSON.stringify(res.data);
      return typeof res === 'string' ? res : (res.html || res.livre || JSON.stringify(res));
    },

    _exportToPDF() {
      if (typeof html2pdf === 'undefined') { alert('html2pdf.js não carregado.'); return; }
      const node = document.getElementById('iadOut'); if (!node) return;
      const opt = {
        margin: [0, 0, 0, 0],
        filename: `IA-Conversa_${new Date().toISOString().slice(0, 10)}.pdf`,
        image: { type: 'jpeg', quality: 0.98 },
        html2canvas: { scale: 2, useCORS: true, backgroundColor: null, windowWidth: node.scrollWidth, windowHeight: node.scrollHeight },
        jsPDF: { unit: 'px', format: 'a4', orientation: 'portrait' },
        pagebreak: { mode: ['css', 'legacy'] }
      };
      html2pdf().set(opt).from(node).save();
    },

    /* ===================== Sanitização ===================== */
    _cleanGroq(raw) {
      let s = String(raw || '');
      s = s.replace(/\r\n/g, '\n').replace(/\\n/g, '\n').replace(/\\t/g, '  ');
      // remove blocos de código
      s = s.replace(/```[\s\S]*?```/g, ' ');
      // remove Tabelas ASCII
      s = s.replace(/^\s*[|+\-─═┌┐└┘┼┤├┬┴]+.*$/gm, ' ');
      s = s.replace(/^\s*\|\s*(?:[^|\n]+\|)+\s*$/gm, ' ');
      s = s.replace(/^.*\|.*\|.*$/gm, ' ');
      // remove LaTeX
      s = s.replace(/\\begin\{aligned\}[\s\S]*?\\end\{aligned\}/g, ' ');
      s = s.replace(/\$\$[\s\S]*?\$\$/g, ' ');
      s = s.replace(/\\\[[\s\S]*?\\\]/g, ' ');
      s = s.replace(/\\\([\s\S]*?\\\)/g, ' ');
      // headings e quebras
      s = s.replace(/[ \t]+\n/g, '\n');
      s = s.replace(/###\s*/g, '\n\n### ');
      s = s.replace(/\n{3,}/g, '\n\n');
      return s.trim();
    }
  };

  window.ChatIA = ChatIA;
})();
