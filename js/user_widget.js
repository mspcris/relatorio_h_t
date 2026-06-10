/**
 * user_widget.js — Widget de usuário no canto superior direito (todas as páginas).
 *
 * Injetado globalmente pelo app.py antes de </body>. Mostra:
 *   - Avatar: foto do idCamim (claim "picture") se existir; senão, círculo com iniciais.
 *   - Nome do usuário.
 *   - Dropdown ao clicar com:
 *       • "Acessar minha área do IDCamim"  → área do usuário no idCamim
 *       • "Sair"                           → POST /session/logout
 *
 * Fonte dos dados: GET /session/me  → { email, nome, foto_url, is_admin }
 */
(function () {
  'use strict';

  var IDCAMIM_AREA = 'https://idcamim.camim.com.br/conta/login?returnTo=%2Fconta';

  function iniciais(nome, email) {
    var base = (nome || '').trim();
    if (base) {
      var partes = base.split(/\s+/).filter(Boolean);
      if (partes.length === 1) return partes[0].slice(0, 2).toUpperCase();
      return (partes[0][0] + partes[partes.length - 1][0]).toUpperCase();
    }
    return ((email || '?').trim()[0] || '?').toUpperCase();
  }

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function injectStyles() {
    if (document.getElementById('user-widget-style')) return;
    var css = ''
      + '.uw-wrap{margin-left:auto;display:flex;align-items:center;position:relative;padding-right:.5rem}'
      + '.uw-btn{display:flex;align-items:center;gap:.5rem;background:none;border:0;cursor:pointer;'
      +   'padding:.25rem .5rem;border-radius:24px;color:inherit;font:inherit}'
      + '.uw-btn:hover{background:rgba(0,0,0,.05)}'
      + '.uw-avatar{width:32px;height:32px;border-radius:50%;object-fit:cover;flex:0 0 32px;'
      +   'display:flex;align-items:center;justify-content:center;background:#6c757d;color:#fff;'
      +   'font-size:.8rem;font-weight:600;overflow:hidden}'
      + '.uw-avatar img{width:100%;height:100%;object-fit:cover}'
      + '.uw-name{font-weight:600;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
      + '.uw-caret{font-size:.7rem;opacity:.6}'
      + '.uw-menu{position:absolute;top:calc(100% + 6px);right:.5rem;min-width:240px;background:#fff;'
      +   'border:1px solid rgba(0,0,0,.12);border-radius:8px;box-shadow:0 6px 24px rgba(0,0,0,.15);'
      +   'padding:.4rem 0;z-index:1080;display:none}'
      + '.uw-menu.open{display:block}'
      + '.uw-head{padding:.5rem .9rem;border-bottom:1px solid rgba(0,0,0,.08);margin-bottom:.3rem}'
      + '.uw-head .n{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
      + '.uw-head .e{font-size:.78rem;color:#6c757d;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}'
      + '.uw-item{display:flex;align-items:center;gap:.6rem;width:100%;padding:.55rem .9rem;'
      +   'background:none;border:0;text-align:left;cursor:pointer;color:#212529;font:inherit;text-decoration:none}'
      + '.uw-item:hover{background:#f1f3f5;color:#212529;text-decoration:none}'
      + '.uw-item i{width:18px;text-align:center;opacity:.75}'
      + '.uw-item.danger{color:#c0392b}'
      + '@media (max-width:575px){.uw-name{display:none}}';
    var st = document.createElement('style');
    st.id = 'user-widget-style';
    st.textContent = css;
    document.head.appendChild(st);
  }

  function avatarHTML(data) {
    var ini = esc(iniciais(data.nome, data.email));
    if (data.foto_url) {
      // onerror: se a imagem falhar (URL quebrada/privada), cai pras iniciais sem quebrar o layout.
      return '<span class="uw-avatar" data-ini="' + ini + '"><img src="' + esc(data.foto_url) + '" alt="" '
        + 'onerror="this.parentNode.textContent=this.parentNode.getAttribute(\'data-ini\')"></span>';
    }
    return '<span class="uw-avatar" data-ini="' + ini + '">' + ini + '</span>';
  }

  function build(data) {
    var header = document.querySelector('.main-header');
    if (!header || document.querySelector('.uw-wrap')) return;

    var nome = data.nome || data.email || 'Usuário';

    var wrap = document.createElement('div');
    wrap.className = 'uw-wrap';
    wrap.innerHTML =
        '<button type="button" class="uw-btn" id="uwToggle" aria-haspopup="true" aria-expanded="false">'
      +   avatarHTML(data)
      +   '<span class="uw-name">' + esc(nome) + '</span>'
      +   '<i class="fas fa-chevron-down uw-caret"></i>'
      + '</button>'
      + '<div class="uw-menu" id="uwMenu" role="menu">'
      +   '<div class="uw-head"><div class="n">' + esc(nome) + '</div>'
      +     '<div class="e">' + esc(data.email || '') + '</div></div>'
      +   '<a class="uw-item" href="' + IDCAMIM_AREA + '" target="_blank" rel="noopener" role="menuitem">'
      +     '<i class="fas fa-id-badge"></i><span>Acessar minha área do IDCamim</span></a>'
      +   '<form method="POST" action="/session/logout" style="margin:0">'
      +     '<button type="submit" class="uw-item danger" role="menuitem">'
      +       '<i class="fas fa-sign-out-alt"></i><span>Sair</span></button>'
      +   '</form>'
      + '</div>';

    header.appendChild(wrap);

    var toggle = wrap.querySelector('#uwToggle');
    var menu   = wrap.querySelector('#uwMenu');
    toggle.addEventListener('click', function (e) {
      e.stopPropagation();
      var open = menu.classList.toggle('open');
      toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    });
    document.addEventListener('click', function (e) {
      if (!wrap.contains(e.target)) {
        menu.classList.remove('open');
        toggle.setAttribute('aria-expanded', 'false');
      }
    });
  }

  function init() {
    // Só faz sentido em páginas com a navbar AdminLTE (.main-header).
    if (!document.querySelector('.main-header')) return;
    injectStyles();
    fetch('/session/me', { credentials: 'same-origin' })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) { if (data && data.email) build(data); })
      .catch(function () {});
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
