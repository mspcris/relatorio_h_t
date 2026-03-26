/**
 * menu.js — Menu lateral canônico CAMIM
 *
 * Roda SEM defer (tag <script> síncrona no final do body).
 * Detecta admin do DOM já renderizado pelo Jinja2 (busca link /admin).
 * Garante menu idêntico em todas as páginas, sem flash do HTML antigo.
 */
(function () {

  // ── Lista canônica — única fonte de verdade ──────────────────────────
  var MENU = [
    { href: '/index.html',                         icon: 'fa-home',                    label: 'Home' },
    { href: '/monitorarrobos.html',                 icon: 'fa-tachometer-alt',          label: 'Monitorar Robôs' },
    { href: '/kpi_home.html',                      icon: 'fa-book-open',               label: 'Leia-me' },
    { type: 'sep' },
    { href: '/kpi_v2.html',                        icon: 'fa-chart-line',              label: 'KPI Mensalidades' },
    { href: '/kpi_alimentacao.html',               icon: 'fa-utensils',                label: 'KPI Custo Alimentação' },
    { href: '/kpi_medicos.html',                   icon: 'fa-notes-medical',           label: 'KPI Custo Médico' },
    { href: '/ctrlq_relatorio.html',               icon: 'fa-user-md',                 label: 'KPI Médicos (Qualidade)' },
    { href: '/kpi_vendas.html',                    icon: 'fa-shopping-cart',           label: 'KPI Vendas' },
    { href: '/kpi_clientes.html',                  icon: 'fa-users',                   label: 'KPI Clientes' },
    { href: '/KPI_prescricao.html',                icon: 'fa-prescription-bottle-alt', label: 'KPI Prescrições' },
    { href: '/kpi_fidelizacao_cliente.html',       icon: 'fa-handshake',               label: 'KPI Fidelização Churn' },
    { href: '/kpi_consultas_status.html',          icon: 'fa-stethoscope',             label: 'KPI Consultas (Status)' },
    { href: '/kpi_notas_rps.html',                 icon: 'fa-file-invoice',            label: 'KPI Notas x RPS' },
    { href: '/kpi_metas_vendas_mensalidades.html', icon: 'fa-bullseye',                label: 'KPI Metas (Mens/Vendas)' },
    { href: '/kpi_governo.html',                   icon: 'fa-landmark',                label: 'KPI Índices Oficiais' },
    { href: '/kpi_liberty.html',                   icon: 'fa-passport',                label: 'KPI CAMIM Liberty' },
    { href: '/kpi_receita_despesa.html',           icon: 'fa-balance-scale',           label: 'KPI Receitas x Despesas' },
    { href: '/kpi_receita_despesa_rateio.html',    icon: 'fa-balance-scale',           label: 'KPI R x D com Rateio' },
    { href: '/mais_servicos.html',                 icon: 'fa-th-large',                label: 'Mais Serviços' },
    { type: 'sep' },
    { href: '/admin', icon: 'fa-users-cog', label: 'Admin', adminOnly: true,
      style: 'border-left:3px solid #f39c12', iconStyle: 'color:#f39c12' },
    { type: 'sep' },
    { type: 'logout' }
  ];

  // ── Detecta página ativa ──────────────────────────────────────────────
  function isActive(href) {
    var path = window.location.pathname;
    var name = href.replace(/^\//, '').replace(/\.html$/, '');
    return path === href
      || path.endsWith(href)
      || path === '/' + name
      || path.endsWith('/' + name + '.html')
      || path.endsWith('/' + name);
  }

  // ── Gera HTML do menu ─────────────────────────────────────────────────
  function buildHTML(isAdmin) {
    return MENU
      .filter(function(i) { return !i.adminOnly || isAdmin; })
      .map(function(i) {
        if (i.type === 'sep') {
          return '<li style="border-top:1px solid rgba(255,255,255,0.1);margin:5px 14px;padding:0;height:0;list-style:none;pointer-events:none"></li>';
        }
        if (i.type === 'logout') {
          return '<li class="nav-item">'
            + '<form method="POST" action="/session/logout" style="display:inline;width:100%">'
            + '<button type="submit" class="nav-link btn btn-link p-0 w-100" style="text-align:left">'
            + '<i class="nav-icon fas fa-sign-out-alt"></i><p>Sair</p>'
            + '</button></form></li>';
        }
        var active    = isActive(i.href) ? ' active' : '';
        var liStyle   = i.style     ? ' style="' + i.style + '"'     : '';
        var icoStyle  = i.iconStyle ? ' style="' + i.iconStyle + '"' : '';
        return '<li class="nav-item">'
          + '<a href="' + i.href + '" class="nav-link' + active + '"' + liStyle + '>'
          + '<i class="nav-icon fas ' + i.icon + '"' + icoStyle + '></i>'
          + '<p>' + i.label + '</p>'
          + '</a></li>';
      })
      .join('');
  }

  // ── Renderiza imediatamente ───────────────────────────────────────────
  function render(isAdmin) {
    var nav = document.querySelector('ul.nav-sidebar');
    if (!nav) return;
    nav.innerHTML = buildHTML(isAdmin);
  }

  // ── Detecta admin do DOM já renderizado pelo Jinja2 ──────────────────
  // O Jinja2 já processou {% if USER_IS_ADMIN %} e inseriu (ou não) o link /admin.
  // Lemos isso antes de substituir o HTML — sem fetch assíncrono.
  function detectAdminFromDOM() {
    var existing = document.querySelector('.nav-sidebar a[href="/admin"]');
    if (existing) return true;
    // Fallback: variável global injetada por Jinja2
    if (typeof window.USER_IS_ADMIN !== 'undefined') return !!window.USER_IS_ADMIN;
    return false;
  }

  function init() {
    // Oculta a nav enquanto substitui — evita flash do HTML antigo
    var nav = document.querySelector('ul.nav-sidebar');
    if (nav) nav.style.visibility = 'hidden';

    var isAdmin = detectAdminFromDOM();
    render(isAdmin);

    if (nav) nav.style.visibility = '';

    // Confirma em background (sem re-renderizar se igual)
    fetch('/session/me', { credentials: 'same-origin' })
      .then(function(r) { return r.ok ? r.json() : {}; })
      .then(function(data) {
        var adminNow = !!data.is_admin;
        window.USER_IS_ADMIN = adminNow;
        if (adminNow !== isAdmin) render(adminNow);
      })
      .catch(function() {});
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
