/**
 * menu.js — Menu lateral canônico CAMIM
 * Substitui o HTML hardcoded de todas as páginas KPI.
 * Detecta a página ativa pelo pathname. Admin só aparece se is_admin=true.
 */
(function () {

  // ── Menu canônico — ordem e itens definitivos ──────────────────────────
  const MENU = [
    { href: '/index.html',                         icon: 'fa-home',                      label: 'Home' },
    { href: '/indicadores.html',                   icon: 'fa-tachometer-alt',            label: 'Indicadores' },
    { href: '/kpi_home.html',                      icon: 'fa-book-open',                 label: 'Leia-me' },
    { type: 'divider' },
    { href: '/kpi_v2.html',                        icon: 'fa-chart-line',                label: 'KPI Mensalidades' },
    { href: '/kpi_alimentacao.html',               icon: 'fa-utensils',                  label: 'KPI Custo Alimentação' },
    { href: '/kpi_medicos.html',                   icon: 'fa-notes-medical',             label: 'KPI Custo Médico' },
    { href: '/ctrlq_relatorio.html',               icon: 'fa-user-md',                   label: 'KPI Médicos (Qualidade)' },
    { href: '/kpi_vendas.html',                    icon: 'fa-shopping-cart',             label: 'KPI Vendas' },
    { href: '/kpi_clientes.html',                  icon: 'fa-users',                     label: 'KPI Clientes' },
    { href: '/KPI_prescricao.html',                icon: 'fa-prescription-bottle-alt',   label: 'KPI Prescrições' },
    { href: '/kpi_fidelizacao_cliente.html',       icon: 'fa-handshake',                 label: 'KPI Fidelização' },
    { href: '/kpi_consultas_status.html',          icon: 'fa-stethoscope',               label: 'KPI Consultas (Status)' },
    { href: '/kpi_notas_rps.html',                 icon: 'fa-file-invoice',              label: 'KPI Notas x RPS' },
    { href: '/kpi_metas_vendas_mensalidades.html', icon: 'fa-bullseye',                  label: 'KPI Metas (Mens/Vendas)' },
    { href: '/kpi_governo.html',                   icon: 'fa-landmark',                  label: 'KPI Índices Oficiais' },
    { href: '/kpi_liberty.html',                   icon: 'fa-passport',                  label: 'KPI CAMIM Liberty' },
    { href: '/kpi_receita_despesa.html',           icon: 'fa-balance-scale',             label: 'KPI Receitas x Despesas' },
    { href: '/kpi_receita_despesa_rateio.html',    icon: 'fa-balance-scale',             label: 'KPI R x D com Rateio' },
    { href: '/mais_servicos.html',                 icon: 'fa-th-large',                  label: 'Mais Serviços' },
    { type: 'divider' },
    { href: '/admin', icon: 'fa-users-cog', label: 'Admin', adminOnly: true,
      style: 'border-left:3px solid #f39c12', iconStyle: 'color:#f39c12' },
    { type: 'divider' },
    { type: 'logout' },
  ];

  // ── Detecta página ativa ───────────────────────────────────────────────
  function isActive(href) {
    const path = window.location.pathname;
    const base = href.replace(/^\//, '').replace(/\.html$/, '');
    return (
      path === href ||
      path.endsWith(href) ||
      path === '/' + base ||
      path.endsWith('/' + base + '.html') ||
      path.endsWith('/' + base)
    );
  }

  // ── Renderiza o menu ───────────────────────────────────────────────────
  function renderMenu(isAdmin) {
    const nav = document.querySelector('ul.nav-sidebar');
    if (!nav) return;

    const html = MENU
      .filter(item => !item.adminOnly || isAdmin)
      .map(item => {
        if (item.type === 'divider') {
          return `<li style="border-top:1px solid rgba(255,255,255,0.08);margin:6px 12px;padding:0;height:0;list-style:none;pointer-events:none;"></li>`;
        }
        if (item.type === 'logout') {
          return `<li class="nav-item">
            <form method="POST" action="/session/logout" style="display:inline;width:100%">
              <button type="submit" class="nav-link btn btn-link p-0 w-100" style="text-align:left">
                <i class="nav-icon fas fa-sign-out-alt"></i><p>Sair</p>
              </button>
            </form>
          </li>`;
        }
        const active    = isActive(item.href) ? ' active' : '';
        const liStyle   = item.style     ? ` style="${item.style}"`     : '';
        const iconStyle = item.iconStyle ? ` style="${item.iconStyle}"` : '';
        return `<li class="nav-item">
          <a href="${item.href}" class="nav-link${active}"${liStyle}>
            <i class="nav-icon fas ${item.icon}"${iconStyle}></i>
            <p>${item.label}</p>
          </a>
        </li>`;
      })
      .join('');

    nav.innerHTML = html;
  }

  // ── Inicializa ─────────────────────────────────────────────────────────
  function init() {
    // Se Jinja2 já injetou window.USER_IS_ADMIN, usa direto
    if (typeof window.USER_IS_ADMIN !== 'undefined') {
      renderMenu(!!window.USER_IS_ADMIN);
      return;
    }
    // Senão, busca da sessão (já feito pelo contexto, mas fallback seguro)
    fetch('/session/me', { credentials: 'same-origin' })
      .then(r => r.ok ? r.json() : {})
      .then(data => {
        window.USER_IS_ADMIN = !!data.is_admin;
        renderMenu(window.USER_IS_ADMIN);
      })
      .catch(() => renderMenu(false));
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
