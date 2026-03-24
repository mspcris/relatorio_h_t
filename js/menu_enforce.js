(function () {
  const path = (location.pathname || '').toLowerCase();
  if (path === '/' || path === '/index.html') return;

  const ITEMS = [
    {
      key: 'home',
      text: 'Home page',
      href: '/index.html',
      paths: ['/index.html', '/'],
      icon: 'fas fa-home',
    },
    {
      key: 'indicadores',
      text: 'Indicadores',
      href: '/indicadores',
      paths: ['/indicadores', '/indicadores.html'],
      icon: 'fas fa-chart-line',
    },
    {
      key: 'mais',
      text: 'Mais Servicos',
      href: '/mais_servicos',
      paths: ['/mais_servicos'],
      icon: 'fas fa-th-large',
    },
  ];

  function normHref(href) {
    try {
      const u = new URL(href, location.origin);
      return canonicalPath(u.pathname);
    } catch (_) {
      return canonicalPath(String(href || ''));
    }
  }

  function canonicalPath(path) {
    let p = String(path || '').trim().toLowerCase();
    if (!p) return '/';
    if (!p.startsWith('/')) p = '/' + p;
    p = p.replace(/\/{2,}/g, '/');
    if (p.endsWith('/') && p !== '/') p = p.slice(0, -1);
    if (p.endsWith('.html')) p = p.slice(0, -5);
    return p || '/';
  }

  function findLinks(root) {
    return Array.from(root.querySelectorAll('a[href]'));
  }

  function hasAnyPath(root, paths) {
    const wanted = (paths || []).map((p) => canonicalPath(p));
    return findLinks(root).some((a) => wanted.includes(normHref(a.getAttribute('href'))));
  }

  function makeLiAdminLte(item) {
    const li = document.createElement('li');
    li.className = 'nav-item';
    li.innerHTML = '<a href="' + item.href + '" class="nav-link">'
      + '<i class="nav-icon ' + item.icon + '"></i><p>' + item.text + '</p></a>';
    return li;
  }

  function makeLiGeneric(item) {
    const li = document.createElement('li');
    li.innerHTML = '<a href="' + item.href + '" class="nav-link">' + item.text + '</a>';
    return li;
  }

  function ensureInAdminLteMenus() {
    const lists = Array.from(document.querySelectorAll('ul.nav-sidebar'));
    lists.forEach((ul) => {
      ITEMS.forEach((item) => {
        if (!hasAnyPath(ul, item.paths)) {
          ul.insertBefore(makeLiAdminLte(item), ul.firstChild);
        }
      });
    });
  }

  function ensureInGenericSidebars() {
    const sidebars = Array.from(document.querySelectorAll('#sidebar, .sidebar'));
    sidebars.forEach((sb) => {
      const ul = sb.querySelector('ul.nav, ul.nav-sidebar, ul.menu-list');
      if (!ul) return;
      ITEMS.forEach((item) => {
        if (!hasAnyPath(ul, item.paths)) {
          ul.insertBefore(makeLiGeneric(item), ul.firstChild);
        }
      });
    });
  }

  function ensureAdminForAdminUsers() {
    fetch('/session/me', { credentials: 'include', cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .then((me) => {
        if (!me || !me.is_admin) return;

        const adminItem = {
          text: 'Admin',
          href: '/admin',
          paths: ['/admin'],
          icon: 'fas fa-users-cog',
        };

        Array.from(document.querySelectorAll('ul.nav-sidebar')).forEach((ul) => {
          if (!hasAnyPath(ul, adminItem.paths)) {
            ul.insertBefore(makeLiAdminLte(adminItem), ul.firstChild);
          }
        });

        Array.from(document.querySelectorAll('#sidebar, .sidebar')).forEach((sb) => {
          const ul = sb.querySelector('ul.nav, ul.nav-sidebar, ul.menu-list');
          if (!ul) return;
          if (!hasAnyPath(ul, adminItem.paths)) {
            ul.insertBefore(makeLiGeneric(adminItem), ul.firstChild);
          }
        });
      })
      .catch(() => {});
  }

  document.addEventListener('DOMContentLoaded', function () {
    ensureInAdminLteMenus();
    ensureInGenericSidebars();
    ensureAdminForAdminUsers();
  });
})();
