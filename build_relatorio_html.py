#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera relatório HTML consolidando Trello + Harvest.
- Trello: última subpasta em export_trello/export_YYYYMMDD_HHMMSS com cards.csv
- Harvest: CSV mais recente em export_harvest/
- Saída:
  relatorio/relatorio_YYYYMMDD_HHMMSS.html
  trello_harvest.html  (sempre sobrescrito)
"""
#Teste de Deploy em  04/11/25 - 7h:40m
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import os
import argparse
import pandas as pd
import re
from typing import Iterable
from string import Template



def set_mtime(path: Path, dt: datetime) -> None:
    """Ajusta atime/mtime do arquivo para dt."""
    ts = dt.timestamp()
    os.utime(path, (ts, ts))

# -------------------- utils --------------------
def read_csv_safe(p: Path | None):
    if not p or not p.exists():
        return None
    try:
        return pd.read_csv(p, encoding="utf-8")
    except Exception:
        return pd.read_csv(p, encoding="latin-1")

def latest_harvest_csv(root: Path) -> Path | None:
    base = root / "export_harvest"
    if not base.exists():
        return None
    files = list(base.glob("*.csv"))
    return max(files, key=lambda f: f.stat().st_mtime) if files else None

def latest_trello_dir(root: Path) -> Path | None:
    base = root / "export_trello"
    if not base.exists():
        return None
    candidates = [p for p in base.iterdir() if p.is_dir() and (p / "cards.csv").exists()]
    return max(candidates, key=lambda d: d.stat().st_mtime) if candidates else None

def df_to_html_table(df: pd.DataFrame | None, table_id: str, raw_html_cols: set[str] | None = None) -> str:
    if df is None or df.empty:
        return f"""
        <div class="table-wrap">
          <input class="filter" placeholder="Filtrar nesta tabela..." oninput="filterTable('{table_id}', this.value)"/>
          <table id="{table_id}" class="data">
            <thead><tr><th>Sem dados</th></tr></thead>
            <tbody><tr><td class="empty">Sem dados.</td></tr></tbody>
          </table>
        </div>"""

    raw_html_cols = raw_html_cols or set()
    df = df.fillna("")

    safe = df.copy()
    for c in safe.columns:
        if safe[c].dtype == object and c not in raw_html_cols:
            safe[c] = (
                safe[c]
                .astype(str)
                .str.replace("<", "&lt;", regex=False)
                .str.replace(">", "&gt;", regex=False)
            )

    thead = "".join(f"<th>{c}</th>" for c in safe.columns)
    rows_html = []
    for _, r in safe.iterrows():
        tds = []
        for c in safe.columns:
            tds.append(f"<td>{r[c]}</td>")
        rows_html.append("<tr>" + "".join(tds) + "</tr>")
    rows = "\n".join(rows_html)

    return f"""
    <div class="table-wrap">
      <input class="filter" placeholder="Filtrar nesta tabela..." oninput="filterTable('{table_id}', this.value)"/>
      <table id="{table_id}" class="data">
        <thead><tr>{thead}</tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""

def fmt_hours_hhmm(x):
    if pd.isna(x):
        return ""
    try:
        m = int(round(float(x) * 60))
        return f"{m//60:02d}:{m%60:02d}"
    except Exception:
        return str(x)

def pick_username_column(df: pd.DataFrame) -> str | None:
    if df is None or df.empty:
        return None
    if "user.name" in df.columns:
        return "user.name"
    candidates = [
        c for c in df.columns
        if re.search(r"^(user(\.name)?|username|usuario|colaborador|user[_ ]?name|nome[_ ]?usuario)$", c, re.I)
    ]
    best = None
    best_score = -1.0
    for c in candidates:
        s = df[c].astype(str)
        letters = s.str.contains(r"[A-Za-zÀ-ú]", regex=True, na=False).mean()
        bonus = 0.1 if c.lower() == "username" else 0.0
        score = letters + bonus
        if score > best_score:
            best_score, best = score, c
    if best is not None and best_score >= 0.5:
        df.rename(columns={best: "user.name"}, inplace=True)
        return "user.name"
    return None

def sort_by_recent(df: pd.DataFrame | None, cand_cols: Iterable[str]) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    cols = [c for c in cand_cols if c in df.columns]
    if not cols:
        return df
    for c in cols:
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    key = next((c for c in cols if df[c].notna().any()), cols[0])
    return df.sort_values(key, ascending=False, na_position="last").reset_index(drop=True)

# -------------------- template --------------------
HTML_TMPL = Template(r"""<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Shortcut_info</title>
  <link rel="shortcut icon" href="../images/Logo Camim-01_50px.png" type="image/x-icon">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
  <link href="https://fonts.googleapis.com/css2?family=Baloo+2:wght@600;700&display=swap" rel="stylesheet">
  <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500;700&display=swap" rel="stylesheet">
  <link href="https://fonts.googleapis.com/css2?family=Nunito+Rounded:wght@800&display=swap" rel="stylesheet">
  <script src="../js/menu.js" defer></script>
  <script src="../js/header.js" defer></script>
  <script src="../js/footer.js" defer></script>
  <script src="../js/overlay.js" defer></script>
  <link rel="stylesheet" href="../css/style.css?v=5">
  <style>
    #overlay { position: fixed; inset: 0; background: rgba(255,255,255,0.2); backdrop-filter: blur(12px); display:flex; justify-content:center; align-items:center; z-index:9999; opacity:1; visibility:visible; transition:opacity 1s ease, visibility 0s linear; }
    #overlay.fade-out { opacity:0; visibility:hidden; transition:opacity 1s ease, visibility 0s linear 1s; }
    h1, h2, h3 { margin:16px 0 8px; }
    p { margin:8px 0 16px; }
    ul { margin:0 0 16px 20px; }
    code, pre { background:#f6f8fa; border:1px solid #eaecef; border-radius:6px; }
    pre { padding:12px; overflow:auto; }
    .kpi { display:grid; grid-template-columns:repeat(4, minmax(180px,1fr)); gap:12px; margin:16px 0; }
    .card { border:1px solid #e5e7eb; border-radius:8px; padding:12px; }
    .muted { color:#555; }
    table { width:100%; border-collapse:collapse; margin:12px 0 16px; }
    th, td { border:1px solid #e5e7eb; padding:8px 10px; text-align:left; }
    th { background:#fafafa; }
    small { color:#666; }
    .header2 { display:flex; gap:12px; padding:14px 16px; border-bottom:1px solid #ffffff1a; position:sticky; top:0; background:var(--cor4); align-items:center; justify-content:space-between }
    .h12 { color:var(--cor0); }
    .badge2 { color:var(--muted); padding:4px 8px; border-radius:999px; font-size:12px }
    .link-btn2 { border:1px solid #ffffff22; background:#ffffff10; color:#fff; padding:8px 12px; border-radius:10px; text-decoration:none; font-weight:700 }
    .link-btn2:hover { background:#ffffff20 }
    .right2 { display:flex; gap:8px; align-items:center }
    .table-wrap { margin-top:8px; overflow:auto }
    .filter { width:100%; padding:8px; border-radius:8px; border:1px solid #e5e7eb; }
    .empty { color:#666; font-style:italic }
    /* MENU FUNCIONAL */
    .menu-nav { overflow-y: auto; scrollbar-gutter: stable both-edges; padding-right: 12px; }
    .menu-drawer { overflow: hidden; }
    .menu-list { padding-right: 4px; }
    .menu-list form { margin: 0; }
    .menu-list form .menu-link { display: block; width: 100%; text-align: left; }
  </style>
  <script>
    function selectTab(id) {
      document.querySelectorAll('.panel').forEach(p => p.setAttribute('aria-hidden', 'true'));
      const btn = document.querySelector('[data-tab="' + id + '"]');
      if (btn) {
        document.querySelectorAll('.tab-btn').forEach(b => b.setAttribute('aria-selected', 'false'));
        btn.setAttribute('aria-selected', 'true');
      }
      const panel = document.getElementById(id);
      if (panel) panel.setAttribute('aria-hidden', 'false');
    }
    function filterTable(id, q) {
      q = (q || '').toLowerCase();
      const rows = document.querySelectorAll('#' + id + ' tbody tr');
      rows.forEach(r => {
        const t = r.textContent.toLowerCase();
        r.style.display = t.indexOf(q) >= 0 ? '' : 'none';
      });
    }
  </script>
</head>

<body>

  <!-- Overlay de mensagem -->
  <div id="overlay"></div>

  <!-- Botão hamburguer que abre o menu -->
  <button id="menuToggle" class="menu-btn" aria-label="Abrir menu" aria-controls="app-drawer" aria-expanded="false">
    <svg width="24" height="24" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M3 6h18v2H3zm0 5h18v2H3zm0 5h18v2H3z"></path>
    </svg>
  </button>

  
    <!-- Fundo escuro atrás do menu -->
    <div id="drawerOverlay" class="menu-overlay" hidden></div>
    <!-- O menu lateral -->
    <aside id="app-drawer" class="menu-drawer" role="dialog" aria-modal="true" aria-labelledby="drawerTitle" hidden>
        <div class="menu-splash">&nbsp;</div>
        <header class="drawer-header">
            <img src="/images/Logo Camim-01_50px.png" alt="Logo Camim">
            <h2 id="drawerTitle">&nbsp;</h2>
            <button id="drawerClose" class="menu-icon-btn" aria-label="Fechar menu">✖</button>
        </header>
        <!-- MENU LATERAL -->
        <nav class="menu-nav" aria-label="Menu principal">
            <ul class="menu-list">
                <!-- <li>
                    <a class="menu-link" href="/index.html">
                        <i class="fas fa-home"></i>
                        <span>Início</span>
                    </a>
                </li> -->
                <li>
                    <a class="menu-link" href="/kpi_home.html">
                        <i class="nav-icon fas fa-chart-line"></i>
                        <span>KPIs</span>
                    </a>
                </li>
                <li>
                    <a class="menu-link" href="/trello_harvest.html">
                        <i class="fab fa-trello" style="color:#0079bf"></i>
                        <span>Harvest / Trello</span>
                    </a>
                </li>
                <li>
                    <a class="menu-link" href="#" id="btnLogout">
                        <i class="fas fa-sign-out-alt"></i>
                        <span>Sair</span>
                    </a>
                </li>
            </ul>
        </nav>
        <div class="menu-brand">&nbsp;</div>
    </aside>
    <script>
        document.getElementById('btnLogout')?.addEventListener('click', async (e) => {
            e.preventDefault();
            try { await fetch('/session/logout', { method: 'POST', credentials: 'include' }); } catch (_) { }
            location.href = '/login.html';
        });
    </script>





  <div id="header"></div>

  <main>
    <div class="header2">
      <div class="left">
        <h1 class="h12">Relatório — Trello & Harvest</h1>
        <span class="badge2">$now</span>
        <span class="badge2">Trello: $trello_hint</span>
        <span class="badge2">Harvest: $harvest_hint</span>
      </div>
      <div class="right2">
        <a class="link-btn2" href="#harvest" onclick="selectTab('harvest')">Harvest</a>
        <a class="link-btn2" href="#trello" onclick="selectTab('trello')">Trello</a>
      </div>
    </div>

    <section id="trello" class="panel" aria-hidden="false">
      <div class="card">
        <h3>Cards - Trello</h3>
        $tbl_cards
      </div>
    </section>

    <section id="harvest" class="panel" aria-hidden="true">
      <div class="card">
        <h3>Entradas - Harvest</h3>
        $tbl_harvest
      </div>
      <div class="card">
        <h3>Horas por Cliente</h3>
        $tbl_by_client
      </div>
      <div class="card">
        <h3>Horas por Projeto</h3>
        $tbl_by_project
      </div>
      <div class="card">
        <h3>Horas por Usuário</h3>
        $tbl_by_user
      </div>
    </section>

    <script>
      document.addEventListener('DOMContentLoaded', () => {
        const target = (location.hash || '#harvest').slice(1);
        selectTab(target);
      });
    </script>

  </main>

  <div id="footer"></div>
  <script src="../js/footer.js" defer></script>
</body>
</html>
""")

# -------------------- app --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="raiz do projeto")
    ap.add_argument("--outdir", default="relatorio", help="pasta de saída")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    outdir = (root / args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_html = outdir / f"relatorio_{stamp}.html"
    out_index = root / "trello_harvest.html"  # sobrescreve o index do relatório

    # --- fontes ---
    trello_dir = latest_trello_dir(root)
    hv_csv = latest_harvest_csv(root)

    # Trello
    lists = read_csv_safe(trello_dir / "lists.csv") if trello_dir else None
    cards = read_csv_safe(trello_dir / "cards.csv") if trello_dir else None

    if cards is not None and not cards.empty:
        # mapear listas
        if lists is not None and not lists.empty:
            lists_map = dict(zip(lists.get("id", []), lists.get("name", [])))
            if "idList" in cards.columns and "list" not in cards.columns:
                cards["list"] = cards["idList"].map(lists_map).fillna("")
        # ordenar por recência
        cards = sort_by_recent(cards, ("updated", "dateLastActivity", "due", "start", "created", "created_at"))
        # selecionar colunas e renomear
        wanted_cols = ["name", "list", "url", "closed", "updated", "desc"]
        for c in wanted_cols:
            if c not in cards.columns:
                cards[c] = ""
        cards = cards[wanted_cols].rename(columns={"name": "card"})
        # link
        cards["url"] = cards["url"].astype(str).apply(
            lambda u: f'<a href="{u}" target="_blank" rel="noopener">abrir</a>' if u else ""
        )

    # Harvest
    harvest_raw = read_csv_safe(hv_csv) if hv_csv else None
    harvest_display = None
    pivots: dict[str, pd.DataFrame] = {}

    if harvest_raw is not None and not harvest_raw.empty:
        # normalizar nomes
        ren, seen = {}, set()
        for c in harvest_raw.columns:
            lc = c.lower()
            target = None
            if lc in ("spent_date", "data", "date"): target = "date"
            elif ("project" in lc and "id" not in lc): target = "project"
            elif ("client" in lc and "id" not in lc): target = "client"
            elif ("task" in lc and "id" not in lc): target = "task"
            elif ("hours" in lc or "hour" in lc): target = "hours"
            elif ("notes" in lc or "descri" in lc): target = "notes"
            elif lc in ("user", "user.name", "username", "user name"): target = None
            if target and target not in seen:
                ren[c] = target
                seen.add(target)
        harvest = harvest_raw.rename(columns=ren)
        harvest = harvest.loc[:, ~harvest.columns.duplicated()]

        harvest = sort_by_recent(harvest, ("updated_at", "created_at", "date", "spent_date"))

        user_col = pick_username_column(harvest)

        if "hours" in harvest.columns:
            harvest["hours"] = pd.to_numeric(harvest["hours"], errors="coerce")

        if {"client", "project", "hours"} <= set(harvest.columns):
            pivots["by_client"] = (
                harvest.groupby("client", dropna=False)["hours"].sum().reset_index().sort_values("hours", ascending=False)
            )
            pivots["by_project"] = (
                harvest.groupby(["client", "project"], dropna=False)["hours"]
                .sum().reset_index().sort_values(["client", "hours"], ascending=[True, False])
            )
        if (user_col or "user.name" in harvest.columns) and "hours" in harvest.columns:
            if "user.name" not in harvest.columns and user_col:
                harvest.rename(columns={user_col: "user.name"}, inplace=True)
            pivots["by_user"] = (
                harvest.groupby("user.name", dropna=False)["hours"]
                .sum().reset_index().sort_values("hours", ascending=False)
            )

        harvest_display = harvest.copy()
        cols_drop = ["client.id", "billable", "is_locked", "user", "project.id", "task.id"]
        harvest_display.drop(columns=[c for c in cols_drop if c in harvest_display.columns], errors="ignore", inplace=True)

        wanted_h_cols = ["id", "date", "hours", "notes", "user.id", "user.name", "project", "task", "created_at", "updated_at"]
        for c in wanted_h_cols:
            if c not in harvest_display.columns:
                harvest_display[c] = ""
        harvest_display = harvest_display[wanted_h_cols]

        if "hours" in harvest_display.columns:
            harvest_display["hours"] = harvest_display["hours"].apply(fmt_hours_hhmm)
        for k, dfp in list(pivots.items()):
            if dfp is not None and not dfp.empty and "hours" in dfp.columns:
                dfp["hours"] = dfp["hours"].apply(fmt_hours_hhmm)

    # placeholders
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    trello_hint = trello_dir.name if trello_dir else "não encontrado"
    harvest_hint = hv_csv.name if hv_csv else "não encontrado"

    html = HTML_TMPL.substitute(
        now=now,
        trello_hint=trello_hint,
        harvest_hint=harvest_hint,
        tbl_cards=df_to_html_table(cards, "tbl_cards", raw_html_cols={"url"}),
        tbl_harvest=df_to_html_table(harvest_display, "tbl_harvest"),
        tbl_by_client=df_to_html_table(pivots.get("by_client"), "tbl_by_client"),
        tbl_by_project=df_to_html_table(pivots.get("by_project"), "tbl_by_project"),
        tbl_by_user=df_to_html_table(pivots.get("by_user"), "tbl_by_user"),
    )
    
    gen_dt = datetime.now(timezone.utc).astimezone()
    out_html.write_text(html, encoding="utf-8")
    out_index.write_text(html, encoding="utf-8")
    set_mtime(out_html, gen_dt)
    set_mtime(out_index, gen_dt)
    print(f"OK: {out_html}")
    print(f"OK: {out_index} (sobrescrito)")

if __name__ == "__main__":
    main()