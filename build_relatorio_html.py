#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório HTML consolidando Trello + Harvest.
- Trello: lê a última subpasta export_trello/export_YYYYMMDD_HHMMSS
- Harvest: lê o CSV mais recente em export_harvest/
- Saída: relatorio/relatorio_YYYYMMDD_HHMMSS.html
"""
from pathlib import Path
from datetime import datetime
import argparse
import pandas as pd
import re

# -------------------- utils --------------------
def read_csv_safe(p: Path):
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
        return '<div class="empty">Sem dados.</div>'
    raw_html_cols = raw_html_cols or set()
    df = df.fillna("")

    # escapar HTML apenas para colunas não marcadas como raw
    safe = df.copy()
    for c in safe.columns:
        if safe[c].dtype == object and c not in raw_html_cols:
            safe[c] = (safe[c].astype(str)
                             .str.replace("<","&lt;", regex=False)
                             .str.replace(">","&gt;", regex=False))

    thead = "".join(f"<th>{c}</th>" for c in safe.columns)
    rows_html = []
    for _, r in safe.iterrows():
        tds = []
        for c in safe.columns:
            val = r[c]
            # valores já estão escapados quando necessário; manter como está
            tds.append(f"<td>{val}</td>")
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
    """Seleciona a coluna com NOME do usuário e assegura o rótulo 'user.name'."""
    if df is None or df.empty:
        return None
    if "user.name" in df.columns:
        return "user.name"
    candidates = [c for c in df.columns if re.search(r"^(user(\.name)?|username|usuario|colaborador|user[_ ]?name|nome[_ ]?usuario)$", c, re.I)]
    best = None
    best_score = -1.0
    for c in candidates:
        s = df[c].astype(str)
        letters = s.str.contains(r"[A-Za-zÀ-ú]", regex=True, na=False).mean()
        bonus = 0.1 if c.lower() in ("username",) else 0.0
        score = letters + bonus
        if score > best_score:
            best_score, best = score, c
    if best is not None and best_score >= 0.5:
        df.rename(columns={best: "user.name"}, inplace=True)
        return "user.name"
    return None

# -------------------- app --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="raiz do projeto")
    ap.add_argument("--outdir", default="relatorio", help="pasta de saída para o HTML")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    outdir = (root / args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_html = outdir / f"relatorio_{stamp}.html"

    # --- fontes ---
    trello_dir = latest_trello_dir(root)
    hv_csv = latest_harvest_csv(root)

    # Trello
    board      = read_csv_safe(trello_dir / "board.csv")       if trello_dir else None
    lists      = read_csv_safe(trello_dir / "lists.csv")       if trello_dir else None
    members    = read_csv_safe(trello_dir / "members.csv")     if trello_dir else None
    labels     = read_csv_safe(trello_dir / "labels.csv")      if trello_dir else None
    cards      = read_csv_safe(trello_dir / "cards.csv")       if trello_dir else None
    checklists = read_csv_safe(trello_dir / "checklists.csv")  if trello_dir else None
    atts       = read_csv_safe(trello_dir / "attachments.csv") if trello_dir else None

    # enriquecer e higienizar cards (remoções solicitadas + link clicável)
    if cards is not None and not cards.empty:
        # mapear lista se necessário
        if lists is not None and not lists.empty:
            lists_map = dict(zip(lists.get("id", []), lists.get("name", [])))
            if "idList" in cards.columns and "list" not in cards.columns:
                cards["list"] = cards["idList"].map(lists_map).fillna("")
        # datas
        for col in ("due", "start", "updated"):
            if col in cards.columns:
                cards[col] = pd.to_datetime(cards[col], errors="coerce")
        if "updated" in cards.columns:
            cards = cards.sort_values(["list","updated"], ascending=[True, False])

        # remover colunas solicitadas
        drop_cols = ["card_id","labels","members","due","sart","start","due_complete","short_link", "attachments_urls","attachments_count"]
        keep_cols = [c for c in cards.columns if c not in drop_cols]
        cards = cards[keep_cols]

        # transformar url em link clicável
        if "url" in cards.columns:
            cards["url"] = cards["url"].astype(str).apply(
                lambda u: f'<a href="{u}" target="_blank" rel="noopener">abrir</a>' if u else ""
            )

    # --- Harvest ---
    harvest_raw = read_csv_safe(hv_csv) if hv_csv else None
    harvest_display = None
    pivots = {}

    if harvest_raw is not None and not harvest_raw.empty:
        # Canonizar nomes SEM criar duplicatas
        ren, seen = {}, set()
        for c in harvest_raw.columns:
            lc = c.lower()
            target = None
            if lc in ("spent_date","data","date"): target = "date"
            elif ("project" in lc and "id" not in lc): target = "project"
            elif ("client"  in lc and "id" not in lc): target = "client"
            elif ("task"    in lc and "id" not in lc): target = "task"
            elif ("hours" in lc or "hour" in lc):      target = "hours"
            elif ("notes" in lc or "descri" in lc):    target = "notes"
            # não decidir 'user.name' aqui; será feito pela heurística
            if target and target not in seen:
                ren[c] = target
                seen.add(target)
        harvest = harvest_raw.rename(columns=ren)
        harvest = harvest.loc[:, ~harvest.columns.duplicated()]

        # Selecionar/forçar coluna de nome -> 'user.name' sem tocar em IDs
        user_col = pick_username_column(harvest)

        # Tipos
        if "date"  in harvest.columns:  harvest["date"]  = pd.to_datetime(harvest["date"], errors="coerce").dt.date
        if "hours" in harvest.columns:  harvest["hours"] = pd.to_numeric(harvest["hours"], errors="coerce")

        # Pivôs
        if {"client","project","hours"} <= set(harvest.columns):
            pivots["by_client"] = (
                harvest.groupby("client", dropna=False)["hours"]
                       .sum().reset_index().sort_values("hours", ascending=False)
            )
            pivots["by_project"] = (
                harvest.groupby(["client","project"], dropna=False)["hours"]
                       .sum().reset_index().sort_values(["client","hours"], ascending=[True, False])
            )
        if user_col and "hours" in harvest.columns and "user.name" in harvest.columns:
            pivots["by_user"] = (
                harvest.groupby("user.name", dropna=False)["hours"]
                       .sum().reset_index().sort_values("hours", ascending=False)
            )

        # Grid de entradas (remoções + horas HH:MM)
        harvest_display = harvest.copy()
        cols_drop = ["client.id","client","billable","is_locked","user","project.id","task.id"]
        harvest_display.drop(columns=[c for c in cols_drop if c in harvest_display.columns],
                             inplace=True, errors="ignore")
        if "user.name" not in harvest_display.columns:
            harvest_display["user.name"] = ""
        if "hours" in harvest_display.columns:
            harvest_display["hours"] = harvest_display["hours"].apply(fmt_hours_hhmm)
        for k, df in list(pivots.items()):
            if df is not None and not df.empty and "hours" in df.columns:
                df["hours"] = df["hours"].apply(fmt_hours_hhmm)

    # --- HTML ---
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    trello_hint = trello_dir.name if trello_dir else "não encontrado"
    harvest_hint = hv_csv.name if hv_csv else "não encontrado"

    html = f"""<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Relatório — Trello & Harvest</title>
<style>
:root {{ --bg:#0b1020; --card:#101935; --muted:#a3acc2; --fg:#e9eefb; --brand:#ffd000; --radius:14px; }}
* {{ box-sizing:border-box }}
body {{ margin:0; font:14px/1.45 system-ui,Segoe UI,Roboto,Arial; background:var(--bg); color:var(--fg) }}
.header {{ display:flex; gap:12px; padding:14px 16px; border-bottom:1px solid #ffffff1a; position:sticky; top:0; background:#0b1020 }}
.header h1 {{ margin:0; font-size:18px }}
.badge {{ background:#ffffff14; color:var(--muted); padding:4px 8px; border-radius:999px; font-size:12px }}
.tabs {{ display:flex; gap:8px; padding:12px 16px; flex-wrap:wrap }}
.tab-btn {{ border:1px solid #ffffff22; background:#ffffff10; color:var(--fg); padding:8px 12px; border-radius:10px; cursor:pointer; font-weight:700 }}
.tab-btn[aria-selected="true"] {{ background:var(--brand); color:#111; border-color:#00000044 }}
.panel {{ display:none; padding:16px }}
.panel[aria-hidden="false"] {{ display:block }}
.card {{ background:var(--card); border:1px solid #ffffff1a; border-radius:var(--radius); padding:16px; margin-bottom:16px }}
.table-wrap {{ margin-top:8px; overflow:auto }}
.filter {{ width:100%; padding:8px; border-radius:8px; border:1px solid #ffffff22; background:#ffffff10; color:#fff }}
table.data {{ width:100%; border-collapse:collapse; margin-top:8px }}
table.data th, table.data td {{ border:1px solid #ffffff1a; padding:6px 8px; vertical-align:top }}
.empty {{ color:var(--muted); font-style:italic }}
</style>
<script>
function selectTab(id){{
  for(const b of document.querySelectorAll('.tab-btn')) b.setAttribute('aria-selected','false');
  for(const p of document.querySelectorAll('.panel')) p.setAttribute('aria-hidden','true');
  document.querySelector('[data-tab="'+id+'"]').setAttribute('aria-selected','true');
  document.getElementById(id).setAttribute('aria-hidden','false');
}}
function filterTable(id, q){{
  q = q.toLowerCase();
  const rows = document.querySelectorAll('#'+id+' tbody tr');
  rows.forEach(r=>{{
    const t = r.textContent.toLowerCase();
    r.style.display = t.indexOf(q) >= 0 ? '' : 'none';
  }});
}}
</script>
</head>
<body>
  <div class="header">
    <h1>Relatório — Trello & Harvest</h1>
    <span class="badge">{now}</span>
    <span class="badge">Trello: {trello_hint}</span>
    <span class="badge">Harvest: {harvest_hint}</span>
  </div>

  <div class="tabs">
    <button class="tab-btn" data-tab="trello" aria-selected="true" onclick="selectTab('trello')">Trello</button>
    <button class="tab-btn" data-tab="harvest" aria-selected="false" onclick="selectTab('harvest')">Harvest</button>
  </div>

  <section id="trello" class="panel" aria-hidden="false">
    <div class="card">
      <h3>Cards</h3>
      {df_to_html_table(cards, "tbl_cards", raw_html_cols={'url'})}
    </div>
    <div class="card">
      <h3>Checklists</h3>
      {df_to_html_table(checklists, "tbl_checklists")}
    </div>
  </section>

  <section id="harvest" class="panel" aria-hidden="true">
    <div class="card"><h3>Entradas</h3>{df_to_html_table(harvest_display, "tbl_harvest")}</div>
    <div class="card"><h3>Horas por Cliente</h3>{df_to_html_table(pivots.get("by_client"), "tbl_by_client")}</div>
    <div class="card"><h3>Horas por Projeto</h3>{df_to_html_table(pivots.get("by_project"), "tbl_by_project")}</div>
    <div class="card"><h3>Horas por Usuário</h3>{df_to_html_table(pivots.get("by_user"), "tbl_by_user")}</div>
  </section>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")
    print(f"OK: {out_html}")

if __name__ == "__main__":
    main()
