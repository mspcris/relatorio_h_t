"""
Indicadores ETL: PTAX (USD/BRL), IPCA (IBGE) e IGP-M (FGV via Ipeadata).
- Saída direta em um diretório (ex.: ./public ou ./json_consolidado): CSV + JSON no próprio diretório.
- Geração opcional de site estático simples (index.html + assets/style.css).

Uso (exemplos):
  pip install -U requests pandas ipeadatapy
  python indicadores_etl.py --ptax 2020-01-01 today --ipca 2020-01 thismonth --igpm 2020-01 thismonth --out ./public
  python indicadores_etl.py --site --out ./public
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import random
import time
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests

# -------------------- util: ajustar mtime --------------------
from datetime import datetime, timezone
def _set_mtime(path: str) -> None:
    """Ajusta atime/mtime do arquivo para 'agora' no timezone local."""
    try:
        ts = datetime.now(timezone.utc).astimezone().timestamp()
        os.utime(path, (ts, ts))
    except Exception:
        pass

# -------------------- Constantes e bases --------------------
OLINDA_PTAX_BASE = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
SIDRA_BASE = "https://servicodados.ibge.gov.br/api/v3"
IPEADATA_BASE = "https://ipeadata.gov.br/api/odata4"

IPCA_TABLE = 1737         # IPCA agregados v3
IPCA_VAR_MOM = 63         # variação mensal (%)

SGS_BUY = 1               # Dólar (compra)
SGS_SELL = 10813          # Dólar (venda)

# -------------------- Utilidades de tempo --------------------
def _parse_date(s: str) -> dt.date:
    today = dt.date.today()
    if s == "today":
        return today
    if s == "yesterday":
        return today - dt.timedelta(days=1)
    return dt.date.fromisoformat(s)

def _parse_month(s: str) -> Tuple[int, int]:
    today = dt.date.today()
    if s == "thismonth":
        return today.year, today.month
    if s == "lastmonth":
        first = (today.replace(day=1) - dt.timedelta(days=1)).replace(day=1)
        return first.year, first.month
    y, m = s.split("-")
    return int(y), int(m)

# -------------------- HTTP com retry --------------------
def http_get(url: str, params: Optional[Dict[str, Any]] = None, *,
             retries: int = 5, base_delay: float = 0.8, timeout: int = 60) -> requests.Response:
    attempt = 0
    while True:
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code >= 500:
                raise requests.HTTPError(f"server {r.status_code}")
            r.raise_for_status()
            return r
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(base_delay * (2 ** (attempt - 1)) * (1 + random.random() * 0.25))

# -------------------- PTAX --------------------
def _parse_bcb_datetime(raw: Any) -> dt.datetime:
    if raw is None:
        return dt.datetime.combine(dt.date.today(), dt.time(12, 0))
    s = str(raw)
    if s.startswith("/Date("):
        millis = int(s.split("(")[1].split("-")[0])
        return dt.datetime.utcfromtimestamp(millis / 1000.0)
    try:
        return dt.datetime.strptime(s.split(".")[0], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.fromisoformat(s)

def fetch_ptax_period(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    di = start_date.strftime("%d-%m-%Y")
    df_ = end_date.strftime("%d-%m-%Y")
    url = f"{OLINDA_PTAX_BASE}CotacaoDolarPeriodo(dataInicial='{di}',dataFinalCotacao='{df_}')"
    r = http_get(url, params={"$format": "json"})
    data = r.json().get("value", [])
    if not data:
        return pd.DataFrame(columns=["date", "buy", "sell", "mid", "source", "as_of"])
    recs = []
    for row in data:
        ts = _parse_bcb_datetime(row.get("dataHoraCotacao"))
        buy = float(row.get("cotacaoCompra"))
        sell = float(row.get("cotacaoVenda"))
        recs.append({
            "date": ts.date().isoformat(),
            "buy": buy,
            "sell": sell,
            "mid": (buy + sell) / 2.0,
            "source": "bcb-ptax-olinda",
            "as_of": ts.isoformat(),
        })
    df_ptax = pd.DataFrame.from_records(recs).sort_values(["date", "as_of"])
    # consolida por dia: último registro do dia
    df_ptax = df_ptax.groupby("date", as_index=False).tail(1).reset_index(drop=True)
    return df_ptax

def fetch_ptax_sgs(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    def _series(code: int) -> Dict[str, float]:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
        params = {"dataInicial": start_date.strftime("%d/%m/%Y"),
                  "dataFinal": end_date.strftime("%d/%m/%Y")}
        r = http_get(url, params=params)
        out: Dict[str, float] = {}
        for obs in r.json():
            d = dt.datetime.strptime(obs["data"], "%d/%m/%Y").date().isoformat()
            v = float(obs["valor"].replace(".", "").replace(",", "."))
            out[d] = v
        return out

    buy = _series(SGS_BUY)
    sell = _series(SGS_SELL)
    dates = sorted(set(buy) | set(sell))
    recs = []
    for d in dates:
        b = buy.get(d)
        s = sell.get(d)
        recs.append({
            "date": d,
            "buy": b,
            "sell": s,
            "mid": (b + s) / 2.0 if b is not None and s is not None else None,
            "source": "bcb-sgs",
            "as_of": f"{d}T00:00:00-03:00",
        })
    return pd.DataFrame.from_records(recs)

# -------------------- IPCA (SIDRA v3) --------------------
def fetch_ipca_period(start: Tuple[int, int], end: Tuple[int, int]) -> pd.DataFrame:
    y0, m0 = start
    y1, m1 = end
    period_str = f"{y0:04d}{m0:02d}-{y1:04d}{m1:02d}"
    url = f"{SIDRA_BASE}/agregados/{IPCA_TABLE}/periodos/{period_str}/variaveis/{IPCA_VAR_MOM}?localidades=N1[all]"
    r = http_get(url)
    payload = r.json()
    try:
        resultados = payload[0]["resultados"][0]["series"][0]
        serie = resultados.get("serie", {})
        unidade = resultados.get("unidade", "%")
    except Exception:
        return pd.DataFrame(columns=["ref_date", "variable", "value", "unit", "source"])

    recs = []
    for ref, val in serie.items():
        year = int(ref[:4]); month = int(ref[4:6])
        value = float(str(val).replace(".", "").replace(",", ".")) if val not in (None, "-") else None
        recs.append({
            "ref_date": f"{year:04d}-{month:02d}",
            "variable": "ipca_mom",
            "value": value,
            "unit": unidade,
            "source": "ibge-sidra-1737-63",
        })
    return pd.DataFrame.from_records(recs).sort_values("ref_date").reset_index(drop=True)

# -------------------- IGP-M (Ipeadata) --------------------
def fetch_igpm_period(start: Tuple[int, int], end: Tuple[int, int], sercodigo: str | None = None) -> pd.DataFrame:
    y0, m0 = start
    y1, m1 = end
    period_start = dt.date(y0, m0, 1)
    # define período fim como fim do mês end
    tmp = dt.date(y1, m1, 28) + dt.timedelta(days=10)
    period_end = tmp.replace(day=1) - dt.timedelta(days=1)

    # Tentativa 1: ipeadatapy
    try:
        import ipeadatapy as ipea  # type: ignore
        cat = ipea.list_series()
        m = cat["NAME"].str.contains("IGP-M", case=False, na=False)
        cand = cat[m]
        if not cand.empty:
            code = sercodigo or cand["CODE"].iloc[0]
            ts = ipea.timeseries(series=code)
            cols = {c.upper(): c for c in ts.columns}
            date_col = None
            for c in ("DATE", "DATA", "VALDATA", "RAW DATE"):
                if c in cols:
                    date_col = cols[c]; break
            if date_col is None and "YEAR" in cols and "MONTH" in cols:
                ts["__DATE__"] = pd.to_datetime(
                    ts[cols["YEAR"]].astype(int).astype(str) + "-" +
                    ts[cols["MONTH"]].astype(int).astype(str) + "-01",
                    errors="coerce")
                date_col = "__DATE__"
            if date_col is None:
                raise RuntimeError("coluna de data ausente na série IGP-M")
            value_col = cols.get("VALUE") or cols.get("VALOR") or cols.get("VALVALOR")
            if value_col is None:
                raise RuntimeError("coluna de valor ausente na série IGP-M")

            ts = ts.copy()
            ts[date_col] = pd.to_datetime(ts[date_col], errors="coerce")
            ts = ts.dropna(subset=[date_col])
            ts = ts[(ts[date_col] >= pd.Timestamp(period_start)) & (ts[date_col] <= pd.Timestamp(period_end))]
            ts["ref_date"] = ts[date_col].dt.strftime("%Y-%m")
            out = (ts.groupby("ref_date", as_index=False)[value_col]
                     .last().rename(columns={value_col: "value"}))
            out["variable"] = "igpm_mom"; out["unit"] = "%"; out["source"] = f"ipea-odata4-{code}"
            return out[["ref_date", "variable", "value", "unit", "source"]].sort_values("ref_date").reset_index(drop=True)
    except Exception:
        pass

    # Tentativa 2: OData direto, precisa de SERCODIGO
    if not sercodigo:
        candidate_codes = ("IGP12_IGPMG12", "IGPM12_IGPMG12", "IGPM12_IGPM12")
    else:
        candidate_codes = (sercodigo,)
    for code in candidate_codes:
        try:
            url = f"{IPEADATA_BASE}/ValoresSerie(SERCODIGO='{code}')"
            js = http_get(url).json().get("value", [])
            if not js:
                continue
            df = pd.DataFrame(js)
            if "VALDATA" not in df.columns or "VALVALOR" not in df.columns:
                continue
            df["VALDATA"] = pd.to_datetime(df["VALDATA"].str.slice(0, 19), errors="coerce")
            df = df.dropna(subset=["VALDATA"])
            df = df[(df["VALDATA"] >= pd.Timestamp(period_start)) & (df["VALDATA"] <= pd.Timestamp(period_end))]
            df["ref_date"] = df["VALDATA"].dt.strftime("%Y-%m")
            out = (df.groupby("ref_date", as_index=False)["VALVALOR"]
                     .last().rename(columns={"VALVALOR": "value"}))
            out["variable"] = "igpm_mom"; out["unit"] = "%"; out["source"] = f"ipea-odata4-{code}"
            return out[["ref_date", "variable", "value", "unit", "source"]].sort_values("ref_date").reset_index(drop=True)
        except Exception:
            continue

    return pd.DataFrame(columns=["ref_date", "variable", "value", "unit", "source"])

# -------------------- Persistência + Site --------------------
def write_outputs(df: pd.DataFrame, out_dir: str, name: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, f"{name}.csv")
    json_path = os.path.join(out_dir, f"{name}.json")
    df.to_csv(csv_path, index=False)
    _set_mtime(csv_path)
    df.to_json(json_path, orient="records", force_ascii=False)
    _set_mtime(json_path)
    print(f"wrote {csv_path} & {json_path} ({len(df)} rows)")

def build_site(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    assets = os.path.join(out_dir, "assets")
    os.makedirs(assets, exist_ok=True)
    css = """
:root{font-family:system-ui,Arial,sans-serif}
body{max-width:1000px;margin:2rem auto;padding:0 1rem}
header{margin-bottom:1rem}
h1{font-size:1.4rem}
.card{border:1px solid #e2e8f0;border-radius:12px;padding:1rem;margin:0 0 1rem}
table{width:100%;border-collapse:collapse}
th,td{padding:.5rem;border-bottom:1px solid #e2e8f0;text-align:left}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem}
.downloads a{margin-right:.75rem}
footer{color:#64748b;font-size:.9rem;margin-top:1rem}
"""
    style_path = os.path.join(assets, "style.css")
    with open(style_path, "w", encoding="utf-8") as f:
        f.write(css)
    _set_mtime(style_path)

    html = """<!doctype html>
<html lang="pt-br"><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<link rel="stylesheet" href="assets/style.css"/>
<title>Indicadores — PTAX, IPCA, IGP-M</title>
<body>
<header><h1>Indicadores para análise financeira</h1>
<p>Fonte: BCB/PTAX, IBGE/SIDRA e Ipeadata/FGV. Período: 2020-01+</p></header>
<div class="grid">
<section class="card"><h2>USD/BRL — PTAX (diário)</h2>
<div class="downloads"><a href="ptax_daily.json">JSON</a> <a href="ptax_daily.csv">CSV</a></div>
<div id="ptax"></div></section>
<section class="card"><h2>IPCA — var. mensal</h2>
<div class="downloads"><a href="ipca_monthly.json">JSON</a> <a href="ipca_monthly.csv">CSV</a></div>
<div id="ipca"></div></section>
<section class="card"><h2>IGP-M — var. mensal</h2>
<div class="downloads"><a href="igpm_monthly.json">JSON</a> <a href="igpm_monthly.csv">CSV</a></div>
<div id="igpm"></div></section>
</div>
<footer><p>Arquivos prontos para consumo por IA.</p></footer>
<script>
async function load(id, url){
  const el=document.getElementById(id);
  try{
    const r=await fetch(url); const data=await r.json();
    const rows=data.slice(-6).reverse();
    const tbl=document.createElement('table');
    tbl.innerHTML='<thead><tr><th>Data</th><th>Valor</th></tr></thead>';
    const tb=document.createElement('tbody');
    for(const row of rows){
      const ref=row.date||row.ref_date;
      const val=(row.mid ?? row.value);
      const v=(typeof val==='number')? val.toFixed(4): val;
      const tr=document.createElement('tr');
      tr.innerHTML=`<td>${ref}</td><td>${v}</td>`; tb.appendChild(tr);
    }
    tbl.appendChild(tb); el.appendChild(tbl);
  }catch(e){ el.textContent='Falha ao carregar.' }
}
load('ptax','ptax_daily.json');
load('ipca','ipca_monthly.json');
load('igpm','igpm_monthly.json');
</script>
</body></html>"""
    index_path = os.path.join(out_dir, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)
    _set_mtime(index_path)
    print(f"wrote {index_path} and {style_path}")

# -------------------- Runners --------------------
def run_ptax(args: argparse.Namespace) -> Optional[pd.DataFrame]:
    if not args.ptax:
        return None
    d0 = _parse_date(args.ptax[0])
    d1 = _parse_date(args.ptax[1]) if len(args.ptax) > 1 else d0
    try:
        df = fetch_ptax_period(d0, d1)
        if df.empty:
            raise ValueError("olinda empty; trying sgs")
    except Exception:
        df = fetch_ptax_sgs(d0, d1)
    write_outputs(df, args.out, "ptax_daily")
    return df

def run_ipca(args: argparse.Namespace) -> Optional[pd.DataFrame]:
    if not args.ipca:
        return None
    start = _parse_month(args.ipca[0])
    end = _parse_month(args.ipca[1]) if len(args.ipca) > 1 else start
    df = fetch_ipca_period(start, end)
    write_outputs(df, args.out, "ipca_monthly")
    return df

def run_igpm(args: argparse.Namespace) -> Optional[pd.DataFrame]:
    if not args.igpm:
        return None
    start = _parse_month(args.igpm[0])
    end = _parse_month(args.igpm[1]) if len(args.igpm) > 1 else start
    df = fetch_igpm_period(start, end, args.igpm_code)
    if df.empty:
        raise SystemExit("IGP-M não retornou. Informe --igpm-code com um SERCODIGO válido do Ipeadata.")
    write_outputs(df, args.out, "igpm_monthly")
    return df

def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ETL PTAX + IPCA + IGP-M")
    p.add_argument("--ptax", nargs="*", help="YYYY-MM-DD [YYYY-MM-DD] | today | yesterday")
    p.add_argument("--ipca", nargs="*", help="YYYY-MM [YYYY-MM] | thismonth | lastmonth")
    p.add_argument("--igpm", nargs="*", help="YYYY-MM [YYYY-MM] | thismonth | lastmonth")
    p.add_argument("--igpm-code", help="SERCODIGO para IGP-M no Ipeadata (opcional)")
    p.add_argument("--out", default="./json_consolidado", help="diretório de saída (default: ./json_consolidado)")
    p.add_argument("--site", action="store_true", help="gera site estático em OUT")
    return p

if __name__ == "__main__":
    ap = build_argparser()
    ns = ap.parse_args()
    any_run = False

    # Importa ETLMeta de forma tolerante (fallback se executado fora do repo)
    try:
        import sys as _sys, os as _os
        _here = _os.path.dirname(_os.path.abspath(__file__))
        if _here not in _sys.path:
            _sys.path.insert(0, _here)
        from etl_meta import ETLMeta  # type: ignore
        meta = ETLMeta("indicadores_etl", ns.out)
    except Exception:
        meta = None

    def _run(tag, fn):
        try:
            fn(ns)
            if meta:
                meta.ok(tag)
        except Exception as exc:
            if meta:
                meta.error(tag, exc)
            raise

    if ns.ptax:
        any_run = True
        _run("ptax", run_ptax)
    if ns.ipca:
        any_run = True
        _run("ipca", run_ipca)
    if ns.igpm:
        any_run = True
        _run("igpm", run_igpm)
    if ns.site:
        build_site(ns.out)

    if meta and any_run:
        meta.save()

    if not any_run and not ns.site:
        print("Nada a executar. Use --ptax, --ipca e/ou --igpm. -h para ajuda.")
