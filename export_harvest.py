#Teste Deploy 8h07m 04/11/25

import os
import requests
import pandas as pd
from datetime import date, timedelta, datetime, timezone
from dotenv import load_dotenv

load_dotenv()  # carrega o .env da raiz do projeto

TOKEN = os.getenv("HARVEST_TOKEN")
ACCOUNT_ID = os.getenv("HARVEST_ACCOUNT_ID")
USER_AGENT = os.getenv("USER_AGENT", "CamimReports/1.0")

if not TOKEN or not ACCOUNT_ID:
    raise SystemExit("HARVEST_TOKEN e HARVEST_ACCOUNT_ID são obrigatórios no .env")

BASE = "https://api.harvestapp.com/v2/time_entries"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Harvest-Account-Id": ACCOUNT_ID,
    "User-Agent": USER_AGENT,
}

EXPORT_DIR = "export_harvest"

def _set_mtime(path: str, dt: datetime | None = None) -> None:
    """Ajusta atime/mtime do arquivo para dt (ou agora) no timezone local."""
    when = dt or datetime.now(timezone.utc).astimezone()
    ts = when.timestamp()
    os.utime(path, (ts, ts))

def fetch_time_entries(dt_from: str, dt_to: str) -> list[dict]:
    page, rows = 1, []
    while True:
        r = requests.get(
            BASE,
            headers=HEADERS,
            params={"from": dt_from, "to": dt_to, "per_page": 2000, "page": page},
            timeout=60,
        )
        r.raise_for_status()
        payload = r.json()
        rows.extend(payload.get("time_entries", []))
        if page >= payload.get("total_pages", 1):
            break
        page += 1
    return rows


def export_last_7_days(out_dir: str = EXPORT_DIR) -> str:
    """
    Exporta sempre os últimos 7 dias corridos, incluindo hoje.
    Cria um novo arquivo a cada execução, sem sobrescrever.
    """
    # janela [hoje-6, hoje]
    today = date.today()
    dt_from_d = today - timedelta(days=6)
    dt_to_d = today

    dt_from = dt_from_d.strftime("%Y-%m-%d")
    dt_to = dt_to_d.strftime("%Y-%m-%d")

    entries = fetch_time_entries(dt_from, dt_to)
    table = pd.json_normalize(entries)

    if not table.empty:
        keep = [
            "id", "spent_date", "hours", "notes",
            "user.id", "user.name",
            "project.id", "project.name",
            "task.id", "task.name",
            "client.id", "client.name",
            "billable", "is_locked",
            "created_at", "updated_at",
        ]
        cols = [c for c in keep if c in table.columns]
        table = table[cols].sort_values(["spent_date", "user.name", "project.name"])

    # garante pasta e nome único
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"harvest_time_entries_{dt_from}_{dt_to}_{stamp}.csv"
    out_path = os.path.join(out_dir, fname)

    table.to_csv(out_path, index=False)
    _set_mtime(out_path)
    return f"OK: {len(table)} linhas > {out_path}"


if __name__ == "__main__":
    print(export_last_7_days())
