import os
import requests
import pandas as pd
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

def export_week(week_of: str, out_path: str) -> str:
    # week_of = segunda-feira da semana alvo, formato YYYY-MM-DD
    # to = domingo correspondente
    df = pd.to_datetime(week_of)
    dt_from = df.strftime("%Y-%m-%d")
    dt_to = (df + pd.Timedelta(days=6)).strftime("%Y-%m-%d")

    entries = fetch_time_entries(dt_from, dt_to)
    table = pd.json_normalize(entries)

    # opcional: ordenação e colunas core
    if not table.empty:
        keep = [
            "id","spent_date","hours","notes",
            "user.id","user.name",
            "project.id","project.name",
            "task.id","task.name",
            "client.id","client.name",
            "billable","is_locked",
            "created_at","updated_at",
        ]
        cols = [c for c in keep if c in table.columns]
        table = table[cols].sort_values(["spent_date","user.name","project.name"])

    table.to_csv(out_path, index=False)
    return f"OK: {len(table)} linhas > {out_path}"

if __name__ == "__main__":
    print(export_week("2025-10-13", "harvest_time_entries_2025-10-13_19.csv"))
