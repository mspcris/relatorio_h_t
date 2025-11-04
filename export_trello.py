#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exporta um quadro do Trello para CSVs.
Requer env vars: TRELLO_KEY, TRELLO_TOKEN
Uso: python export_trello.py --board <BOARD_ID> --out ./export_trello
"""
#Teste Deploy 8h07m 04/11/25
from dotenv import load_dotenv; load_dotenv()
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))  # carrega ./.env

import os, csv, json, time, argparse, pathlib
from urllib.parse import urlencode
import requests
from dateutil import parser as dtp
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Python 3.9+

API = "https://api.trello.com/1"

def _set_mtime(path: Path | os.PathLike | str, dt: datetime | None = None) -> None:
    """Ajusta atime/mtime para dt (ou agora) no timezone local."""
    when = dt or datetime.now(timezone.utc).astimezone()
    ts = when.timestamp()
    os.utime(path, (ts, ts))

def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"ERRO: defina a variável de ambiente {name}")
    return v

def trello_get(path: str, **params):
    key = env("TRELLO_KEY")
    token = env("TRELLO_TOKEN")
    qp = {"key": key, "token": token}
    qp.update(params)
    url = f"{API}{path}?{urlencode(qp, doseq=True)}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"Falha {r.status_code} em {url}\n{r.text}")
    return r.json()

def write_csv(path, rows, fieldnames, mtime_dt: datetime | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})
    _set_mtime(path, mtime_dt)

def iso(s):
    if not s: return ""
    try:
        return dtp.parse(s).isoformat()
    except Exception:
        return s

def fetch_board(board_id):
    board = trello_get(
        f"/boards/{board_id}",
        fields="name,url,desc,closed,dateLastActivity,prefs"
    )
    lists = trello_get(f"/boards/{board_id}/lists", fields="name,closed,pos")
    lists_open = [l for l in lists if not l.get("closed")]
    list_map = {l["id"]: l["name"] for l in lists}

    members = trello_get(f"/boards/{board_id}/members", fields="fullName,username")
    member_map = {m["id"]: m for m in members}

    labels = trello_get(f"/boards/{board_id}/labels", fields="name,color")
    label_map = {lab["id"]: lab for lab in labels}

    try:
        custom_fields = trello_get(f"/boards/{board_id}/customFields")
    except SystemExit:
        custom_fields = []
    cf_map = {cf["id"]: cf for cf in custom_fields}

    cards = trello_get(
        f"/boards/{board_id}/cards",
        fields="name,desc,closed,url,idList,idMembers,due,start,dueComplete,labels,dateLastActivity,shortLink",
        attachments="true",
        attachment_fields="name,url,bytes,date,isUpload,mimeType",
        members="true",
        checklists="all"
    )

    checklists = trello_get(f"/boards/{board_id}/checklists")

    return {
        "board": board,
        "lists": lists_open,
        "members": members,
        "labels": labels,
        "custom_fields": custom_fields,
        "cards": cards,
        "checklists": checklists,
        "maps": {"list": list_map, "member": member_map, "label": label_map, "cf": cf_map},
    }

def flatten_cards(data):
    list_map = data["maps"]["list"]
    member_map = data["maps"]["member"]
    rows = []
    for c in data["cards"]:
        labels = c.get("labels", []) or []
        lab_str = ", ".join([lab["name"] or lab.get("color","") for lab in labels])
        members = c.get("idMembers", []) or []
        member_str = ", ".join([member_map[m]["username"] for m in members if m in member_map])
        atts = c.get("attachments", []) or []
        att_count = len(atts)
        att_urls = " | ".join(a.get("url","") for a in atts)
        rows.append({
            "card_id": c["id"],
            "card": c["name"],
            "list": list_map.get(c["idList"], ""),
            "labels": lab_str,
            "members": member_str,
            "due": iso(c.get("due")),
            "start": iso(c.get("start")),
            "due_complete": c.get("dueComplete"),
            "url": c.get("url"),
            "short_link": c.get("shortLink"),
            "closed": c.get("closed"),
            "updated": iso(c.get("dateLastActivity")),
            "desc": c.get("desc",""),
            "attachments_count": att_count,
            "attachments_urls": att_urls,
        })
    return rows

def flatten_checklists(data):
    rows = []
    for cl in data["checklists"]:
        for item in cl.get("checkItems", []):
            rows.append({
                "checklist_id": cl["id"],
                "checklist": cl.get("name",""),
                "card_id": cl.get("idCard",""),
                "item_id": item.get("id",""),
                "item": item.get("name",""),
                "state": item.get("state",""),
                "pos": item.get("pos",""),
                "due": iso(item.get("due")),
                "member_id": item.get("idMember"),
            })
    return rows

def main():
    ap = argparse.ArgumentParser(description="Exporta quadro do Trello para CSVs")
    ap.add_argument("--board", required=True, help="ID do quadro")
    ap.add_argument(
        "--out",
        default="./export_trello",
        help="pasta PAI de saída; será criada subpasta export_YYYYMMDD_HHMMSS"
    )
    args = ap.parse_args()

    # subpasta exclusiva por execução, em timezone São Paulo
    tz = ZoneInfo("America/Sao_Paulo")
    stamp = datetime.now(tz).strftime("export_%Y%m%d_%H%M%S")
    outdir = pathlib.Path(args.out) / stamp

    t0 = time.time()
    data = fetch_board(args.board)
    # timestamp de referência: máximo entre board e cards/attachments
cand = []
try:
    cand.append(dtp.parse(data["board"].get("dateLastActivity") or ""))
except Exception:
    pass
for c in data.get("cards", []):
    try:
        cand.append(dtp.parse(c.get("dateLastActivity") or ""))
    except Exception:
        pass
    for a in c.get("attachments", []) or []:
        try:
            cand.append(dtp.parse(a.get("date") or ""))
        except Exception:
            pass
    ref_dt = max([d for d in cand if isinstance(d, datetime)], default=datetime.now(timezone.utc))
    ref_dt_local = ref_dt.astimezone(ZoneInfo("America/Sao_Paulo"))

    write_csv(
        outdir / "board.csv",
        [data["board"]],
        fieldnames=["id","name","url","desc","closed","dateLastActivity"],
        mtime_dt=ref_dt_local
    write_csv(
        outdir / "lists.csv",
        data["lists"],
        fieldnames=["id","name","pos","closed","idBoard"],
        mtime_dt=ref_dt_local
    )
    write_csv(
        outdir / "members.csv",
        [{"id": m["id"], "username": m["username"], "fullName": m["fullName"]} for m in data["members"]],
        fieldnames=["id","username","fullName"]
    )
    write_csv(
        outdir / "labels.csv",
        [{"id": l["id"], "name": l.get("name",""), "color": l.get("color","")} for l in data["labels"]],
        fieldnames=["id","name","color"],
        mtime_dt=ref_dt_local
    )

    card_rows = flatten_cards(data)
    write_csv(
        outdir / "cards.csv",
        card_rows,
        fieldnames=["card_id","card","list","labels","members","due","start","due_complete","url","short_link","closed","updated","desc","attachments_count","attachments_urls"],
        mtime_dt=ref_dt_local
    )

    chk_rows = flatten_checklists(data)
    write_csv(
        outdir / "checklists.csv",
        chk_rows,
        fieldnames=["checklist_id","checklist","card_id","item_id","item","state","pos","due","member_id"],
        mtime_dt=ref_dt_local
    )

    att_rows = []
    for c in data["cards"]:
        for a in c.get("attachments", []) or []:
            att_rows.append({
                "card_id": c["id"],
                "card": c["name"],
                "attachment_id": a.get("id",""),
                "name": a.get("name",""),
                "url": a.get("url",""),
                "bytes": a.get("bytes",""),
                "mimeType": a.get("mimeType",""),
                "date": iso(a.get("date")),
                "isUpload": a.get("isUpload",""),
            })
    write_csv(
        outdir / "attachments.csv",
        att_rows,
        fieldnames=["card_id","card","attachment_id","name","url","bytes","mimeType","date","isUpload"],
        mtime_dt=ref_dt_local
    )

    if data["custom_fields"]:
        write_csv(
            outdir / "custom_fields.csv",
            data["custom_fields"],
            fieldnames=["id","name","type","idModel","modelType","pos","options"],
            mtime_dt=ref_dt_local
        )

    print(f"Export OK: {outdir}  ({time.time()-t0:.1f}s)")

if __name__ == "__main__":
    main()
