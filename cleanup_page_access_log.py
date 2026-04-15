#!/usr/bin/env python3
"""
cleanup_page_access_log.py — remove eventos antigos do page_access_log.

Retenção: RETENTION_DAYS (default 15).
Banco: PAGE_ACCESS_DB (default /opt/camim-auth/page_access.db).

Rodar via cron (diário). Exemplo em /etc/cron.d/camim-auditoria:
    15 3 * * * www-data /opt/camim-auth/venv/bin/python /opt/relatorio_h_t/cleanup_page_access_log.py
"""
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

PAGE_ACCESS_DB  = os.getenv("PAGE_ACCESS_DB", "/opt/camim-auth/page_access.db")
RETENTION_DAYS  = int(os.getenv("PAGE_ACCESS_RETENTION_DAYS", "15"))


def main() -> int:
    if not os.path.exists(PAGE_ACCESS_DB):
        print(f"[cleanup] db não existe: {PAGE_ACCESS_DB}")
        return 0

    cutoff = (datetime.now(timezone.utc).astimezone()
              - timedelta(days=RETENTION_DAYS)).isoformat(timespec="seconds")

    conn = sqlite3.connect(PAGE_ACCESS_DB)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        cur = conn.execute(
            "DELETE FROM page_access_log WHERE ts < ?", (cutoff,)
        )
        removidos = cur.rowcount
        conn.commit()
        conn.execute("VACUUM")
    finally:
        conn.close()

    print(f"[cleanup] removidos {removidos} eventos anteriores a {cutoff} "
          f"(retenção {RETENTION_DAYS} dias)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
