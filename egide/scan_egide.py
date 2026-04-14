"""Varredura completa do banco Égide (MySQL 8.4 — Aurora/RDS).

Descoberta durante o primeiro ensaio: apesar do nome do host ser
`egide.cc0jc67g6tt1.sa-east-1.rds.amazonaws.com`, o servidor responde
MySQL 8.4.8 (handshake começa com byte `I`), então este script usa
PyMySQL em vez de psycopg2.

Saídas:
    egide/scan_output/
      00_server_info.json
      01_schemas.json            (bases/schemas visíveis ao usuário)
      02_tables.json             (tabelas + colunas + PK + row_count + tamanho)
      03_foreign_keys.json
      04_indexes.json
      05_views.json              (com CREATE VIEW)
      06_routines.json           (FUNCTION / PROCEDURE + definição)
      07_samples.json            (até 5 linhas por tabela)
      08_triggers.json
      09_events.json
      10_users_grants.json
      SCHEMA_REPORT.md
"""

from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent / "scan_output"
OUT.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT / ".env")

CONN_KW = dict(
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"],
    charset="utf8mb4",
    connect_timeout=15,
    read_timeout=60,
    cursorclass=pymysql.cursors.DictCursor,
)

SKIP_SCHEMAS = ("mysql", "information_schema", "performance_schema", "sys")


def json_default(o: Any):
    if isinstance(o, (datetime, date, time)):
        return o.isoformat()
    if isinstance(o, timedelta):
        return str(o)
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (bytes, memoryview)):
        raw = bytes(o)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return f"<bytes len={len(raw)} hex={raw[:16].hex()}…>"
    if isinstance(o, set):
        return sorted(o)
    return str(o)


def dump(name: str, data: Any):
    path = OUT / name
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=json_default)
    print(f"  → {path.relative_to(ROOT)}  ({path.stat().st_size:,} B)")


def q(cur, sql: str, params: tuple | list = ()):
    cur.execute(sql, params)
    return cur.fetchall()


def main():
    print("Conectando ao MySQL Égide…")
    conn = pymysql.connect(**CONN_KW)
    cur = conn.cursor()

    # Modo read-only onde possível (alguns RDS não permitem SET session; ignorar erro)
    try:
        cur.execute("SET SESSION TRANSACTION READ ONLY")
    except Exception as e:
        print(f"  (read-only não aplicado: {e})")

    # 0) server info
    print("[0] Server info")
    info = {
        "version": q(cur, "SELECT VERSION() AS v")[0]["v"],
        "current_database": q(cur, "SELECT DATABASE() AS d")[0]["d"],
        "current_user": q(cur, "SELECT CURRENT_USER() AS u")[0]["u"],
        "now": q(cur, "SELECT NOW() AS n")[0]["n"],
        "tz": q(cur, "SELECT @@global.time_zone AS gtz, @@session.time_zone AS stz")[0],
        "sql_mode": q(cur, "SELECT @@sql_mode AS m")[0]["m"],
        "charset": q(cur, "SELECT @@character_set_server AS cs, @@collation_server AS col")[0],
    }
    dump("00_server_info.json", info)

    # 1) schemas visíveis
    print("[1] Schemas")
    schemas = q(
        cur,
        """
        SELECT SCHEMA_NAME AS schema_name,
               DEFAULT_CHARACTER_SET_NAME AS charset,
               DEFAULT_COLLATION_NAME AS collation
        FROM information_schema.SCHEMATA
        WHERE SCHEMA_NAME NOT IN %s
        ORDER BY SCHEMA_NAME
        """,
        (SKIP_SCHEMAS,),
    )
    dump("01_schemas.json", schemas)
    active = [s["schema_name"] for s in schemas]
    print(f"    {len(active)} schema(s): {', '.join(active)}")
    if not active:
        print("  (nenhum schema de usuário visível — usando o default)")
        active = [info["current_database"]]

    placeholders = ", ".join(["%s"] * len(active))

    # 2) tabelas + tamanho
    print("[2] Tabelas (metadados)")
    tables_meta = q(
        cur,
        f"""
        SELECT TABLE_SCHEMA AS schema_name,
               TABLE_NAME   AS table_name,
               TABLE_TYPE   AS table_type,
               ENGINE       AS engine,
               TABLE_ROWS   AS est_rows,
               DATA_LENGTH  AS data_bytes,
               INDEX_LENGTH AS index_bytes,
               DATA_LENGTH+IFNULL(INDEX_LENGTH,0) AS total_bytes,
               TABLE_COLLATION AS collation,
               CREATE_TIME, UPDATE_TIME,
               TABLE_COMMENT AS comment
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA IN ({placeholders})
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """,
        tuple(active),
    )

    # 2b) colunas
    cols = q(
        cur,
        f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME,
               COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_KEY,
               COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT,
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
               CHARACTER_SET_NAME, COLLATION_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA IN ({placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """,
        tuple(active),
    )
    cols_by: dict[tuple[str, str], list[dict]] = {}
    for c in cols:
        cols_by.setdefault((c["TABLE_SCHEMA"], c["TABLE_NAME"]), []).append(c)

    # 2c) PKs
    pks = q(
        cur,
        f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE CONSTRAINT_NAME = 'PRIMARY'
          AND TABLE_SCHEMA IN ({placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """,
        tuple(active),
    )
    pk_by: dict[tuple[str, str], list[str]] = {}
    for p in pks:
        pk_by.setdefault((p["TABLE_SCHEMA"], p["TABLE_NAME"]), []).append(p["COLUMN_NAME"])

    # 2d) row_count real (exato p/ tabelas pequenas; estimado p/ grandes)
    tables: list[dict] = []
    for t in tables_meta:
        key = (t["schema_name"], t["table_name"])
        entry = dict(t)
        entry["columns"] = cols_by.get(key, [])
        entry["primary_key"] = pk_by.get(key, [])
        est = int(t["est_rows"] or 0)
        if est > 500_000:
            entry["row_count"] = None
            entry["row_count_kind"] = "estimate_only"
        else:
            try:
                r = q(
                    cur,
                    f"SELECT COUNT(*) AS c FROM `{t['schema_name']}`.`{t['table_name']}`",
                )
                entry["row_count"] = int(r[0]["c"])
                entry["row_count_kind"] = "exact"
            except Exception as e:
                entry["row_count"] = None
                entry["row_count_kind"] = f"error: {e.__class__.__name__}: {e}"
        tables.append(entry)
    dump("02_tables.json", tables)
    print(f"    {len(tables)} tabela(s)")

    # 3) FKs
    print("[3] Foreign keys")
    fks = q(
        cur,
        f"""
        SELECT kcu.TABLE_SCHEMA     AS from_schema,
               kcu.TABLE_NAME       AS from_table,
               kcu.COLUMN_NAME      AS from_column,
               kcu.REFERENCED_TABLE_SCHEMA AS to_schema,
               kcu.REFERENCED_TABLE_NAME   AS to_table,
               kcu.REFERENCED_COLUMN_NAME  AS to_column,
               kcu.CONSTRAINT_NAME,
               rc.UPDATE_RULE, rc.DELETE_RULE
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
          ON rc.CONSTRAINT_NAME  = kcu.CONSTRAINT_NAME
         AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
        WHERE kcu.TABLE_SCHEMA IN ({placeholders})
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.ORDINAL_POSITION
        """,
        tuple(active),
    )
    dump("03_foreign_keys.json", fks)
    print(f"    {len(fks)} FK(s)")

    # 4) índices (via STATISTICS)
    print("[4] Índices")
    idxs_raw = q(
        cur,
        f"""
        SELECT TABLE_SCHEMA AS schema_name,
               TABLE_NAME   AS table_name,
               INDEX_NAME   AS index_name,
               NON_UNIQUE, INDEX_TYPE,
               SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, NULLABLE
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA IN ({placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
        """,
        tuple(active),
    )
    # agrupa colunas por índice
    idx_map: dict[tuple, dict] = {}
    for r in idxs_raw:
        key = (r["schema_name"], r["table_name"], r["index_name"])
        idx_map.setdefault(
            key,
            {
                "schema_name": r["schema_name"],
                "table_name": r["table_name"],
                "index_name": r["index_name"],
                "unique": r["NON_UNIQUE"] == 0,
                "type": r["INDEX_TYPE"],
                "columns": [],
            },
        )["columns"].append(r["COLUMN_NAME"])
    idxs = list(idx_map.values())
    dump("04_indexes.json", idxs)
    print(f"    {len(idxs)} índice(s)")

    # 5) views
    print("[5] Views")
    views_meta = q(
        cur,
        f"""
        SELECT TABLE_SCHEMA AS schema_name,
               TABLE_NAME   AS view_name,
               VIEW_DEFINITION AS definition,
               IS_UPDATABLE, DEFINER, SECURITY_TYPE
        FROM information_schema.VIEWS
        WHERE TABLE_SCHEMA IN ({placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """,
        tuple(active),
    )
    # SHOW CREATE VIEW p/ cada uma
    for v in views_meta:
        try:
            cur.execute(f"SHOW CREATE VIEW `{v['schema_name']}`.`{v['view_name']}`")
            row = cur.fetchone()
            v["create_view"] = row.get("Create View") if row else None
        except Exception as e:
            v["create_view"] = f"<erro: {e}>"
    dump("05_views.json", views_meta)
    print(f"    {len(views_meta)} view(s)")

    # 6) routines (FUNCTION / PROCEDURE)
    print("[6] Functions / procedures")
    routines_meta = q(
        cur,
        f"""
        SELECT ROUTINE_SCHEMA AS schema_name,
               ROUTINE_NAME   AS routine_name,
               ROUTINE_TYPE   AS kind,
               DATA_TYPE      AS returns,
               DEFINER, CREATED, LAST_ALTERED,
               ROUTINE_COMMENT AS comment,
               ROUTINE_DEFINITION AS definition
        FROM information_schema.ROUTINES
        WHERE ROUTINE_SCHEMA IN ({placeholders})
        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """,
        tuple(active),
    )
    dump("06_routines.json", routines_meta)
    print(f"    {len(routines_meta)} routine(s)")

    # 7) samples
    print("[7] Amostras (até 5 linhas / tabela)")
    samples: dict[str, list[dict]] = {}
    errors: dict[str, str] = {}
    for t in tables:
        key = f"{t['schema_name']}.{t['table_name']}"
        try:
            cur.execute(
                f"SELECT * FROM `{t['schema_name']}`.`{t['table_name']}` LIMIT 5"
            )
            samples[key] = cur.fetchall()
        except Exception as e:
            errors[key] = f"{e.__class__.__name__}: {e}"
    dump("07_samples.json", {"samples": samples, "errors": errors})
    print(f"    {len(samples)} com amostra, {len(errors)} com erro")

    # 8) triggers
    print("[8] Triggers")
    triggers = q(
        cur,
        f"""
        SELECT TRIGGER_SCHEMA AS schema_name, TRIGGER_NAME AS trigger_name,
               EVENT_MANIPULATION, EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE,
               ACTION_TIMING, ACTION_STATEMENT, DEFINER
        FROM information_schema.TRIGGERS
        WHERE TRIGGER_SCHEMA IN ({placeholders})
        ORDER BY TRIGGER_SCHEMA, EVENT_OBJECT_TABLE, TRIGGER_NAME
        """,
        tuple(active),
    )
    dump("08_triggers.json", triggers)
    print(f"    {len(triggers)} trigger(s)")

    # 9) events
    print("[9] Events")
    try:
        events = q(
            cur,
            f"""
            SELECT EVENT_SCHEMA AS schema_name, EVENT_NAME AS event_name,
                   STATUS, EVENT_TYPE, EXECUTE_AT, INTERVAL_VALUE, INTERVAL_FIELD,
                   STARTS, ENDS, EVENT_DEFINITION, DEFINER
            FROM information_schema.EVENTS
            WHERE EVENT_SCHEMA IN ({placeholders})
            ORDER BY EVENT_SCHEMA, EVENT_NAME
            """,
            tuple(active),
        )
    except Exception as e:
        events = [{"error": str(e)}]
    dump("09_events.json", events)
    print(f"    {len(events)} event(s)")

    # 10) usuário e grants (apenas para o usuário atual)
    print("[10] Grants do usuário atual")
    try:
        cur.execute("SHOW GRANTS FOR CURRENT_USER()")
        grants = [list(r.values())[0] for r in cur.fetchall()]
    except Exception as e:
        grants = [f"<erro: {e}>"]
    dump("10_users_grants.json", {"user": info["current_user"], "grants": grants})

    # ---- relatório markdown ----
    print("[R] Gerando SCHEMA_REPORT.md")
    build_report(
        info, schemas, tables, fks, idxs, views_meta, routines_meta, triggers, events, grants
    )

    cur.close()
    conn.close()
    print("\n✔ Varredura concluída.")


def build_report(info, schemas, tables, fks, idxs, views, routines, triggers, events, grants):
    lines: list[str] = []
    L = lines.append
    L(f"# Varredura do banco Égide — `{info['current_database']}`")
    L("")
    L(f"- **Servidor:** `{os.environ['DB_HOST']}:{os.environ['DB_PORT']}`")
    L(f"- **Usuário:** `{info['current_user']}`")
    L(f"- **Versão MySQL:** `{info['version']}`")
    L(f"- **SQL mode:** `{info['sql_mode']}`")
    L(f"- **Charset:** `{info['charset']['cs']}` / `{info['charset']['col']}`")
    L(f"- **Timezone:** global `{info['tz']['gtz']}` / sessão `{info['tz']['stz']}`")
    L(f"- **Coletado em:** `{info['now']}`")
    L("")

    L("## Sumário")
    L(f"- Schemas visíveis: **{len(schemas)}**")
    L(f"- Tabelas base: **{len(tables)}**")
    L(f"- Views: **{len(views)}**")
    L(f"- Foreign keys: **{len(fks)}**")
    L(f"- Índices: **{len(idxs)}**")
    L(f"- Routines (func/proc): **{len(routines)}**")
    L(f"- Triggers: **{len(triggers)}**")
    L(f"- Events: **{len(events)}**")
    L("")

    if grants:
        L("## Permissões (CURRENT_USER)")
        L("```")
        for g in grants:
            L(str(g))
        L("```")
        L("")

    L("## Schemas")
    for s in schemas:
        L(f"- `{s['schema_name']}` — charset `{s['charset']}`, collation `{s['collation']}`")
    L("")

    # Tabelas por schema
    by_schema: dict[str, list[dict]] = {}
    for t in tables:
        by_schema.setdefault(t["schema_name"], []).append(t)

    L("## Tabelas")
    for sch, ts in by_schema.items():
        L(f"### Schema `{sch}` — {len(ts)} tabela(s)")
        L("")
        L("| Tabela | Linhas | Engine | Bytes totais | PK | Comentário |")
        L("|---|---:|---|---:|---|---|")
        for t in ts:
            rc = t["row_count"]
            rc_s = f"{rc:,}" if isinstance(rc, int) else f"~{int(t['est_rows'] or 0):,}*"
            pk = ", ".join(t["primary_key"]) or "—"
            cm = (t["comment"] or "").replace("\n", " ")[:80]
            tb = int(t["total_bytes"] or 0)
            L(f"| `{t['table_name']}` | {rc_s} | {t['engine']} | {tb:,} | {pk} | {cm} |")
        L("")
        for t in ts:
            L(f"#### `{sch}.{t['table_name']}`")
            if t["comment"]:
                L(f"> {t['comment']}")
            L("")
            L("| # | coluna | tipo | null | key | default | extra | comentário |")
            L("|--:|---|---|:-:|:-:|---|---|---|")
            for c in t["columns"]:
                nn = "✓" if c["IS_NULLABLE"] == "YES" else "✗"
                df = str(c["COLUMN_DEFAULT"]) if c["COLUMN_DEFAULT"] is not None else ""
                df = df.replace("\n", " ")[:40]
                cm = (c["COLUMN_COMMENT"] or "").replace("\n", " ")[:60]
                L(
                    f"| {c['ORDINAL_POSITION']} | `{c['COLUMN_NAME']}` | `{c['COLUMN_TYPE']}` "
                    f"| {nn} | {c['COLUMN_KEY'] or ''} | {df} | {c['EXTRA'] or ''} | {cm} |"
                )
            L("")

    L("## Foreign Keys")
    if not fks:
        L("_nenhuma_")
    else:
        L("| Origem | → | Destino | on update | on delete |")
        L("|---|---|---|---|---|")
        for f in fks:
            L(
                f"| `{f['from_schema']}.{f['from_table']}.{f['from_column']}` | → "
                f"| `{f['to_schema']}.{f['to_table']}.{f['to_column']}` "
                f"| {f['UPDATE_RULE']} | {f['DELETE_RULE']} |"
            )
    L("")

    L("## Views")
    if not views:
        L("_nenhuma_")
    else:
        for v in views:
            L(f"### `{v['schema_name']}.{v['view_name']}`")
            L("```sql")
            L((v.get("create_view") or v["definition"] or "").strip())
            L("```")
            L("")

    L("## Functions / Procedures")
    if not routines:
        L("_nenhuma_")
    else:
        for r in routines:
            L(f"### `{r['schema_name']}.{r['routine_name']}` — {r['kind']} → `{r['returns']}`")
            if r["comment"]:
                L(f"> {r['comment']}")
            if r["definition"]:
                L("```sql")
                L(r["definition"].strip())
                L("```")
            L("")

    L("## Triggers")
    if not triggers:
        L("_nenhuma_")
    else:
        for t in triggers:
            L(
                f"- `{t['schema_name']}.{t['trigger_name']}` — "
                f"{t['ACTION_TIMING']} {t['EVENT_MANIPULATION']} on "
                f"`{t['EVENT_OBJECT_SCHEMA']}.{t['EVENT_OBJECT_TABLE']}`"
            )
    L("")

    L("## Events (MySQL scheduler)")
    if not events or (len(events) == 1 and "error" in events[0]):
        L("_nenhum (ou sem permissão)_")
    else:
        for e in events:
            L(f"- `{e['schema_name']}.{e['event_name']}` — status `{e['STATUS']}`")
    L("")

    L("## Índices (resumo)")
    L(f"Total: **{len(idxs)}**. Detalhes em `scan_output/04_indexes.json`.")
    L("")

    (OUT / "SCHEMA_REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"  → {(OUT / 'SCHEMA_REPORT.md').relative_to(ROOT)}")


if __name__ == "__main__":
    try:
        main()
    except pymysql.err.OperationalError as e:
        print(f"ERRO de conexão: {e}", file=sys.stderr)
        sys.exit(2)
