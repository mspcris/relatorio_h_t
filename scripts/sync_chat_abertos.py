#!/usr/bin/env python3
"""Gera o JSON enxuto dos tickets de chat em aberto para o widget da home.

Lê o export completo do relatorio_h_t (chat_dashboard.json) e escreve um arquivo
pequeno com só os tickets ABERTOS, do mais antigo para o mais novo, contendo
apenas os campos que o widget mostra (número, cliente, fila, data de abertura).

O widget da intranet lê o resultado same-origin em /media/chat_abertos.json.
Como o kpi.camim.com.br roda em outra VM, rode este script onde o export existe
(ou após copiá-lo) e entregue o --out no volume `media` da intranet.

Uso típico (cron):
    python3 sync_chat_abertos.py \
        --src /opt/relatorio_h_t/json_consolidado/chat_dashboard.json \
        --out /opt/intranet/media/chat_abertos.json

Campos do export (tickets_index[id]):
    n=número  c=cliente  f=fila  d=abertura "DD/MM/YYYY HH:MM"  z=fechamento
    z == "aberto"  ->  ticket ainda aberto.
"""
import argparse
import json
import os
import sys
import tempfile


def parse_br(s):
    """'DD/MM/YYYY HH:MM' -> (ano, mes, dia, hora, min) p/ ordenar. None se inválido."""
    s = (s or "").strip()
    try:
        data, _, hora = s.partition(" ")
        d, m, a = data.split("/")
        hh, _, mm = hora.partition(":")
        return (int(a), int(m), int(d), int(hh or 0), int(mm or 0))
    except (ValueError, AttributeError):
        return None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="/opt/relatorio_h_t/json_consolidado/chat_dashboard.json",
                    help="export completo do chat (chat_dashboard.json)")
    ap.add_argument("--out", default="-",
                    help="JSON enxuto de saída ('-' = stdout, default)")
    ap.add_argument("--limit", type=int, default=80,
                    help="máximo de linhas no arquivo (os mais antigos primeiro)")
    ap.add_argument("--include-outbound", action="store_true",
                    help="inclui também tickets outbound (default: só inbound)")
    args = ap.parse_args()

    try:
        with open(args.src, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as e:
        print(f"erro lendo {args.src}: {e}", file=sys.stderr)
        return 1

    idx = data.get("tickets_index") or {}
    abertos = []
    for t in idx.values():
        if t.get("z") != "aberto":
            continue
        if not args.include_outbound and not str(t.get("b") or "").startswith("inbound_"):
            continue
        ts = parse_br(t.get("d"))
        if ts is None:
            continue
        abertos.append((ts, {
            "n": t.get("n"),
            "c": t.get("c") or "",
            "f": t.get("f") or "",
            "d": t.get("d") or "",
        }))

    abertos.sort(key=lambda x: x[0])  # mais antigo primeiro
    items = [it for _, it in abertos[:args.limit]]

    payload = {
        "gerado_em_br": (data.get("meta") or {}).get("gerado_em_br", ""),
        "count": len(abertos),   # total real de abertos (items pode estar limitado)
        "items": items,
    }

    # allow_nan=False: o browser rejeita NaN no JSON.parse (cai num 404 enganoso).
    if args.out == "-":
        json.dump(payload, sys.stdout, ensure_ascii=False, allow_nan=False)
        sys.stdout.write("\n")
        return 0

    # Escrita atômica em arquivo.
    out_dir = os.path.dirname(os.path.abspath(args.out)) or "."
    os.makedirs(out_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=out_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, allow_nan=False)
        os.replace(tmp, args.out)
    except Exception:
        os.path.exists(tmp) and os.unlink(tmp)
        raise

    print(f"ok: {len(items)} linhas (de {len(abertos)} abertos) -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
