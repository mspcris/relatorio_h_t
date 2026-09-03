#!/usr/bin/env python3
"""
alerta_impostos.py — e-mail diário dos impostos em VERMELHO (Painel Financeiro).

Regra (Cristiano, 2026-09-03): cada imposto tem faixa amarela e vermelha; a
vermelha dispara e-mail para o Leonardo (leonardo@camim.com.br, ou o e-mail
gravado na faixa). Amarelo NÃO manda e-mail — fica só na tela.

Anti-rajada: grava em fin_imposto_alerta_envio (posto, imposto, mes_ref,
status) com UNIQUE — cada combinação recebe UM e-mail por mês de referência.
Como o mês avaliado é o último fechado, o mesmo alerta não repete todo dia;
quando vira o mês, avalia o mês novo e, se continuar vermelho, avisa de novo.

Kill-switches (padrão da casa, ver CLAUDE.md):
  • sem --run é DRY-RUN: mostra o que mandaria e não grava nada;
  • IMPOSTOS_ALERTA_EMAIL=0 no .env desliga mesmo com --run.

Cron (cron/relatorio_ht): 15 8 * * * … alerta_impostos.py --run
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
from dotenv import load_dotenv
for _p in (os.path.join(BASE_DIR, ".env"), "/opt/relatorio_h_t/.env"):
    if os.path.isfile(_p):
        load_dotenv(_p); break
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, "/opt/relatorio_h_t")

import painel_financeiro as pf

_BRT = timezone(timedelta(hours=-3))
BASE_URL = (os.getenv("APP_BASE_URL") or "https://kpi.camim.com.br").rstrip("/")


def _brl(v):
    return "R$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if v is not None else "—"


def _mes_br(ym):
    m = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
    return f"{m[int(ym[5:7]) - 1]}/{ym[:4]}"


def corpo_email(linhas: list[dict], nomes: dict) -> str:
    trs = []
    for r in linhas:
        motivo = "NÃO PAGO no mês" if r["nao_pago"] else f"desvio de {r['desvio_pct']:+.1f} % sobre a média de {r['base_meses']} meses"
        trs.append(
            f"<tr><td>{nomes.get(r['posto'], r['posto'])} ({r['posto']})</td><td>{r['rotulo']}</td>"
            f"<td>{_mes_br(r['mes_ref'])}</td><td style='text-align:right'>{_brl(r['valor_ref'])}</td>"
            f"<td style='text-align:right'>{_brl(r['media_base'])}</td><td><b style='color:#a12020'>{motivo}</b></td>"
            f"<td>faixa vermelha ≥ {r['faixa']['vermelho']:.0f} %</td></tr>")
    return f"""
    <div style="font-family:Arial,sans-serif;font-size:14px">
      <h2 style="color:#a12020;margin:0 0 8px">Painel Financeiro · impostos fora da faixa vermelha</h2>
      <p>Mês avaliado = último mês fechado. Média = meses com pagamento na base cadastrada, sem o mês avaliado.</p>
      <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;border-color:#ddd">
        <tr style="background:#f4f4f4"><th>Posto</th><th>Imposto</th><th>Mês</th><th>Pago</th><th>Média</th><th>Motivo</th><th>Faixa</th></tr>
        {''.join(trs)}
      </table>
      <p style="margin-top:12px"><a href="{BASE_URL}/painel_financeiro">Abrir o painel</a> — clique no imposto para ver os 36 meses e os lançamentos.</p>
      <p style="color:#888;font-size:12px">Enviado pelo robô alerta_impostos.py em {datetime.now(_BRT):%d/%m/%Y %H:%M}. Cada posto × imposto × mês recebe um único e-mail.</p>
    </div>"""


def nomes_postos() -> dict:
    try:
        from alarmes_db import POSTOS_NOMES
        return dict(POSTOS_NOMES)
    except Exception:
        return {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", action="store_true", help="envia de verdade e grava o registro (sem isso é dry-run)")
    args = ap.parse_args()
    ligado = os.getenv("IMPOSTOS_ALERTA_EMAIL", "1").strip() not in ("0", "false", "nao", "não")
    print(f"{datetime.now(_BRT):%Y-%m-%d %H:%M:%S} início run={args.run} envio_ligado={ligado}")

    dados = pf.painel()
    vermelhos = [r for r in dados["linhas"] if r["status"] == "vermelho"]
    print(f"  linhas avaliadas={len(dados['linhas'])} vermelhas={len(vermelhos)}")
    if not vermelhos:
        print("  nada a enviar"); return 0

    pg = pf.pg_conn()
    try:
        pf.ensure_schema(pg)
        with pg.cursor() as c:
            c.execute("SELECT posto, imposto, mes_ref FROM fin_imposto_alerta_envio WHERE status='vermelho'")
            ja = {(a, b, m) for a, b, m in c.fetchall()}
        novos = [r for r in vermelhos if (r["posto"], r["imposto"], r["mes_ref"]) not in ja]
        print(f"  já avisados={len(vermelhos) - len(novos)} novos={len(novos)}")
        if not novos:
            return 0
        # agrupa por e-mail de destino (faixa pode ter e-mail próprio)
        por_email: dict[str, list] = {}
        for r in novos:
            dest = (r["faixa"] or {}).get("email") or dados["email_padrao"]
            por_email.setdefault(dest, []).append(r)
        nomes = nomes_postos()
        for dest, linhas in por_email.items():
            assunto = f"[Painel Financeiro] {len(linhas)} imposto(s) fora da faixa vermelha — {_mes_br(linhas[0]['mes_ref'])}"
            for r in linhas:
                print(f"   → {dest}: {r['posto']} {r['imposto']} {r['mes_ref']} pago={r['valor_ref']} média={r['media_base']} desvio={r['desvio_pct']}")
            if not args.run:
                print(f"  DRY-RUN: não enviei para {dest}"); continue
            if not ligado:
                print(f"  IMPOSTOS_ALERTA_EMAIL=0: não enviei para {dest}"); continue
            from disparar_alarmes import enviar_email
            ok, msg = enviar_email(dest, assunto, corpo_email(linhas, nomes))
            print(f"  e-mail para {dest}: {'ok' if ok else 'FALHOU ' + msg}")
            if ok:
                with pg.cursor() as c:
                    for r in linhas:
                        c.execute("""INSERT INTO fin_imposto_alerta_envio (posto, imposto, mes_ref, status, valor, media, desvio_pct, email)
                                     VALUES (%s,%s,%s,'vermelho',%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                                  (r["posto"], r["imposto"], r["mes_ref"], r["valor_ref"], r["media_base"], r["desvio_pct"], dest))
                pg.commit()
    finally:
        pg.close()
    print(f"{datetime.now(_BRT):%Y-%m-%d %H:%M:%S} fim")
    return 0


if __name__ == "__main__":
    sys.exit(main())
