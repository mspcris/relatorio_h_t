"""
monitor_previsao_wpp.py — Robô-fiscal TEMPORÁRIO dos disparos de cobrança.

Criado em 2026-08-10 a pedido do Cristiano, depois do incidente do fd leak
(cron de cobrança morrendo no meio da rodada por dias sem ninguém ver).

O que faz, de hora em hora (08h–20h, via /etc/cron.d/monitor-previsao-wpp):
  1. Refaz a análise do Controle de Disparos para HOJE (wpp_previsao) — o que
     também mantém a página /wpp/previsao sempre atualizada;
  2. Compara com a hora anterior: previstas devem virar enviadas;
  3. Manda e-mail para o Cristiano com o quadro por campanha + avisos + link
     da página. Se havia previstas e os envios NÃO andaram (ou o cron
     crashou), o assunto vira [ALERTA] e o corpo traz o diagnóstico completo
     (tail do log do cron, erros de posto, motivos);
  4. Grava log robusto em /var/log/relatorio_h_t/monitor_previsao_wpp.log.

TEMPORÁRIO — EXPIRA SOZINHO em 2026-08-17 (1 semana). Depois disso o script
sai sem fazer nada e o cron pode ser removido:
    rm /etc/cron.d/monitor-previsao-wpp

Roda como www-data (mesmo usuário do serviço web) para que os arquivos de
estado da previsão continuem graváveis pela página.
"""

import os
import sys
import json
import time
import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, datetime

sys.path.insert(0, "/opt/relatorio_h_t")

from dotenv import load_dotenv
load_dotenv("/opt/relatorio_h_t/.env")

# --- Configuração -----------------------------------------------------------
EXPIRA_EM = date(2026, 8, 17)          # TEMPORÁRIO: morre sozinho em 1 semana
DESTINO = os.getenv("MONITOR_PREVISAO_EMAIL", "cristiano@camim.com.br")
LINK_PAGINA = "https://camila1.ia.camim.com.br/wpp/previsao"
SYNC_LOG = "/var/log/relatorio_h_t/sync_wpp.log"
TIMEOUT_ANALISE_S = 15 * 60

EMAIL_HOST = os.getenv("ALARM_EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("ALARM_EMAIL_PORT", "465"))
EMAIL_USER = os.getenv("ALARM_EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("ALARM_EMAIL_PASSWORD", "")
EMAIL_FROM = os.getenv("ALARM_EMAIL_FROM", "") or EMAIL_USER


def log(msg: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} {msg}", flush=True)


# --- Análise ----------------------------------------------------------------

def rodar_analise():
    """Dispara/espera a análise de hoje e devolve (resultado, status)."""
    import wpp_previsao as pv
    hoje = date.today().isoformat()
    ok, msg = pv.iniciar(hoje)
    log(f"analise iniciar: ok={ok} msg={msg}")
    ini = time.time()
    while time.time() - ini < TIMEOUT_ANALISE_S:
        time.sleep(10)
        s = pv.status()
        if not s.get("running"):
            break
    s = pv.status()
    res = pv.resultado()
    if res and res.get("data") != hoje:
        res = None
    return res, s


def estado_anterior(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def diagnostico_cron() -> dict:
    """Sinais de vida/morte do cron de cobrança no log de hoje."""
    d = {"tracebacks_hoje": 0, "tail": "", "rodando_agora": False}
    try:
        with open(SYNC_LOG, encoding="utf-8", errors="replace") as f:
            linhas = f.readlines()
        d["tracebacks_hoje"] = sum(1 for l in linhas
                                   if "Traceback" in l or "unable to open database" in l)
        d["tail"] = "".join(linhas[-25:])
    except Exception as e:
        d["tail"] = f"(não consegui ler {SYNC_LOG}: {e})"
    try:
        out = subprocess.run(["pgrep", "-f", "send_whatsapp_cobranca.py"],
                             capture_output=True, text=True, timeout=10)
        d["rodando_agora"] = bool(out.stdout.strip())
    except Exception:
        pass
    return d


# --- E-mail -----------------------------------------------------------------

def enviar_email(assunto: str, corpo: str) -> bool:
    if not EMAIL_USER or not EMAIL_PASSWORD:
        log("EMAIL: credencial ALARM_EMAIL_* ausente — e-mail NÃO enviado")
        return False
    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    msg["From"] = EMAIL_FROM
    msg["To"] = DESTINO
    msg.attach(MIMEText(corpo, "plain", "utf-8"))
    try:
        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, timeout=30) as s:
            s.login(EMAIL_USER, EMAIL_PASSWORD)
            s.sendmail(EMAIL_FROM, [DESTINO], msg.as_string())
        log(f"EMAIL enviado para {DESTINO}: {assunto}")
        return True
    except Exception as e:
        log(f"EMAIL FALHOU: {e}")
        return False


def montar_corpo(res: dict, alertas: list[str], antes: dict, diag: dict) -> str:
    r = res["resumo"]
    linhas = [
        f"Controle de Disparos — retrato de {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        f"Página: {LINK_PAGINA}",
        "",
    ]
    if alertas:
        linhas.append("*** ALERTAS ***")
        linhas += [f"  ! {a}" for a in alertas]
        linhas.append("")
    linhas += [
        f"Aguardam envio hoje: {r['previstas']}",
        f"Já enviadas hoje:    {r['enviadas']}"
        + (f"  (eram {antes.get('enviadas')} na última checagem)" if antes else ""),
        f"Não serão enviadas:  {r['bloqueadas']} (regras — detalhe na página)",
        f"Erros:               {r['erros']}",
        "",
        "Por campanha:",
    ]
    for c in res["campanhas"]:
        cont = c["contagem"]
        total = cont["previstas"] + cont["enviadas"] + cont["bloqueadas"] + cont["erros"]
        if c["status_campanha"] != "ok" and total == 0:
            continue  # suspensas/sem movimento não poluem o e-mail
        linhas.append(
            f"  [{c['id']:>2}] {c['nome'][:48]:<48} "
            f"previstas={cont['previstas']:>5}  enviadas={cont['enviadas']:>5}  "
            f"bloqueadas={cont['bloqueadas']:>5}  erros={cont['erros']}")
        for e in (c.get("erros_postos") or []):
            linhas.append(f"        ! {e}")
        if c.get("motivo_campanha"):
            linhas.append(f"        · {c['motivo_campanha']}")
    if res.get("avisos"):
        linhas.append("")
        linhas.append("Avisos da análise:")
        linhas += [f"  · {a}" for a in res["avisos"]]
    linhas += [
        "",
        f"Cron de cobrança: {'RODANDO agora' if diag['rodando_agora'] else 'parado neste instante (normal entre rodadas)'}"
        f" · tracebacks no log de hoje: {diag['tracebacks_hoje']}",
        "",
        "— Robô-fiscal temporário (expira 17/08/2026). Log completo em "
        "/var/log/relatorio_h_t/monitor_previsao_wpp.log na vps154.",
    ]
    return "\n".join(linhas)


# --- Main -------------------------------------------------------------------

def main() -> int:
    hoje = date.today()
    if hoje > EXPIRA_EM:
        log(f"EXPIRADO em {EXPIRA_EM.isoformat()} — nada a fazer. "
            "Remover: rm /etc/cron.d/monitor-previsao-wpp")
        return 0

    log("=== monitor_previsao_wpp: início ===")
    import wpp_previsao as pv
    estado_path = os.path.join(pv._STATE_DIR, "monitor_previsao_estado.json")
    antes = estado_anterior(estado_path)
    if antes.get("data") != hoje.isoformat():
        antes = {}  # virou o dia — não comparar com ontem

    res, st = rodar_analise()
    diag = diagnostico_cron()

    if res is None:
        corpo = (
            f"A análise do Controle de Disparos NÃO CONCLUIU "
            f"(status: {json.dumps(st, ensure_ascii=False)}).\n\n"
            f"Cron de cobrança: tracebacks hoje={diag['tracebacks_hoje']}\n\n"
            f"Últimas linhas do log do cron:\n{diag['tail']}\n\n"
            f"Página: {LINK_PAGINA}")
        log("ALERTA: análise não concluiu")
        enviar_email("[ALERTA] WPP Cobrança — análise da previsão falhou", corpo)
        return 1

    r = res["resumo"]
    alertas: list[str] = []

    # 1) Cron crashando hoje
    if diag["tracebacks_hoje"] > 0:
        alertas.append(
            f"O log do cron tem {diag['tracebacks_hoje']} traceback(s) hoje — "
            "rodadas podem estar morrendo no meio. Tail do log no fim do e-mail... "
            "conferir /var/log/relatorio_h_t/sync_wpp.log na vps154.")

    # 2) Previstas não viram enviadas (dentro da janela 08-20h, com base anterior)
    agora = datetime.now()
    if antes and 9 <= agora.hour < 21:
        if r["previstas"] > 0 and r["enviadas"] <= antes.get("enviadas", 0):
            alertas.append(
                f"Há {r['previstas']} mensagens previstas e o total enviado NÃO "
                f"aumentou desde a última checagem ({antes.get('enviadas', 0)} → "
                f"{r['enviadas']}). Se o cron não estiver rodando neste momento, "
                "algo está travado.")

    # 3) Erros de API
    if r["erros"] > 0:
        alertas.append(f"{r['erros']} envio(s) com erro de API hoje — detalhe na página.")

    # 4) Postos fora da análise
    postos_fora = sum(len(c.get("erros_postos") or []) for c in res["campanhas"])
    if postos_fora:
        alertas.append(f"{postos_fora} posto(s) ficaram fora da análise (sem conexão/erro).")

    corpo = montar_corpo(res, alertas, antes, diag)
    if alertas:
        corpo += f"\n\nÚltimas linhas do log do cron:\n{diag['tail']}"
    prefixo = "[ALERTA] " if alertas else ""
    hora = agora.strftime("%H:%M")
    enviar_email(
        f"{prefixo}WPP Cobrança {hora} — {r['previstas']} previstas · "
        f"{r['enviadas']} enviadas · {r['erros']} erros", corpo)

    # Log robusto local (o corpo inteiro + motivos por campanha)
    log(corpo)
    for c in res["campanhas"]:
        pm = c["contagem"].get("por_motivo") or {}
        if pm:
            log(f"  motivos [{c['id']}] {c['nome'][:40]}: "
                + json.dumps(pm, ensure_ascii=False))

    with open(estado_path, "w", encoding="utf-8") as f:
        json.dump({"data": hoje.isoformat(), "enviadas": r["enviadas"],
                   "previstas": r["previstas"], "hora": hora}, f)
    log("=== monitor_previsao_wpp: fim ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
