"""Sentry (monitoramento de erros) dos scripts de ETL/cron — ligado só se SENTRY_DSN
estiver em /opt/relatorio_h_t/.env. Sem DSN o sentry-sdk nem é importado.

Quem chama é o etl_meta.py, que os 21 exports já importam: um ponto só cobre todos.
Os serviços (app.py, analyze_groq.py, wpp-campanhas, agenda_f3) têm o mesmo bloco
embutido no próprio arquivo, porque o deploy copia cada um para uma pasta diferente.
"""
import os
import sys

_ligado = None


def init_sentry(componente: str) -> bool:
    global _ligado
    if _ligado is not None:
        return _ligado

    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        # o etl_meta pode ser importado antes do load_dotenv() do script
        try:
            from dotenv import dotenv_values
            dsn = (dotenv_values("/opt/relatorio_h_t/.env").get("SENTRY_DSN") or "").strip()
        except Exception:
            dsn = ""
    if not dsn:
        _ligado = False
        return False

    import logging

    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    sentry_sdk.init(
        dsn=dsn,
        environment=os.environ.get("SENTRY_ENVIRONMENT", "prod"),
        release=os.environ.get("SENTRY_RELEASE", "").strip() or None,
        # False por padrão: os exports trafegam dado de paciente e o Sentry guarda
        # os eventos nos EUA. Só ligue sabendo o que vai junto.
        send_default_pii=os.environ.get("SENTRY_SEND_PII", "false").lower() == "true",
        traces_sample_rate=0.0,  # script de cron: só erro, sem amostragem de performance
        enable_logs=os.environ.get("SENTRY_ENABLE_LOGS", "true").lower() == "true",
        integrations=[LoggingIntegration(sentry_logs_level=logging.WARNING)],
    )
    sentry_sdk.set_tag("componente", componente)
    _ligado = True
    return True


def capturar_erro_posto(script: str, posto, msg) -> None:
    """Erro de UM posto que o export trata e segue em frente: hoje só ia pro JSON do
    _etl_meta. Vira evento no Sentry, agrupado por script + posto."""
    if not _ligado:
        return
    import sentry_sdk

    with sentry_sdk.new_scope() as scope:
        scope.set_tag("script", script)
        scope.set_tag("posto", str(posto))
        scope.fingerprint = ["etl-posto", script, str(posto)]
        if isinstance(msg, BaseException):
            sentry_sdk.capture_exception(msg)
        else:
            sentry_sdk.capture_message(f"{script}: posto {posto} falhou — {str(msg)[:300]}", level="error")
