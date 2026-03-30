#!/usr/bin/env python3
"""
export_leads_analytics_cache.py
Gera JSONs pre-calculados para o dashboard Leads Analytics.
Roda via cron (noturno) ou via API (botao Atualizar).

Arquivos gerados em json_consolidado/:
  leads_analytics_geral.json
  leads_analytics_postos.json
  leads_analytics_corretores.json

Logica incremental:
  1. Faz COUNT(*) rapido no MySQL
  2. Compara com contagem do ultimo cache
  3. Se igual -> skip (dados nao mudaram)
  4. Se diferente -> refresh completo
  5. Se flag force_full -> sempre refresh completo

Validacao dupla:
  Apos gerar, revalida contagem MySQL vs dados no JSON.
  Se inconsistente, marca flag para proximo run ser completo.

Lock: /tmp/leads_analytics_cache.lock (impede execucao concorrente)
"""

import os, sys, json, time, fcntl, hashlib
from datetime import date, datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Sempre usar /opt/relatorio_h_t/json_consolidado/ como diretorio canonico
# para que o script funcione tanto de /opt/camim-auth/ quanto /opt/relatorio_h_t/
_CANONICAL_JSON = "/opt/relatorio_h_t/json_consolidado"
JSON_DIR = _CANONICAL_JSON if os.path.isdir(os.path.dirname(_CANONICAL_JSON)) else os.path.join(BASE_DIR, "json_consolidado")
META_FILE = os.path.join(JSON_DIR, "leads_analytics_cache_meta.json")
LOCK_FILE = "/tmp/leads_analytics_cache.lock"
DAILY_RUN_PREFIX = os.path.join(JSON_DIR, ".leads_cache_runs_")

# Import lazy — so carrega pymysql/queries quando realmente for gerar cache
sys.path.insert(0, BASE_DIR)
_etl = None

def _get_etl():
    """Import lazy do modulo de queries MySQL."""
    global _etl
    if _etl is None:
        import export_leads_analytics as m
        _etl = m
    return _etl


def log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def acquire_lock():
    """Tenta adquirir lock exclusivo. Retorna file handle ou None."""
    try:
        fh = open(LOCK_FILE, 'w')
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write(str(os.getpid()))
        fh.flush()
        return fh
    except (IOError, OSError):
        return None


def release_lock(fh):
    if fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()
            os.unlink(LOCK_FILE)
        except Exception:
            pass


def is_running():
    """Verifica se ja tem uma instancia rodando."""
    try:
        fh = open(LOCK_FILE, 'w')
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()
        return False
    except (IOError, OSError):
        return True


def get_daily_run_count():
    """Retorna quantas vezes rodou hoje."""
    today_str = date.today().isoformat()
    run_file = DAILY_RUN_PREFIX + today_str
    if os.path.exists(run_file):
        try:
            return int(open(run_file).read().strip())
        except Exception:
            return 0
    return 0


def increment_daily_run():
    """Incrementa contador de execucoes do dia."""
    today_str = date.today().isoformat()
    run_file = DAILY_RUN_PREFIX + today_str
    count = get_daily_run_count() + 1
    with open(run_file, 'w') as f:
        f.write(str(count))
    # Limpar arquivos de dias anteriores
    for fn in os.listdir(JSON_DIR):
        if fn.startswith(".leads_cache_runs_") and today_str not in fn:
            try:
                os.unlink(os.path.join(JSON_DIR, fn))
            except Exception:
                pass
    return count


def load_meta():
    """Carrega metadados do ultimo cache."""
    if os.path.exists(META_FILE):
        try:
            return json.load(open(META_FILE))
        except Exception:
            pass
    return {}


def save_meta(meta):
    with open(META_FILE, 'w') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def mysql_count(conn, ini, fim):
    """Contagem rapida de leads no periodo."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) as cnt FROM leads WHERE created_at >= %s AND created_at < %s AND deleted_at IS NULL",
            (str(ini), str(fim))
        )
        return cur.fetchone()['cnt']


def serialize_result(result):
    """Converte tipos para JSON."""
    s = _get_etl()._serialize
    if isinstance(result, dict):
        return {k: s(v) for k, v in result.items()}
    elif isinstance(result, list):
        return [
            {k: s(v) for k, v in row.items()} if isinstance(row, dict) else row
            for row in result
        ]
    return result


def calc_periodo():
    """Calcula periodo padrao: ultimos 12 meses."""
    today = date.today()
    ini = date(today.year - 1, today.month, 1)
    if today.month == 12:
        fim = date(today.year + 1, 1, 1)
    else:
        fim = date(today.year, today.month + 1, 1)
    return ini, fim


def generate_geral(conn, ini, fim):
    """Gera dados da aba Visao Geral."""
    m = _get_etl()
    data = {}
    queries = [
        ("resumo_geral",           m.fetch_resumo_geral),
        ("funil_por_status",       m.fetch_funil_por_status),
        ("funil_conversao",        m.fetch_funil_conversao),
        ("conversao_por_posto",    m.fetch_conversao_por_posto),
        ("conversao_por_fonte",    m.fetch_conversao_por_fonte),
        ("corretor_performance",   m.fetch_corretor_performance),
        ("tempo_primeiro_contato", m.fetch_tempo_primeiro_contato_impacto),
        ("contatos_vs_conversao",  m.fetch_contatos_vs_conversao),
        ("motivos_perda",          m.fetch_motivos_perda),
        ("evolucao_mensal",        m.fetch_evolucao_mensal),
        ("dia_semana",             m.fetch_dia_semana),
        ("hora_dia",               m.fetch_hora_dia),
        ("hora_fechamento",        m.fetch_hora_fechamento),
        ("tempo_ciclo_conversao",  m.fetch_tempo_ciclo_conversao),
        ("idade_leads",            m.fetch_idade_leads),
        ("gargalos_funil",         m.fetch_gargalos_funil),
        ("piores_dias",            m.fetch_piores_dias),
    ]
    for name, fn in queries:
        t0 = time.time()
        try:
            result = fn(conn, ini, fim)
            data[name] = serialize_result(result)
            log(f"  geral.{name} -> {len(result) if isinstance(result, list) else 1} rows ({time.time()-t0:.1f}s)")
        except Exception as e:
            log(f"  geral.{name} -> ERRO: {e}")
            data[name] = []

    data["insights"] = m.generate_insights(data)
    return data


def generate_postos(conn, ini, fim):
    """Gera dados da aba Por Posto."""
    m = _get_etl()
    data = {}
    queries = [
        ("posto_mensal",   m.fetch_posto_mensal_agg),
        ("posto_corretor", m.fetch_posto_corretor),
        ("posto_fonte",    m.fetch_posto_fonte),
        ("posto_ciclo",    m.fetch_posto_ciclo),
    ]
    for name, fn in queries:
        t0 = time.time()
        try:
            result = fn(conn, ini, fim)
            data[name] = serialize_result(result)
            log(f"  postos.{name} -> {len(result)} rows ({time.time()-t0:.1f}s)")
        except Exception as e:
            log(f"  postos.{name} -> ERRO: {e}")
            data[name] = []
    return data


def generate_corretores(conn, ini, fim):
    """Gera dados da aba Corretores."""
    m = _get_etl()
    data = {}
    queries = [
        ("corretor_ranking",          m.fetch_corretor_performance),
        ("corretor_mensal",           m.fetch_corretor_mensal),
        ("corretor_hora",             m.fetch_corretor_hora),
        ("corretor_dia_semana",       m.fetch_corretor_dia_semana),
        ("corretor_fonte",            m.fetch_corretor_fonte),
        ("corretor_desperdicio",      m.fetch_corretor_desperdicio),
        ("corretor_ciclo",            m.fetch_corretor_ciclo),
        ("corretor_hora_fechamento",  m.fetch_corretor_hora_fechamento),
    ]
    for name, fn in queries:
        t0 = time.time()
        try:
            result = fn(conn, ini, fim)
            data[name] = serialize_result(result)
            log(f"  corretores.{name} -> {len(result)} rows ({time.time()-t0:.1f}s)")
        except Exception as e:
            log(f"  corretores.{name} -> ERRO: {e}")
            data[name] = []
    return data


def run_cache(force_full=False):
    """Executa a geracao de cache. Retorna dict com resultado."""
    os.makedirs(JSON_DIR, exist_ok=True)

    lock = acquire_lock()
    if not lock:
        return {"ok": False, "erro": "Ja esta rodando outra atualizacao."}

    try:
        t_total = time.time()
        ini, fim = calc_periodo()
        log(f"=== LEADS ANALYTICS CACHE === periodo: {ini} a {fim}")

        conn = _get_etl().get_conn()
        meta = load_meta()

        # --- Verificacao incremental ---
        current_count = mysql_count(conn, ini, fim)
        cached_count = meta.get("mysql_count", -1)
        needs_force = meta.get("force_full_next", False)

        if not force_full and not needs_force and current_count == cached_count and cached_count > 0:
            elapsed = time.time() - t_total
            log(f"Incremental: contagem identica ({current_count}), skip. ({elapsed:.1f}s)")
            conn.close()
            release_lock(lock)
            return {
                "ok": True,
                "modo": "incremental_skip",
                "mysql_count": current_count,
                "msg": f"Dados nao mudaram ({current_count} leads). Cache mantido.",
                "elapsed": round(elapsed, 1),
            }

        modo = "full" if (force_full or needs_force) else "incremental_refresh"
        log(f"Modo: {modo} (mysql={current_count}, cache={cached_count}, force={force_full or needs_force})")

        # --- Gerar os 3 JSONs ---
        log("Gerando aba Geral...")
        data_geral = generate_geral(conn, ini, fim)

        log("Gerando aba Postos...")
        data_postos = generate_postos(conn, ini, fim)

        log("Gerando aba Corretores...")
        data_corretores = generate_corretores(conn, ini, fim)

        # --- Validacao dupla: recontar MySQL ---
        recount = mysql_count(conn, ini, fim)
        inconsistente = (recount != current_count)
        if inconsistente:
            log(f"ATENCAO: contagem mudou durante execucao ({current_count} -> {recount})")

        conn.close()

        # --- Validar dados gerados ---
        geral_total = (data_geral.get("resumo_geral") or {}).get("total_leads", 0)
        count_ok = abs(int(geral_total or 0) - current_count) <= 5  # margem de tolerancia minima

        now_iso = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

        # --- Gravar JSONs ---
        meta_block = {
            "gerado_em": now_iso,
            "periodo_ini": str(ini),
            "periodo_fim": str(fim),
            "origem": "export_leads_analytics_cache.py",
        }

        for fname, payload in [
            ("leads_analytics_geral.json", data_geral),
            ("leads_analytics_postos.json", data_postos),
            ("leads_analytics_corretores.json", data_corretores),
        ]:
            out = {"meta": {**meta_block, "arquivo": fname}, **payload}
            path = os.path.join(JSON_DIR, fname)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(out, f, ensure_ascii=False)
            log(f"  Gravado: {fname}")

        # --- Atualizar meta ---
        new_meta = {
            "gerado_em": now_iso,
            "periodo_ini": str(ini),
            "periodo_fim": str(fim),
            "mysql_count": recount if inconsistente else current_count,
            "json_count": int(geral_total or 0),
            "force_full_next": inconsistente or not count_ok,
            "validacao_ok": count_ok and not inconsistente,
            "modo": modo,
        }
        save_meta(new_meta)
        increment_daily_run()

        # --- Copiar para /var/www/ (se existir) ---
        www_json = "/var/www/json_consolidado"
        if os.path.isdir(www_json):
            import shutil
            for fname in ["leads_analytics_geral.json", "leads_analytics_postos.json",
                          "leads_analytics_corretores.json", "leads_analytics_cache_meta.json"]:
                src = os.path.join(JSON_DIR, fname)
                if os.path.exists(src):
                    shutil.copy2(src, os.path.join(www_json, fname))
            log("  Copiado para /var/www/json_consolidado/")

        elapsed = time.time() - t_total
        log(f"=== CONCLUIDO em {elapsed:.1f}s | validacao={'OK' if new_meta['validacao_ok'] else 'INCONSISTENTE'} ===")

        return {
            "ok": True,
            "modo": modo,
            "mysql_count": current_count,
            "json_count": int(geral_total or 0),
            "validacao_ok": new_meta["validacao_ok"],
            "elapsed": round(elapsed, 1),
            "gerado_em": now_iso,
        }

    except Exception as e:
        log(f"ERRO FATAL: {e}")
        # Marcar para proximo run ser completo
        meta = load_meta()
        meta["force_full_next"] = True
        save_meta(meta)
        return {"ok": False, "erro": str(e)}

    finally:
        release_lock(lock)


if __name__ == "__main__":
    force = "--force" in sys.argv
    result = run_cache(force_full=force)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    sys.exit(0 if result.get("ok") else 1)
