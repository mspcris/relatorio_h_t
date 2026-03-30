# export_leads_analytics.py
# Gera json_consolidado/leads_analytics.json com metricas de funil,
# conversao, timing, corretores e insights do banco camim_leads (MySQL).

import os, json, sys, time
from datetime import date, datetime, timedelta
from collections import defaultdict
import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR  = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, f"export_leads_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

POSTOS = list("ANXYBRPCDGIMJ")

# ========================= Logging =========================

class Logger:
    def __init__(self, log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.fh = open(log_file, 'w', encoding='utf-8')

    def write(self, msg: str):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] {msg}"
        print(msg, flush=True)
        self.fh.write(line + '\n')
        self.fh.flush()

    def close(self):
        self.fh.close()

logger = Logger(LOG_FILE)

def fmt_time(elapsed):
    if elapsed < 1:
        return f"{elapsed*1000:.0f}ms"
    elif elapsed < 60:
        return f"{elapsed:.1f}s"
    return f"{int(elapsed//60)}m {elapsed%60:.0f}s"

# ========================= DB =========================

def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def get_conn():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    cfg = {
        "host": env("LEADS_DB_HOST"),
        "port": int(env("LEADS_DB_PORT", "3306")),
        "user": env("LEADS_DB_USER"),
        "password": env("LEADS_DB_PASSWORD"),
        "database": env("LEADS_DB_NAME"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }
    if not cfg["host"] or not cfg["database"]:
        logger.write("ERRO: variaveis LEADS_DB_* nao configuradas no .env")
        sys.exit(1)
    return pymysql.connect(**cfg)

def q(conn, sql, params=None):
    """Executa query e retorna lista de dicts."""
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

# ========================= Queries =========================

def fetch_resumo_geral(conn, ini, fim):
    """KPIs gerais do periodo."""
    return q(conn, """
        SELECT
            COUNT(*) as total_leads,
            SUM(finish_lead_signup = 1) as convertidos,
            SUM(finish_lead_signup = 0 OR finish_lead_signup IS NULL) as nao_convertidos,
            ROUND(AVG(CASE WHEN finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa_conversao,
            COUNT(DISTINCT filialCode) as postos_ativos
        FROM leads
        WHERE created_at >= %s AND created_at < %s
          AND deleted_at IS NULL
    """, (str(ini), str(fim)))[0]


def fetch_funil_por_status(conn, ini, fim):
    """Contagem de leads por ultimo status no timeline (funil)."""
    return q(conn, """
        SELECT
            ts.title as status_name,
            ts.id as status_id,
            ts.is_finish_status,
            COUNT(DISTINCT t.lead_id) as total
        FROM timelines t
        JOIN timelinestatuses ts ON ts.id = t.timelinestatus_id
        JOIN leads l ON l.id = t.lead_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
        GROUP BY ts.id, ts.title, ts.is_finish_status
        ORDER BY total DESC
    """, (str(ini), str(fim)))


def fetch_funil_conversao(conn, ini, fim):
    """Funil de conversao: quantos leads passaram por cada etapa."""
    return q(conn, """
        SELECT
            ts.title as etapa,
            ts.id as status_id,
            COUNT(DISTINCT t.lead_id) as leads_passaram
        FROM timelines t
        JOIN timelinestatuses ts ON ts.id = t.timelinestatus_id
        JOIN leads l ON l.id = t.lead_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
        GROUP BY ts.id, ts.title
        ORDER BY leads_passaram DESC
    """, (str(ini), str(fim)))


def fetch_conversao_por_posto(conn, ini, fim):
    """Conversao por posto (filialCode)."""
    return q(conn, """
        SELECT
            l.filialCode as posto,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND l.filialCode IS NOT NULL
          AND LENGTH(l.filialCode) = 1
        GROUP BY l.filialCode
        ORDER BY taxa DESC
    """, (str(ini), str(fim)))


def fetch_conversao_por_fonte(conn, ini, fim):
    """Conversao por fonte de lead (leadsource)."""
    return q(conn, """
        SELECT
            COALESCE(ls.title, 'Sem fonte') as fonte,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        LEFT JOIN leadsources ls ON ls.id = l.leadsource_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
        GROUP BY ls.id, ls.title
        HAVING total >= 5
        ORDER BY taxa DESC
    """, (str(ini), str(fim)))


def fetch_corretor_performance(conn, ini, fim):
    """Performance dos corretores (vendedores)."""
    return q(conn, """
        SELECT
            u.id as user_id,
            u.name as corretor,
            COUNT(DISTINCT l.id) as total_leads,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa_conversao,
            ROUND(AVG(timeline_stats.contatos), 1) as media_contatos,
            ROUND(AVG(timeline_stats.tempo_primeiro_contato_h), 1) as media_h_primeiro_contato
        FROM leads l
        JOIN users u ON u.id = l.user_id
        LEFT JOIN (
            SELECT
                t1.lead_id,
                COUNT(*) as contatos,
                TIMESTAMPDIFF(HOUR, l2.created_at, MIN(t1.created_at)) as tempo_primeiro_contato_h
            FROM timelines t1
            JOIN leads l2 ON l2.id = t1.lead_id
            WHERE l2.created_at >= %s AND l2.created_at < %s
              AND l2.deleted_at IS NULL
            GROUP BY t1.lead_id
        ) timeline_stats ON timeline_stats.lead_id = l.id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
        GROUP BY u.id, u.name
        HAVING total_leads >= 5
        ORDER BY taxa_conversao DESC
    """, (str(ini), str(fim), str(ini), str(fim)))


def fetch_tempo_primeiro_contato_impacto(conn, ini, fim):
    """Impacto do tempo de primeiro contato na conversao.
    Exclui status automaticos (VISUALIZADO=6, CONTATO INICIAL=1) para pegar
    o primeiro contato real do corretor."""
    return q(conn, """
        SELECT
            CASE
                WHEN h_primeiro <= 1 THEN '0-1h'
                WHEN h_primeiro <= 4 THEN '1-4h'
                WHEN h_primeiro <= 12 THEN '4-12h'
                WHEN h_primeiro <= 24 THEN '12-24h'
                WHEN h_primeiro <= 48 THEN '24-48h'
                ELSE '48h+'
            END as faixa,
            COUNT(*) as total,
            SUM(convertido) as convertidos,
            ROUND(AVG(convertido) * 100, 2) as taxa
        FROM (
            SELECT
                l.id,
                TIMESTAMPDIFF(MINUTE, l.created_at, MIN(t.created_at)) / 60.0 as h_primeiro,
                (l.finish_lead_signup = 1) as convertido
            FROM leads l
            JOIN timelines t ON t.lead_id = l.id
              AND t.timelinestatus_id NOT IN (1, 6)
            WHERE l.created_at >= %s AND l.created_at < %s
              AND l.deleted_at IS NULL
            GROUP BY l.id, l.created_at, l.finish_lead_signup
        ) sub
        WHERE h_primeiro IS NOT NULL
        GROUP BY faixa
        ORDER BY FIELD(faixa, '0-1h', '1-4h', '4-12h', '12-24h', '24-48h', '48h+')
    """, (str(ini), str(fim)))


def fetch_contatos_vs_conversao(conn, ini, fim):
    """Numero de contatos vs taxa de conversao."""
    return q(conn, """
        SELECT
            CASE
                WHEN n_contatos = 1 THEN '1'
                WHEN n_contatos = 2 THEN '2'
                WHEN n_contatos = 3 THEN '3'
                WHEN n_contatos BETWEEN 4 AND 5 THEN '4-5'
                WHEN n_contatos BETWEEN 6 AND 10 THEN '6-10'
                ELSE '11+'
            END as faixa_contatos,
            COUNT(*) as total,
            SUM(convertido) as convertidos,
            ROUND(AVG(convertido) * 100, 2) as taxa
        FROM (
            SELECT
                l.id,
                COUNT(t.id) as n_contatos,
                (l.finish_lead_signup = 1) as convertido
            FROM leads l
            LEFT JOIN timelines t ON t.lead_id = l.id
            WHERE l.created_at >= %s AND l.created_at < %s
              AND l.deleted_at IS NULL
            GROUP BY l.id, l.finish_lead_signup
        ) sub
        GROUP BY faixa_contatos
        ORDER BY FIELD(faixa_contatos, '1', '2', '3', '4-5', '6-10', '11+')
    """, (str(ini), str(fim)))


def fetch_motivos_perda(conn, ini, fim):
    """Motivos de perda (leadreasons) para leads nao convertidos."""
    return q(conn, """
        SELECT
            COALESCE(lr.title, 'Sem motivo registrado') as motivo,
            COUNT(*) as total,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
        FROM leads l
        LEFT JOIN leadreasons lr ON lr.id = l.leadreason_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND (l.finish_lead_signup = 0 OR l.finish_lead_signup IS NULL)
          AND l.leadreason_id IS NOT NULL
        GROUP BY lr.id, lr.title
        ORDER BY total DESC
    """, (str(ini), str(fim)))


def fetch_evolucao_mensal(conn, ini, fim):
    """Evolucao mensal de leads e conversao."""
    return q(conn, """
        SELECT
            DATE_FORMAT(l.created_at, '%%Y-%%m') as mes,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
        GROUP BY mes
        ORDER BY mes
    """, (str(ini), str(fim)))


def fetch_dia_semana(conn, ini, fim):
    """Conversao por dia da semana."""
    return q(conn, """
        SELECT
            DAYOFWEEK(l.created_at) as dow,
            CASE DAYOFWEEK(l.created_at)
                WHEN 1 THEN 'Domingo'
                WHEN 2 THEN 'Segunda'
                WHEN 3 THEN 'Terca'
                WHEN 4 THEN 'Quarta'
                WHEN 5 THEN 'Quinta'
                WHEN 6 THEN 'Sexta'
                WHEN 7 THEN 'Sabado'
            END as dia,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
        GROUP BY dow, dia
        ORDER BY dow
    """, (str(ini), str(fim)))


def fetch_hora_dia(conn, ini, fim):
    """Conversao por hora do dia (hora de criacao do lead)."""
    return q(conn, """
        SELECT
            HOUR(l.created_at) as hora,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
        GROUP BY hora
        ORDER BY hora
    """, (str(ini), str(fim)))


def fetch_tempo_ciclo_conversao(conn, ini, fim):
    """Distribuicao do tempo entre criacao do lead e conversao (finish_lead_date)."""
    return q(conn, """
        SELECT
            CASE
                WHEN dias <= 0 THEN 'Mesmo dia'
                WHEN dias <= 1 THEN '1 dia'
                WHEN dias <= 3 THEN '2-3 dias'
                WHEN dias <= 7 THEN '4-7 dias'
                WHEN dias <= 14 THEN '8-14 dias'
                WHEN dias <= 30 THEN '15-30 dias'
                WHEN dias <= 60 THEN '31-60 dias'
                ELSE '60+ dias'
            END as faixa,
            COUNT(*) as total,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
        FROM (
            SELECT DATEDIFF(l.finish_lead_date, l.created_at) as dias
            FROM leads l
            WHERE l.created_at >= %s AND l.created_at < %s
              AND l.deleted_at IS NULL
              AND l.finish_lead_signup = 1
              AND l.finish_lead_date IS NOT NULL
        ) sub
        GROUP BY faixa
        ORDER BY FIELD(faixa, 'Mesmo dia', '1 dia', '2-3 dias', '4-7 dias', '8-14 dias', '15-30 dias', '31-60 dias', '60+ dias')
    """, (str(ini), str(fim)))


def fetch_idade_leads(conn, ini, fim):
    """Distribuicao por faixa etaria e conversao."""
    return q(conn, """
        SELECT
            CASE
                WHEN age < 18 THEN '<18'
                WHEN age BETWEEN 18 AND 25 THEN '18-25'
                WHEN age BETWEEN 26 AND 35 THEN '26-35'
                WHEN age BETWEEN 36 AND 45 THEN '36-45'
                WHEN age BETWEEN 46 AND 55 THEN '46-55'
                WHEN age BETWEEN 56 AND 65 THEN '56-65'
                ELSE '65+'
            END as faixa,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND l.age IS NOT NULL AND l.age > 0 AND l.age < 120
        GROUP BY faixa
        ORDER BY FIELD(faixa, '<18', '18-25', '26-35', '36-45', '46-55', '56-65', '65+')
    """, (str(ini), str(fim)))


def fetch_gargalos_funil(conn, ini, fim):
    """Identifica gargalos: status onde leads ficam parados (ultimo status de leads nao convertidos)."""
    return q(conn, """
        SELECT
            ts.title as status_parado,
            COUNT(*) as total,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
        FROM (
            SELECT t.lead_id, t.timelinestatus_id
            FROM timelines t
            JOIN (
                SELECT lead_id, MAX(id) as max_id
                FROM timelines
                GROUP BY lead_id
            ) latest ON latest.lead_id = t.lead_id AND latest.max_id = t.id
            JOIN leads l ON l.id = t.lead_id
            WHERE l.created_at >= %s AND l.created_at < %s
              AND l.deleted_at IS NULL
              AND (l.finish_lead_signup = 0 OR l.finish_lead_signup IS NULL)
        ) sub
        JOIN timelinestatuses ts ON ts.id = sub.timelinestatus_id
        GROUP BY ts.id, ts.title
        ORDER BY total DESC
        LIMIT 15
    """, (str(ini), str(fim)))


def fetch_piores_dias(conn, ini, fim):
    """Top 20 datas com pior taxa de conversao (minimo 5 leads no dia)."""
    return q(conn, """
        SELECT
            DATE(l.created_at) as dia,
            DAYNAME(l.created_at) as dia_semana,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
        GROUP BY dia, dia_semana
        HAVING total >= 5
        ORDER BY taxa ASC, total DESC
        LIMIT 20
    """, (str(ini), str(fim)))


def fetch_corretor_mensal(conn, ini, fim):
    """Dados mensais por corretor: total, convertidos, taxa."""
    return q(conn, """
        SELECT
            u.id as user_id,
            u.name as corretor,
            DATE_FORMAT(l.created_at, '%%Y-%%m') as mes,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        JOIN users u ON u.id = l.user_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
        GROUP BY u.id, u.name, mes
        ORDER BY u.name, mes
    """, (str(ini), str(fim)))


def fetch_corretor_hora(conn, ini, fim):
    """Conversao por hora do dia por corretor."""
    return q(conn, """
        SELECT
            u.id as user_id,
            HOUR(l.created_at) as hora,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        JOIN users u ON u.id = l.user_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
        GROUP BY u.id, hora
        ORDER BY u.id, hora
    """, (str(ini), str(fim)))


def fetch_corretor_dia_semana(conn, ini, fim):
    """Conversao por dia da semana por corretor."""
    return q(conn, """
        SELECT
            u.id as user_id,
            DAYOFWEEK(l.created_at) as dow,
            CASE DAYOFWEEK(l.created_at)
                WHEN 1 THEN 'Domingo'
                WHEN 2 THEN 'Segunda'
                WHEN 3 THEN 'Terca'
                WHEN 4 THEN 'Quarta'
                WHEN 5 THEN 'Quinta'
                WHEN 6 THEN 'Sexta'
                WHEN 7 THEN 'Sabado'
            END as dia,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        JOIN users u ON u.id = l.user_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
        GROUP BY u.id, dow, dia
        ORDER BY u.id, dow
    """, (str(ini), str(fim)))


def fetch_corretor_fonte(conn, ini, fim):
    """Conversao por fonte de lead por corretor."""
    return q(conn, """
        SELECT
            u.id as user_id,
            COALESCE(ls.title, 'Sem fonte') as fonte,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 1 THEN 1 ELSE 0 END) * 100, 2) as taxa
        FROM leads l
        JOIN users u ON u.id = l.user_id
        LEFT JOIN leadsources ls ON ls.id = l.leadsource_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
        GROUP BY u.id, ls.id, ls.title
        ORDER BY u.id, total DESC
    """, (str(ini), str(fim)))


def fetch_corretor_desperdicio(conn, ini, fim):
    """Corretores que mais desperdicam leads (nao convertidos com poucos contatos)."""
    return q(conn, """
        SELECT
            u.id as user_id,
            u.name as corretor,
            COUNT(*) as total,
            SUM(l.finish_lead_signup = 1) as convertidos,
            SUM(l.finish_lead_signup = 0 OR l.finish_lead_signup IS NULL) as desperdicados,
            ROUND(AVG(CASE WHEN l.finish_lead_signup = 0 OR l.finish_lead_signup IS NULL THEN 1 ELSE 0 END) * 100, 2) as taxa_desperdicio,
            ROUND(AVG(tc.contatos), 1) as media_contatos
        FROM leads l
        JOIN users u ON u.id = l.user_id
        LEFT JOIN (
            SELECT lead_id, COUNT(*) as contatos
            FROM timelines
            GROUP BY lead_id
        ) tc ON tc.lead_id = l.id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
        GROUP BY u.id, u.name
        HAVING total >= 5
        ORDER BY taxa_desperdicio DESC
    """, (str(ini), str(fim)))


def fetch_corretor_ciclo(conn, ini, fim):
    """Ciclo medio de conversao por corretor (dias entre criacao e finish_lead_date)."""
    return q(conn, """
        SELECT
            u.id as user_id,
            u.name as corretor,
            ROUND(AVG(DATEDIFF(l.finish_lead_date, l.created_at)), 1) as media_dias,
            COUNT(*) as total_convertidos
        FROM leads l
        JOIN users u ON u.id = l.user_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
          AND l.finish_lead_signup = 1
          AND l.finish_lead_date IS NOT NULL
        GROUP BY u.id, u.name
        HAVING total_convertidos >= 2
        ORDER BY media_dias ASC
    """, (str(ini), str(fim)))


def fetch_posto_mensal_agg(conn, ini, fim):
    """Evolucao mensal de leads convertidos por posto."""
    return q(conn, """
        SELECT
            l.filialCode as posto,
            DATE_FORMAT(l.created_at, '%%Y-%%m') as mes,
            COUNT(*) as convertidos
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND l.finish_lead_signup = 1
          AND l.filialCode IS NOT NULL
          AND LENGTH(l.filialCode) = 1
        GROUP BY l.filialCode, mes
        ORDER BY l.filialCode, mes
    """, (str(ini), str(fim)))


def fetch_posto_corretor(conn, ini, fim):
    """Corretores que fecharam leads por posto."""
    return q(conn, """
        SELECT
            l.filialCode as posto,
            u.id as user_id,
            u.name as corretor,
            COUNT(*) as convertidos
        FROM leads l
        JOIN users u ON u.id = l.user_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND u.deleted_at IS NULL
          AND l.finish_lead_signup = 1
          AND l.filialCode IS NOT NULL
          AND LENGTH(l.filialCode) = 1
        GROUP BY l.filialCode, u.id, u.name
        ORDER BY l.filialCode, convertidos DESC
    """, (str(ini), str(fim)))


def fetch_posto_fonte(conn, ini, fim):
    """Fontes dos leads fechados por posto."""
    return q(conn, """
        SELECT
            l.filialCode as posto,
            COALESCE(ls.title, 'Sem fonte') as fonte,
            COUNT(*) as convertidos
        FROM leads l
        LEFT JOIN leadsources ls ON ls.id = l.leadsource_id
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND l.finish_lead_signup = 1
          AND l.filialCode IS NOT NULL
          AND LENGTH(l.filialCode) = 1
        GROUP BY l.filialCode, ls.id, ls.title
        ORDER BY l.filialCode, convertidos DESC
    """, (str(ini), str(fim)))


def fetch_posto_ciclo(conn, ini, fim):
    """Ciclo medio de conversao por posto."""
    return q(conn, """
        SELECT
            l.filialCode as posto,
            ROUND(AVG(DATEDIFF(l.finish_lead_date, l.created_at)), 1) as media_dias,
            COUNT(*) as total
        FROM leads l
        WHERE l.created_at >= %s AND l.created_at < %s
          AND l.deleted_at IS NULL
          AND l.finish_lead_signup = 1
          AND l.finish_lead_date IS NOT NULL
          AND l.filialCode IS NOT NULL
          AND LENGTH(l.filialCode) = 1
        GROUP BY l.filialCode
        ORDER BY media_dias ASC
    """, (str(ini), str(fim)))


def generate_insights(data):
    """Gera insights automaticos baseados nos dados."""
    insights = []

    # Insight: melhor e pior posto
    postos = data.get("conversao_por_posto", [])
    if postos:
        melhores = [p for p in postos if (p.get("total") or 0) >= 20]
        if melhores:
            best = melhores[0]
            worst = melhores[-1]
            insights.append({
                "tipo": "destaque",
                "icone": "trophy",
                "titulo": f"Melhor posto: {best['posto']}",
                "texto": f"Taxa de {best['taxa']}% ({best['convertidos']}/{best['total']} leads). Pior: {worst['posto']} com {worst['taxa']}%."
            })

    # Insight: impacto do tempo de resposta
    tempo = data.get("tempo_primeiro_contato", [])
    if len(tempo) >= 2:
        rapido = next((t for t in tempo if t["faixa"] == "0-1h"), None)
        lento = next((t for t in tempo if t["faixa"] == "48h+"), None)
        if rapido and lento and rapido["taxa"] > 0:
            diff = rapido["taxa"] - lento["taxa"]
            if diff > 0:
                insights.append({
                    "tipo": "alerta",
                    "icone": "clock",
                    "titulo": "Velocidade de resposta importa",
                    "texto": f"Leads contatados em ate 1h convertem {rapido['taxa']}% vs {lento['taxa']}% (48h+). Diferenca de {diff:.1f}pp."
                })

    # Insight: fonte mais eficiente
    fontes = data.get("conversao_por_fonte", [])
    if fontes:
        top_fonte = fontes[0]
        insights.append({
            "tipo": "info",
            "icone": "bullseye",
            "titulo": f"Fonte mais eficiente: {top_fonte['fonte']}",
            "texto": f"Taxa de {top_fonte['taxa']}% com {top_fonte['total']} leads."
        })

    # Insight: principal gargalo
    gargalos = data.get("gargalos_funil", [])
    if gargalos:
        top_g = gargalos[0]
        insights.append({
            "tipo": "alerta",
            "icone": "exclamation-triangle",
            "titulo": f"Principal gargalo: {top_g['status_parado']}",
            "texto": f"{top_g['total']} leads ({top_g['pct']}%) param neste status sem converter."
        })

    # Insight: melhor corretor
    corretores = data.get("corretor_performance", [])
    if corretores:
        top_c = corretores[0]
        insights.append({
            "tipo": "destaque",
            "icone": "user-tie",
            "titulo": f"Top corretor: {top_c['corretor']}",
            "texto": f"Taxa de {top_c['taxa_conversao']}% com {top_c['total_leads']} leads. Media de {top_c.get('media_h_primeiro_contato', '?')}h para primeiro contato."
        })

    # Insight: dia e hora ideais
    dias = data.get("dia_semana", [])
    horas = data.get("hora_dia", [])
    if dias:
        melhor_dia = max(dias, key=lambda d: d.get("taxa", 0))
        insights.append({
            "tipo": "info",
            "icone": "calendar-alt",
            "titulo": f"Melhor dia: {melhor_dia['dia']}",
            "texto": f"Taxa de {melhor_dia['taxa']}% ({melhor_dia['convertidos']}/{melhor_dia['total']})."
        })
    if horas:
        melhores_h = [h for h in horas if (h.get("total") or 0) >= 10]
        if melhores_h:
            melhor_h = max(melhores_h, key=lambda h: h.get("taxa", 0))
            insights.append({
                "tipo": "info",
                "icone": "clock",
                "titulo": f"Melhor horario: {melhor_h['hora']}h",
                "texto": f"Taxa de {melhor_h['taxa']}% ({melhor_h['convertidos']}/{melhor_h['total']})."
            })

    # Insight: contatos necessarios
    contatos = data.get("contatos_vs_conversao", [])
    if contatos:
        melhor_faixa = max(contatos, key=lambda c: c.get("taxa", 0))
        insights.append({
            "tipo": "info",
            "icone": "phone",
            "titulo": f"Contatos ideais: {melhor_faixa['faixa_contatos']}",
            "texto": f"Taxa de {melhor_faixa['taxa']}% com {melhor_faixa['faixa_contatos']} contatos ({melhor_faixa['total']} leads)."
        })

    # Insight: principal motivo de perda
    motivos = data.get("motivos_perda", [])
    if motivos:
        top_m = motivos[0]
        insights.append({
            "tipo": "alerta",
            "icone": "times-circle",
            "titulo": f"Principal motivo de perda: {top_m['motivo']}",
            "texto": f"{top_m['total']} leads ({top_m['pct']}%) perdidos por este motivo."
        })

    return insights


# ========================= Main =========================

def main():
    t_total = time.time()
    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    logger.write("=" * 70)
    logger.write("EXPORT LEADS ANALYTICS")
    logger.write(f"  Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.write("=" * 70)

    conn = get_conn()
    logger.write(f"MySQL conectado: {env('LEADS_DB_HOST')}:{env('LEADS_DB_PORT', '3306')}/{env('LEADS_DB_NAME')}")

    # Periodo: ultimos 12 meses
    today = date.today()
    ini = date(today.year - 1, today.month, 1)
    fim = date(today.year, today.month, 1)  # ate inicio do mes atual
    # Incluir mes atual
    if today.month == 12:
        fim = date(today.year + 1, 1, 1)
    else:
        fim = date(today.year, today.month + 1, 1)

    logger.write(f"Periodo: {ini} a {fim}")

    data = {}
    queries = [
        ("resumo_geral",           fetch_resumo_geral),
        ("funil_por_status",       fetch_funil_por_status),
        ("funil_conversao",        fetch_funil_conversao),
        ("conversao_por_posto",    fetch_conversao_por_posto),
        ("conversao_por_fonte",    fetch_conversao_por_fonte),
        ("corretor_performance",   fetch_corretor_performance),
        ("tempo_primeiro_contato", fetch_tempo_primeiro_contato_impacto),
        ("contatos_vs_conversao",  fetch_contatos_vs_conversao),
        ("motivos_perda",          fetch_motivos_perda),
        ("evolucao_mensal",        fetch_evolucao_mensal),
        ("dia_semana",             fetch_dia_semana),
        ("hora_dia",               fetch_hora_dia),
        ("tempo_ciclo_conversao",  fetch_tempo_ciclo_conversao),
        ("idade_leads",            fetch_idade_leads),
        ("gargalos_funil",         fetch_gargalos_funil),
    ]

    for name, fn in queries:
        t0 = time.time()
        try:
            result = fn(conn, ini, fim)
            elapsed = time.time() - t0
            # Converter Decimal/date para JSON serializavel
            if isinstance(result, dict):
                result = {k: _serialize(v) for k, v in result.items()}
            elif isinstance(result, list):
                result = [
                    {k: _serialize(v) for k, v in row.items()} if isinstance(row, dict) else row
                    for row in result
                ]
            data[name] = result
            count = len(result) if isinstance(result, list) else 1
            logger.write(f"  {name:35s} -> {count:>5} registros | {fmt_time(elapsed)}")
        except Exception as e:
            elapsed = time.time() - t0
            logger.write(f"  {name:35s} -> ERRO: {e} | {fmt_time(elapsed)}")
            data[name] = []

    conn.close()

    # Gerar insights
    data["insights"] = generate_insights(data)
    logger.write(f"  {'insights':35s} -> {len(data['insights']):>5} insights gerados")

    # Gravar JSON
    out = {
        "meta": {
            "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
            "periodo_ini": str(ini),
            "periodo_fim": str(fim),
            "arquivo": "leads_analytics.json",
            "origem": "export_leads_analytics.py"
        },
        **data
    }

    out_path = os.path.join(JSON_DIR, "leads_analytics.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    elapsed_total = time.time() - t_total
    logger.write(f"\nJSON gravado: {out_path}")
    logger.write(f"Tempo total: {fmt_time(elapsed_total)}")
    logger.write(f"Log: {LOG_FILE}")
    logger.close()


def _serialize(v):
    """Converte tipos nao JSON-serializaveis."""
    import decimal
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, timedelta):
        return v.total_seconds()
    return v


if __name__ == "__main__":
    main()
