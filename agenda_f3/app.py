"""
agenda_f3/app.py — Flask app pra camila2.ia.camim.com.br.

Auth: IDCAMIM OpenID Connect (sem whitelist — qualquer login válido entra).
Dados: lê de Postgres f3 (fonte primária). Fallback pra JSON em disco se
       o Postgres estiver indisponível.

Rotas:
  GET  /                  → redirect /agenda_dia
  GET  /agenda_dia        → template agenda_dia.html
  GET  /api/agenda_dia    → JSON {ok, posto, data, pacientes, gerado_em, meta}
  GET  /api/agenda_dia/meta → status por posto (gerado_em, sucesso, n_registros)
  GET  /session/me        → {email, postos} (postos = todos os 13)
  GET  /auth/login        → inicia OIDC
  GET  /auth/callback     → callback OIDC
  POST /auth/logout       → encerra sessão + redirect end_session do IDCAMIM
  POST /session/logout    → alias do logout (compat com template)
"""
import os, json, secrets, time
from urllib.parse import urlencode
from functools import wraps
from datetime import datetime, timezone

import requests as http
from flask import (Flask, request, redirect, session, render_template,
                   url_for, jsonify, render_template_string)
from dotenv import load_dotenv

load_dotenv('/opt/agenda_f3/.env')

# ── Flask app ────────────────────────────────────────────────────────────────

app = Flask(__name__, template_folder='/opt/agenda_f3/templates')
app.secret_key = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
app.config['SESSION_COOKIE_SECURE']   = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = 3600 * 8

# ── Constantes ───────────────────────────────────────────────────────────────

POSTOS_TODOS = list("ABCDGIJMNPRXY")  # todos os 13, exibidos a qualquer usuário
JSON_FALLBACK = '/opt/agenda_f3/json_consolidado/agenda_dia.json'

# Claims do idCamim usados pra vincular o login ao usuário do sistema CAMIM.
# O que importa pra confirmar presença é o `login_campinho` (a string `Usuario`
# do sis_usuario, ex.: 'cristiano2.a') — ela é igual em todos os postos; o
# idUsuario é resolvido POR POSTO no momento da confirmação (ver pegadinha em
# project_confirmar_presenca_schema). NÃO se confia num id_usuario_sqlserver
# fixo: o mesmo Usuario tem idUsuario diferente em cada banco de posto.
_CLAIM_LOGIN_CAMPINHO = 'user_camim'  # claim do idCamim com o login CAMIM (ex.: "CRISTIANO2.A")

# Auditoria (Sis_Historico) da confirmação de presença em Cad_Lancamento.
ID_TABELA_CAD_LANCAMENTO = 44   # de Sis_HistoricoTabela
ID_COMANDO_EDICAO        = 2    # de Sis_HistoricoComando (não há "Confirmação")
COMPUTADOR_ORIGEM        = 'camila2.ia.camim.com.br'
ODBC_DRIVER              = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')

# ── OIDC config (lazy) ───────────────────────────────────────────────────────

IDCAMIM_CLIENT_ID     = os.environ['IDCAMIM_CLIENT_ID']
IDCAMIM_CLIENT_SECRET = os.environ['IDCAMIM_CLIENT_SECRET']
IDCAMIM_REDIRECT_URI  = os.environ['IDCAMIM_REDIRECT_URI']
IDCAMIM_DISCOVERY     = 'https://idcamim.camim.com.br/.well-known/openid-configuration'

_oidc_cfg = None

def _oidc_config():
    global _oidc_cfg
    if _oidc_cfg is None:
        _oidc_cfg = http.get(IDCAMIM_DISCOVERY, timeout=10).json()
    return _oidc_cfg


# ── Decorador ────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if 'sub' not in session:
            session['next'] = request.url
            return redirect(url_for('auth_login'))
        return f(*args, **kwargs)
    return wrapper


# ── Auth ─────────────────────────────────────────────────────────────────────

@app.get('/auth/login')
def auth_login():
    state = secrets.token_urlsafe(16)
    session['oidc_state'] = state
    cfg = _oidc_config()
    params = urlencode({
        'response_type': 'code',
        'client_id':     IDCAMIM_CLIENT_ID,
        'redirect_uri':  IDCAMIM_REDIRECT_URI,
        'scope':         'openid email profile',
        'state':         state,
    })
    return redirect(f"{cfg['authorization_endpoint']}?{params}")


@app.get('/auth/callback')
def auth_callback():
    err = request.args.get('error')
    if err:
        return render_template_string(TMPL_ERRO, titulo='Erro', msg=err), 400

    state    = request.args.get('state', '')
    expected = session.pop('oidc_state', None)
    if not expected or state != expected:
        return render_template_string(TMPL_ERRO, titulo='Erro de segurança',
                                      msg='State inválido.'), 400

    code = request.args.get('code')
    if not code:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='Código ausente.'), 400

    cfg = _oidc_config()
    resp = http.post(cfg['token_endpoint'], data={
        'grant_type':    'authorization_code',
        'code':          code,
        'redirect_uri':  IDCAMIM_REDIRECT_URI,
        'client_id':     IDCAMIM_CLIENT_ID,
        'client_secret': IDCAMIM_CLIENT_SECRET,
    }, timeout=15)
    if not resp.ok:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='Falha ao trocar code por token.'), 502

    tokens = resp.json()
    ui = http.get(cfg['userinfo_endpoint'],
                  headers={'Authorization': f"Bearer {tokens.get('access_token')}"},
                  timeout=10)
    if not ui.ok:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='Falha ao obter userinfo.'), 502

    userinfo = ui.json()
    sub = userinfo.get('sub')
    if not sub:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='sub ausente.'), 502

    session.permanent = True
    session['sub']   = sub
    session['email'] = userinfo.get('email', '')
    session['nome']  = userinfo.get('name') or userinfo.get('preferred_username', '')
    session['foto_url'] = (userinfo.get('picture') or '').strip()
    # Vínculo com o usuário do sistema CAMIM (idCamim é fonte de verdade).
    session['login_campinho'] = (userinfo.get(_CLAIM_LOGIN_CAMPINHO) or '').strip()

    return redirect(session.pop('next', '/'))


@app.route('/auth/logout',    methods=['GET', 'POST'])
@app.route('/session/logout', methods=['GET', 'POST'])
def auth_logout():
    session.clear()
    try:
        cfg = _oidc_config()
        end = cfg.get('end_session_endpoint', '')
        if end:
            return redirect(end)
    except Exception:
        pass
    return render_template_string(TMPL_LOGOUT)


# ── Session API ──────────────────────────────────────────────────────────────

@app.get('/session/me')
def session_me():
    if 'sub' not in session:
        return jsonify({'email': None, 'postos': []}), 401
    # Decisão de produto: qualquer login IDCamim vê todos os 13 postos.
    # `vinculado` = tem login_campinho do idCamim; sem ele não dá pra confirmar
    # presença (não há usuário CAMIM pra assinar a alteração/Sis_Historico).
    login_campinho = (session.get('login_campinho') or '').strip()
    return jsonify({
        'email':          session.get('email'),
        'nome':           session.get('nome'),
        'foto_url':       session.get('foto_url') or '',
        'login_campinho': login_campinho,
        'vinculado':      bool(login_campinho),
        'postos':         POSTOS_TODOS,
    })


# ── Rota raiz / template ─────────────────────────────────────────────────────

@app.get('/')
@login_required
def index():
    return redirect('/agenda_dia')


@app.get('/agenda_dia')
@login_required
def agenda_dia_page():
    return render_template('agenda_dia.html',
                           USER_EMAIL=session.get('email'),
                           USER_POSTOS=json.dumps(POSTOS_TODOS))


# ── API: agenda do dia ───────────────────────────────────────────────────────

@app.get('/api/agenda_dia')
@login_required
def api_agenda_dia():
    posto    = (request.args.get('posto') or '').strip().upper()
    data_str = (request.args.get('data')  or '').strip()
    if not posto or not data_str:
        return jsonify({'ok': False, 'error': 'posto e data são obrigatórios'}), 400
    if posto not in POSTOS_TODOS:
        return jsonify({'ok': False, 'error': 'posto inválido'}), 400

    # Tenta Postgres primeiro
    try:
        from f3_db import fetch_agenda
        pacientes, gerado_em = fetch_agenda(posto, data_str)
        return jsonify({
            'ok':         True,
            'posto':      posto,
            'data':       data_str,
            'total':      len(pacientes),
            'pacientes':  pacientes,
            'gerado_em':  gerado_em.isoformat() if gerado_em else None,
            'fonte':      'postgres',
        })
    except Exception as e:
        app.logger.warning('Postgres indisponível, usando JSON fallback: %s', e)

    # Fallback: JSON em disco
    if not os.path.exists(JSON_FALLBACK):
        return jsonify({'ok': False,
                        'error': 'Postgres indisponível e JSON fallback ausente'}), 503
    try:
        with open(JSON_FALLBACK, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Falha ao ler JSON: {e}'}), 500

    dados_posto = payload.get('dados', {}).get(posto, {})
    pacientes = dados_posto.get(data_str, [])
    return jsonify({
        'ok':        True,
        'posto':     posto,
        'data':      data_str,
        'total':     len(pacientes),
        'pacientes': pacientes,
        'gerado_em': payload.get('meta', {}).get('gerado_em', ''),
        'fonte':     'json_fallback',
    })


@app.get('/api/agenda_dia/meta')
@login_required
def api_agenda_dia_meta():
    """Status da última rodada do ETL por posto. Usado pelo banner do topo."""
    try:
        from f3_db import fetch_meta_all
        meta = fetch_meta_all()
        return jsonify({
            'ok':         True,
            'postos':     {p: {
                'gerado_em':   m['gerado_em'].isoformat() if m['gerado_em'] else None,
                'sucesso':     m['sucesso'],
                'erro':        m['erro'],
                'n_registros': m['n_registros'],
            } for p, m in meta.items()},
            'fonte':      'postgres',
        })
    except Exception as e:
        app.logger.warning('Postgres indisponível para meta: %s', e)

    if not os.path.exists(JSON_FALLBACK):
        return jsonify({'ok': False, 'error': 'fontes indisponíveis'}), 503
    try:
        with open(JSON_FALLBACK, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        gerado_em = payload.get('meta', {}).get('gerado_em', '')
        postos_meta = {p: {'gerado_em': gerado_em, 'sucesso': True,
                           'erro': None, 'n_registros': 0}
                       for p in payload.get('meta', {}).get('postos_no_json', [])}
        return jsonify({'ok': True, 'postos': postos_meta, 'fonte': 'json_fallback'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ── Confirmar presença (escreve no SQL Server do posto) ──────────────────────
#
# Grava em Cad_Lancamento: dataconfirmacaoConsulta = agora + idUsuarioConfirmaPresenca
# = idUsuario do operador NAQUELE posto. Auditado em Sis_Historico (idTabela=44,
# idComando=2). O idUsuario é resolvido por posto via login_campinho (o mesmo
# Usuario tem id diferente em cada banco — ver project_confirmar_presenca_schema).

def _mssql_conn_for_posto(posto: str):
    """Abre conexão pyodbc no SQL Server do posto. Mesma convenção de env do ETL
    (DB_HOST_{p}/DB_BASE_{p}/DB_USER_{p}/DB_PASSWORD_{p}/DB_PORT_{p})."""
    import pyodbc
    p = (posto or '').strip().upper()
    if not p or len(p) != 1:
        raise ValueError('posto inválido')
    host = (os.getenv(f'DB_HOST_{p}', '') or '').strip()
    base = (os.getenv(f'DB_BASE_{p}', '') or '').strip()
    if not host or not base:
        raise ValueError(f'posto {p} sem configuração no .env')
    user    = (os.getenv(f'DB_USER_{p}', '') or '').strip()
    pwd     = (os.getenv(f'DB_PASSWORD_{p}', '') or '').strip()
    port    = (os.getenv(f'DB_PORT_{p}', '1433') or '1433').strip()
    encrypt = (os.getenv('DB_ENCRYPT', 'yes') or 'yes').strip()
    trust   = (os.getenv('DB_TRUST_CERT', 'yes') or 'yes').strip()
    timeout = (os.getenv('DB_TIMEOUT', '20') or '20').strip()
    cs = (
        f'DRIVER={{{ODBC_DRIVER}}};'
        f'SERVER=tcp:{host},{port};DATABASE={base};'
        f'Encrypt={encrypt};TrustServerCertificate={trust};'
        f'Connection Timeout={timeout};'
    )
    cs += f'UID={user};PWD={pwd}' if user else 'Trusted_Connection=yes'
    return pyodbc.connect(cs, timeout=int(timeout) if timeout.isdigit() else 20)


def _resolver_idusuario_no_posto(con, login_campinho: str):
    """idUsuario do operador NO banco do posto, pelo Usuario (login_campinho).
    None se não existir/estiver desativado — significa que não opera nesse posto."""
    if not login_campinho:
        return None
    cur = con.cursor()
    cur.execute(
        "SELECT TOP 1 idUsuario FROM sis_usuario WHERE Usuario = ? AND Desativado = 0",
        login_campinho,
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


@app.post('/api/confirmar_presenca')
@login_required
def api_confirmar_presenca():
    login_campinho = (session.get('login_campinho') or '').strip()
    if not login_campinho:
        # Sem vínculo no idCamim → não há usuário CAMIM pra assinar a confirmação.
        return jsonify({
            'ok': False, 'sem_vinculo': True,
            'error': ('Você ainda não tem um usuário do sistema CAMIM vinculado '
                      'ao seu login idCamim. Vincule na sua área do IDCamim para '
                      'poder confirmar presenças.'),
        }), 403

    data = request.get_json(silent=True) or {}
    posto = (data.get('posto') or '').strip().upper()
    if posto not in POSTOS_TODOS:
        return jsonify({'ok': False, 'error': 'posto inválido'}), 400
    try:
        idlanc = int(data.get('idlancamento'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'idlancamento obrigatório'}), 400

    try:
        con = _mssql_conn_for_posto(posto)
    except Exception as e:
        app.logger.warning('Falha ao conectar no posto %s: %s', posto, e)
        return jsonify({'ok': False, 'error': f'Falha ao conectar no posto {posto}'}), 502

    try:
        id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
        if not id_usuario_op:
            return jsonify({
                'ok': False, 'sem_vinculo_posto': True,
                'error': (f'Seu usuário CAMIM "{login_campinho}" não existe (ou está '
                          f'desativado) no posto {posto}. Sem isso a confirmação '
                          f'ficaria sem responsável e o sistema bloqueia.'),
            }), 403

        cur = con.cursor()
        # Já confirmado? Não sobrescreve o confirmante original (idempotente).
        cur.execute(
            "SELECT CONVERT(varchar(5), dataconfirmacaoConsulta, 108) "
            "FROM Cad_Lancamento WHERE idLancamento = ?", idlanc)
        row = cur.fetchone()
        if not row:
            return jsonify({'ok': False, 'error': 'lançamento não encontrado neste posto'}), 404
        if row[0]:
            return jsonify({'ok': True, 'ja_confirmado': True, 'hora_confirmacao': row[0]})

        # Cad_Lancamento tem 6 triggers ENABLED → cur.rowcount é não-confiável
        # (conta linhas das triggers) e OUTPUT direto falha. Usa OUTPUT INTO
        # @table (forma trigger-safe, igual medico_novo): devolve linha SÓ se a
        # confirmação realmente gravou. WHERE ... IS NULL garante idempotência
        # e segurança contra corrida.
        cur.execute(
            "SET NOCOUNT ON;"
            "DECLARE @upd TABLE (hora varchar(5));"
            "UPDATE Cad_Lancamento "
            "  SET dataconfirmacaoConsulta = GETDATE(), idUsuarioConfirmaPresenca = ?, Confere = 1 "
            "  OUTPUT CONVERT(varchar(5), INSERTED.dataconfirmacaoConsulta, 108) INTO @upd "
            "  WHERE idLancamento = ? AND dataconfirmacaoConsulta IS NULL;"
            "SELECT hora FROM @upd;",
            id_usuario_op, idlanc)
        while cur.description is None:
            if not cur.nextset():
                break
        upd = cur.fetchone() if cur.description else None
        if not upd:
            # Confirmado por outro entre o SELECT e o UPDATE (corrida) — devolve o existente.
            con.rollback()
            cur2 = con.cursor()
            cur2.execute(
                "SELECT CONVERT(varchar(5), dataconfirmacaoConsulta, 108) "
                "FROM Cad_Lancamento WHERE idLancamento = ?", idlanc)
            hora_ex = (cur2.fetchone() or [None])[0]
            return jsonify({'ok': True, 'ja_confirmado': True, 'hora_confirmacao': hora_ex or ''})
        hora = upd[0]

        # Auditoria obrigatória — mesma transação, antes do commit.
        cur.execute(
            "INSERT INTO Sis_Historico (id, idTabela, idComando, idUsuario, DataHora, Detalhe, Computador) "
            "VALUES (?, ?, ?, ?, GETDATE(), ?, ?)",
            idlanc, ID_TABELA_CAD_LANCAMENTO, ID_COMANDO_EDICAO, id_usuario_op,
            'Confirmação de presença via Agenda (camila2/kpi)', COMPUTADOR_ORIGEM)
        con.commit()

        # Reflete no cache Postgres na hora (a agenda lê de lá; sem isso só
        # apareceria no próximo ciclo do ETL). Best-effort: se falhar, a fonte
        # de verdade (SQL Server) já está gravada e o ETL corrige depois.
        try:
            from f3_db import set_hora_confirmacao
            set_hora_confirmacao(posto, idlanc, hora or '')
        except Exception as e:
            app.logger.warning('confirmou no SQL Server mas falhou ao refletir no '
                               'Postgres (posto=%s, idlanc=%s): %s', posto, idlanc, e)

        return jsonify({'ok': True, 'hora_confirmacao': hora or ''})
    except Exception as e:
        try:
            con.rollback()
        except Exception:
            pass
        app.logger.exception('confirmar_presenca falhou (posto=%s, idlanc=%s)', posto, idlanc)
        return jsonify({'ok': False, 'error': str(e)[:300]}), 500
    finally:
        try:
            con.close()
        except Exception:
            pass


@app.post('/api/desconfirmar_presenca')
@login_required
def api_desconfirmar_presenca():
    """Desfaz a confirmação (toggle, igual o F3): limpa Confere/dataconfirmacaoConsulta/
    idUsuarioConfirmaPresenca. Auditado. Mesmo gate de vínculo do confirmar."""
    login_campinho = (session.get('login_campinho') or '').strip()
    if not login_campinho:
        return jsonify({
            'ok': False, 'sem_vinculo': True,
            'error': ('Você ainda não tem um usuário do sistema CAMIM vinculado ao seu '
                      'login idCamim. Vincule na sua área do IDCamim.'),
        }), 403

    data = request.get_json(silent=True) or {}
    posto = (data.get('posto') or '').strip().upper()
    if posto not in POSTOS_TODOS:
        return jsonify({'ok': False, 'error': 'posto inválido'}), 400
    try:
        idlanc = int(data.get('idlancamento'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'idlancamento obrigatório'}), 400

    try:
        con = _mssql_conn_for_posto(posto)
    except Exception as e:
        app.logger.warning('Falha ao conectar no posto %s: %s', posto, e)
        return jsonify({'ok': False, 'error': f'Falha ao conectar no posto {posto}'}), 502

    try:
        id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
        if not id_usuario_op:
            return jsonify({
                'ok': False, 'sem_vinculo_posto': True,
                'error': (f'Seu usuário CAMIM "{login_campinho}" não existe (ou está '
                          f'desativado) no posto {posto}.'),
            }), 403

        cur = con.cursor()
        # Toggle off. WHERE ... IS NOT NULL = idempotente. OUTPUT INTO @table
        # por causa das 6 triggers (cur.rowcount mente).
        cur.execute(
            "SET NOCOUNT ON;"
            "DECLARE @upd TABLE (x int);"
            "UPDATE Cad_Lancamento "
            "  SET dataconfirmacaoConsulta = NULL, idUsuarioConfirmaPresenca = NULL, Confere = 0 "
            "  OUTPUT 1 INTO @upd "
            "  WHERE idLancamento = ? AND dataconfirmacaoConsulta IS NOT NULL;"
            "SELECT x FROM @upd;",
            idlanc)
        while cur.description is None:
            if not cur.nextset():
                break
        upd = cur.fetchone() if cur.description else None
        if not upd:
            con.rollback()
            return jsonify({'ok': True, 'ja_desconfirmado': True})

        cur.execute(
            "INSERT INTO Sis_Historico (id, idTabela, idComando, idUsuario, DataHora, Detalhe, Computador) "
            "VALUES (?, ?, ?, ?, GETDATE(), ?, ?)",
            idlanc, ID_TABELA_CAD_LANCAMENTO, ID_COMANDO_EDICAO, id_usuario_op,
            'Desconfirmação de presença via Agenda (camila2/kpi)', COMPUTADOR_ORIGEM)
        con.commit()

        try:
            from f3_db import set_hora_confirmacao
            set_hora_confirmacao(posto, idlanc, '')
        except Exception as e:
            app.logger.warning('desconfirmou no SQL Server mas falhou ao refletir no '
                               'Postgres (posto=%s, idlanc=%s): %s', posto, idlanc, e)

        return jsonify({'ok': True})
    except Exception as e:
        try:
            con.rollback()
        except Exception:
            pass
        app.logger.exception('desconfirmar_presenca falhou (posto=%s, idlanc=%s)', posto, idlanc)
        return jsonify({'ok': False, 'error': str(e)[:300]}), 500
    finally:
        try:
            con.close()
        except Exception:
            pass


# ── Quem confirmou (lê do SQL Server do posto, sob demanda) ──────────────────

@app.get('/api/quem_confirmou')
@login_required
def api_quem_confirmou():
    posto = (request.args.get('posto') or '').strip().upper()
    if posto not in POSTOS_TODOS:
        return jsonify({'ok': False, 'error': 'posto inválido'}), 400
    try:
        idlanc = int(request.args.get('idlancamento'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'idlancamento obrigatório'}), 400

    try:
        con = _mssql_conn_for_posto(posto)
    except Exception as e:
        app.logger.warning('Falha ao conectar no posto %s: %s', posto, e)
        return jsonify({'ok': False, 'error': f'Falha ao conectar no posto {posto}'}), 502

    try:
        cur = con.cursor()
        cur.execute(
            "SELECT CONVERT(varchar(10), l.dataconfirmacaoConsulta, 103) + ' ' + "
            "       CONVERT(varchar(5),  l.dataconfirmacaoConsulta, 108) AS confirmado_em, "
            "       u.Usuario, u.Nome "
            "FROM Cad_Lancamento l "
            "LEFT JOIN sis_usuario u ON u.idUsuario = l.idUsuarioConfirmaPresenca "
            "WHERE l.idLancamento = ?", idlanc)
        row = cur.fetchone()
        if not row:
            return jsonify({'ok': False, 'error': 'lançamento não encontrado neste posto'}), 404
        if not row[0]:
            return jsonify({'ok': True, 'confirmado': False})
        return jsonify({
            'ok':            True,
            'confirmado':    True,
            'confirmado_em': row[0],
            'usuario':       (row[1] or '').strip(),
            'nome':          (row[2] or '').strip(),
        })
    except Exception as e:
        app.logger.exception('quem_confirmou falhou (posto=%s, idlanc=%s)', posto, idlanc)
        return jsonify({'ok': False, 'error': str(e)[:300]}), 500
    finally:
        try:
            con.close()
        except Exception:
            pass


# ── CRM (local-first → Campinho via camila3, em background) ──────────────────
#
# POST /api/crm grava no Postgres f3 (crm_local) e responde na hora; o upload
# pra Campinho (sp_CRM_Insert via API camila3) roda em background com retry.
# O idUsuario que assina o CRM em Campinho é resolvido em sis_usuario do banco
# C pelo login_campinho do operador — mesmo gate de vínculo do confirmar.

import campinho_crm

@app.get('/api/crm/lookup')
@login_required
def api_crm_lookup():
    try:
        return jsonify({'ok': True, **campinho_crm.get_lookup()})
    except Exception as e:
        return jsonify({'ok': False,
                        'error': f'Falha ao carregar motivos/tipos: {e}'}), 502


@app.post('/api/crm')
@login_required
def api_crm_criar():
    login_campinho = (session.get('login_campinho') or '').strip()
    if not login_campinho:
        return jsonify({
            'ok': False, 'sem_vinculo': True,
            'error': ('Você ainda não tem um usuário do sistema CAMIM vinculado '
                      'ao seu login idCamim. Vincule na sua área do IDCamim para '
                      'poder criar CRM.'),
        }), 403

    data = request.get_json(silent=True) or {}
    posto = (data.get('posto') or '').strip().upper()
    if posto not in POSTOS_TODOS:
        return jsonify({'ok': False, 'error': 'posto inválido'}), 400
    try:
        idlanc = int(data.get('idlancamento'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'idlancamento obrigatório'}), 400
    try:
        id_motivo = int(data.get('id_motivo'))
        id_tipo   = int(data.get('id_tipo'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'motivo e tipo são obrigatórios'}), 400
    historico = (data.get('historico') or '').strip()
    if not historico:
        return jsonify({'ok': False, 'error': 'histórico é obrigatório'}), 400

    # idUsuario que assina o CRM em Campinho (banco C) — resolve agora pra
    # falhar rápido se o operador não existir lá; o upload em background fica
    # 100% HTTP (camila3) e re-tentável.
    try:
        con = _mssql_conn_for_posto('C')
    except Exception as e:
        app.logger.warning('Falha ao conectar em Campinho (C): %s', e)
        return jsonify({'ok': False, 'error': 'Falha ao conectar em Campinho'}), 502
    try:
        id_usuario_campinho = _resolver_idusuario_no_posto(con, login_campinho)
    finally:
        try:
            con.close()
        except Exception:
            pass
    if not id_usuario_campinho:
        return jsonify({
            'ok': False, 'sem_vinculo_posto': True,
            'error': (f'Seu usuário CAMIM "{login_campinho}" não existe (ou está '
                      f'desativado) em Campinho. Sem isso o CRM ficaria sem '
                      f'responsável e o sistema bloqueia.'),
        }), 403

    try:
        matricula = int(data.get('matricula'))
    except (TypeError, ValueError):
        matricula = None

    try:
        from f3_db import crm_insert
        reg = crm_insert({
            'posto':               posto,
            'idlancamento':        idlanc,
            'posto_cliente':       (data.get('posto_cliente') or posto).strip().upper()[:4],
            'matricula':           matricula,
            'paciente':            (data.get('paciente') or '').strip(),
            'id_motivo':           id_motivo,
            'motivo':              (data.get('motivo') or '').strip(),
            'id_tipo':             id_tipo,
            'tipo':                (data.get('tipo') or '').strip(),
            'pessoa':              (data.get('pessoa') or '').strip() or None,
            'telefone':            (data.get('telefone') or '').strip() or None,
            'historico':           historico,
            'criado_por':          login_campinho,
            'id_usuario_campinho': id_usuario_campinho,
        })
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 409
    except Exception as e:
        app.logger.exception('crm_insert falhou (posto=%s, idlanc=%s)', posto, idlanc)
        return jsonify({'ok': False, 'error': str(e)[:300]}), 500

    # resposta imediata; upload pra Campinho segue em background com retry
    campinho_crm.sync_async(reg['id'])

    return jsonify({'ok': True, 'crm': reg})


@app.get('/api/crm_detalhe')
@login_required
def api_crm_detalhe():
    posto = (request.args.get('posto') or '').strip().upper()
    if posto not in POSTOS_TODOS:
        return jsonify({'ok': False, 'error': 'posto inválido'}), 400
    try:
        idlanc = int(request.args.get('idlancamento'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'idlancamento obrigatório'}), 400
    try:
        from f3_db import crm_get
        reg = crm_get(posto, idlanc)
    except Exception as e:
        app.logger.exception('crm_get falhou (posto=%s, idlanc=%s)', posto, idlanc)
        return jsonify({'ok': False, 'error': str(e)[:300]}), 500
    if not reg:
        return jsonify({'ok': False, 'error': 'CRM não encontrado para este agendamento'}), 404
    return jsonify({'ok': True, 'crm': reg})


# Tabela crm_local + worker de retry (idempotentes; toleram Postgres fora no boot)
try:
    from f3_db import ensure_crm_table
    ensure_crm_table()
except Exception as _e:
    app.logger.warning('crm_local: falha ao garantir tabela no boot: %s', _e)
campinho_crm.start_retry_worker()


# ── Templates inline (erro/logout) ───────────────────────────────────────────

TMPL_ERRO = '''<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<title>Erro — Agenda F3</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
</head><body class="d-flex align-items-center justify-content-center" style="min-height:100vh;background:#f4f6f9">
<div class="card shadow" style="max-width:420px;width:100%">
  <div class="card-body text-center p-5">
    <h4 class="text-danger mb-3">{{ titulo }}</h4>
    <p class="text-muted">{{ msg }}</p>
    <a href="/auth/login" class="btn btn-primary mt-3">Tentar novamente</a>
  </div>
</div></body></html>'''

TMPL_LOGOUT = '''<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<title>Desconectado — Agenda F3</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
</head><body class="d-flex align-items-center justify-content-center" style="min-height:100vh;background:#f4f6f9">
<div class="card shadow" style="max-width:420px;width:100%">
  <div class="card-body text-center p-5">
    <h4 class="mb-3">Você saiu</h4>
    <p class="text-muted">Sessão encerrada.</p>
    <a href="/auth/login" class="btn btn-primary mt-3">Entrar novamente</a>
  </div>
</div></body></html>'''


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8024, debug=False)
