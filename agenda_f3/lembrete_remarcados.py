"""lembrete_remarcados.py — lembrete de consulta a quem foi REMARCADO pelo ERP.

Pedido do Cristiano (chamado de 24/09/2026, Agatha/Campinho, call center): a
remarcação continua no ERP; a Agenda do Dia só avisa o paciente. Um botão,
todos os remarcados pendentes do posto, e quem já recebeu não recebe de novo.

- Remarcado = lançamento criado por `WEB_API_TransferirAgenda` — é a ÚNICA
  procedure que reagenda no ERP (ela grava Observacao 'WEB_API_TransferirAgenda'
  no lançamento novo). Medido em 2026-09-30: usada em A C D J M P R Y; B G I N X
  não usam. Só consultas que ainda não aconteceram, sem desistência nem estorno.
- Modelo Meta `transferencia_de_agenda` (aprovado como MARKETING — a Meta
  recusou como utilidade). Marketing é ~R$ 0,34/msg e a Meta não entrega a quem
  bloqueou marketing: a tela mostra isso por paciente (código 131050/131049).
- Trava de duplicidade no Postgres f3 (`lembrete_remarcado`, único por
  posto+idlancamento), reservada ANTES do envio — ver f3_db.lembrete_claim.
- Status de entrega: a whatsapp-api não tem rota de status; o webhook da Meta
  cai no chat, que grava tudo em `Webhook` (índice por wamid). SÓ LEITURA lá —
  é banco do time do chat, nada de DDL nem escrita.
- NÃO cria campanha no whatsapp_cobranca.db de propósito: o cron de cobrança
  itera as campanhas, e campanha nova foi exatamente o gatilho do incidente de
  2026-05-06.
- Kill-switch: LEMBRETE_REMARCADO_ENVIO=0 no .env da agenda desliga o envio.
"""
import json
import logging
import os
import re
import threading
from datetime import datetime

import requests
from dotenv import dotenv_values

log = logging.getLogger(__name__)

# Credenciais da API Meta e do MySQL do chat moram no .env do relatorio_h_t
# (www-data está no grupo deploy e lê o arquivo). O .env da agenda tem
# precedência se um dia receber as mesmas chaves.
_REL = dotenv_values('/opt/relatorio_h_t/.env')


def _cfg(nome: str, padrao: str = '') -> str:
    return (os.getenv(nome) or _REL.get(nome) or padrao).strip()


TEMPLATE = _cfg('LEMBRETE_REMARCADO_TEMPLATE', 'transferencia_de_agenda')
PRECO_MSG_BRL = float(_cfg('LEMBRETE_REMARCADO_PRECO_BRL', '0.34'))


def envio_ligado() -> bool:
    return _cfg('LEMBRETE_REMARCADO_ENVIO', '1') != '0'


# Postos do grupo Couto saem pelo 3529-6666; os demais pelo número padrão da
# conta (2455-9600). Mesma regra do aviso de falta (medico_falta_routes.py).
COUTO_POSTOS = frozenset('CDJMP')
WPP_FROM_COUTO = '552135296666'

MARCA_TRANSFERENCIA = 'WEB_API_TransferirAgenda'

SQL_REMARCADOS = """
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT n.idLancamento, CAST(n.DataConsulta AS date), n.HoraPrevistaConsulta, n.Data,
       CASE WHEN d.idDependente IS NOT NULL
            THEN COALESCE(NULLIF(LTRIM(RTRIM(d.NomeSocial)), ''), d.Nome)
            ELSE COALESCE(NULLIF(LTRIM(RTRIM(c.NomeSocial)), ''), c.Nome) END,
       COALESCE(NULLIF(LTRIM(RTRIM(d.TelefoneWhatsApp)), ''), c.TelefoneWhatsApp),
       m.Nome, e.Especialidade
  FROM Cad_Lancamento n
  JOIN Cad_Cliente c ON c.idCliente = n.idCliente
  LEFT JOIN Cad_ClienteDependente d ON d.idDependente = n.idDependente
  LEFT JOIN Cad_Medico m ON m.idMedico = n.idMedico
  LEFT JOIN Cad_Especialidade e ON e.idEspecialidade = n.idEspecialidade
 WHERE n.Data >= DATEADD(day, -90, GETDATE())
   AND n.DataConsulta >= CAST(GETDATE() AS date)
   AND n.DataEstorno IS NULL
   AND CAST(n.Observacao AS varchar(60)) LIKE ?
   AND EXISTS (SELECT 1 FROM Cad_LancamentoServico ls
                WHERE ls.idLancamento = n.idLancamento AND ls.DataDesistencia IS NULL)
 ORDER BY n.DataConsulta, n.HoraPrevistaConsulta
"""

_PARTICULAS = {'de', 'da', 'do', 'das', 'dos', 'e'}
_DIAS = ('segunda-feira', 'terça-feira', 'quarta-feira', 'quinta-feira',
         'sexta-feira', 'sábado', 'domingo')


def _titulo(txt: str) -> str:
    palavras = (txt or '').strip().lower().split()
    return ' '.join(p if (i and p in _PARTICULAS) else p.capitalize()
                    for i, p in enumerate(palavras))


def primeiro_nome(nome: str) -> str:
    p = (nome or '').strip().split()
    return p[0].capitalize() if p else 'cliente'


def nome_profissional(nome: str) -> str:
    """Primeiro nome + o primeiro sobrenome que não seja partícula
    ('TATIANE DE ALBUQUERQUE MOURA' → 'Tatiane Albuquerque')."""
    p = [x for x in (nome or '').strip().split()]
    if not p:
        return ''
    resto = next((x for x in p[1:] if x.lower() not in _PARTICULAS), '')
    return _titulo(f'{p[0]} {resto}'.strip())


def data_extenso(d) -> str:
    return f'{_DIAS[d.weekday()]}, {d.strftime("%d/%m/%Y")}'


def limpar_telefone(raw: str | None) -> str | None:
    """Celular brasileiro no formato 55 + DDD + número; None se não servir."""
    dig = re.sub(r'\D', '', raw or '')
    if dig.startswith('0'):
        dig = dig.lstrip('0')
    if len(dig) in (10, 11):
        dig = '55' + dig
    if len(dig) in (12, 13) and dig.startswith('55'):
        return dig
    return None


def local_do_posto(con, posto: str) -> str:
    cur = con.cursor()
    cur.execute("SELECT TOP 1 Descricao FROM cad_endereco WHERE Codigo = ?", posto)
    row = cur.fetchone()
    return f"CAMIM {(row[0] or '').strip()}".strip() if row else 'CAMIM'


def remarcados(con, posto: str, agora: datetime | None = None) -> list[dict]:
    """Remarcados (TransferirAgenda) com consulta que ainda não aconteceu."""
    agora = agora or datetime.now()
    hoje, hhmm = agora.date(), agora.strftime('%H:%M')
    cur = con.cursor()
    cur.execute(SQL_REMARCADOS, MARCA_TRANSFERENCIA + '%')
    out = []
    for r in cur.fetchall():
        data = r[1] if not isinstance(r[1], datetime) else r[1].date()
        hora = (r[2] or '').strip()[:5]
        if data == hoje and hora and hora <= hhmm:
            continue            # já passou: lembrete não serve mais
        out.append({
            'idlancamento': int(r[0]),
            'data_consulta': data,
            'hora': hora,
            'transferido_em': r[3],
            'paciente': (r[4] or '').strip(),
            'telefone_raw': (r[5] or '').strip(),
            'telefone': limpar_telefone(r[5]),
            'medico': (r[6] or '').strip(),
            'especialidade': (r[7] or '').strip(),
        })
    return out


def params_template(item: dict, local: str) -> dict:
    return {
        'nome': primeiro_nome(item['paciente']),
        'medico': nome_profissional(item['medico']) or _titulo(item['especialidade']),
        'especialidade': _titulo(item['especialidade']),
        'data': data_extenso(item['data_consulta']),
        'hora': item['hora'] or 'horário a confirmar no aplicativo',
        'local': local,
    }


def enviar_meta(telefone: str, params: dict, posto: str) -> tuple[bool, str | None, str | None]:
    """(aceito, wamid, erro). Mesmo payload de send_whatsapp_cobranca.enviar_via_meta."""
    url, token = _cfg('WAPP_API_URL', 'https://whatsapp-api.camim.com.br'), _cfg('WAPP_TOKEN')
    if not token:
        return False, None, 'WAPP_TOKEN ausente no .env'
    payload = {'template': TEMPLATE,
               'people': [{'phone': telefone, 'data': {'BODY': params}}]}
    if posto in COUTO_POSTOS:
        payload['from'] = WPP_FROM_COUTO
    try:
        r = requests.post(f'{url}/templates/send', json=payload, timeout=20,
                          headers={'Authorization': f'Bearer {token}'})
        if r.status_code >= 400:
            return False, None, f'HTTP {r.status_code} {r.text[:200]}'
        d = r.json()
        return True, (d.get('id') or d.get('wamid')), None
    except Exception as e:  # noqa: BLE001
        return False, None, str(e)[:200]


# ── envio em segundo plano ───────────────────────────────────────────────────
# O request só dispara; o laço roda numa thread e o progresso vive no Postgres
# (a tela relê de lá), então tanto faz qual dos 2 workers do gunicorn responde.
_rodando: set[str] = set()
_lock = threading.Lock()


def iniciar_envio(posto: str, itens: list[dict], local: str, por: str) -> bool:
    import f3_db
    with _lock:
        if posto in _rodando:
            return False
        _rodando.add(posto)

    def _laco():
        try:
            for it in itens:
                if not f3_db.lembrete_claim(posto, it, por):
                    continue                     # já enviado (ou enviando) — nunca de novo
                ok, wamid, erro = enviar_meta(it['telefone'], params_template(it, local), posto)
                f3_db.lembrete_finish(posto, it['idlancamento'],
                                      'enviado' if ok else 'erro', wamid, erro)
                log.info('lembrete remarcado %s/%s: %s', posto, it['idlancamento'],
                         'enviado' if ok else f'erro {erro}')
        except Exception:  # noqa: BLE001
            log.exception('lembrete remarcado: laço de envio do posto %s parou', posto)
        finally:
            with _lock:
                _rodando.discard(posto)

    threading.Thread(target=_laco, daemon=True, name=f'lembrete-{posto}').start()
    return True


def rodando(posto: str) -> bool:
    with _lock:
        return posto in _rodando


# ── status de entrega (webhook da Meta gravado pelo chat) ────────────────────

MOTIVOS = {
    131050: 'O cliente bloqueou mensagens de marketing da CAMIM',
    131049: 'A Meta segurou: limite de mensagens de marketing para este cliente',
    130472: 'A Meta segurou a mensagem (experimento de marketing da Meta)',
    131026: 'Número sem WhatsApp ou aplicativo desatualizado',
    131000: 'Falha na Meta ao entregar — tente depois',
}
_RANK = {'sent': 1, 'delivered': 2, 'read': 3, 'failed': 9}
_ROTULO = {'sent': 'Enviada', 'delivered': 'Entregue', 'read': 'Lida', 'failed': 'Não entregue'}


def status_meta(wamids: list[str]) -> dict:
    """{wamid: {status, rotulo, motivo}}; {} se o chat estiver fora (a tela
    diz "sem leitura do status" — nunca vira "entregue")."""
    ws = [w for w in wamids if w]
    if not ws:
        return {}
    try:
        import pymysql
        con = pymysql.connect(
            host=_cfg('CHAT_MYSQL_HOST'), port=int(_cfg('CHAT_MYSQL_PORT', '3306')),
            user=_cfg('CHAT_MYSQL_USER'), password=_cfg('CHAT_MYSQL_PASSWORD'),
            database=_cfg('CHAT_MYSQL_DATABASE', 'camim_chat_production'),
            charset='utf8mb4', connect_timeout=5, read_timeout=15, autocommit=True)
    except Exception as e:  # noqa: BLE001
        log.warning('status do lembrete: chat indisponível: %s', str(e)[:200])
        return {}
    melhor: dict[str, tuple] = {}
    try:
        with con.cursor() as cur:
            for i in range(0, len(ws), 100):
                lote = ws[i:i + 100]
                cur.execute('SELECT wamid, CAST(body AS CHAR) FROM Webhook WHERE wamid IN ('
                            + ','.join(['%s'] * len(lote)) + ')', lote)
                for w, body in cur.fetchall():
                    try:
                        entradas = json.loads(body).get('entry', [])
                    except Exception:  # noqa: BLE001
                        continue
                    for e in entradas:
                        for ch in e.get('changes', []):
                            for s in ch.get('value', {}).get('statuses', []):
                                rk = _RANK.get(s.get('status'), 0)
                                if rk >= melhor.get(w, (0,))[0]:
                                    melhor[w] = (rk, s.get('status'), s.get('errors') or [])
    finally:
        con.close()
    out = {}
    for w, (_, st, erros) in melhor.items():
        motivo = None
        if st == 'failed':
            e = erros[0] if erros else {}
            motivo = MOTIVOS.get(e.get('code')) or e.get('title') or 'Recusada pela Meta'
        out[w] = {'status': st, 'rotulo': _ROTULO.get(st, st), 'motivo': motivo}
    return out
