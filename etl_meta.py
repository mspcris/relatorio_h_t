"""
Utilidade para gravar metadados de execução ETL por posto.

Uso:
    from etl_meta import ETLMeta

    meta = ETLMeta('export_governanca', 'json_consolidado')
    for posto in postos:
        try:
            ...
            meta.ok(posto)
        except Exception as e:
            meta.error(posto, e)
    meta.save()

Gera:  json_consolidado/_etl_meta_export_governanca.json

Bug histórico (corrigido 2026-05-19): quando `output_dir` era relativo e o
script rodava via cron como root sem `cd`, o cwd era /root e o JSON do meta
ia parar em /root/json_consolidado/. O monitor lia /opt/relatorio_h_t/
json_consolidado/ (path correto) e mostrava "ETL atrasado" porque o
arquivo lá ficava parado no mtime antigo. Agora paths relativos são
resolvidos contra o diretório do script principal (sys.argv[0]).
"""
import json, os, sys
from datetime import datetime


class ETLMeta:
    def __init__(self, script_name, output_dir='.'):
        self.script = script_name
        if not os.path.isabs(output_dir):
            # Resolve contra o dir do script (sys.argv[0]), não contra o cwd.
            # Cron como root tem cwd=/root, o que jogava o meta em /root/json_consolidado/.
            base = os.path.dirname(os.path.abspath(sys.argv[0])) if sys.argv and sys.argv[0] else os.getcwd()
            output_dir = os.path.join(base, output_dir)
        self.output_dir = output_dir
        self.started_at = datetime.now().isoformat(timespec='seconds')
        self.postos = {}

    def ok(self, posto, **extra):
        self.postos[posto] = {
            'status': 'ok',
            'at': datetime.now().isoformat(timespec='seconds'),
            **extra,
        }

    def error(self, posto, msg, **extra):
        self.postos[posto] = {
            'status': 'error',
            'at': datetime.now().isoformat(timespec='seconds'),
            'msg': str(msg)[:300],
            **extra,
        }

    def save(self):
        data = {
            'script': self.script,
            'started_at': self.started_at,
            'finished_at': datetime.now().isoformat(timespec='seconds'),
            'postos': self.postos,
        }
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, f'_etl_meta_{self.script}.json')
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
