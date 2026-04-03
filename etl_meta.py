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
"""
import json, os
from datetime import datetime


class ETLMeta:
    def __init__(self, script_name, output_dir='.'):
        self.script = script_name
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
