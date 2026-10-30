"""API da contabilidade com XML: resumo (sem_xml / xml_desc), cnpj=todos, download
de um XML e ZIP — com JSON falso em disco e sem SQL Server.
Rodar: .venv/bin/python -m unittest tests.test_contabilidade_notas_xml"""
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TMP = tempfile.mkdtemp(prefix="contab_xml_")
os.environ["CONTAB_NOTAS_DIR"] = os.path.join(TMP, "json")
os.environ["NOTAS_XML_DIR"] = os.path.join(TMP, "zip")
os.makedirs(os.environ["CONTAB_NOTAS_DIR"])

from flask import Flask  # noqa: E402
import contabilidade_notas_routes as cr  # noqa: E402
import notas_xml  # noqa: E402

XML = b'<?xml version="1.0"?><NFSe><infNFSe Id="1">' + b"z" * 100 + b"</infNFSe></NFSe>"


def nota(posto, nf, cnpj, **kw):
    base = {"origem": "Clinica", "NotaFiscal": nf, "Empresa": "EMPRESA " + cnpj[-2:], "cnpj": cnpj, "data_emissao": "2026-09-0%d" % (nf % 9 + 1),
            "Cliente": "CLIENTE %d" % nf, "cpf": "", "valor": 100.0 + nf, "Cancelada": "Não", "chave": "CH%s%d" % (cnpj, nf),
            "id_retorno": 1000 + nf, "tem_xml": 1}
    base.update(kw)
    return base


class TestApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        d = os.environ["CONTAB_NOTAS_DIR"]
        a = [nota("A", 1, "11111111000111"), nota("A", 2, "11111111000111", tem_xml=0),
             nota("A", 3, "22222222000122", id_retorno=0, tem_xml=0), nota("A", 4, "22222222000122")]
        b = [nota("B", 5, "22222222000122")]
        del b[0]["id_retorno"]; del b[0]["tem_xml"]          # JSON antigo, sem o campo
        for p, notas in (("A", a), ("B", b)):
            with open(os.path.join(d, f"{p}_2026-09.json"), "w", encoding="utf-8") as f:
                json.dump({"posto": p, "ym": "2026-09", "gerado_em": "2026-10-30T10:00:00-03:00", "notas": notas}, f)
        cr._pode = lambda: (True, 200)
        cr._quem = lambda: "teste@camim.com.br"
        notas_xml.ler_xml = lambda posto, i: XML if i != 1002 else None
        cls._ler_xmls = notas_xml.ler_xmls
        notas_xml.ler_xmls = lambda posto, ids, con=None: {i: XML for i in ids if i != 1002}
        app = Flask(__name__)
        app.register_blueprint(cr.contabilidade_notas_bp)
        cls.c = app.test_client()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(TMP, ignore_errors=True)

    def test_resumo_conta_sem_xml(self):
        r = self.c.get("/api/contabilidade_notas/resumo?ym=2026-09").get_json()
        self.assertTrue(r["ok"])
        self.assertEqual(r["total"]["emit_qtd"], 5)
        self.assertEqual(r["total"]["xml_ok"], 2)
        self.assertEqual(r["total"]["sem_xml"], 2)
        self.assertEqual(r["total"]["xml_desc"], 1)
        e = {x["cnpj"]: x for x in r["empresas"]}
        self.assertEqual(e["11111111000111"]["sem_xml"], 1)
        self.assertEqual(e["22222222000122"]["sem_xml"], 1)
        self.assertEqual(e["22222222000122"]["xml_desc"], 1)
        motivos = {x["NotaFiscal"]: x["motivo"] for x in r["sem_xml_lista"]}
        self.assertIn("sem o XML", motivos[2])
        self.assertIn("sem retorno", motivos[3])
        self.assertFalse(r["sem_xml_cortada"])

    def test_notas_todos_e_por_empresa(self):
        r = self.c.get("/api/contabilidade_notas/notas?ym=2026-09&cnpj=todos").get_json()
        self.assertEqual(len(r["notas"]), 5)
        self.assertEqual(r["nome"], "Todas as empresas")
        self.assertEqual({n["xml"] for n in r["notas"]}, {"ok", "sem", "desc"})
        r = self.c.get("/api/contabilidade_notas/notas?ym=2026-09&cnpj=11.111.111/0001-11").get_json()
        self.assertEqual(len(r["notas"]), 2)

    def test_xml_um(self):
        r = self.c.get("/api/contabilidade_notas/xml?ym=2026-09&posto=A&id=1001")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.data, XML)
        self.assertIn("NF_1_A.xml", r.headers["Content-Disposition"])
        # id que não está no mês (ou sem XML) não sai, mesmo existindo na tabela
        self.assertEqual(self.c.get("/api/contabilidade_notas/xml?ym=2026-09&posto=A&id=1002").status_code, 404)
        self.assertEqual(self.c.get("/api/contabilidade_notas/xml?ym=2026-09&posto=A&id=999999").status_code, 404)

    def test_zip(self):
        r = self.c.post("/api/contabilidade_notas/xml/zip", json={"ym": "2026-09", "itens": [["A", 1001], ["A", 1004], ["A", 1002], ["A", 42]]}).get_json()
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["total"], 2)
        self.assertEqual(r["ignorados"], 2)
        for _ in range(100):
            st = self.c.get("/api/contabilidade_notas/xml/zip/" + r["job"]).get_json()
            if st["pronto"] or st["estado"] == "erro":
                break
            time.sleep(0.02)
        self.assertTrue(st["pronto"], st)
        self.assertEqual(st["gravados"], 2)
        self.assertEqual(st["nome"], "xml_notas_2026-09_todas_as_empresas.zip")
        z = self.c.get("/api/contabilidade_notas/xml/zip/" + r["job"] + "/arquivo")
        self.assertEqual(z.status_code, 200)
        with zipfile.ZipFile(__import__("io").BytesIO(z.data)) as zf:
            nomes = zf.namelist()
        self.assertIn("11111111000111_EMPRESA_11/NF_1_A.xml", nomes)
        self.assertIn("22222222000122_EMPRESA_22/NF_4_A.xml", nomes)
        self.assertIn("LEIA-ME.txt", nomes)
        # outro usuário não vê o job
        cr._quem = lambda: "outro@camim.com.br"
        try:
            self.assertEqual(self.c.get("/api/contabilidade_notas/xml/zip/" + r["job"]).status_code, 404)
        finally:
            cr._quem = lambda: "teste@camim.com.br"

    def test_zip_sem_itens(self):
        r = self.c.post("/api/contabilidade_notas/xml/zip", json={"ym": "2026-09", "itens": [["A", 1002]]})
        self.assertEqual(r.status_code, 400)


if __name__ == "__main__":
    unittest.main()
