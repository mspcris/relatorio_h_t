"""Testes do notas_xml (ZIP em thread, posto que falha, nota sem XML). Sem banco:
o leitor é falso. Rodar: .venv/bin/python -m unittest tests.test_notas_xml"""
import io
import os
import sys
import time
import unittest
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["NOTAS_XML_DIR"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_tmp_notas_xml")

import notas_xml  # noqa: E402

XML = b'<?xml version="1.0" encoding="UTF-8"?><NFSe xmlns="x"><infNFSe Id="1">' + b"a" * 80 + b"</infNFSe></NFSe>"


def leitor_falso(posto, ids, con=None):
    if posto == "Z":
        raise RuntimeError("posto fora do ar")
    return {i: XML for i in ids if i % 10 != 0}      # ids múltiplos de 10 não têm XML


def esperar(job):
    for _ in range(200):
        if job.estado != "andando":
            return job
        time.sleep(0.02)
    raise AssertionError("job não terminou")


class TestZip(unittest.TestCase):
    def test_zip_completo_com_faltando_e_posto_em_erro(self):
        itens = [{"posto": "A", "id": i, "nome": f"A/NF_{i}_A.xml", "rotulo": f"NF {i} (A)"} for i in range(1, 23)]
        itens += [{"posto": "Z", "id": 500, "nome": "Z/NF_500_Z.xml", "rotulo": "NF 500 (Z)"}]
        job = esperar(notas_xml.iniciar_zip("eu", "teste.zip", itens, "Cabeçalho", leitor=leitor_falso))
        self.assertEqual(job.estado, "pronto", job.erro)
        self.assertEqual(job.total, 23)
        self.assertEqual(job.gravados, 20)                  # 22 de A menos 10 e 20
        self.assertEqual(job.faltando, ["NF 10 (A)", "NF 20 (A)"])
        self.assertIn("Z", job.erros)
        self.assertEqual(job.feitos, job.total)
        with zipfile.ZipFile(job.arquivo) as zf:
            nomes = zf.namelist()
            self.assertIn("LEIA-ME.txt", nomes)
            self.assertIn("A/NF_1_A.xml", nomes)
            self.assertNotIn("A/NF_10_A.xml", nomes)
            self.assertNotIn("Z/NF_500_Z.xml", nomes)
            leia = zf.read("LEIA-ME.txt").decode("utf-8")
            self.assertIn("XMLs gravados neste ZIP: 20", leia)
            self.assertIn("Notas sem XML no sistema: 2", leia)
            self.assertIn("posto Z", leia)
            self.assertEqual(zf.read("A/NF_1_A.xml"), XML)
        d = job.dict()
        self.assertTrue(d["pronto"]); self.assertEqual(d["faltando"], 2)
        # só o dono enxerga
        self.assertIsNone(notas_xml.obter(job.id, "outro"))
        self.assertIs(notas_xml.obter(job.id, "eu"), job)

    def test_sem_itens_recusa(self):
        with self.assertRaises(ValueError):
            notas_xml.iniciar_zip("eu", "x.zip", [], "", leitor=leitor_falso)

    def test_nome_xml(self):
        self.assertEqual(notas_xml.nome_xml({"NotaFiscal": 123, "posto": "a", "Cancelada": "Sim"}), "NF_123_A_CANCELADA.xml")
        self.assertEqual(notas_xml.nome_xml({"NotaFiscal": 7, "posto": "C", "Cancelada": "Não"}), "NF_7_C.xml")

    def test_bytes_limpa_bom_e_recusa_vazio(self):
        self.assertIsNone(notas_xml._bytes(None))
        self.assertIsNone(notas_xml._bytes("<x/>"))
        self.assertEqual(notas_xml._bytes("﻿  " + XML.decode()), XML)


if __name__ == "__main__":
    unittest.main()
