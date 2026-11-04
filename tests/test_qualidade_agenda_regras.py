"""Regra de "dia com vaga válida" da Qualidade da Agenda (caso Leonardo, 04/11/2026):
Pediatria em Campo Grande atendia 18 e tinha 1 vaga (5,56 %) — com mínimo de 20 %
no cad_cbos esse dia NÃO pode ser a próxima vaga. Rodar:
    python3 -m unittest tests.test_qualidade_agenda_regras
"""
import unittest

from qualidade_agenda_regras import dia_passa, pct_livre, proxima_vaga


class PctLivre(unittest.TestCase):
    def test_um_em_dezoito(self):
        self.assertEqual(pct_livre(1, 18), 5.56)

    def test_capacidade_zero_nao_divide(self):
        self.assertIsNone(pct_livre(0, 0))


class DiaPassa(unittest.TestCase):
    def test_caso_leonardo_pediatria_g(self):
        # [offset, vagas, capacidade]
        self.assertFalse(dia_passa([6, 1, 18], 20))

    def test_dia_com_44_por_cento(self):
        self.assertTrue(dia_passa([20, 8, 18], 20))

    def test_exatamente_no_minimo_passa(self):
        self.assertTrue(dia_passa([3, 1, 5], 20))

    def test_minimo_zero_exige_pelo_menos_uma_vaga(self):
        self.assertTrue(dia_passa([0, 1, 40], 0))
        self.assertFalse(dia_passa([0, 0, 40], 0))

    def test_vaga_negativa_nunca_passa(self):
        self.assertFalse(dia_passa([0, -3, 20], 0))

    def test_minimo_none_vale_zero(self):
        self.assertTrue(dia_passa([0, 1, 40], None))


class ProximaVaga(unittest.TestCase):
    LISTA_G = [[6, 1, 18], [13, 1, 18], [20, 8, 18], [27, 15, 18]]

    def test_primeiro_dia_que_passa(self):
        self.assertEqual(proxima_vaga(self.LISTA_G, 20), [20, 8, 18])

    def test_sem_minimo_e_o_primeiro_com_vaga(self):
        self.assertEqual(proxima_vaga(self.LISTA_G, 0), [6, 1, 18])

    def test_nenhum_dia_passa(self):
        self.assertIsNone(proxima_vaga([[6, 1, 18], [13, 1, 18]], 20))

    def test_lista_vazia_ou_none(self):
        self.assertIsNone(proxima_vaga([], 20))
        self.assertIsNone(proxima_vaga(None, 20))

    def test_a_partir_de_n_dias(self):
        self.assertEqual(proxima_vaga(self.LISTA_G, 20, a_partir_de=21), [27, 15, 18])

    def test_ignora_offset_negativo(self):
        self.assertEqual(proxima_vaga([[-1, 9, 10], [2, 9, 10]], 20), [2, 9, 10])


if __name__ == "__main__":
    unittest.main()
