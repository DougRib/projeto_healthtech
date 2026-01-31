import sys
import unittest
from pathlib import Path

import pandas as pd

PROJETO_RAIZ = Path(__file__).resolve().parents[1]
INTEGRACAO_DIR = PROJETO_RAIZ / "integracao_api"
if str(PROJETO_RAIZ) not in sys.path:
    sys.path.insert(0, str(PROJETO_RAIZ))
if str(INTEGRACAO_DIR) not in sys.path:
    sys.path.insert(0, str(INTEGRACAO_DIR))

from integracao_api.processor import ProcessadorArquivos
from integracao_api.utils import limpar_cnpj
from transformacao.validacao import ValidadorDados


class TestETLUtils(unittest.TestCase):
    def test_limpar_cnpj(self):
        self.assertEqual(limpar_cnpj("12.345.678/0001-90"), "12345678000190")
        self.assertEqual(limpar_cnpj("  123  "), "123")

    def test_validar_digito_cnpj(self):
        self.assertTrue(ValidadorDados.validar_digito_cnpj("11222333000181"))
        self.assertFalse(ValidadorDados.validar_digito_cnpj("11222333000182"))

    def test_limpar_dados_flags(self):
        df = pd.DataFrame(
            [
                {
                    "CNPJ": "",
                    "RazaoSocial": "",
                    "Trimestre": 3,
                    "Ano": 2025,
                    "ValorDespesas": 0,
                },
                {
                    "CNPJ": "11.222.333/0001-81",
                    "RazaoSocial": "Operadora Teste",
                    "Trimestre": 2,
                    "Ano": 2025,
                    "ValorDespesas": -10,
                },
            ]
        )
        processador = ProcessadorArquivos()
        df_limpo = processador.limpar_dados(df)

        flags_0 = df_limpo.loc[0, "inconsistencia_flag"]
        self.assertIn("CNPJ_SEM_MATCH", flags_0)
        self.assertIn("RAZAO_VAZIA", flags_0)
        self.assertIn("VALOR_ZERADO", flags_0)

        flags_1 = df_limpo.loc[1, "inconsistencia_flag"]
        self.assertIn("VALOR_NEGATIVO", flags_1)


if __name__ == "__main__":
    unittest.main()
