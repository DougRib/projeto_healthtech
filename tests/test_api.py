import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from api_web.backend.app import main
from api_web.backend.app.config import Settings
from api_web.backend.app.data_loader import DataRepository


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    content = "\n".join([header] + rows) + "\n"
    path.write_text(content, encoding="utf-8")


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        base = Path(self.temp_dir.name)

        consolidado = base / "consolidado.csv"
        agregado = base / "agregado.csv"
        cadastro = base / "cadastro.csv"

        _write_csv(
            consolidado,
            "CNPJ;RazaoSocial;Trimestre;Ano;ValorDespesas;inconsistencia_flag",
            [
                "11222333000181;OPERADORA TESTE;1;2025;100.50;",
                "11222333000181;OPERADORA TESTE;2;2025;200.00;",
                "11222333000181;OPERADORA TESTE;3;2025;300.00;",
            ],
        )

        _write_csv(
            agregado,
            "Ranking;RazaoSocial;UF;TotalDespesas;MediaDespesas;MediaPorTrimestre;DesvioPadrao;CoeficienteVariacao;NumeroTrimestres;AltaVariabilidade",
            [
                "1;OPERADORA TESTE;SP;600.50;200.17;200.17;100.00;50.00;3;false",
            ],
        )

        _write_csv(
            cadastro,
            "Registro_Operadora;CNPJ;Razao_Social;Modalidade;UF",
            [
                "123;11222333000181;OPERADORA TESTE;Medicina de Grupo;SP",
            ],
        )

        os.environ["HT_DATA_CONSOLIDADO_PATH"] = str(consolidado)
        os.environ["HT_DATA_AGREGADO_PATH"] = str(agregado)
        os.environ["HT_DATA_CADASTRO_PATH"] = str(cadastro)

        settings = Settings()
        main.repo = DataRepository(settings)
        self.client = TestClient(main.app)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_listar_operadoras(self):
        resp = self.client.get("/api/operadoras?page=1&limit=5")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertGreater(payload["total"], 0)
        self.assertIn("tem_despesas", payload["data"][0])

    def test_obter_despesas(self):
        resp = self.client.get("/api/operadoras/11222333000181/despesas")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(len(payload), 3)

    def test_operadora_inexistente(self):
        resp = self.client.get("/api/operadoras/00000000000000")
        self.assertEqual(resp.status_code, 404)

    def test_estatisticas(self):
        resp = self.client.get("/api/estatisticas")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertIn("total_despesas", payload)
        self.assertIn("media_por_operadora", payload)
        self.assertIn("top_5_operadoras", payload)


if __name__ == "__main__":
    unittest.main()
