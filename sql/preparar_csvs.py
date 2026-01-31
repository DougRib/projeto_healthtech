"""
Etapa 3 - Preparacao de CSVs para carga no banco
=================================================

Este script cria arquivos CSV com colunas padrao para a Etapa 3 (SQL),
reduzindo risco de falha na importacao devido a colunas extras.

Saidas:
- sql_data/consolidado_despesas_sql.csv
- sql_data/despesas_agregadas_sql.csv
- sql_data/operadoras_cadastro.csv (copia do arquivo informado)
"""

from pathlib import Path
import shutil
import pandas as pd


COLUNAS_CONSOLIDADO = [
    "CNPJ",
    "RazaoSocial",
    "Trimestre",
    "Ano",
    "ValorDespesas",
]

COLUNAS_CONSOLIDADO_OPCIONAIS = [
    "inconsistencia_flag",
]

COLUNAS_AGREGADAS = [
    "Ranking",
    "RazaoSocial",
    "UF",
    "TotalDespesas",
    "MediaDespesas",
    "MediaPorTrimestre",
    "DesvioPadrao",
    "CoeficienteVariacao",
    "NumeroTrimestres",
    "AltaVariabilidade",
]


def preparar_consolidado(caminho: Path, destino: Path) -> None:
    df = pd.read_csv(caminho, sep=";", encoding="utf-8", low_memory=False)
    colunas_faltantes = [c for c in COLUNAS_CONSOLIDADO if c not in df.columns]
    if colunas_faltantes:
        raise ValueError(f"Consolidado sem colunas: {colunas_faltantes}")
    if "inconsistencia_flag" not in df.columns:
        df["inconsistencia_flag"] = ""
    colunas_saida = COLUNAS_CONSOLIDADO + COLUNAS_CONSOLIDADO_OPCIONAIS
    df[colunas_saida].to_csv(destino, sep=";", index=False, encoding="utf-8")


def preparar_agregadas(caminho: Path, destino: Path) -> None:
    df = pd.read_csv(caminho, sep=";", encoding="utf-8", low_memory=False)
    colunas_faltantes = [c for c in COLUNAS_AGREGADAS if c not in df.columns]
    if colunas_faltantes:
        raise ValueError(f"Agregadas sem colunas: {colunas_faltantes}")
    df[COLUNAS_AGREGADAS].to_csv(destino, sep=";", index=False, encoding="utf-8")


def copiar_cadastro(caminho: Path, destino: Path) -> None:
    shutil.copyfile(caminho, destino)


def main() -> None:
    raiz = Path(__file__).resolve().parents[1]
    saida = raiz / "sql_data"
    saida.mkdir(parents=True, exist_ok=True)

    consolidado = raiz / "output" / "consolidado_despesas.csv"
    agregadas = raiz / "output" / "despesas_agregadas.csv"

    if not consolidado.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {consolidado}")
    if not agregadas.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {agregadas}")

    preparar_consolidado(consolidado, saida / "consolidado_despesas_sql.csv")
    preparar_agregadas(agregadas, saida / "despesas_agregadas_sql.csv")

    # Cadastro: usar o arquivo da ANS se estiver disponivel
    cadastro = raiz / "data" / "cadastro_operadoras.csv"
    if cadastro.exists():
        copiar_cadastro(cadastro, saida / "operadoras_cadastro.csv")
    else:
        print("Aviso: cadastro_operadoras.csv nao encontrado. Copie manualmente para sql_data/")

    print("CSVs prontos em:", saida)


if __name__ == "__main__":
    main()
