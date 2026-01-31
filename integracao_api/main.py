"""
ETAPA 1: Integração com API Pública da ANS
==========================================

Este script realiza a integração completa com a API de Dados Abertos da ANS:

1. Download dos arquivos dos últimos 3 trimestres
2. Extração e processamento dos arquivos ZIP
3. Normalização de estruturas diferentes
4. Tratamento de inconsistências
5. Consolidação em formato padronizado
6. Geração do arquivo final consolidado_despesas.zip
"""

import os
import sys
import zipfile
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

import pandas as pd

# Importar módulos locais
from downloader import DownloaderANS
from processor import ProcessadorArquivos
from utils import (
    configurar_logging,
    criar_diretorios,
    tempo_decorrido,
    bytes_para_humano,
    COLUNAS_PADRAO_DESPESAS
)


# Configuração de logging
logger = configurar_logging("etapa1_main.log")

PROJETO_RAIZ = Path(__file__).resolve().parents[1]


class IntegradorANS:
    """
    Classe principal que orquestra todo o processo da Etapa 1.
    
    Responsabilidades:
    - Coordenar download e processamento
    - Consolidar dados de múltiplos trimestres
    - Gerar arquivo final ZIP
    - Gerar relatório de execução
    """
    
    def __init__(self):
        """Inicializa o integrador."""
        self.downloader = DownloaderANS(diretorio_destino=PROJETO_RAIZ / "data" / "raw")
        self.processador = ProcessadorArquivos(diretorio_extracao=PROJETO_RAIZ / "data" / "extracted")
        self.diretorio_saida = PROJETO_RAIZ / "output"
        
        criar_diretorios(self.diretorio_saida)
        
        logger.info("="*70)
        logger.info("INICIANDO ETAPA 1: INTEGRAÇÃO COM API ANS")
        logger.info("="*70)
    
    def executar(self, quantidade_trimestres: int = 3) -> Path:
        """
        Executa o processo completo da Etapa 1.
        
        Args:
            quantidade_trimestres: Número de trimestres para processar
        
        Returns:
            Path do arquivo ZIP final gerado
        """
        inicio = datetime.now()
        
        print("\n" + "="*70)
        print("ETAPA 1: INTEGRAÇÃO COM API ANS")
        print("="*70)
        print(f"Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}\n")
        
        try:
            # FASE 1: Download dos arquivos
            print("\n📥 FASE 1: Download de Arquivos")
            print("-" * 70)
            
            resultados_download = self.downloader.baixar_ultimos_trimestres(
                quantidade=quantidade_trimestres
            )
            
            if not resultados_download:
                raise Exception("Nenhum arquivo foi baixado!")
            
            # FASE 2: Processamento dos arquivos
            print("\n⚙️  FASE 2: Processamento de Arquivos")
            print("-" * 70)
            
            dataframes_consolidados = []
            
            for ano, trimestre, arquivos in resultados_download:
                print(f"\nProcessando trimestre {ano}/Q{trimestre}...")
                
                if not arquivos:
                    logger.warning(f"  Nenhum arquivo para processar em {ano}/Q{trimestre}")
                    continue
                
                for arquivo in arquivos:
                    print(f"  → {arquivo.name}")
                    
                    try:
                        df = self.processador.processar_arquivo(arquivo, ano, trimestre)
                        
                        if df is not None and not df.empty:
                            dataframes_consolidados.append(df)
                            print(f"    ✓ {len(df)} registros processados")
                        else:
                            print(f"    ⚠️  Arquivo vazio ou erro no processamento")
                    
                    except Exception as e:
                        logger.error(f"  ✗ Erro ao processar {arquivo.name}: {e}")
                        print(f"    ✗ Erro: {e}")
            
            if not dataframes_consolidados:
                raise Exception("Nenhum dado foi processado com sucesso!")
            
            # FASE 3: Consolidação
            print("\n📊 FASE 3: Consolidação dos Dados")
            print("-" * 70)
            
            df_final = self._consolidar_dados(dataframes_consolidados)
            
            # FASE 4: Geração do arquivo final
            print("\n💾 FASE 4: Geração do Arquivo Final")
            print("-" * 70)
            
            arquivo_final = self._gerar_arquivo_final(df_final)
            self._gerar_relatorio_inconsistencias(df_final)
            
            # FASE 5: Relatório final
            print("\n📋 FASE 5: Relatório de Execução")
            print("-" * 70)
            
            self._gerar_relatorio(
                df_final,
                resultados_download,
                tempo_decorrido(inicio)
            )
            
            print("\n" + "="*70)
            print("✅ ETAPA 1 CONCLUÍDA COM SUCESSO!")
            print("="*70)
            print(f"⏱️  Tempo total: {tempo_decorrido(inicio)}")
            print(f"📁 Arquivo gerado: {arquivo_final}")
            print("="*70 + "\n")
            
            logger.info(f"Etapa 1 concluída com sucesso. Arquivo: {arquivo_final}")
            
            return arquivo_final
        
        except Exception as e:
            logger.error(f"ERRO CRÍTICO na Etapa 1: {e}")
            print(f"\n❌ ERRO: {e}\n")
            raise
    
    def _consolidar_dados(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Consolida múltiplos DataFrames em um único dataset.
        
        
        dataframes: Lista de DataFrames para consolidar
        
        Returns:
            DataFrame consolidado
        """
        logger.info(f"Consolidando {len(dataframes)} DataFrames...")
        
        # Concatenar todos os DataFrames
        df_consolidado = pd.concat(dataframes, ignore_index=True)
        
        print(f"  Total de registros: {len(df_consolidado):,}")
        
        # Garantir que todas as colunas padrão existam
        for col in COLUNAS_PADRAO_DESPESAS:
            if col not in df_consolidado.columns:
                df_consolidado[col] = None
                logger.warning(f"  Coluna {col} não encontrada, adicionada com valores nulos")
        
        # Ordenar colunas na ordem padrão
        colunas_ordenadas = COLUNAS_PADRAO_DESPESAS + [
            col for col in df_consolidado.columns 
            if col not in COLUNAS_PADRAO_DESPESAS
        ]
        df_consolidado = df_consolidado[colunas_ordenadas]
        
        # Remover duplicatas completas
        tamanho_original = len(df_consolidado)
        df_consolidado = df_consolidado.drop_duplicates()
        duplicatas_removidas = tamanho_original - len(df_consolidado)
        
        if duplicatas_removidas > 0:
            print(f"  ⚠️  {duplicatas_removidas} duplicatas removidas")
            logger.info(f"  Duplicatas removidas: {duplicatas_removidas}")
        
        # Ordenar por Ano, Trimestre, CNPJ
        df_consolidado = df_consolidado.sort_values(
            ['Ano', 'Trimestre', 'CNPJ'],
            ascending=[False, False, True]
        ).reset_index(drop=True)
        
        print(f"  ✓ Dados consolidados: {len(df_consolidado):,} registros")
        logger.info(f"✓ Consolidação concluída: {len(df_consolidado)} registros")
        
        return df_consolidado
    
    def _gerar_arquivo_final(self, df: pd.DataFrame) -> Path:
        """
        Gera o arquivo CSV final e o compacta em ZIP.
        """
        # Gerar CSV
        arquivo_csv = self.diretorio_saida / "consolidado_despesas.csv"
        
        print(f"  Gerando CSV: {arquivo_csv.name}")
        df[COLUNAS_PADRAO_DESPESAS].to_csv(
            arquivo_csv,
            index=False,
            encoding='utf-8',
            sep=';'
        )
        
        tamanho_csv = arquivo_csv.stat().st_size
        print(f"  ✓ CSV gerado: {bytes_para_humano(tamanho_csv)}")
        
        # Compactar em ZIP
        arquivo_zip = self.diretorio_saida / "consolidado_despesas.zip"
        
        print(f"  Compactando em ZIP: {arquivo_zip.name}")
        with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(arquivo_csv, arquivo_csv.name)
        
        tamanho_zip = arquivo_zip.stat().st_size
        compressao = (1 - tamanho_zip / tamanho_csv) * 100
        
        print(f"  ✓ ZIP gerado: {bytes_para_humano(tamanho_zip)}")
        print(f"  ✓ Compressão: {compressao:.1f}%")
        
        logger.info(f"Arquivo final gerado: {arquivo_zip}")
        
        return arquivo_zip

    def _gerar_relatorio_inconsistencias(self, df: pd.DataFrame) -> None:
        """
        Gera relatório CSV apenas com registros inconsistentes.
        """
        if 'inconsistencia_flag' not in df.columns:
            logger.warning("Coluna inconsistência não encontrada para relatório dedicado")
            return

        df_inconsistencias = df[df['inconsistencia_flag'] != ''].copy()
        if df_inconsistencias.empty:
            logger.info("Nenhuma inconsistência encontrada para relatório dedicado")
            return

        arquivo_csv = self.diretorio_saida / "inconsistencias_despesas.csv"
        colunas_relatorio = COLUNAS_PADRAO_DESPESAS + ['inconsistencia_flag']
        colunas_relatorio = [col for col in colunas_relatorio if col in df_inconsistencias.columns]

        df_inconsistencias.to_csv(
            arquivo_csv,
            index=False,
            encoding='utf-8',
            sep=';',
            columns=colunas_relatorio,
        )

        logger.info(f"Relatório de inconsistências salvo em: {arquivo_csv}")
    
    def _gerar_relatorio(
        self,
        df: pd.DataFrame,
        resultados_download: List[Tuple],
        tempo_total: str
    ) -> None:
        """
        Gera relatório detalhado da execução.
        
        Args:
            df: DataFrame consolidado
            resultados_download: Resultados do download
            tempo_total: Tempo total de execução
        """
        print("\n📊 Estatísticas dos Dados Consolidados:")
        print("-" * 70)
        
        # Estatísticas gerais
        print(f"  Total de registros: {len(df):,}")
        trimestres_df = df[["Ano", "Trimestre"]].dropna()
        trimestres_series = (
            trimestres_df["Ano"].astype(int).astype(str)
            + "/Q"
            + trimestres_df["Trimestre"].astype(int).astype(str)
        )
        trimestres_unicos = sorted(trimestres_series.unique(), reverse=True)
        trimestres_texto = ", ".join(trimestres_unicos) if len(trimestres_unicos) > 0 else "N/A"
        print(f"  Trimestres processados: {trimestres_texto}")
        print(f"  Operadoras únicas: {df['CNPJ'].nunique():,}")
        
        # Estatísticas de valores
        if 'ValorDespesas' in df.columns:
            valores_validos = df['ValorDespesas'].dropna()
            print(f"\n  Valores de Despesas:")
            print(f"    Total: R$ {valores_validos.sum():,.2f}")
            print(f"    Média: R$ {valores_validos.mean():,.2f}")
            print(f"    Mediana: R$ {valores_validos.median():,.2f}")
            print(f"    Mínimo: R$ {valores_validos.min():,.2f}")
            print(f"    Máximo: R$ {valores_validos.max():,.2f}")
        
        # Inconsistências
        if 'inconsistencia_flag' in df.columns:
            registros_com_problema = (df['inconsistencia_flag'] != '').sum()
            percentual_problema = (registros_com_problema / len(df)) * 100
            
            print(f"\n  ⚠️  Inconsistências Encontradas:")
            print(f"    Total: {registros_com_problema:,} ({percentual_problema:.1f}%)")
            
            # Detalhar tipos de inconsistências
            flags_series = df[df['inconsistencia_flag'] != '']['inconsistencia_flag']
            tipos_inconsistencias = {}
            
            for flags in flags_series:
                for flag in flags.split(';'):
                    if flag:
                        tipos_inconsistencias[flag] = tipos_inconsistencias.get(flag, 0) + 1
            
            if tipos_inconsistencias:
                print(f"\n    Tipos:")
                for tipo, quantidade in sorted(tipos_inconsistencias.items(), key=lambda x: x[1], reverse=True):
                    print(f"      - {tipo}: {quantidade:,}")
        
        # Distribuição por trimestre
        print(f"\n  Distribuição por Trimestre:")
        dist_trimestre = df.groupby(['Ano', 'Trimestre']).size().sort_index(ascending=False)
        for (ano, trim), qtd in dist_trimestre.items():
            print(f"    {ano}/Q{trim}: {qtd:,} registros")
        
        # Salvar relatório em arquivo
        arquivo_relatorio = self.diretorio_saida / "relatorio_etapa1.txt"
        
        with open(arquivo_relatorio, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("RELATÓRIO DE EXECUÇÃO - ETAPA 1\n")
            f.write("="*70 + "\n\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"Tempo de Execução: {tempo_total}\n\n")
            f.write(f"Total de Registros: {len(df):,}\n")
            f.write(f"Operadoras Únicas: {df['CNPJ'].nunique():,}\n")
            f.write(f"Registros com Inconsistências: {registros_com_problema:,}\n")
            f.write("\n")
            f.write("="*70 + "\n")
        
        print(f"\n  📄 Relatório detalhado salvo em: {arquivo_relatorio.name}")


def main():
    """Função principal."""
    try:
        # Criar e executar integrador
        integrador = IntegradorANS()
        arquivo_gerado = integrador.executar(quantidade_trimestres=3)
        
        return 0  # Sucesso
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Execução interrompida pelo usuário.\n")
        logger.warning("Execução interrompida pelo usuário")
        return 1
    
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}\n")
        logger.error(f"Erro fatal: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())