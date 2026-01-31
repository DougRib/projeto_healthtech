"""
ETAPA 2.3: Agregação e Análise Estatística
===========================================

Este módulo realiza agregações estatísticas dos dados enriquecidos:

1. Agregação por RazaoSocial e UF
2. Cálculo de métricas:
   - Total de despesas
   - Média por trimestre
   - Desvio padrão
   - Contagem de trimestres
3. Ordenação por valor total
4. Geração do arquivo final

Decisão Técnica: Pandas GroupBy + Sort
- QuickSort (padrão Pandas): O(n log n)
- Eficiente para ~1.500 operadoras
- In-place quando possível

Autor: [Seu Nome]
Data: 29/01/2025

Uso:
    python agregacao.py
"""

import pandas as pd
import numpy as np
import sys
import zipfile
from pathlib import Path
from datetime import datetime
import logging

PROJETO_RAIZ = Path(__file__).resolve().parents[1]
if str(PROJETO_RAIZ) not in sys.path:
    sys.path.insert(0, str(PROJETO_RAIZ))

from integracao_api.utils import configurar_logging, bytes_para_humano


# Configuração de logging
logger = configurar_logging("agregacao.log")


class AgregadorDados:
    """
    Classe responsável por agregar e analisar dados enriquecidos.
    
    Funcionalidades:
    - Agregação por RazaoSocial e UF
    - Cálculos estatísticos (soma, média, desvio, contagem)
    - Ordenação por valor total
    - Análise de variabilidade
    """
    
    def __init__(self, arquivo_entrada: Path):
        """
        Inicializa o agregador.
        
        Args:
            arquivo_entrada: Path do CSV com dados enriquecidos
        """
        self.arquivo_entrada = arquivo_entrada
        self.df_enriquecido = None
        self.df_agregado = None
        
        logger.info("="*70)
        logger.info("INICIANDO ETAPA 2.3: AGREGAÇÃO E ESTATÍSTICAS")
        logger.info("="*70)
    
    def carregar_dados(self) -> None:
        """Carrega dados enriquecidos."""
        print(f"📥 Carregando dados enriquecidos...")
        logger.info(f"Carregando: {self.arquivo_entrada}")
        
        try:
            self.df_enriquecido = pd.read_csv(
                self.arquivo_entrada,
                sep=';',
                encoding='utf-8',
                low_memory=False
            )
            print(f"  ✓ {len(self.df_enriquecido):,} registros carregados")
            logger.info(f"Dados carregados: {len(self.df_enriquecido)} registros")
        
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            raise
    
    def preparar_dados(self) -> None:
        """
        Prepara dados para agregação.
        
        - Remove registros com valores inválidos
        - Garante tipos de dados corretos
        - Trata valores nulos em campos chave
        """
        print(f"\n⚙️  Preparando dados para agregação...")
        logger.info("Iniciando preparação de dados")
        
        tamanho_original = len(self.df_enriquecido)
        
        # Converter ValorDespesas para numérico
        self.df_enriquecido['ValorDespesas'] = pd.to_numeric(
            self.df_enriquecido['ValorDespesas'],
            errors='coerce'
        )
        
        # Remover registros com valor nulo ou negativo
        self.df_enriquecido = self.df_enriquecido[
            (self.df_enriquecido['ValorDespesas'].notna()) &
            (self.df_enriquecido['ValorDespesas'] >= 0)
        ].copy()
        
        registros_removidos = tamanho_original - len(self.df_enriquecido)
        
        if registros_removidos > 0:
            print(f"  ⚠️  {registros_removidos:,} registros removidos (valores inválidos)")
            logger.warning(f"Registros removidos: {registros_removidos}")
        
        # Tratar UF nulo
        if 'UF' in self.df_enriquecido.columns:
            uf_nulos = self.df_enriquecido['UF'].isna().sum()
            if uf_nulos > 0:
                self.df_enriquecido['UF'] = self.df_enriquecido['UF'].fillna('NÃO_INFORMADO')
                print(f"  ⚠️  {uf_nulos:,} UFs nulas preenchidas com 'NÃO_INFORMADO'")
        
        # Tratar RazaoSocial nulo
        if 'RazaoSocial' in self.df_enriquecido.columns:
            razao_nulos = self.df_enriquecido['RazaoSocial'].isna().sum()
            if razao_nulos > 0:
                self.df_enriquecido['RazaoSocial'] = self.df_enriquecido['RazaoSocial'].fillna('NÃO_INFORMADO')
                print(f"  ⚠️  {razao_nulos:,} Razões Sociais nulas preenchidas")
        
        print(f"  ✓ Dados preparados: {len(self.df_enriquecido):,} registros válidos")
        logger.info(f"Preparação concluída: {len(self.df_enriquecido)} registros válidos")
    
    def agregar_dados(self) -> None:
        """
        Realiza agregação por RazaoSocial e UF.
        
        Métricas calculadas:
        - TotalDespesas: Soma de ValorDespesas
        - MediaDespesas: Média de ValorDespesas
        - DesvioPadrao: Desvio padrão de ValorDespesas
        - NumeroTrimestres: Contagem de registros
        - MediaPorTrimestre: Total / Número de trimestres
        """
        print(f"\n📊 Agregando dados...")
        logger.info("Iniciando agregação")
        
        # Definir campos de agrupamento
        campos_agrupamento = ['RazaoSocial', 'UF']
        
        # Verificar se campos existem
        for campo in campos_agrupamento:
            if campo not in self.df_enriquecido.columns:
                raise ValueError(f"Campo {campo} não encontrado nos dados!")
        
        # Agregar primeiro por trimestre para evitar vieses por quantidade de linhas
        print(f"  Agrupando por: {', '.join(campos_agrupamento)} e trimestre")
        df_trimestres = (
            self.df_enriquecido
            .groupby(['RazaoSocial', 'UF', 'Ano', 'Trimestre'])['ValorDespesas']
            .sum()
            .reset_index()
        )

        self.df_agregado = df_trimestres.groupby(campos_agrupamento).agg(
            TotalDespesas=('ValorDespesas', 'sum'),
            MediaDespesas=('ValorDespesas', 'mean'),
            DesvioPadrao=('ValorDespesas', 'std'),
            NumeroTrimestres=('ValorDespesas', 'count'),
        ).reset_index()

        # Média por trimestre (equivalente à média de trimestres agregados)
        self.df_agregado['MediaPorTrimestre'] = self.df_agregado['MediaDespesas']
        
        # Preencher desvio padrão nulo (quando só há 1 registro) com 0
        self.df_agregado['DesvioPadrao'] = self.df_agregado['DesvioPadrao'].fillna(0)
        
        # Adicionar flag de alta variabilidade
        # Consideramos alta variabilidade quando CV > 50%
        self.df_agregado['CoeficienteVariacao'] = (
            self.df_agregado['DesvioPadrao'] / self.df_agregado['MediaDespesas']
        ) * 100
        
        self.df_agregado['AltaVariabilidade'] = (
            self.df_agregado['CoeficienteVariacao'] > 50
        )
        
        print(f"  ✓ Agregação concluída: {len(self.df_agregado):,} grupos")
        logger.info(f"Agregação concluída: {len(self.df_agregado)} grupos")
    
    def ordenar_dados(self) -> None:
        """
        Ordena dados por TotalDespesas (maior para menor).
        
        Decisão Técnica: QuickSort (padrão Pandas)
        - Complexidade: O(n log n)
        - Performance adequada para ~1.500 registros
        - In-place quando possível
        """
        print(f"\n🔃 Ordenando dados...")
        logger.info("Ordenando por TotalDespesas")
        
        self.df_agregado = self.df_agregado.sort_values(
            'TotalDespesas',
            ascending=False
        ).reset_index(drop=True)
        
        # Adicionar ranking
        self.df_agregado['Ranking'] = range(1, len(self.df_agregado) + 1)
        
        # Reordenar colunas
        colunas_ordenadas = [
            'Ranking',
            'RazaoSocial',
            'UF',
            'TotalDespesas',
            'MediaDespesas',
            'MediaPorTrimestre',
            'DesvioPadrao',
            'CoeficienteVariacao',
            'NumeroTrimestres',
            'AltaVariabilidade'
        ]
        
        self.df_agregado = self.df_agregado[colunas_ordenadas]
        
        print(f"  ✓ Dados ordenados por TotalDespesas")
        logger.info("Ordenação concluída")
    
    def gerar_analise_estatistica(self) -> dict:
        """
        Gera análise estatística detalhada dos dados agregados.
        
        Returns:
            Dicionário com estatísticas
        """
        print(f"\n📈 Gerando análise estatística...")
        
        estatisticas = {
            'total_grupos': len(self.df_agregado),
            'soma_total_despesas': self.df_agregado['TotalDespesas'].sum(),
            'media_total_despesas': self.df_agregado['TotalDespesas'].mean(),
            'mediana_total_despesas': self.df_agregado['TotalDespesas'].median(),
            'min_total_despesas': self.df_agregado['TotalDespesas'].min(),
            'max_total_despesas': self.df_agregado['TotalDespesas'].max(),
            'desvio_total_despesas': self.df_agregado['TotalDespesas'].std(),
            'grupos_alta_variabilidade': self.df_agregado['AltaVariabilidade'].sum(),
            'ufs_unicas': self.df_agregado['UF'].nunique()
        }
        
        # Top 10 operadoras
        estatisticas['top_10'] = self.df_agregado.head(10)[
            ['Ranking', 'RazaoSocial', 'UF', 'TotalDespesas']
        ].to_dict('records')
        
        # Top 5 UFs
        top_ufs = self.df_agregado.groupby('UF')['TotalDespesas'].sum().sort_values(ascending=False).head(5)
        estatisticas['top_5_ufs'] = top_ufs.to_dict()
        
        # Distribuição de variabilidade
        estatisticas['distribuicao_variabilidade'] = {
            'baixa': (self.df_agregado['CoeficienteVariacao'] < 25).sum(),
            'media': ((self.df_agregado['CoeficienteVariacao'] >= 25) & 
                      (self.df_agregado['CoeficienteVariacao'] <= 50)).sum(),
            'alta': (self.df_agregado['CoeficienteVariacao'] > 50).sum()
        }
        
        print(f"  ✓ Análise estatística concluída")
        logger.info("Análise estatística gerada")
        
        return estatisticas
    
    def exibir_estatisticas(self, estatisticas: dict) -> None:
        """
        Exibe estatísticas no console.
        
        Args:
            estatisticas: Dicionário com estatísticas
        """
        print(f"\n" + "="*70)
        print("ESTATÍSTICAS DOS DADOS AGREGADOS")
        print("="*70)
        
        print(f"\n📊 Métricas Gerais:")
        print(f"  Total de grupos (Operadora/UF): {estatisticas['total_grupos']:,}")
        print(f"  UFs únicas: {estatisticas['ufs_unicas']}")
        print(f"  Soma total de despesas: R$ {estatisticas['soma_total_despesas']:,.2f}")
        
        print(f"\n💰 Estatísticas de Despesas:")
        print(f"  Média: R$ {estatisticas['media_total_despesas']:,.2f}")
        print(f"  Mediana: R$ {estatisticas['mediana_total_despesas']:,.2f}")
        print(f"  Mínimo: R$ {estatisticas['min_total_despesas']:,.2f}")
        print(f"  Máximo: R$ {estatisticas['max_total_despesas']:,.2f}")
        print(f"  Desvio Padrão: R$ {estatisticas['desvio_total_despesas']:,.2f}")
        
        print(f"\n🏆 Top 10 Operadoras (Maior Total de Despesas):")
        for item in estatisticas['top_10']:
            print(f"  {item['Ranking']}º. {item['RazaoSocial'][:40]:<40} ({item['UF']}) - R$ {item['TotalDespesas']:,.2f}")
        
        print(f"\n🗺️  Top 5 UFs (Maior Total de Despesas):")
        for i, (uf, total) in enumerate(estatisticas['top_5_ufs'].items(), 1):
            print(f"  {i}º. {uf} - R$ {total:,.2f}")
        
        print(f"\n📉 Variabilidade de Despesas:")
        print(f"  Baixa variabilidade (CV < 25%): {estatisticas['distribuicao_variabilidade']['baixa']:,}")
        print(f"  Média variabilidade (25% ≤ CV ≤ 50%): {estatisticas['distribuicao_variabilidade']['media']:,}")
        print(f"  Alta variabilidade (CV > 50%): {estatisticas['distribuicao_variabilidade']['alta']:,}")
    
    def gerar_relatorio(self, arquivo_saida: Path, estatisticas: dict) -> None:
        """
        Gera relatório detalhado em arquivo texto.
        
        Args:
            arquivo_saida: Path do arquivo de relatório
            estatisticas: Dicionário com estatísticas
        """
        print(f"\n📄 Gerando relatório...")
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("RELATÓRIO DE AGREGAÇÃO E ESTATÍSTICAS - ETAPA 2.3\n")
            f.write("="*70 + "\n\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
            
            f.write("ESTATÍSTICAS GERAIS:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Total de grupos (Operadora/UF): {estatisticas['total_grupos']:,}\n")
            f.write(f"UFs únicas: {estatisticas['ufs_unicas']}\n")
            f.write(f"Soma total de despesas: R$ {estatisticas['soma_total_despesas']:,.2f}\n\n")
            
            f.write("MÉTRICAS DE DESPESAS:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Média: R$ {estatisticas['media_total_despesas']:,.2f}\n")
            f.write(f"Mediana: R$ {estatisticas['mediana_total_despesas']:,.2f}\n")
            f.write(f"Mínimo: R$ {estatisticas['min_total_despesas']:,.2f}\n")
            f.write(f"Máximo: R$ {estatisticas['max_total_despesas']:,.2f}\n")
            f.write(f"Desvio Padrão: R$ {estatisticas['desvio_total_despesas']:,.2f}\n\n")
            
            f.write("TOP 10 OPERADORAS:\n")
            f.write("-" * 70 + "\n")
            for item in estatisticas['top_10']:
                f.write(f"{item['Ranking']}º. {item['RazaoSocial']} ({item['UF']}) - R$ {item['TotalDespesas']:,.2f}\n")
            f.write("\n")
            
            f.write("TOP 5 UFS:\n")
            f.write("-" * 70 + "\n")
            for i, (uf, total) in enumerate(estatisticas['top_5_ufs'].items(), 1):
                f.write(f"{i}º. {uf} - R$ {total:,.2f}\n")
            f.write("\n")
            
            f.write("="*70 + "\n")
        
        print(f"  ✓ Relatório salvo em: {arquivo_saida}")
        logger.info(f"Relatório gerado: {arquivo_saida}")
    
    def salvar_dados_agregados(self, arquivo_saida: Path) -> None:
        """
        Salva dados agregados em CSV.
        
        Args:
            arquivo_saida: Path do arquivo de saída
        """
        print(f"\n💾 Salvando dados agregados...")
        
        self.df_agregado.to_csv(
            arquivo_saida,
            index=False,
            encoding='utf-8',
            sep=';',
            float_format='%.2f'  # 2 casas decimais
        )
        
        tamanho = arquivo_saida.stat().st_size
        print(f"  ✓ Arquivo salvo: {arquivo_saida.name} ({bytes_para_humano(tamanho)})")
        logger.info(f"Dados agregados salvos: {arquivo_saida}")
    
    def compactar_arquivo_final(self, arquivo_csv: Path, arquivo_zip: Path) -> None:
        """
        Compacta CSV final em ZIP.
        
        Args:
            arquivo_csv: Path do CSV para compactar
            arquivo_zip: Path do ZIP de saída
        """
        print(f"\n🗜️  Compactando arquivo final...")
        
        with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(arquivo_csv, arquivo_csv.name)
        
        tamanho_csv = arquivo_csv.stat().st_size
        tamanho_zip = arquivo_zip.stat().st_size
        compressao = (1 - tamanho_zip / tamanho_csv) * 100
        
        print(f"  ✓ ZIP gerado: {arquivo_zip.name}")
        print(f"  ✓ Tamanho original: {bytes_para_humano(tamanho_csv)}")
        print(f"  ✓ Tamanho compactado: {bytes_para_humano(tamanho_zip)}")
        print(f"  ✓ Compressão: {compressao:.1f}%")
        
        logger.info(f"Arquivo compactado: {arquivo_zip}")


def main():
    """Função principal."""
    print("="*70)
    print("ETAPA 2.3: AGREGAÇÃO E ESTATÍSTICAS")
    print("="*70)
    
    try:
        # Caminhos
        saida_dir = PROJETO_RAIZ / "output"
        arquivo_entrada = saida_dir / "dados_enriquecidos.csv"
        arquivo_saida_csv = saida_dir / "despesas_agregadas.csv"
        arquivo_saida_zip = saida_dir / "Teste_Douglas_Ribeiro.zip"
        arquivo_relatorio = saida_dir / "relatorio_agregacao.txt"
        
        # Criar diretório de saída
        arquivo_saida_csv.parent.mkdir(parents=True, exist_ok=True)
        
        # Criar agregador
        agregador = AgregadorDados(arquivo_entrada)
        
        # Executar agregação
        agregador.carregar_dados()
        agregador.preparar_dados()
        agregador.agregar_dados()
        agregador.ordenar_dados()
        
        # Análise estatística
        estatisticas = agregador.gerar_analise_estatistica()
        agregador.exibir_estatisticas(estatisticas)
        
        # Gerar relatório
        agregador.gerar_relatorio(arquivo_relatorio, estatisticas)
        
        # Salvar dados
        agregador.salvar_dados_agregados(arquivo_saida_csv)
        
        # Compactar arquivo final
        agregador.compactar_arquivo_final(arquivo_saida_csv, arquivo_saida_zip)
        
        print("\n" + "="*70)
        print("✅ ETAPA 2.3 CONCLUÍDA COM SUCESSO!")
        print("="*70)
        print(f"📁 Arquivo final: {arquivo_saida_zip}")
        print("="*70 + "\n")
        
        return 0
    
    except Exception as e:
        print(f"\n❌ ERRO: {e}\n")
        logger.error(f"Erro na agregação: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())