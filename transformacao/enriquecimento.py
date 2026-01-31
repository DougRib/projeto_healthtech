"""
ETAPA 2.2: Enriquecimento de Dados
===================================

Este módulo enriquece os dados consolidados com informações cadastrais:

1. Download de dados cadastrais das operadoras ativas (ANS)
2. Join com dados consolidados usando CNPJ
3. Adição de colunas: RegistroANS, Modalidade, UF
4. Tratamento de registros sem match

Decisão Técnica: Left Join
- Mantém todos os registros de despesas
- Não perde dados mesmo sem cadastro
- Identifica CNPJs sem match para análise

Trade-off: Pandas para processamento
- Volume < 1GB: Pandas eficiente o suficiente
- Join em memória: rápido e simples
- Alternativa (Dask) seria over-engineering

Autor: [Seu Nome]
Data: 29/01/2025

Uso:
    python enriquecimento.py
"""

import pandas as pd
import requests
import sys
from pathlib import Path
from datetime import datetime
import logging
import re

PROJETO_RAIZ = Path(__file__).resolve().parents[1]
if str(PROJETO_RAIZ) not in sys.path:
    sys.path.insert(0, str(PROJETO_RAIZ))

from integracao_api.utils import (
    configurar_logging,
    limpar_cnpj,
    URL_ANS_OPERADORAS,
    bytes_para_humano
)


# Configuração de logging
logger = configurar_logging("enriquecimento.log")


class EnriquecedorDados:
    """
    Classe responsável por enriquecer dados com informações cadastrais.
    
    Funcionalidades:
    - Download de dados cadastrais da ANS
    - Limpeza e normalização dos dados cadastrais
    - Join (Left) entre despesas e cadastro
    - Análise de cobertura do match
    """
    
    def __init__(self, arquivo_entrada: Path):
        """
        Inicializa o enriquecedor.
        
        Args:
            arquivo_entrada: Path do CSV com dados validados
        """
        self.arquivo_entrada = arquivo_entrada
        self.df_despesas = None
        self.df_cadastro = None
        self.df_enriquecido = None
        
        logger.info("="*70)
        logger.info("INICIANDO ETAPA 2.2: ENRIQUECIMENTO DE DADOS")
        logger.info("="*70)
    
    def carregar_despesas(self) -> None:
        """Carrega dados de despesas validados."""
        print(f"📥 Carregando dados de despesas...")
        logger.info(f"Carregando: {self.arquivo_entrada}")
        
        try:
            self.df_despesas = pd.read_csv(
                self.arquivo_entrada,
                sep=';',
                encoding='utf-8',
                low_memory=False
            )
            print(f"  ✓ {len(self.df_despesas):,} registros carregados")
            logger.info(f"Despesas carregadas: {len(self.df_despesas)} registros")
        
        except Exception as e:
            logger.error(f"Erro ao carregar despesas: {e}")
            raise
    
    def baixar_dados_cadastrais(self) -> Path:
        """
        Baixa arquivo CSV de dados cadastrais da ANS.
        
        Returns:
            Path do arquivo baixado
        
        Nota:
            A URL exata pode variar. Este código busca o arquivo mais recente.
        """
        print(f"\n📥 Baixando dados cadastrais da ANS...")
        logger.info("Iniciando download de dados cadastrais")
        
        try:
            # URL base do FTP da ANS
            url_base = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
            
            # Fazer requisição para listar arquivos
            resposta = requests.get(url_base, timeout=30)
            resposta.raise_for_status()
            
            # Procurar arquivo CSV mais recente
            padrao = r'href="(Relatorio_cadop[^"]*\.csv)"'
            arquivos = re.findall(padrao, resposta.text, re.IGNORECASE)
            
            if not arquivos:
                raise Exception("Nenhum arquivo cadastral encontrado na ANS")
            
            # Usar o arquivo mais recente (geralmente o último da lista)
            nome_arquivo = arquivos[-1]
            url_arquivo = url_base + nome_arquivo
            
            print(f"  Arquivo encontrado: {nome_arquivo}")
            logger.info(f"Arquivo cadastral: {nome_arquivo}")
            
            # Baixar arquivo
            print(f"  Baixando...")
            resposta_arquivo = requests.get(url_arquivo, stream=True, timeout=300)
            resposta_arquivo.raise_for_status()
            
            # Salvar localmente
            caminho_local = PROJETO_RAIZ / "data" / "cadastro_operadoras.csv"
            caminho_local.parent.mkdir(parents=True, exist_ok=True)
            
            tamanho_total = int(resposta_arquivo.headers.get('content-length', 0))
            
            with open(caminho_local, 'wb') as f:
                tamanho_baixado = 0
                for chunk in resposta_arquivo.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        tamanho_baixado += len(chunk)
                        
                        if tamanho_total > 0:
                            percentual = (tamanho_baixado / tamanho_total) * 100
                            barra = '█' * int(percentual // 2) + '░' * (50 - int(percentual // 2))
                            print(f"\r  |{barra}| {percentual:.1f}%", end='')
            
            print()  # Nova linha
            print(f"  ✓ Download concluído: {bytes_para_humano(tamanho_baixado)}")
            logger.info(f"Arquivo cadastral baixado: {caminho_local}")
            
            return caminho_local
        
        except Exception as e:
            logger.error(f"Erro ao baixar dados cadastrais: {e}")
            raise
    
    def carregar_dados_cadastrais(self, caminho: Path) -> None:
        """
        Carrega e processa dados cadastrais.
        
        Args:
            caminho: Path do arquivo CSV cadastral
        """
        print(f"\n📋 Processando dados cadastrais...")
        logger.info(f"Carregando cadastro de: {caminho}")
        
        try:
            # Tentar diferentes encodings
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                try:
                    self.df_cadastro = pd.read_csv(
                        caminho,
                        sep=';',
                        encoding=encoding,
                        low_memory=False
                    )
                    break
                except UnicodeDecodeError:
                    continue
            
            if self.df_cadastro is None:
                raise Exception("Não foi possível ler o arquivo cadastral")
            
            print(f"  ✓ {len(self.df_cadastro):,} operadoras no cadastro")
            logger.info(f"Cadastro carregado: {len(self.df_cadastro)} operadoras")
            
            # Normalizar nomes de colunas
            self.df_cadastro.columns = self.df_cadastro.columns.str.strip().str.lower()
            
            # Identificar colunas necessárias (nomes podem variar)
            mapeamento_colunas = {
                'cnpj': 'CNPJ',
                'cd_cnpj': 'CNPJ',
                'registro_ans': 'RegistroANS',
                'cd_registro_ans': 'RegistroANS',
                'registro_operadora': 'RegistroANS',
                'registro_operadora_ans': 'RegistroANS',
                'razao_social': 'RazaoSocialCadastro',
                'nm_razao_social': 'RazaoSocialCadastro',
                'modalidade': 'Modalidade',
                'ds_modalidade': 'Modalidade',
                'sg_modalidade': 'Modalidade',
                'uf': 'UF',
                'sg_uf': 'UF'
            }
            
            self.df_cadastro = self.df_cadastro.rename(columns=mapeamento_colunas)
            
            # Selecionar colunas necessárias
            colunas_necessarias = ['CNPJ', 'RegistroANS', 'Modalidade', 'UF']
            colunas_disponiveis = [col for col in colunas_necessarias if col in self.df_cadastro.columns]
            
            if 'CNPJ' not in colunas_disponiveis:
                raise Exception("Coluna CNPJ não encontrada no cadastro!")
            
            self.df_cadastro = self.df_cadastro[colunas_disponiveis].copy()
            
            # Limpar CNPJs
            self.df_cadastro['CNPJ'] = self.df_cadastro['CNPJ'].astype(str).apply(limpar_cnpj)
            
            # Remover registros com CNPJ inválido
            self.df_cadastro = self.df_cadastro[
                self.df_cadastro['CNPJ'].str.len() == 14
            ].copy()
            
            # Remover duplicatas de CNPJ (manter primeiro registro)
            duplicados_antes = len(self.df_cadastro)
            self.df_cadastro = self.df_cadastro.drop_duplicates(subset='CNPJ', keep='first')
            duplicados_removidos = duplicados_antes - len(self.df_cadastro)
            
            if duplicados_removidos > 0:
                print(f"  ⚠️  {duplicados_removidos} CNPJs duplicados removidos")
                logger.warning(f"CNPJs duplicados removidos: {duplicados_removidos}")
            
            print(f"  ✓ {len(self.df_cadastro):,} operadoras únicas após limpeza")
            print(f"  ✓ Colunas disponíveis: {', '.join(colunas_disponiveis)}")
            
        except Exception as e:
            logger.error(f"Erro ao processar cadastro: {e}")
            raise
    
    def realizar_join(self) -> None:
        """
        Realiza LEFT JOIN entre despesas e cadastro.
        
        Decisão Técnica: Left Join
        - Mantém todos os registros de despesas
        - Adiciona informações cadastrais quando disponíveis
        - Permite identificar CNPJs sem cadastro
        """
        print(f"\n🔗 Realizando join dos dados...")
        logger.info("Iniciando join LEFT entre despesas e cadastro")
        
        # Garantir que CNPJ está limpo em ambos
        self.df_despesas['CNPJ'] = self.df_despesas['CNPJ'].astype(str).apply(limpar_cnpj)
        
        # Realizar Left Join
        self.df_enriquecido = pd.merge(
            self.df_despesas,
            self.df_cadastro,
            on='CNPJ',
            how='left',
            indicator=True  # Adiciona coluna _merge para análise
        )
        
        # Renomear coluna _merge para mais clareza
        self.df_enriquecido = self.df_enriquecido.rename(
            columns={'_merge': 'status_match'}
        )
        
        # Traduzir status
        self.df_enriquecido['status_match'] = self.df_enriquecido['status_match'].map({
            'both': 'MATCH_CADASTRO',
            'left_only': 'SEM_CADASTRO',
            'right_only': 'APENAS_CADASTRO'  # Não deve ocorrer em Left Join
        })
        
        # Estatísticas do join
        total = len(self.df_enriquecido)
        com_match = (self.df_enriquecido['status_match'] == 'MATCH_CADASTRO').sum()
        sem_match = (self.df_enriquecido['status_match'] == 'SEM_CADASTRO').sum()
        
        percentual_match = (com_match / total) * 100
        
        print(f"  ✓ Join concluído:")
        print(f"    Total de registros: {total:,}")
        print(f"    Com match no cadastro: {com_match:,} ({percentual_match:.1f}%)")
        print(f"    Sem match no cadastro: {sem_match:,} ({100-percentual_match:.1f}%)")
        
        logger.info(f"Join concluído: {com_match} matches, {sem_match} sem match")
        
        # Listar CNPJs sem match (top 10)
        if sem_match > 0:
            print(f"\n  ⚠️  Top 10 CNPJs sem cadastro:")
            cnpjs_sem_match = self.df_enriquecido[
                self.df_enriquecido['status_match'] == 'SEM_CADASTRO'
            ]['CNPJ'].value_counts().head(10)
            
            for cnpj, qtd in cnpjs_sem_match.items():
                print(f"    - {cnpj}: {qtd} registro(s)")
    
    def gerar_relatorio_enriquecimento(self, arquivo_saida: Path) -> None:
        """
        Gera relatório detalhado do enriquecimento.
        
        Args:
            arquivo_saida: Path do arquivo de relatório
        """
        print(f"\n📊 Gerando relatório de enriquecimento...")
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("RELATÓRIO DE ENRIQUECIMENTO - ETAPA 2.2\n")
            f.write("="*70 + "\n\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
            
            # Estatísticas gerais
            f.write("ESTATÍSTICAS GERAIS:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Total de registros: {len(self.df_enriquecido):,}\n")
            f.write(f"Operadoras únicas: {self.df_enriquecido['CNPJ'].nunique():,}\n\n")
            
            # Match de cadastro
            f.write("MATCH COM CADASTRO ANS:\n")
            f.write("-" * 70 + "\n")
            matches = self.df_enriquecido['status_match'].value_counts()
            for status, qtd in matches.items():
                perc = (qtd / len(self.df_enriquecido)) * 100
                f.write(f"{status}: {qtd:,} ({perc:.1f}%)\n")
            f.write("\n")
            
            # Distribuição por UF
            if 'UF' in self.df_enriquecido.columns:
                f.write("DISTRIBUIÇÃO POR UF:\n")
                f.write("-" * 70 + "\n")
                dist_uf = self.df_enriquecido['UF'].value_counts().head(10)
                for uf, qtd in dist_uf.items():
                    f.write(f"{uf}: {qtd:,}\n")
                f.write("\n")
            
            # Distribuição por Modalidade
            if 'Modalidade' in self.df_enriquecido.columns:
                f.write("DISTRIBUIÇÃO POR MODALIDADE:\n")
                f.write("-" * 70 + "\n")
                dist_mod = self.df_enriquecido['Modalidade'].value_counts()
                for mod, qtd in dist_mod.items():
                    f.write(f"{mod}: {qtd:,}\n")
                f.write("\n")
            
            f.write("="*70 + "\n")
        
        print(f"  ✓ Relatório salvo em: {arquivo_saida}")
        logger.info(f"Relatório de enriquecimento gerado: {arquivo_saida}")
    
    def salvar_dados_enriquecidos(self, arquivo_saida: Path) -> None:
        """
        Salva dados enriquecidos em CSV.
        
        Args:
            arquivo_saida: Path do arquivo de saída
        """
        print(f"\n💾 Salvando dados enriquecidos...")
        
        self.df_enriquecido.to_csv(
            arquivo_saida,
            index=False,
            encoding='utf-8',
            sep=';'
        )
        
        tamanho = arquivo_saida.stat().st_size
        print(f"  ✓ Dados salvos: {arquivo_saida.name} ({bytes_para_humano(tamanho)})")
        logger.info(f"Dados enriquecidos salvos: {arquivo_saida}")


def main():
    """Função principal."""
    print("="*70)
    print("ETAPA 2.2: ENRIQUECIMENTO DE DADOS")
    print("="*70)
    
    try:
        # Caminhos
        saida_dir = PROJETO_RAIZ / "output"
        arquivo_entrada = saida_dir / "dados_validados.csv"
        arquivo_saida = saida_dir / "dados_enriquecidos.csv"
        arquivo_relatorio = saida_dir / "relatorio_enriquecimento.txt"
        
        # Criar diretório de saída
        arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
        
        # Criar enriquecedor
        enriquecedor = EnriquecedorDados(arquivo_entrada)
        
        # Executar enriquecimento
        enriquecedor.carregar_despesas()
        
        caminho_cadastro = enriquecedor.baixar_dados_cadastrais()
        enriquecedor.carregar_dados_cadastrais(caminho_cadastro)
        
        enriquecedor.realizar_join()
        
        # Gerar relatório
        enriquecedor.gerar_relatorio_enriquecimento(arquivo_relatorio)
        
        # Salvar dados
        enriquecedor.salvar_dados_enriquecidos(arquivo_saida)
        
        print("\n" + "="*70)
        print("✅ ETAPA 2.2 CONCLUÍDA COM SUCESSO!")
        print("="*70 + "\n")
        
        return 0
    
    except Exception as e:
        print(f"\n❌ ERRO: {e}\n")
        logger.error(f"Erro no enriquecimento: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())