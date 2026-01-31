"""
Módulo de Processamento de Arquivos ANS
========================================

Este módulo processa arquivos baixados da ANS:
- Extrai arquivos ZIP
- Detecta formato automaticamente (CSV, TXT, XLSX)
- Normaliza estruturas diferentes
- Trata inconsistências nos dados
- Consolida dados em formato padrão
"""

import os
import zipfile
import pandas as pd
import logging
import requests
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import re

from utils import (
    configurar_logging,
    criar_diretorios,
    limpar_cnpj,
    validar_formato_cnpj,
    bytes_para_humano,
    COLUNAS_PADRAO_DESPESAS,
    ProgressoConsole,
    URL_ANS_OPERADORAS
)


# Configuração de logging
logger = configurar_logging("processor.log")

PROJETO_RAIZ = Path(__file__).resolve().parents[1]


class ProcessadorArquivos:
    """
    Classe responsável por processar arquivos da ANS.
    
    Funcionalidades:
    - Extração de arquivos ZIP
    - Detecção automática de formato
    - Normalização de colunas
    - Tratamento de inconsistências
    - Consolidação de dados
    
    Exemplo:
        >>> processador = ProcessadorArquivos()
        >>> df = processador.processar_arquivo("arquivo.zip", 2024, 3)
    """
    
    def __init__(
        self,
        diretorio_extracao: str = "data/extracted",
        encoding_padrao: str = "utf-8"
    ):
        """
        Inicializa o processador.
        
        Args:
            diretorio_extracao: Pasta para extrair arquivos ZIP
            encoding_padrao: Encoding padrão para leitura de arquivos
        """
        self.diretorio_extracao = Path(diretorio_extracao)
        self.encoding_padrao = encoding_padrao
        self.encodings_alternativos = ['latin-1', 'iso-8859-1', 'cp1252']
        
        criar_diretorios(self.diretorio_extracao)
        logger.info(f"Processador iniciado. Diretório de extração: {self.diretorio_extracao}")
    
    def extrair_zip(self, caminho_zip: Path) -> List[Path]:
        """
        Extrai arquivos de um ZIP.
        
        Args:
            caminho_zip: Caminho do arquivo ZIP
        
        Returns:
            Lista de caminhos dos arquivos extraídos
        
        Raises:
            zipfile.BadZipFile: Se o arquivo não for um ZIP válido
        """
        logger.info(f"Extraindo: {caminho_zip.name}")
        
        # Criar pasta específica para este ZIP
        pasta_destino = self.diretorio_extracao / caminho_zip.stem
        criar_diretorios(pasta_destino)
        
        arquivos_extraidos = []
        
        try:
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                # Listar arquivos
                nomes_arquivos = zip_ref.namelist()
                logger.debug(f"Arquivos no ZIP: {len(nomes_arquivos)}")
                
                # Extrair cada arquivo
                for nome in nomes_arquivos:
                    # Ignorar diretórios
                    if nome.endswith('/'):
                        continue
                    
                    # Extrair arquivo
                    caminho_extraido = pasta_destino / Path(nome).name
                    
                    with zip_ref.open(nome) as fonte, open(caminho_extraido, 'wb') as destino:
                        destino.write(fonte.read())
                    
                    arquivos_extraidos.append(caminho_extraido)
                    logger.debug(f"  Extraído: {nome}")
                
                logger.info(f"✓ Extraídos {len(arquivos_extraidos)} arquivos")
                return arquivos_extraidos
                
        except zipfile.BadZipFile as e:
            logger.error(f"✗ Erro ao extrair ZIP: {e}")
            return []
    
    def detectar_formato_arquivo(self, caminho: Path) -> Optional[str]:
        """
        Detecta o formato de um arquivo.
        
        Args:
            caminho: Caminho do arquivo
        
        Returns:
            'csv', 'txt', 'xlsx', 'xls' ou None se não suportado
        """
        extensao = caminho.suffix.lower()
        
        if extensao in ['.csv', '.txt']:
            return 'csv'
        elif extensao in ['.xlsx', '.xls']:
            return 'excel'
        else:
            logger.warning(f"Formato não suportado: {extensao}")
            return None
    
    def ler_arquivo_csv(self, caminho: Path, **kwargs) -> Optional[pd.DataFrame]:
        """
        Lê arquivo CSV/TXT com tratamento de encoding.
        
        Args:
            caminho: Caminho do arquivo
            **kwargs: Argumentos adicionais para pd.read_csv
        
        Returns:
            DataFrame ou None em caso de erro
        """
        # Tentar diferentes encodings
        encodings = [self.encoding_padrao] + self.encodings_alternativos
        
        for encoding in encodings:
            try:
                logger.debug(f"Tentando ler com encoding: {encoding}")
                
                # Configurações padrão para CSV da ANS
                config_padrao = {
                    'encoding': encoding,
                    'sep': ';',  # ANS geralmente usa ponto-e-vírgula
                    'decimal': ',',  # Vírgula para decimais (padrão BR)
                    'thousands': '.',  # Ponto para milhares
                    'low_memory': False  # Evita warnings de tipos mistos
                }
                
                # Mesclar com kwargs fornecidos
                config = {**config_padrao, **kwargs}
                
                df = pd.read_csv(caminho, **config)
                logger.info(f"✓ Arquivo lido: {caminho.name} ({len(df)} linhas, {len(df.columns)} colunas)")
                return df
                
            except UnicodeDecodeError:
                logger.debug(f"Encoding {encoding} não funcionou")
                continue
            except Exception as e:
                logger.warning(f"Erro ao ler arquivo com {encoding}: {e}")
                continue
        
        logger.error(f"✗ Não foi possível ler o arquivo: {caminho.name}")
        return None
    
    def ler_arquivo_excel(self, caminho: Path, **kwargs) -> Optional[pd.DataFrame]:
        """
        Lê arquivo Excel (.xlsx ou .xls).
        
        Args:
            caminho: Caminho do arquivo
            **kwargs: Argumentos adicionais para pd.read_excel
        
        Returns:
            DataFrame ou None em caso de erro
        """
        try:
            logger.debug(f"Lendo arquivo Excel: {caminho.name}")
            df = pd.read_excel(caminho, **kwargs)
            logger.info(f"✓ Arquivo Excel lido: {caminho.name} ({len(df)} linhas)")
            return df
        except Exception as e:
            logger.error(f"✗ Erro ao ler Excel: {e}")
            return None
    
    def normalizar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza nomes de colunas para o padrão do projeto.
        
        Esta função tenta mapear colunas com nomes diferentes para o padrão:
        ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
        
        Args:
            df: DataFrame com colunas originais
        
        Returns:
            DataFrame com colunas normalizadas
        """
        logger.debug("Normalizando colunas...")
        
        # Dicionário de mapeamento (possíveis variações → padrão)
        mapeamento = {
            # CNPJ
            'cnpj': 'CNPJ',
            'cnpj_operadora': 'CNPJ',
            'cd_cnpj': 'CNPJ',
            'num_cnpj': 'CNPJ',
            
            # Razão Social
            'razao_social': 'RazaoSocial',
            'razaosocial': 'RazaoSocial',
            'nm_razao_social': 'RazaoSocial',
            'nome': 'RazaoSocial',
            'operadora': 'RazaoSocial',
            
            # Trimestre
            'trimestre': 'Trimestre',
            'tri': 'Trimestre',
            'cd_trimestre': 'Trimestre',
            'num_trimestre': 'Trimestre',
            
            # Ano
            'ano': 'Ano',
            'ano_competencia': 'Ano',
            'cd_ano': 'Ano',
            
            # Valor Despesas
            'valor': 'ValorDespesas',
            'valor_despesa': 'ValorDespesas',
            'valor_despesas': 'ValorDespesas',
            'vl_despesa': 'ValorDespesas',
            'despesa': 'ValorDespesas',
            'despesas': 'ValorDespesas',
            'eventos': 'ValorDespesas',
            'sinistros': 'ValorDespesas'
        }
        
        # Criar cópia do DataFrame
        df_normalizado = df.copy()
        
        # Normalizar nomes das colunas (lowercase, sem espaços)
        df_normalizado.columns = df_normalizado.columns.str.lower().str.strip().str.replace(' ', '_')
        
        # Aplicar mapeamento
        df_normalizado = df_normalizado.rename(columns=mapeamento)
        
        # Verificar quais colunas padrão existem
        colunas_encontradas = [col for col in COLUNAS_PADRAO_DESPESAS if col in df_normalizado.columns]
        
        logger.debug(f"Colunas encontradas após normalização: {colunas_encontradas}")
        
        # Selecionar apenas colunas padrão (se existirem)
        colunas_disponiveis = [col for col in COLUNAS_PADRAO_DESPESAS if col in df_normalizado.columns]
        
        if colunas_disponiveis:
            df_final = df_normalizado[colunas_disponiveis].copy()
            logger.info(f"✓ Colunas normalizadas: {colunas_disponiveis}")
            return df_final
        else:
            logger.warning("Nenhuma coluna padrão identificada!")
            return df_normalizado

    def _carregar_cadastro_operadoras(self) -> pd.DataFrame:
        """
        Carrega o cadastro de operadoras da ANS (para mapear RegistroANS -> CNPJ/RazaoSocial).
        """
        caminho_local = PROJETO_RAIZ / "data" / "cadastro_operadoras.csv"
        caminho_local.parent.mkdir(parents=True, exist_ok=True)

        if not caminho_local.exists():
            logger.info("Cadastro ANS não encontrado. Baixando arquivo cadastral...")
            url_base = f"{URL_ANS_OPERADORAS}/"
            resposta = requests.get(url_base, timeout=30)
            resposta.raise_for_status()

            padrao = r'href="(Relatorio_cadop[^"]*\.csv)"'
            arquivos = re.findall(padrao, resposta.text, re.IGNORECASE)
            if not arquivos:
                raise Exception("Nenhum arquivo cadastral encontrado na ANS")

            nome_arquivo = arquivos[-1]
            url_arquivo = url_base + nome_arquivo

            resposta_arquivo = requests.get(url_arquivo, stream=True, timeout=300)
            resposta_arquivo.raise_for_status()

            with open(caminho_local, "wb") as f:
                for chunk in resposta_arquivo.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Cadastro ANS salvo em: {caminho_local}")

        # Carregar CSV com encoding flexível
        df_cadastro = None
        for encoding in ["utf-8", "latin-1", "iso-8859-1"]:
            try:
                df_cadastro = pd.read_csv(caminho_local, sep=";", encoding=encoding, low_memory=False)
                break
            except UnicodeDecodeError:
                continue

        if df_cadastro is None:
            raise Exception("Não foi possível ler o cadastro ANS")

        df_cadastro.columns = df_cadastro.columns.str.strip().str.lower()
        mapeamento = {
            "registro_ans": "RegistroANS",
            "cd_registro_ans": "RegistroANS",
            "registro_operadora": "RegistroANS",
            "registro_operadora_ans": "RegistroANS",
            "cnpj": "CNPJ",
            "cd_cnpj": "CNPJ",
            "razao_social": "RazaoSocial",
            "nm_razao_social": "RazaoSocial",
        }
        df_cadastro = df_cadastro.rename(columns=mapeamento)
        df_cadastro = df_cadastro[[c for c in ["RegistroANS", "CNPJ", "RazaoSocial"] if c in df_cadastro.columns]]
        df_cadastro["RegistroANS"] = df_cadastro["RegistroANS"].astype(str).str.strip()
        df_cadastro["CNPJ"] = df_cadastro["CNPJ"].astype(str).apply(limpar_cnpj)
        return df_cadastro.dropna(subset=["RegistroANS"]).drop_duplicates(subset=["RegistroANS"])

    def _processar_despesas_reg_ans(self, df: pd.DataFrame, ano: int, trimestre: int) -> pd.DataFrame:
        """
        Processa arquivos contábeis com colunas REG_ANS/descricao e gera o padrão final.
        """
        df_temp = df.copy()
        df_temp.columns = df_temp.columns.str.strip().str.lower()

        renomear = {
            "reg_ans": "registro_ans",
            "registro_ans": "registro_ans",
            "descricao": "descricao",
            "vl_saldo_final": "valor_saldo_final",
        }
        df_temp = df_temp.rename(columns=renomear)

        if "registro_ans" not in df_temp.columns or "descricao" not in df_temp.columns:
            return pd.DataFrame()

        df_temp["descricao"] = df_temp["descricao"].astype(str)
        mask_eventos = df_temp["descricao"].str.contains("evento", case=False, na=False)
        mask_sinistros = df_temp["descricao"].str.contains("sinistro", case=False, na=False)
        df_filtrado = df_temp[mask_eventos | mask_sinistros].copy()

        if df_filtrado.empty:
            logger.warning("Nenhuma linha de eventos/sinistros encontrada no arquivo contábil")
            return pd.DataFrame()

        df_filtrado["valor_saldo_final"] = pd.to_numeric(
            df_filtrado.get("valor_saldo_final"), errors="coerce"
        )

        cadastro = self._carregar_cadastro_operadoras()
        mapa_cnpj = cadastro.set_index("RegistroANS")["CNPJ"].to_dict() if not cadastro.empty else {}
        mapa_razao = cadastro.set_index("RegistroANS")["RazaoSocial"].to_dict() if not cadastro.empty else {}

        df_filtrado["registro_ans"] = df_filtrado["registro_ans"].astype(str).str.strip()
        df_filtrado["CNPJ"] = df_filtrado["registro_ans"].map(mapa_cnpj)
        df_filtrado["RazaoSocial"] = df_filtrado["registro_ans"].map(mapa_razao)
        df_filtrado["Ano"] = ano
        df_filtrado["Trimestre"] = trimestre
        df_filtrado["ValorDespesas"] = df_filtrado["valor_saldo_final"]

        df_final = df_filtrado[COLUNAS_PADRAO_DESPESAS].copy()
        return df_final
    
    def limpar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e trata inconsistências nos dados.
        
        Tratamentos aplicados:
        - Remove espaços em branco extras
        - Limpa CNPJ (remove formatação)
        - Converte tipos de dados
        - Marca registros com inconsistências
        
        Args:
            df: DataFrame com dados brutos
        
        Returns:
            DataFrame limpo com coluna de flags de inconsistência
        """
        logger.info("Limpando dados...")
        df_limpo = df.copy()
        
        # Adicionar coluna de flags de inconsistências
        df_limpo['inconsistencia_flag'] = ''
        
        # 1. Limpar strings (remover espaços extras)
        colunas_string = df_limpo.select_dtypes(include=['object']).columns
        for col in colunas_string:
            if col in df_limpo.columns:
                df_limpo[col] = df_limpo[col].astype(str).str.strip()

        # Marcar Razao Social vazia
        if 'RazaoSocial' in df_limpo.columns:
            razao = df_limpo['RazaoSocial'].astype(str).str.strip()
            mask_razao_vazia = razao.eq('') | razao.str.lower().eq('nan')
            df_limpo['RazaoSocial'] = razao
            df_limpo.loc[mask_razao_vazia, 'inconsistencia_flag'] += 'RAZAO_VAZIA;'
        
        # 2. Limpar e validar CNPJ
        if 'CNPJ' in df_limpo.columns:
            logger.debug("Limpando CNPJs...")
            
            # Limpar formatação
            df_limpo['CNPJ'] = df_limpo['CNPJ'].apply(limpar_cnpj)

            # Marcar CNPJ sem informação (sem match)
            mask_cnpj_vazio = df_limpo['CNPJ'].eq('')
            df_limpo.loc[mask_cnpj_vazio, 'inconsistencia_flag'] += 'CNPJ_SEM_MATCH;'
            
            # Validar formato
            def validar_e_marcar(cnpj):
                valido, motivo = validar_formato_cnpj(cnpj)
                return valido, motivo
            
            df_limpo[['cnpj_valido', 'motivo_cnpj']] = df_limpo['CNPJ'].apply(
                lambda x: pd.Series(validar_e_marcar(x))
            )
            
            # Marcar CNPJs inválidos
            mask_invalido = ~df_limpo['cnpj_valido']
            df_limpo.loc[mask_invalido, 'inconsistencia_flag'] += 'CNPJ_INVALIDO;'
            
            cnpjs_invalidos = mask_invalido.sum()
            if cnpjs_invalidos > 0:
                logger.warning(f"  {cnpjs_invalidos} CNPJs inválidos encontrados")
        
        # 3. Limpar e validar valores numéricos
        if 'ValorDespesas' in df_limpo.columns:
            logger.debug("Limpando valores...")
            
            # Converter para numérico (forçar erros para NaN)
            df_limpo['ValorDespesas'] = pd.to_numeric(
                df_limpo['ValorDespesas'],
                errors='coerce'
            )
            
            # Marcar valores nulos
            mask_nulo = df_limpo['ValorDespesas'].isna()
            df_limpo.loc[mask_nulo, 'inconsistencia_flag'] += 'VALOR_NULO;'
            
            # Marcar valores negativos
            mask_negativo = df_limpo['ValorDespesas'] < 0
            df_limpo.loc[mask_negativo, 'inconsistencia_flag'] += 'VALOR_NEGATIVO;'
            
            # Marcar valores zerados
            mask_zero = df_limpo['ValorDespesas'] == 0
            df_limpo.loc[mask_zero, 'inconsistencia_flag'] += 'VALOR_ZERADO;'
            
            logger.warning(f"  Valores nulos: {mask_nulo.sum()}")
            logger.warning(f"  Valores negativos: {mask_negativo.sum()}")
            logger.warning(f"  Valores zerados: {mask_zero.sum()}")
        
        # 4. Validar Trimestre e Ano
        if 'Trimestre' in df_limpo.columns:
            df_limpo['Trimestre'] = pd.to_numeric(df_limpo['Trimestre'], errors='coerce')
            mask_trim_invalido = (df_limpo['Trimestre'] < 1) | (df_limpo['Trimestre'] > 4)
            df_limpo.loc[mask_trim_invalido, 'inconsistencia_flag'] += 'TRIMESTRE_INVALIDO;'
        
        if 'Ano' in df_limpo.columns:
            df_limpo['Ano'] = pd.to_numeric(df_limpo['Ano'], errors='coerce')
            mask_ano_invalido = (df_limpo['Ano'] < 2000) | (df_limpo['Ano'] > 2030)
            df_limpo.loc[mask_ano_invalido, 'inconsistencia_flag'] += 'ANO_INVALIDO;'
        
        # 5. Identificar CNPJs duplicados com razões sociais diferentes
        if 'CNPJ' in df_limpo.columns and 'RazaoSocial' in df_limpo.columns:
            duplicados = df_limpo.groupby('CNPJ')['RazaoSocial'].nunique()
            cnpjs_duplicados = duplicados[duplicados > 1].index
            
            if len(cnpjs_duplicados) > 0:
                mask_dup = df_limpo['CNPJ'].isin(cnpjs_duplicados)
                df_limpo.loc[mask_dup, 'inconsistencia_flag'] += 'CNPJ_RAZAO_DIVERGENTE;'
                logger.warning(f"  {len(cnpjs_duplicados)} CNPJs com razões sociais diferentes")
        
        # Remover ponto e vírgula final
        df_limpo['inconsistencia_flag'] = df_limpo['inconsistencia_flag'].str.rstrip(';')
        
        # Estatísticas
        registros_com_problema = (df_limpo['inconsistencia_flag'] != '').sum()
        logger.info(f"✓ Limpeza concluída. {registros_com_problema} registros com inconsistências")
        
        return df_limpo
    
    def processar_arquivo(
        self,
        caminho: Path,
        ano: int,
        trimestre: int
    ) -> Optional[pd.DataFrame]:
        """
        Processa um arquivo completo (extração, leitura, normalização, limpeza).
        
        Args:
            caminho: Caminho do arquivo (pode ser ZIP, CSV, etc)
            ano: Ano do trimestre
            trimestre: Trimestre (1-4)
        
        Returns:
            DataFrame processado ou None
        """
        logger.info(f"Processando arquivo: {caminho.name}")
        
        # Se for ZIP, extrair primeiro
        if caminho.suffix.lower() == '.zip':
            arquivos_extraidos = self.extrair_zip(caminho)
            
            if not arquivos_extraidos:
                return None
            
            # Processar cada arquivo extraído e concatenar
            dfs = []
            for arquivo in arquivos_extraidos:
                df_temp = self._processar_arquivo_individual(arquivo, ano, trimestre)
                if df_temp is not None:
                    dfs.append(df_temp)
            
            if dfs:
                df_final = pd.concat(dfs, ignore_index=True)
                logger.info(f"✓ Consolidados {len(dfs)} arquivos em {len(df_final)} registros")
                return df_final
            else:
                return None
        else:
            return self._processar_arquivo_individual(caminho, ano, trimestre)
    
    def _processar_arquivo_individual(
        self,
        caminho: Path,
        ano: int,
        trimestre: int
    ) -> Optional[pd.DataFrame]:
        """
        Processa um arquivo individual (não ZIP).
        
        Args:
            caminho: Caminho do arquivo
            ano: Ano
            trimestre: Trimestre
        
        Returns:
            DataFrame processado
        """
        # Detectar formato
        formato = self.detectar_formato_arquivo(caminho)
        
        if not formato:
            return None
        
        # Ler arquivo
        if formato == 'csv':
            df = self.ler_arquivo_csv(caminho)
        elif formato == 'excel':
            df = self.ler_arquivo_excel(caminho)
        else:
            return None
        
        if df is None or df.empty:
            return None
        
        # Se arquivo é contábil com REG_ANS, tratar com lógica específica
        colunas_lower = df.columns.str.strip().str.lower()
        if "reg_ans" in colunas_lower or "registro_ans" in colunas_lower:
            df = self._processar_despesas_reg_ans(df, ano, trimestre)
        else:
            # Normalizar colunas
            df = self.normalizar_colunas(df)
        
        # Garantir que ano e trimestre estejam presentes
        if 'Ano' not in df.columns:
            df['Ano'] = ano
        if 'Trimestre' not in df.columns:
            df['Trimestre'] = trimestre
        
        # Limpar dados
        df = self.limpar_dados(df)
        
        return df


def main():
    """Função principal para teste do módulo."""
    print("="*70)
    print("PROCESSADOR DE ARQUIVOS ANS - Teste")
    print("="*70)
    
    processador = ProcessadorArquivos()
    
    # Aqui você testaria com arquivos reais baixados
    # Exemplo:
    # df = processador.processar_arquivo(Path("data/raw/2024_Q3_arquivo.zip"), 2024, 3)
    # print(df.head())
    
    print("\n✓ Módulo de processamento carregado com sucesso!")


if __name__ == "__main__":
    main()