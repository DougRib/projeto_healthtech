"""
Módulo de Download de Arquivos ANS
===================================

Este módulo é responsável por baixar arquivos da API de Dados Abertos da ANS.
Implementa download resiliente com retry, validação de URLs e tratamento de erros.

Funcionalidades:
- Download de arquivos ZIP dos trimestres
- Retry automático em caso de falha
- Validação de tamanho e integridade
- Progresso visual no console
"""

import os
import requests
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime
import time
import logging
from urllib.parse import urljoin
import re

from utils import (
    configurar_logging,
    criar_diretorios,
    bytes_para_humano,
    ProgressoConsole,
    URL_ANS_BASE,
    URL_ANS_DEMONSTRACOES
)


# Configuração de logging
logger = configurar_logging("downloader.log")


class DownloaderANS:
    """
    Classe responsável por realizar downloads de arquivos da ANS.
    
    Attributes:
        diretorio_destino: Pasta onde os arquivos serão salvos
        timeout: Tempo limite para requisições (segundos)
        max_retries: Número máximo de tentativas em caso de falha
    
    Exemplo:
        >>> downloader = DownloaderANS("data/raw")
        >>> downloader.baixar_arquivo(url, "arquivo.zip")
    """
    
    def __init__(
        self,
        diretorio_destino: str = "data/raw",
        timeout: int = 300,
        max_retries: int = 3
    ):
        """
        Inicializa o downloader.
        
        Args:
            diretorio_destino: Pasta para salvar downloads
            timeout: Timeout em segundos para cada requisição
            max_retries: Número de tentativas em caso de erro
        """
        self.diretorio_destino = Path(diretorio_destino)
        self.timeout = timeout
        self.max_retries = max_retries
        
        # Criar diretório se não existir
        criar_diretorios(self.diretorio_destino)
        
        # Configurar sessão HTTP com retry
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Python/DataAnalysis (Intuitive Care Test)'
        })
        
        logger.info(f"Downloader iniciado. Destino: {self.diretorio_destino}")
    
    def _construir_url_trimestre(self, ano: int, trimestre: int) -> str:
        """
        Constrói URL do diretório de um trimestre específico.
        
        Args:
            ano: Ano (ex: 2024)
            trimestre: Trimestre (1-4)
        
        Returns:
            URL completa do diretório
        
        Exemplo:
            >>> _construir_url_trimestre(2024, 3)
            'https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/3T/'
        """
        # URLs da ANS seguem o padrão: /YYYY/XT/
        url = f"{URL_ANS_DEMONSTRACOES}/{ano}/{trimestre}T/"
        logger.debug(f"URL construída: {url}")
        return url

    def listar_trimestres_disponiveis(self) -> List[Tuple[int, int]]:
        """
        Lista trimestres disponíveis no FTP da ANS.

        Returns:
            Lista de tuplas (ano, trimestre) ordenadas do mais recente ao mais antigo.
        """
        try:
            resposta = self.session.get(f"{URL_ANS_DEMONSTRACOES}/", timeout=self.timeout)
            resposta.raise_for_status()

            # Extrair pastas de ano (ex: 2024/)
            anos = re.findall(r'href="(\d{4})/"', resposta.text)
            anos_validos = sorted({int(ano) for ano in anos}, reverse=True)

            trimestres_disponiveis: List[Tuple[int, int]] = []

            for ano in anos_validos:
                url_ano = f"{URL_ANS_DEMONSTRACOES}/{ano}/"
                try:
                    resp_ano = self.session.get(url_ano, timeout=self.timeout)
                    resp_ano.raise_for_status()
                except requests.RequestException:
                    logger.warning(f"Não foi possível listar ano {ano}")
                    continue

                # Caso 1: diretórios no formato "1T/"
                trimestres_dir = re.findall(r'href="(\d)T/"', resp_ano.text, re.IGNORECASE)

                # Caso 2: arquivos ZIP no formato "1T2025.zip"
                trimestres_zip = re.findall(r'href="(\d)T\d{4}\.zip"', resp_ano.text, re.IGNORECASE)

                trimestres = {int(t) for t in trimestres_dir + trimestres_zip}
                for trim in sorted(trimestres, reverse=True):
                    trimestres_disponiveis.append((ano, trim))

            return trimestres_disponiveis
        except requests.RequestException as e:
            logger.error(f"Erro ao listar trimestres disponíveis: {e}")
            return []
    
    def listar_arquivos_trimestre(self, ano: int, trimestre: int) -> List[str]:
        """
        Lista todos os arquivos disponíveis em um trimestre.
        
        Args:
            ano: Ano
            trimestre: Trimestre (1-4)
        
        Returns:
            Lista de nomes de arquivos encontrados
        
        Nota:
            Esta função faz parsing do HTML do diretório FTP da ANS.
            Filtra apenas arquivos ZIP que contenham "despesa" ou "evento" no nome.
        """
        url = self._construir_url_trimestre(ano, trimestre)
        
        try:
            logger.info(f"Listando arquivos do trimestre {ano}/Q{trimestre}")
            resposta = self.session.get(url, timeout=self.timeout)
            if resposta.status_code == 404:
                # Alguns anos usam ZIP direto no diretório do ano (ex: 1T2025.zip)
                url = f"{URL_ANS_DEMONSTRACOES}/{ano}/"
                resposta = self.session.get(url, timeout=self.timeout)
            resposta.raise_for_status()
            
            # Parse simples do HTML (a ANS usa listagem simples de diretório)
            # Procura por links que terminam com .zip
            padrao = r'href="([^"]*\.zip)"'
            arquivos = re.findall(padrao, resposta.text, re.IGNORECASE)
            
            # Filtrar apenas arquivos relevantes (despesas/eventos)
            arquivos_relevantes = [
                arq
                for arq in arquivos
                if any(termo in arq.lower() for termo in ["despesa", "evento", "sinistro"])
                or re.search(rf"{trimestre}t{ano}\.zip", arq.lower())
            ]
            
            logger.info(f"Encontrados {len(arquivos_relevantes)} arquivos relevantes")
            return arquivos_relevantes
            
        except requests.RequestException as e:
            logger.error(f"Erro ao listar arquivos: {e}")
            return []
    
    def baixar_arquivo(
        self,
        url: str,
        nome_arquivo: str,
        mostrar_progresso: bool = True
    ) -> Optional[Path]:
        """
        Baixa um arquivo com retry e validação.
        
        Args:
            url: URL completa do arquivo
            nome_arquivo: Nome para salvar o arquivo
            mostrar_progresso: Se True, exibe barra de progresso
        
        Returns:
            Path do arquivo baixado ou None em caso de falha
        
        Raises:
            Exception: Se todas as tentativas falharem
        """
        caminho_destino = self.diretorio_destino / nome_arquivo
        
        # Se arquivo já existe, verificar integridade
        if caminho_destino.exists():
            tamanho = caminho_destino.stat().st_size
            if tamanho > 0:
                logger.info(f"Arquivo já existe: {nome_arquivo} ({bytes_para_humano(tamanho)})")
                return caminho_destino
        
        # Tentar download com retry
        for tentativa in range(1, self.max_retries + 1):
            try:
                logger.info(f"Baixando {nome_arquivo} (tentativa {tentativa}/{self.max_retries})")
                
                # Fazer requisição inicial para obter tamanho
                resposta = self.session.get(url, stream=True, timeout=self.timeout)
                resposta.raise_for_status()
                
                tamanho_total = int(resposta.headers.get('content-length', 0))
                logger.debug(f"Tamanho do arquivo: {bytes_para_humano(tamanho_total)}")
                
                # Baixar arquivo em chunks
                tamanho_baixado = 0
                chunk_size = 8192  # 8KB por chunk
                
                if mostrar_progresso and tamanho_total > 0:
                    print(f"\n📥 Baixando: {nome_arquivo} ({bytes_para_humano(tamanho_total)})")
                
                with open(caminho_destino, 'wb') as arquivo:
                    for chunk in resposta.iter_content(chunk_size=chunk_size):
                        if chunk:
                            arquivo.write(chunk)
                            tamanho_baixado += len(chunk)
                            
                            # Atualizar progresso
                            if mostrar_progresso and tamanho_total > 0:
                                percentual = (tamanho_baixado / tamanho_total) * 100
                                barra = '█' * int(percentual // 2) + '░' * (50 - int(percentual // 2))
                                print(f"\r   |{barra}| {percentual:.1f}%", end='')
                
                if mostrar_progresso:
                    print()  # Nova linha após progresso
                
                # Validar arquivo baixado
                if tamanho_total > 0 and tamanho_baixado != tamanho_total:
                    raise Exception(f"Tamanho incorreto: esperado {tamanho_total}, recebido {tamanho_baixado}")
                
                logger.info(f"✓ Download concluído: {nome_arquivo}")
                return caminho_destino
                
            except Exception as e:
                logger.warning(f"Tentativa {tentativa} falhou: {str(e)}")
                
                # Se não for a última tentativa, aguardar antes de tentar novamente
                if tentativa < self.max_retries:
                    tempo_espera = 2 ** tentativa  # Backoff exponencial: 2s, 4s, 8s
                    logger.info(f"Aguardando {tempo_espera}s antes de tentar novamente...")
                    time.sleep(tempo_espera)
                else:
                    logger.error(f"✗ Falha após {self.max_retries} tentativas: {nome_arquivo}")
                    # Remover arquivo parcial se existir
                    if caminho_destino.exists():
                        caminho_destino.unlink()
                    return None
    
    def baixar_trimestre(
        self,
        ano: int,
        trimestre: int
    ) -> List[Path]:
        """
        Baixa todos os arquivos de um trimestre específico.
        
        Args:
            ano: Ano
            trimestre: Trimestre (1-4)
        
        Returns:
            Lista de caminhos dos arquivos baixados com sucesso
        """
        logger.info(f"=== Iniciando download do trimestre {ano}/Q{trimestre} ===")
        
        # Listar arquivos disponíveis
        arquivos = self.listar_arquivos_trimestre(ano, trimestre)
        
        if not arquivos:
            logger.warning(f"Nenhum arquivo encontrado para {ano}/Q{trimestre}")
            return []
        
        # Baixar cada arquivo
        arquivos_baixados = []
        url_base_trimestre = self._construir_url_trimestre(ano, trimestre)
        url_base_ano = f"{URL_ANS_DEMONSTRACOES}/{ano}/"
        
        for idx, nome_arquivo in enumerate(arquivos, 1):
            print(f"\n[{idx}/{len(arquivos)}] Processando: {nome_arquivo}")
            
            # Alguns arquivos ficam no diretório do ano (ex: 1T2025.zip)
            if re.search(rf"^{trimestre}t{ano}\.zip$", nome_arquivo.lower()):
                url_completa = urljoin(url_base_ano, nome_arquivo)
            else:
                url_completa = urljoin(url_base_trimestre, nome_arquivo)
            caminho = self.baixar_arquivo(url_completa, f"{ano}_Q{trimestre}_{nome_arquivo}")
            
            if caminho:
                arquivos_baixados.append(caminho)
        
        logger.info(f"✓ Trimestre {ano}/Q{trimestre}: {len(arquivos_baixados)}/{len(arquivos)} arquivos baixados")
        return arquivos_baixados
    
    def baixar_ultimos_trimestres(self, quantidade: int = 3) -> List[Tuple[int, int, List[Path]]]:
        """
        Baixa arquivos dos últimos N trimestres disponíveis.
        
        Args:
            quantidade: Número de trimestres para baixar (padrão: 3)
        
        Returns:
            Lista de tuplas (ano, trimestre, arquivos_baixados)
        
        Exemplo:
            >>> resultado = downloader.baixar_ultimos_trimestres(3)
            >>> for ano, trim, arquivos in resultado:
            ...     print(f"{ano}/Q{trim}: {len(arquivos)} arquivos")
        """
        logger.info(f"=== Iniciando download dos últimos {quantidade} trimestres ===")
        
        trimestres_disponiveis = self.listar_trimestres_disponiveis()
        if not trimestres_disponiveis:
            logger.warning("Nenhum trimestre disponível encontrado na ANS")
            return []

        resultados = []

        for i, (ano, trimestre) in enumerate(trimestres_disponiveis[:quantidade]):
            
            print(f"\n{'='*60}")
            print(f"TRIMESTRE {i+1}/{quantidade}: {ano}/Q{trimestre}")
            print('='*60)
            
            arquivos = self.baixar_trimestre(ano, trimestre)
            resultados.append((ano, trimestre, arquivos))
            
            # Pequena pausa entre trimestres para não sobrecarregar o servidor
            if i < quantidade - 1:
                time.sleep(2)
        
        # Resumo final
        print(f"\n{'='*60}")
        print("📊 RESUMO DO DOWNLOAD")
        print('='*60)
        
        total_arquivos = sum(len(arqs) for _, _, arqs in resultados)
        
        for ano, trim, arquivos in resultados:
            status = "✓" if arquivos else "✗"
            print(f"{status} {ano}/Q{trim}: {len(arquivos)} arquivo(s)")
        
        print(f"\nTotal: {total_arquivos} arquivo(s) baixados")
        logger.info(f"Download concluído. Total: {total_arquivos} arquivos")
        
        return resultados


def main():
    """Função principal para teste do módulo."""
    print("="*70)
    print("DOWNLOADER ANS - Teste de Funcionalidade")
    print("="*70)
    
    # Criar downloader
    downloader = DownloaderANS(diretorio_destino="data/raw")
    
    # Testar download dos últimos 3 trimestres
    inicio = datetime.now()
    resultados = downloader.baixar_ultimos_trimestres(quantidade=3)
    
    # Exibir tempo total
    from utils import tempo_decorrido
    print(f"\n⏱️  Tempo total: {tempo_decorrido(inicio)}")
    
    return resultados


if __name__ == "__main__":
    main()