"""
Módulo de Utilitários
====================

Este módulo contém funções auxiliares reutilizáveis para o projeto.
Inclui configuração de logging, validações e helpers gerais.

Autor: [Seu Nome]
Data: 29/01/2025
"""

import os
import logging
from pathlib import Path
from typing import Optional, Union
from datetime import datetime


def configurar_logging(
    nome_arquivo: str = "app.log",
    nivel: int = logging.INFO,
    formato: Optional[str] = None
) -> logging.Logger:
    """
    Configura sistema de logging para a aplicação.
    
    Args:
        nome_arquivo: Nome do arquivo de log
        nivel: Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        formato: Formato personalizado das mensagens (opcional)
    
    Returns:
        Logger configurado
    
    Exemplo:
        >>> logger = configurar_logging("meu_app.log", logging.DEBUG)
        >>> logger.info("Aplicação iniciada")
    """
    if formato is None:
        formato = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Criar diretório de logs se não existir
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configurar logging
    logging.basicConfig(
        level=nivel,
        format=formato,
        handlers=[
            logging.FileHandler(log_dir / nome_arquivo, encoding='utf-8'),
            logging.StreamHandler()  # Também exibe no console
        ]
    )
    
    logger = logging.getLogger(__name__)
    return logger


def criar_diretorios(*caminhos: Union[str, Path]) -> None:
    """
    Cria múltiplos diretórios se não existirem.
    
    Args:
        *caminhos: Caminhos dos diretórios a serem criados
    
    Exemplo:
        >>> criar_diretorios("data/raw", "data/processed", "output")
    """
    for caminho in caminhos:
        Path(caminho).mkdir(parents=True, exist_ok=True)


def formatar_cnpj(cnpj: str) -> str:
    """
    Formata CNPJ para o padrão XX.XXX.XXX/XXXX-XX.
    
    Args:
        cnpj: CNPJ sem formatação (apenas números)
    
    Returns:
        CNPJ formatado
    
    Raises:
        ValueError: Se o CNPJ não tiver 14 dígitos
    
    Exemplo:
        >>> formatar_cnpj("12345678000190")
        '12.345.678/0001-90'
    """
    # Remove caracteres não numéricos
    cnpj = ''.join(filter(str.isdigit, cnpj))
    
    if len(cnpj) != 14:
        raise ValueError(f"CNPJ deve ter 14 dígitos. Recebido: {len(cnpj)}")
    
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


def limpar_cnpj(cnpj: str) -> str:
    """
    Remove formatação do CNPJ, deixando apenas números.
    
    Args:
        cnpj: CNPJ formatado ou não
    
    Returns:
        CNPJ apenas com números
    
    Exemplo:
        >>> limpar_cnpj("12.345.678/0001-90")
        '12345678000190'
    """
    return ''.join(filter(str.isdigit, str(cnpj)))


def validar_formato_cnpj(cnpj: str) -> tuple[bool, str]:
    """
    Valida formato básico do CNPJ (sem validar dígitos verificadores).
    
    Args:
        cnpj: CNPJ a ser validado
    
    Returns:
        Tupla (é_válido, mensagem_erro)
    
    Exemplo:
        >>> validar_formato_cnpj("12345678000190")
        (True, "")
        >>> validar_formato_cnpj("123")
        (False, "CNPJ deve ter 14 dígitos")
    """
    if not cnpj:
        return False, "CNPJ vazio"
    
    cnpj_limpo = limpar_cnpj(cnpj)
    
    if len(cnpj_limpo) != 14:
        return False, f"CNPJ deve ter 14 dígitos. Recebido: {len(cnpj_limpo)}"
    
    if not cnpj_limpo.isdigit():
        return False, "CNPJ deve conter apenas números"
    
    if cnpj_limpo == "00000000000000":
        return False, "CNPJ não pode ser zerado"
    
    return True, ""


def obter_trimestre_atual() -> tuple[int, int]:
    """
    Retorna o ano e trimestre atual.
    
    Returns:
        Tupla (ano, trimestre)
    
    Exemplo:
        >>> obter_trimestre_atual()  # Se estamos em Janeiro/2025
        (2025, 1)
    """
    hoje = datetime.now()
    ano = hoje.year
    trimestre = (hoje.month - 1) // 3 + 1
    return ano, trimestre


def calcular_trimestre_anterior(ano: int, trimestre: int, n: int = 1) -> tuple[int, int]:
    """
    Calcula o trimestre N períodos antes do trimestre informado.
    
    Args:
        ano: Ano do trimestre base
        trimestre: Trimestre base (1-4)
        n: Número de trimestres para voltar
    
    Returns:
        Tupla (ano, trimestre)
    
    Exemplo:
        >>> calcular_trimestre_anterior(2025, 1, 2)
        (2024, 3)  # 2 trimestres antes de 2025/Q1
    """
    total_trimestres = (ano * 4 + trimestre - 1) - n
    novo_ano = total_trimestres // 4
    novo_trimestre = (total_trimestres % 4) + 1
    return novo_ano, novo_trimestre


def bytes_para_humano(bytes_size: int) -> str:
    """
    Converte tamanho em bytes para formato legível (KB, MB, GB).
    
    Args:
        bytes_size: Tamanho em bytes
    
    Returns:
        String formatada
    
    Exemplo:
        >>> bytes_para_humano(1536)
        '1.50 KB'
        >>> bytes_para_humano(1048576)
        '1.00 MB'
    """
    for unidade in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unidade}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def tempo_decorrido(inicio: datetime) -> str:
    """
    Calcula tempo decorrido desde um momento inicial.
    
    Args:
        inicio: Data/hora inicial
    
    Returns:
        String com tempo formatado
    
    Exemplo:
        >>> inicio = datetime.now()
        >>> # ... processamento ...
        >>> print(tempo_decorrido(inicio))
        '2 minutos e 30 segundos'
    """
    delta = datetime.now() - inicio
    segundos = int(delta.total_seconds())
    
    if segundos < 60:
        return f"{segundos} segundo{'s' if segundos != 1 else ''}"
    
    minutos = segundos // 60
    segundos_restantes = segundos % 60
    
    if minutos < 60:
        return f"{minutos} minuto{'s' if minutos != 1 else ''} e {segundos_restantes} segundo{'s' if segundos_restantes != 1 else ''}"
    
    horas = minutos // 60
    minutos_restantes = minutos % 60
    return f"{horas} hora{'s' if horas != 1 else ''} e {minutos_restantes} minuto{'s' if minutos_restantes != 1 else ''}"


class ProgressoConsole:
    """
    Classe para exibir progresso de operações no console.
    
    Exemplo:
        >>> progresso = ProgressoConsole(total=100, descricao="Processando")
        >>> for i in range(100):
        ...     progresso.atualizar(i + 1)
        >>> progresso.finalizar()
    """
    
    def __init__(self, total: int, descricao: str = "Progresso"):
        """
        Inicializa barra de progresso.
        
        Args:
            total: Total de itens a processar
            descricao: Descrição da operação
        """
        self.total = total
        self.descricao = descricao
        self.atual = 0
        self.inicio = datetime.now()
    
    def atualizar(self, atual: int) -> None:
        """
        Atualiza progresso atual.
        
        Args:
            atual: Número de itens processados
        """
        self.atual = atual
        percentual = (atual / self.total) * 100
        barra = '█' * int(percentual // 2) + '░' * (50 - int(percentual // 2))
        
        print(f"\r{self.descricao}: |{barra}| {percentual:.1f}% ({atual}/{self.total})", end='')
    
    def finalizar(self) -> None:
        """Finaliza e exibe tempo total."""
        tempo = tempo_decorrido(self.inicio)
        print(f"\n✓ Concluído em {tempo}")


# Constantes úteis
COLUNAS_PADRAO_DESPESAS = [
    'CNPJ',
    'RazaoSocial',
    'Trimestre',
    'Ano',
    'ValorDespesas'
]

TIPOS_ARQUIVO_SUPORTADOS = ['.csv', '.txt', '.xlsx', '.xls']

# URLs base da ANS
URL_ANS_BASE = "https://dadosabertos.ans.gov.br/FTP/PDA"
URL_ANS_DEMONSTRACOES = f"{URL_ANS_BASE}/demonstracoes_contabeis"
URL_ANS_OPERADORAS = f"{URL_ANS_BASE}/operadoras_de_plano_de_saude_ativas"


if __name__ == "__main__":
    # Testes das funções
    print("=== Testes do Módulo de Utilidades ===\n")
    
    # Teste 1: Formatação de CNPJ
    print("1. Formatação de CNPJ:")
    cnpj_limpo = "12345678000190"
    print(f"   Limpo: {cnpj_limpo}")
    print(f"   Formatado: {formatar_cnpj(cnpj_limpo)}\n")
    
    # Teste 2: Validação de CNPJ
    print("2. Validação de CNPJ:")
    valido, msg = validar_formato_cnpj("12345678000190")
    print(f"   12345678000190: {valido} - {msg}")
    valido, msg = validar_formato_cnpj("123")
    print(f"   123: {valido} - {msg}\n")
    
    # Teste 3: Trimestres
    print("3. Cálculos de Trimestre:")
    ano, trim = obter_trimestre_atual()
    print(f"   Trimestre atual: {ano}/Q{trim}")
    ano_ant, trim_ant = calcular_trimestre_anterior(ano, trim, 3)
    print(f"   3 trimestres atrás: {ano_ant}/Q{trim_ant}\n")
    
    # Teste 4: Conversão de bytes
    print("4. Conversão de Bytes:")
    print(f"   1536 bytes: {bytes_para_humano(1536)}")
    print(f"   1048576 bytes: {bytes_para_humano(1048576)}\n")
    
    # Teste 5: Progresso
    print("5. Barra de Progresso:")
    import time
    progresso = ProgressoConsole(50, "Teste")
    for i in range(50):
        time.sleep(0.05)  # Simula processamento
        progresso.atualizar(i + 1)
    progresso.finalizar()