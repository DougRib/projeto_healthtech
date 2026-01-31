"""
ETAPA 2.1: Validação de Dados
==============================

Este módulo realiza validações completas nos dados consolidados:

1. Validação de CNPJ (formato e dígitos verificadores)
2. Validação de valores numéricos
3. Validação de razão social
4. Geração de relatório de qualidade dos dados

Decisão Técnica: Manter registros inválidos com flag
- Não descarta dados
- Mantém rastreabilidade
- Permite análise de qualidade

Autor: [Seu Nome]
Data: 29/01/2025

Uso:
    python validacao.py
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import logging

PROJETO_RAIZ = Path(__file__).resolve().parents[1]
if str(PROJETO_RAIZ) not in sys.path:
    sys.path.insert(0, str(PROJETO_RAIZ))

from integracao_api.utils import configurar_logging, limpar_cnpj


# Configuração de logging
logger = configurar_logging("validacao.log")


class ValidadorDados:
    """
    Classe responsável por validar dados do CSV consolidado.
    
    Validações implementadas:
    - CNPJ: formato e dígitos verificadores
    - Valores: positivos e não nulos
    - Razão Social: não vazia
    - Trimestre: entre 1 e 4
    - Ano: dentro de range válido
    """
    
    def __init__(self, arquivo_entrada: Path):
        """
        Inicializa o validador.
        
        Args:
            arquivo_entrada: Path do arquivo CSV consolidado
        """
        self.arquivo_entrada = arquivo_entrada
        self.df = None
        
        logger.info("="*70)
        logger.info("INICIANDO ETAPA 2.1: VALIDAÇÃO DE DADOS")
        logger.info("="*70)
    
    def carregar_dados(self) -> None:
        """Carrega dados do CSV consolidado."""
        logger.info(f"Carregando dados de: {self.arquivo_entrada}")
        
        try:
            self.df = pd.read_csv(
                self.arquivo_entrada,
                sep=';',
                encoding='utf-8',
                low_memory=False
            )
            print(f"✓ Dados carregados: {len(self.df):,} registros")
            logger.info(f"Dados carregados: {len(self.df)} registros")
        
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            raise
    
    @staticmethod
    def validar_digito_cnpj(cnpj: str) -> bool:
        """
        Valida dígitos verificadores do CNPJ.
        
        Algoritmo:
        1. Multiplica cada dígito por peso específico
        2. Calcula módulo 11
        3. Compara com dígitos verificadores
        
        Args:
            cnpj: CNPJ limpo (14 dígitos)
        
        Returns:
            True se válido, False caso contrário
        
        Exemplo:
            >>> validar_digito_cnpj("11222333000181")
            True
        """
        if not cnpj or len(cnpj) != 14 or not cnpj.isdigit():
            return False
        
        # CNPJ não pode ser sequência de números iguais
        if cnpj == cnpj[0] * 14:
            return False
        
        # Calcular primeiro dígito verificador
        pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        soma_1 = sum(int(cnpj[i]) * pesos_1[i] for i in range(12))
        resto_1 = soma_1 % 11
        digito_1 = 0 if resto_1 < 2 else 11 - resto_1
        
        if int(cnpj[12]) != digito_1:
            return False
        
        # Calcular segundo dígito verificador
        pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        soma_2 = sum(int(cnpj[i]) * pesos_2[i] for i in range(13))
        resto_2 = soma_2 % 11
        digito_2 = 0 if resto_2 < 2 else 11 - resto_2
        
        return int(cnpj[13]) == digito_2
    
    def validar_cnpjs(self) -> pd.DataFrame:
        """
        Valida todos os CNPJs do dataset.
        
        Validações:
        1. Formato (14 dígitos)
        2. Não zerado
        3. Dígitos verificadores
        
        Returns:
            DataFrame com colunas de validação adicionadas
        """
        print("\n📋 Validando CNPJs...")
        logger.info("Iniciando validação de CNPJs")
        
        if 'CNPJ' not in self.df.columns:
            logger.warning("Coluna CNPJ não encontrada!")
            return self.df
        
        # Limpar CNPJs
        self.df['CNPJ'] = self.df['CNPJ'].astype(str).apply(limpar_cnpj)
        
        # Validar formato (vetorizado)
        self.df['cnpj_formato_valido'] = (
            self.df['CNPJ'].str.len().eq(14)
            & self.df['CNPJ'].str.isdigit()
            & (self.df['CNPJ'] != "00000000000000")
        )

        # Validar dígitos verificadores somente para CNPJs com formato válido
        self.df['cnpj_digitos_validos'] = False
        mask_formato_valido = self.df['cnpj_formato_valido'].fillna(False)
        if mask_formato_valido.any():
            self.df.loc[mask_formato_valido, 'cnpj_digitos_validos'] = (
                self.df.loc[mask_formato_valido, 'CNPJ']
                .apply(self.validar_digito_cnpj)
            )
        
        # Status final
        self.df['cnpj_valido'] = self.df['cnpj_formato_valido'] & self.df['cnpj_digitos_validos']
        
        # Motivo da invalidez
        def obter_motivo(row):
            if row['cnpj_valido']:
                return ''
            elif not row['cnpj_formato_valido']:
                return 'FORMATO_INVALIDO'
            elif not row['cnpj_digitos_validos']:
                return 'DIGITOS_VERIFICADORES_INVALIDOS'
            return 'DESCONHECIDO'
        
        self.df['motivo_cnpj_invalido'] = self.df.apply(obter_motivo, axis=1)
        
        # Estatísticas
        total = len(self.df)
        validos = self.df['cnpj_valido'].sum()
        invalidos = total - validos
        
        print(f"  ✓ CNPJs válidos: {validos:,} ({validos/total*100:.1f}%)")
        print(f"  ✗ CNPJs inválidos: {invalidos:,} ({invalidos/total*100:.1f}%)")
        
        if invalidos > 0:
            print(f"\n  Motivos de invalidez:")
            motivos = self.df[~self.df['cnpj_valido']]['motivo_cnpj_invalido'].value_counts()
            for motivo, qtd in motivos.items():
                print(f"    - {motivo}: {qtd:,}")
        
        logger.info(f"Validação de CNPJs concluída: {validos} válidos, {invalidos} inválidos")
        
        return self.df
    
    def validar_valores_numericos(self) -> pd.DataFrame:
        """
        Valida valores numéricos (despesas).
        
        Validações:
        1. Não nulo
        2. Numérico
        3. Positivo
        
        Returns:
            DataFrame com validações
        """
        print("\n💰 Validando valores numéricos...")
        logger.info("Iniciando validação de valores")
        
        if 'ValorDespesas' not in self.df.columns:
            logger.warning("Coluna ValorDespesas não encontrada!")
            return self.df
        
        # Converter para numérico
        self.df['ValorDespesas'] = pd.to_numeric(self.df['ValorDespesas'], errors='coerce')
        
        # Validações
        self.df['valor_nulo'] = self.df['ValorDespesas'].isna()
        self.df['valor_negativo'] = self.df['ValorDespesas'] < 0
        self.df['valor_zerado'] = self.df['ValorDespesas'] == 0
        self.df['valor_valido'] = ~(self.df['valor_nulo'] | self.df['valor_negativo'])
        
        # Estatísticas
        total = len(self.df)
        validos = self.df['valor_valido'].sum()
        nulos = self.df['valor_nulo'].sum()
        negativos = self.df['valor_negativo'].sum()
        zerados = self.df['valor_zerado'].sum()
        
        print(f"  ✓ Valores válidos: {validos:,} ({validos/total*100:.1f}%)")
        if nulos > 0:
            print(f"  ⚠️  Valores nulos: {nulos:,} ({nulos/total*100:.1f}%)")
        if negativos > 0:
            print(f"  ⚠️  Valores negativos: {negativos:,} ({negativos/total*100:.1f}%)")
        if zerados > 0:
            print(f"  ⚠️  Valores zerados: {zerados:,} ({zerados/total*100:.1f}%)")
        
        logger.info(f"Validação de valores concluída: {validos} válidos")
        
        return self.df
    
    def validar_razao_social(self) -> pd.DataFrame:
        """
        Valida razão social.
        
        Validações:
        1. Não vazio
        2. Comprimento mínimo
        
        Returns:
            DataFrame com validações
        """
        print("\n🏢 Validando razões sociais...")
        logger.info("Iniciando validação de razões sociais")
        
        if 'RazaoSocial' not in self.df.columns:
            logger.warning("Coluna RazaoSocial não encontrada!")
            return self.df
        
        # Limpar strings
        self.df['RazaoSocial'] = self.df['RazaoSocial'].astype(str).str.strip()
        
        # Validações
        self.df['razao_vazia'] = (
            self.df['RazaoSocial'].isna() | 
            (self.df['RazaoSocial'] == '') | 
            (self.df['RazaoSocial'] == 'nan')
        )
        
        self.df['razao_muito_curta'] = self.df['RazaoSocial'].str.len() < 3
        self.df['razao_valida'] = ~(self.df['razao_vazia'] | self.df['razao_muito_curta'])
        
        # Estatísticas
        total = len(self.df)
        validas = self.df['razao_valida'].sum()
        vazias = self.df['razao_vazia'].sum()
        curtas = self.df['razao_muito_curta'].sum()
        
        print(f"  ✓ Razões válidas: {validas:,} ({validas/total*100:.1f}%)")
        if vazias > 0:
            print(f"  ⚠️  Razões vazias: {vazias:,} ({vazias/total*100:.1f}%)")
        if curtas > 0:
            print(f"  ⚠️  Razões muito curtas: {curtas:,} ({curtas/total*100:.1f}%)")
        
        logger.info(f"Validação de razões sociais concluída: {validas} válidas")
        
        return self.df
    
    def validar_datas(self) -> pd.DataFrame:
        """
        Valida trimestres e anos.
        
        Returns:
            DataFrame com validações
        """
        print("\n📅 Validando datas (trimestre/ano)...")
        logger.info("Iniciando validação de datas")
        
        # Validar trimestre
        if 'Trimestre' in self.df.columns:
            self.df['Trimestre'] = pd.to_numeric(self.df['Trimestre'], errors='coerce')
            self.df['trimestre_valido'] = self.df['Trimestre'].between(1, 4)
            
            invalidos_trim = (~self.df['trimestre_valido']).sum()
            if invalidos_trim > 0:
                print(f"  ⚠️  Trimestres inválidos: {invalidos_trim:,}")
        
        # Validar ano
        if 'Ano' in self.df.columns:
            self.df['Ano'] = pd.to_numeric(self.df['Ano'], errors='coerce')
            self.df['ano_valido'] = self.df['Ano'].between(2000, 2030)
            
            invalidos_ano = (~self.df['ano_valido']).sum()
            if invalidos_ano > 0:
                print(f"  ⚠️  Anos inválidos: {invalidos_ano:,}")
        
        logger.info("Validação de datas concluída")
        
        return self.df
    
    def gerar_relatorio_qualidade(self, arquivo_saida: Path) -> None:
        """
        Gera relatório detalhado de qualidade dos dados.
        
        Args:
            arquivo_saida: Path para salvar o relatório
        """
        print("\n📊 Gerando relatório de qualidade...")
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("RELATÓRIO DE QUALIDADE DOS DADOS - ETAPA 2.1\n")
            f.write("="*70 + "\n\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"Total de Registros: {len(self.df):,}\n\n")
            
            # CNPJs
            f.write("VALIDAÇÃO DE CNPJs:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Válidos: {self.df['cnpj_valido'].sum():,}\n")
            f.write(f"Inválidos: {(~self.df['cnpj_valido']).sum():,}\n\n")
            
            # Valores
            f.write("VALIDAÇÃO DE VALORES:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Válidos: {self.df['valor_valido'].sum():,}\n")
            f.write(f"Nulos: {self.df['valor_nulo'].sum():,}\n")
            f.write(f"Negativos: {self.df['valor_negativo'].sum():,}\n")
            f.write(f"Zerados: {self.df['valor_zerado'].sum():,}\n\n")
            
            # Razões Sociais
            f.write("VALIDAÇÃO DE RAZÕES SOCIAIS:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Válidas: {self.df['razao_valida'].sum():,}\n")
            f.write(f"Vazias: {self.df['razao_vazia'].sum():,}\n\n")
            
            f.write("="*70 + "\n")
        
        print(f"  ✓ Relatório salvo em: {arquivo_saida}")
        logger.info(f"Relatório de qualidade gerado: {arquivo_saida}")
    
    def salvar_dados_validados(self, arquivo_saida: Path) -> None:
        """
        Salva dados validados em novo CSV.
        
        Args:
            arquivo_saida: Path do arquivo de saída
        """
        print(f"\n💾 Salvando dados validados...")
        
        self.df.to_csv(arquivo_saida, index=False, encoding='utf-8', sep=';')
        
        print(f"  ✓ Dados salvos em: {arquivo_saida}")
        logger.info(f"Dados validados salvos em: {arquivo_saida}")


def main():
    """Função principal."""
    print("="*70)
    print("ETAPA 2.1: VALIDAÇÃO DE DADOS")
    print("="*70)
    
    try:
        # Caminhos
        saida_dir = PROJETO_RAIZ / "output"
        arquivo_entrada = saida_dir / "consolidado_despesas.csv"
        arquivo_saida = saida_dir / "dados_validados.csv"
        arquivo_relatorio = saida_dir / "relatorio_validacao.txt"
        
        # Criar diretório de saída
        arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
        
        # Criar validador
        validador = ValidadorDados(arquivo_entrada)
        
        # Executar validações
        validador.carregar_dados()
        validador.validar_cnpjs()
        validador.validar_valores_numericos()
        validador.validar_razao_social()
        validador.validar_datas()
        
        # Gerar relatório
        validador.gerar_relatorio_qualidade(arquivo_relatorio)
        
        # Salvar dados
        validador.salvar_dados_validados(arquivo_saida)
        
        print("\n" + "="*70)
        print("✅ ETAPA 2.1 CONCLUÍDA COM SUCESSO!")
        print("="*70 + "\n")
        
        return 0
    
    except Exception as e:
        print(f"\n❌ ERRO: {e}\n")
        logger.error(f"Erro na validação: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())