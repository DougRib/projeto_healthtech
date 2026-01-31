# Projeto Healthtech - Teste Intuitive Care - Douglas Ribeiro

Este repositório contém a implementação das Etapas do teste técnico.
As etapas seguem o enunciado do PDF e priorizam clareza, rastreabilidade e
boas práticas de engenharia de dados.

## Requisitos

- Python 3.13+
- Windows 11 (testado localmente)
- Dependências: `pip install -r requirements.txt`

## Estrutura

- `integracao_api/`: Etapa 1 (download, processamento e consolidação ANS)
- `transformacao/`: Etapa 2 (validação, enriquecimento e agregação)
- `logs/`: logs gerados em execução
- `output/`: arquivos CSV/ZIP gerados por cada etapa

## Etapa 1 — Integração com API ANS

Responsável por baixar os 3 trimestres mais recentes, extrair, normalizar e
consolidar os dados de despesas/eventos/sinistros.

### Como executar

```
python integracao_api/main.py
```

### Saídas

- `output/consolidado_despesas.csv`
- `output/consolidado_despesas.zip`
- `output/relatorio_etapa1.txt`
- `output/inconsistencias_despesas.csv`

### Decisões e trade-offs

- **Processamento incremental** (arquivo a arquivo) para reduzir uso de memória.
- **Normalização flexível de colunas** para lidar com CSV/TXT/XLSX.
- **Inconsistências** são registradas com flags (não descartadas).

## Etapa 2.1 — Validação de Dados

Validação de CNPJ, valores numéricos, razão social e datas.

### Como executar

```
python transformacao/validacao.py
```

### Saídas

- `output/dados_validados.csv`
- `output/relatorio_validacao.txt`

### Decisões e trade-offs

- **CNPJs inválidos**: mantidos com flag para análise posterior.

## Etapa 2.2 — Enriquecimento de Dados

Download do cadastro de operadoras e join com despesas.

### Como executar

```
python transformacao/enriquecimento.py
```

### Saídas

- `output/dados_enriquecidos.csv`
- `output/relatorio_enriquecimento.txt`

### Decisões e trade-offs

- **Left join** para não perder despesas sem cadastro.
- **Cadastro com CNPJs duplicados**: mantém o primeiro registro e registra o volume removido.
- **Pandas em memória** por volume esperado moderado.

## Etapa 2.3 — Agregação e Estatísticas

Agregação por `RazaoSocial` e `UF`, com total, média e desvio padrão.

### Como executar

```
python transformacao/agregacao.py
```

### Saídas

- `output/despesas_agregadas.csv`
- `output/relatorio_agregacao.txt`
- `output/Teste_SeuNome.zip`

### Decisões e trade-offs

- **GroupBy + Sort (Pandas)** para simplicidade e performance adequada.
- **Desvio padrão nulo** para grupos com um único registro.

## Etapas 3 e 4

### Etapa 3 — Banco de Dados e Analise (PostgreSQL 10+)

Esta etapa cria as tabelas, importa os CSVs e executa queries analiticas.
Os scripts estao em `sql/`.

#### Como executar

1) Preparar CSVs com colunas padrao:
```
python sql/preparar_csvs.py
```

2) No `psql`, executar:
```
\i sql/01_ddl.sql
\set consolidado_path 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/consolidado_despesas_sql.csv'
\set agregadas_path 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/despesas_agregadas_sql.csv'
\set cadastro_path  'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/operadoras_cadastro.csv'
\i sql/02_import.sql
\i sql/03_queries.sql
```

3) Atalho (psql):
```
\i sql/run_all.sql
```

4) Alternativa (pgAdmin Query Tool):
```
sql/run_all_pgadmin.sql
```
Observação: no pgAdmin, o `COPY` lê arquivos no servidor. Se não houver permissão,
importe os CSVs via Import/Export Data nas tabelas `stg_*`.

#### Decisoes e trade-offs

- **Normalizacao**: tabelas separadas para cadastro, despesas e agregados.
- **Tipos monetarios**: `NUMERIC(18,2)` por precisao.
- **Datas**: `ano`/`trimestre` como `SMALLINT` (dados originais nao trazem data completa).
- **Importacao**: uso de staging tables para tratar dados inconsistentes.
- **Inconsistencias**: registros com CNPJ/ano/trimestre invalidos sao descartados na carga final,
  mas permanecem no staging para auditoria.
- **UF**: `VARCHAR(20)` para suportar valores como `NÃO_INFORMADO`.

### Etapa 4 — API e Vue

Backend em FastAPI + frontend em Vue 3 (Vite + TypeScript).

#### Como executar o backend

```
cd api_web/backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Variaveis opcionais (.env na raiz, prefixo `HT_`):
- `HT_DATA_CONSOLIDADO_PATH`
- `HT_DATA_AGREGADO_PATH`
- `HT_DATA_CADASTRO_PATH`
- `HT_API_CACHE_TTL_SECONDS`
- `HT_CORS_ORIGINS`

#### Como executar o frontend

```
cd api_web/frontend
npm install
npm run dev
```

#### Responsividade do layout

- **Mobile (até 767px)**: layout em coluna única.
- **Tablet (>= 768px)**: `summary-grid` e `grid` em duas colunas, com tabelas roláveis.
- **Desktop (>= 1200px)**: largura máxima ampliada e cards de resumo em 3 colunas.

#### Gráfico de distribuição por UF

O card “Distribuição de Despesas por UF” possui um seletor de métrica que alterna:

- **Total de despesas por UF** (barra)
- **Média por operadora (UF)** (linha)
- **Quantidade de operadoras por UF** (barra)

Isso permite analisar volume, intensidade média e capilaridade por estado.

#### Tratamento de erros e estados

- **Backend**: validações de entrada, retorno 404 para operadora inexistente e logs em `logs/`.
- **Scripts Python**: `try/except` com logs e geração de relatórios de inconsistências.
- **SQL**: staging tables e conversões seguras para evitar falhas na carga.
- **Frontend**: mensagens para erro de rede/API, loading e dados vazios.

#### Postman

A colecao esta em `api_web/postman/healthtech_collection.json`.

#### Decisoes e trade-offs

- **Backend**: FastAPI pela produtividade e validacao automatica.
- **Paginacao**: offset-based (`page`/`limit`) por simplicidade e previsibilidade.
- **Cache**: estatisticas em memoria com TTL para reduzir custo de agregacao.
- **Resposta**: dados + metadados para facilitar paginação no frontend.
- **Frontend**: busca no servidor para evitar carregar grande volume no cliente.
- **Estado**: estado local no componente principal (escopo simples).
- **Join despesas/cadastro**: em alguns CNPJs não há match; no dashboard, o histórico usa
  fallback por razão social normalizada quando CNPJ não cruza, para não perder análise.

## Observações de execução

- Os scripts usam logs em `logs/` para rastreabilidade.
- Os caminhos de entrada/saída estão relativos ao diretório do script.
- Caso execute em outro ambiente, garanta que o `PYTHONPATH` inclua a raiz do projeto.

## Justificativas técnicas

As decisões e trade-offs solicitados no enunciado estão detalhados em `JUSTIFICATIVAS.md`.

## Testes

Testes unitários (ETL) e da API:
```
python -m unittest discover -s tests -p "test_*.py"
```