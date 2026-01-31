# Justificativas Técnicas - Douglas Ribeiro

Este documento reúne as decisões e trade-offs solicitados no enunciado.
As escolhas foram feitas priorizando simplicidade, rastreabilidade e execução local.

## Etapa 1 — Integração ANS

### Processamento incremental vs. em memória
- **Escolha**: processamento incremental (arquivo a arquivo).
- **Prós**: menor uso de memória; mais robusto para arquivos grandes.
- **Contras**: execução pode ser um pouco mais lenta que o processamento em lote.

### Normalização de formatos (CSV/TXT/XLSX)
- **Escolha**: detecção automática de formato e normalização por mapeamento de colunas.
- **Prós**: resiliente a variações de estrutura; reduz falhas.
- **Contras**: exige manutenção do dicionário de mapeamento.

### Tratamento de inconsistências
- **Escolha**: não descartar; marcar com flags e gerar relatório separado.
- **Prós**: preserva evidências e permite auditoria.
- **Contras**: exige consumo consciente do relatório ao analisar.
- **Flags principais**: `CNPJ_INVALIDO`, `CNPJ_SEM_MATCH`, `RAZAO_VAZIA`,
  `VALOR_NULO`, `VALOR_NEGATIVO`, `VALOR_ZERADO`, `TRIMESTRE_INVALIDO`, `ANO_INVALIDO`.

## Etapa 2.1 — Validação

### CNPJs inválidos
- **Escolha**: manter registros com flag.
- **Prós**: rastreabilidade e possibilidade de correção futura.
- **Contras**: aumenta ruído nos dados para consumo direto.

## Etapa 2.2 — Enriquecimento

### Estratégia de join
- **Escolha**: `LEFT JOIN` pelo CNPJ.
- **Prós**: não perde despesas sem cadastro.
- **Contras**: surgem linhas sem `RegistroANS/Modalidade/UF`.

### Duplicados no cadastro
- **Escolha**: manter o primeiro registro e registrar quantidade removida.
- **Prós**: evita explosão de linhas no join.
- **Contras**: pode ocultar divergências de cadastro.

### Processamento (Pandas vs. alternativas)
- **Escolha**: Pandas em memória.
- **Prós**: simples, suficiente para o volume esperado.
- **Contras**: não escala bem para dezenas de milhões de linhas.

## Etapa 2.3 — Agregação

### Ordenação
- **Escolha**: ordenação padrão do Pandas (quicksort).
- **Prós**: performance adequada para ~1k grupos.
- **Contras**: não é estável em todos os casos; sem vantagem para cargas muito grandes.

### Desvio padrão em grupos com 1 registro
- **Escolha**: preencher com 0.
- **Prós**: evita NaN e facilita leitura.
- **Contras**: pode ocultar baixa amostra.

## Etapa 3 — Banco de Dados

### Normalização
- **Escolha**: tabelas separadas (cadastro, consolidadas, agregadas).
- **Prós**: melhor manutenção e qualidade analítica.
- **Contras**: mais joins em queries.

### Tipos de dados monetários
- **Escolha**: `NUMERIC(18,2)`.
- **Prós**: precisão financeira.
- **Contras**: maior custo de armazenamento vs. float.

### Datas
- **Escolha**: `SMALLINT` para `ano`/`trimestre`.
- **Prós**: dados não trazem data completa; consulta simples.
- **Contras**: não permite granularidade diária.

### Importação
- **Escolha**: staging tables + limpeza.
- **Prós**: tratamento de inconsistências e auditoria.
- **Contras**: etapa adicional de carga.

### UF
- **Escolha**: `VARCHAR(20)`.
- **Motivo**: suportar `NÃO_INFORMADO`.

## Etapa 4 — API e Frontend

### Framework
- **Escolha**: FastAPI.
- **Prós**: produtividade, validação automática.
- **Contras**: curva para quem só conhece Flask.

### Paginação
- **Escolha**: offset-based (`page/limit`).
- **Prós**: simples, previsível.
- **Contras**: performance pior em grandes offsets.

### Cache de estatísticas
- **Escolha**: cache em memória com TTL.
- **Prós**: evita recomputar agregações.
- **Contras**: não persiste entre reinícios.

### Resposta paginada
- **Escolha**: dados + metadados (`data/total/page/limit`).
- **Prós**: facilita paginação no frontend.

### Busca
- **Escolha**: busca no servidor.
- **Prós**: não carrega tudo no cliente.

### Join despesas/cadastro na UI
- **Escolha**: fallback por razão social normalizada quando CNPJ não cruza.
- **Prós**: evita histórico vazio em casos de mismatch.
- **Contras**: risco de homônimos; precisa ser documentado.
