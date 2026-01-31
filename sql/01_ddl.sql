-- Etapa 3 - DDL (PostgreSQL 10+)
-- Escolha tecnica: schema normalizado para separar cadastro, despesas e agregados.
-- Tipos: valores monetarios em NUMERIC(18,2) por precisao; datas como ano/trimestre (SMALLINT).

CREATE SCHEMA IF NOT EXISTS healthtech;
SET search_path TO healthtech;

-- Tabela de cadastro de operadoras (normalizada)
CREATE TABLE IF NOT EXISTS operadoras_cadastro (
    cnpj CHAR(14) PRIMARY KEY,
    registro_ans VARCHAR(20),
    razao_social TEXT,
    modalidade TEXT,
    uf VARCHAR(20),
    data_cadastro DATE,
    data_importacao TIMESTAMP DEFAULT NOW()
);

-- Tabela de despesas consolidadas (Etapa 1)
CREATE TABLE IF NOT EXISTS despesas_consolidadas (
    id BIGSERIAL PRIMARY KEY,
    cnpj CHAR(14) NOT NULL,
    razao_social TEXT NOT NULL,
    trimestre SMALLINT NOT NULL,
    ano SMALLINT NOT NULL,
    valor_despesas NUMERIC(18,2),
    inconsistencia_flag TEXT,
    data_importacao TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_despesas_cnpj ON despesas_consolidadas (cnpj);
CREATE INDEX IF NOT EXISTS idx_despesas_ano_trim ON despesas_consolidadas (ano, trimestre);

-- Tabela de despesas agregadas (Etapa 2.3)
CREATE TABLE IF NOT EXISTS despesas_agregadas (
    razao_social TEXT NOT NULL,
    uf VARCHAR(20) NOT NULL,
    ranking INT,
    total_despesas NUMERIC(18,2),
    media_despesas NUMERIC(18,2),
    media_por_trimestre NUMERIC(18,2),
    desvio_padrao NUMERIC(18,2),
    coeficiente_variacao NUMERIC(10,2),
    numero_trimestres INT,
    alta_variabilidade BOOLEAN,
    data_importacao TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (razao_social, uf)
);

-- Staging tables (texto) para carga e validacao
CREATE TABLE IF NOT EXISTS stg_despesas_consolidadas (
    cnpj TEXT,
    razao_social TEXT,
    trimestre TEXT,
    ano TEXT,
    valor_despesas TEXT,
    inconsistencia_flag TEXT
);

CREATE TABLE IF NOT EXISTS stg_despesas_agregadas (
    ranking TEXT,
    razao_social TEXT,
    uf TEXT,
    total_despesas TEXT,
    media_despesas TEXT,
    media_por_trimestre TEXT,
    desvio_padrao TEXT,
    coeficiente_variacao TEXT,
    numero_trimestres TEXT,
    alta_variabilidade TEXT
);

-- Staging para o cadastro ANS (ordem esperada do CSV Relatorio_cadop)
CREATE TABLE IF NOT EXISTS stg_operadoras_cadastro (
    registro_ans TEXT,
    cnpj TEXT,
    razao_social TEXT,
    nome_fantasia TEXT,
    modalidade TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cidade TEXT,
    uf TEXT,
    cep TEXT,
    ddd TEXT,
    telefone TEXT,
    fax TEXT,
    email TEXT,
    representante TEXT,
    cargo_representante TEXT,
    regiao_de_comercializacao TEXT,
    data_registro_ans TEXT
);

ALTER TABLE IF EXISTS stg_operadoras_cadastro
    ADD COLUMN IF NOT EXISTS regiao_de_comercializacao TEXT;
