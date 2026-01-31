-- Execucao completa da Etapa 3 (pgAdmin Query Tool)
-- Nao usa \i nem \set. Ajuste os caminhos se necessario.
-- Observacao: COPY no pgAdmin le arquivos do servidor PostgreSQL.
-- Se o servidor nao tiver acesso a esses caminhos locais, use
-- o menu Import/Export Data do pgAdmin nas tabelas stg_*

CREATE SCHEMA IF NOT EXISTS healthtech;
SET search_path TO healthtech;

-- DDL (mesmo conteudo do 01_ddl.sql)
CREATE TABLE IF NOT EXISTS operadoras_cadastro (
    cnpj CHAR(14) PRIMARY KEY,
    registro_ans VARCHAR(20),
    razao_social TEXT,
    modalidade TEXT,
    uf VARCHAR(20),
    data_cadastro DATE,
    data_importacao TIMESTAMP DEFAULT NOW()
);

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

TRUNCATE TABLE stg_despesas_consolidadas;
TRUNCATE TABLE stg_despesas_agregadas;
TRUNCATE TABLE stg_operadoras_cadastro;

COPY stg_despesas_consolidadas (cnpj, razao_social, trimestre, ano, valor_despesas, inconsistencia_flag)
FROM 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/consolidado_despesas_sql.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

COPY stg_despesas_agregadas (
    ranking, razao_social, uf, total_despesas, media_despesas, media_por_trimestre,
    desvio_padrao, coeficiente_variacao, numero_trimestres, alta_variabilidade
)
FROM 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/despesas_agregadas_sql.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

COPY stg_operadoras_cadastro (
    registro_ans, cnpj, razao_social, nome_fantasia, modalidade, logradouro, numero,
    complemento, bairro, cidade, uf, cep, ddd, telefone, fax, email, representante,
    cargo_representante, regiao_de_comercializacao, data_registro_ans
)
FROM 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/operadoras_cadastro.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

-- Imports (mesmo conteudo do 02_import.sql, sem \copy)
INSERT INTO operadoras_cadastro (
    cnpj, registro_ans, razao_social, modalidade, uf, data_cadastro
)
SELECT
    cnpj_limpo,
    NULLIF(TRIM(registro_ans), ''),
    NULLIF(TRIM(razao_social), ''),
    NULLIF(TRIM(modalidade), ''),
    NULLIF(TRIM(uf), ''),
    CASE
        WHEN data_registro_ans ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(data_registro_ans, 'DD/MM/YYYY')
        WHEN data_registro_ans ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(data_registro_ans, 'YYYY-MM-DD')
        ELSE NULL
    END
FROM (
    SELECT
        regexp_replace(cnpj, '\D', '', 'g') AS cnpj_limpo,
        registro_ans,
        razao_social,
        modalidade,
        uf,
        data_registro_ans
    FROM stg_operadoras_cadastro
) src
WHERE length(cnpj_limpo) = 14
ON CONFLICT (cnpj) DO UPDATE
SET
    registro_ans = EXCLUDED.registro_ans,
    razao_social = EXCLUDED.razao_social,
    modalidade = EXCLUDED.modalidade,
    uf = EXCLUDED.uf,
    data_cadastro = EXCLUDED.data_cadastro;

WITH cleaned AS (
    SELECT
        regexp_replace(cnpj, '\D', '', 'g') AS cnpj_limpo,
        NULLIF(TRIM(razao_social), '') AS razao_social,
        NULLIF(TRIM(trimestre), '') AS trimestre_txt,
        NULLIF(TRIM(ano), '') AS ano_txt,
        NULLIF(TRIM(inconsistencia_flag), '') AS inconsistencia_flag,
        NULLIF(TRIM(valor_despesas), '') AS valor_raw
    FROM stg_despesas_consolidadas
),
normalized AS (
    SELECT
        cnpj_limpo,
        razao_social,
        CASE WHEN trimestre_txt ~ '^\d+$' THEN trimestre_txt::SMALLINT END AS trimestre,
        CASE WHEN ano_txt ~ '^\d+$' THEN ano_txt::SMALLINT END AS ano,
        inconsistencia_flag,
        CASE
            WHEN valor_raw IS NULL THEN NULL
            WHEN valor_raw ~ ',' THEN replace(replace(valor_raw, '.', ''), ',', '.')
            ELSE valor_raw
        END AS valor_num_txt
    FROM cleaned
)
INSERT INTO despesas_consolidadas (
    cnpj, razao_social, trimestre, ano, valor_despesas, inconsistencia_flag
)
SELECT
    cnpj_limpo,
    razao_social,
    trimestre,
    ano,
    CASE
        WHEN valor_num_txt ~ '^-?\d+(\.\d+)?$' THEN valor_num_txt::NUMERIC(18,2)
        ELSE NULL
    END AS valor_despesas,
    inconsistencia_flag
FROM normalized
WHERE length(cnpj_limpo) = 14
  AND trimestre BETWEEN 1 AND 4
  AND ano BETWEEN 2000 AND 2030
  AND razao_social IS NOT NULL;

WITH cleaned AS (
    SELECT
        NULLIF(TRIM(razao_social), '') AS razao_social,
        NULLIF(TRIM(uf), '') AS uf,
        NULLIF(TRIM(ranking), '') AS ranking_txt,
        NULLIF(TRIM(total_despesas), '') AS total_txt,
        NULLIF(TRIM(media_despesas), '') AS media_txt,
        NULLIF(TRIM(media_por_trimestre), '') AS media_trim_txt,
        NULLIF(TRIM(desvio_padrao), '') AS desvio_txt,
        NULLIF(TRIM(coeficiente_variacao), '') AS coef_txt,
        NULLIF(TRIM(numero_trimestres), '') AS num_trim_txt,
        NULLIF(TRIM(alta_variabilidade), '') AS alta_txt
    FROM stg_despesas_agregadas
),
normalized AS (
    SELECT
        razao_social,
        uf,
        CASE WHEN ranking_txt ~ '^\d+$' THEN ranking_txt::INT END AS ranking,
        CASE WHEN total_txt ~ '^-?\d+(\.\d+)?$' THEN total_txt::NUMERIC(18,2) END AS total_despesas,
        CASE WHEN media_txt ~ '^-?\d+(\.\d+)?$' THEN media_txt::NUMERIC(18,2) END AS media_despesas,
        CASE WHEN media_trim_txt ~ '^-?\d+(\.\d+)?$' THEN media_trim_txt::NUMERIC(18,2) END AS media_por_trimestre,
        CASE WHEN desvio_txt ~ '^-?\d+(\.\d+)?$' THEN desvio_txt::NUMERIC(18,2) END AS desvio_padrao,
        CASE WHEN coef_txt ~ '^-?\d+(\.\d+)?$' THEN coef_txt::NUMERIC(10,2) END AS coeficiente_variacao,
        CASE WHEN num_trim_txt ~ '^\d+$' THEN num_trim_txt::INT END AS numero_trimestres,
        CASE
            WHEN lower(alta_txt) IN ('true', 't', '1', 'yes', 'y') THEN TRUE
            WHEN lower(alta_txt) IN ('false', 'f', '0', 'no', 'n') THEN FALSE
            ELSE NULL
        END AS alta_variabilidade
    FROM cleaned
)
INSERT INTO despesas_agregadas (
    razao_social, uf, ranking, total_despesas, media_despesas, media_por_trimestre,
    desvio_padrao, coeficiente_variacao, numero_trimestres, alta_variabilidade
)
SELECT
    razao_social, uf, ranking, total_despesas, media_despesas, media_por_trimestre,
    desvio_padrao, coeficiente_variacao, numero_trimestres, alta_variabilidade
FROM normalized
WHERE razao_social IS NOT NULL
  AND uf IS NOT NULL
ON CONFLICT (razao_social, uf) DO UPDATE
SET
    ranking = EXCLUDED.ranking,
    total_despesas = EXCLUDED.total_despesas,
    media_despesas = EXCLUDED.media_despesas,
    media_por_trimestre = EXCLUDED.media_por_trimestre,
    desvio_padrao = EXCLUDED.desvio_padrao,
    coeficiente_variacao = EXCLUDED.coeficiente_variacao,
    numero_trimestres = EXCLUDED.numero_trimestres,
    alta_variabilidade = EXCLUDED.alta_variabilidade;

-- Queries (mesmo conteudo do 03_queries.sql)
WITH despesas_trimestre AS (
    SELECT
        cnpj,
        razao_social,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_trimestre
    FROM despesas_consolidadas
    GROUP BY cnpj, razao_social, ano, trimestre
),
ranked AS (
    SELECT
        cnpj,
        razao_social,
        ano,
        trimestre,
        total_trimestre,
        ROW_NUMBER() OVER (PARTITION BY cnpj ORDER BY ano, trimestre) AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY cnpj ORDER BY ano DESC, trimestre DESC) AS rn_last
    FROM despesas_trimestre
),
pontos AS (
    SELECT
        cnpj,
        MAX(CASE WHEN rn_first = 1 THEN total_trimestre END) AS valor_inicial,
        MAX(CASE WHEN rn_last = 1 THEN total_trimestre END) AS valor_final
    FROM ranked
    GROUP BY cnpj
)
SELECT
    d.cnpj,
    d.razao_social,
    p.valor_inicial,
    p.valor_final,
    ROUND(((p.valor_final - p.valor_inicial) / p.valor_inicial) * 100, 2) AS crescimento_percentual
FROM pontos p
JOIN despesas_consolidadas d ON d.cnpj = p.cnpj
WHERE p.valor_inicial IS NOT NULL
  AND p.valor_final IS NOT NULL
  AND p.valor_inicial > 0
GROUP BY d.cnpj, d.razao_social, p.valor_inicial, p.valor_final
ORDER BY crescimento_percentual DESC
LIMIT 5;

WITH despesas_por_uf AS (
    SELECT
        o.uf,
        SUM(d.valor_despesas) AS total_despesas,
        COUNT(DISTINCT d.cnpj) AS total_operadoras
    FROM despesas_consolidadas d
    JOIN operadoras_cadastro o ON o.cnpj = d.cnpj
    GROUP BY o.uf
)
SELECT
    uf,
    total_despesas,
    ROUND(total_despesas / NULLIF(total_operadoras, 0), 2) AS media_por_operadora
FROM despesas_por_uf
ORDER BY total_despesas DESC
LIMIT 5;

WITH media_geral AS (
    SELECT AVG(valor_despesas) AS media
    FROM despesas_consolidadas
),
despesas_trimestre AS (
    SELECT
        cnpj,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_trimestre
    FROM despesas_consolidadas
    GROUP BY cnpj, ano, trimestre
),
contagem AS (
    SELECT
        d.cnpj,
        SUM(CASE WHEN d.total_trimestre > m.media THEN 1 ELSE 0 END) AS trimestres_acima
    FROM despesas_trimestre d
    CROSS JOIN media_geral m
    GROUP BY d.cnpj
)
SELECT COUNT(*) AS operadoras_acima_2_trimestres
FROM contagem
WHERE trimestres_acima >= 2;
