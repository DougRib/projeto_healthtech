-- Etapa 3 - Importacao (PostgreSQL 10+)
-- Ajuste os caminhos abaixo conforme o local dos CSVs.

-- Exemplo de uso no psql:
-- \set consolidado_path 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/consolidado_despesas_sql.csv'
-- \set agregadas_path 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/despesas_agregadas_sql.csv'
-- \set cadastro_path  'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/operadoras_cadastro.csv'

SET search_path TO healthtech;

TRUNCATE TABLE stg_despesas_consolidadas;
TRUNCATE TABLE stg_despesas_agregadas;
TRUNCATE TABLE stg_operadoras_cadastro;

-- Carga dos CSVs (UTF-8, separador ';')
\copy stg_despesas_consolidadas (cnpj, razao_social, trimestre, ano, valor_despesas, inconsistencia_flag)
FROM :'consolidado_path' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

\copy stg_despesas_agregadas (
    ranking, razao_social, uf, total_despesas, media_despesas, media_por_trimestre,
    desvio_padrao, coeficiente_variacao, numero_trimestres, alta_variabilidade
)
FROM :'agregadas_path' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

-- Atenção: o CSV do cadastro segue a ordem esperada em stg_operadoras_cadastro.
\copy stg_operadoras_cadastro (
    registro_ans, cnpj, razao_social, nome_fantasia, modalidade, logradouro, numero,
    complemento, bairro, cidade, uf, cep, ddd, telefone, fax, email, representante,
    cargo_representante, regiao_de_comercializacao, data_registro_ans
)
FROM :'cadastro_path' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

-- Limpeza e carga final: operadoras_cadastro
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

-- Limpeza e carga final: despesas_consolidadas
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

-- Limpeza e carga final: despesas_agregadas
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
