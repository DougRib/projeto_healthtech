-- Etapa 3 - Queries analiticas (PostgreSQL 10+)
SET search_path TO healthtech;

-- Query 1:
-- 5 operadoras com maior crescimento percentual de despesas entre
-- o primeiro e o ultimo trimestre analisado.
-- Decisao: usa o primeiro e ultimo trimestre disponivel por operadora.
-- Exclui casos com valor inicial <= 0 para evitar divisao por zero.
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

-- Query 2:
-- Distribuicao de despesas por UF (top 5) e media por operadora.
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

-- Query 3:
-- Quantas operadoras ficaram acima da media geral
-- em pelo menos 2 dos 3 trimestres analisados?
-- Decisao: media geral calculada sobre todas as despesas consolidadas.
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
