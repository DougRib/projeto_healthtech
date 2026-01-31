-- Execucao completa da Etapa 3 (PostgreSQL 10+)
-- Ajuste os caminhos se seu workspace estiver em outro local.

\i sql/01_ddl.sql

\set consolidado_path 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/consolidado_despesas_sql.csv'
\set agregadas_path 'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/despesas_agregadas_sql.csv'
\set cadastro_path  'C:/Users/dougl/DevProjects/projeto_healthtech/sql_data/operadoras_cadastro.csv'

\i sql/02_import.sql
\i sql/03_queries.sql
