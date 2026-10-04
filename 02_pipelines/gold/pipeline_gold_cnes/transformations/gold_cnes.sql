-- Intenção: camada Gold - publicação de datasets analíticos como materialized views

-- Fato: capacidade instalada por município e competência
CREATE OR REFRESH MATERIALIZED VIEW fato_capacidade_municipio
COMMENT 'Capacidade instalada dos estabelecimentos CNES por município e competência (RJ)'
CLUSTER BY (id_municipio)
TBLPROPERTIES (
    'quality' = 'gold'
)
AS
SELECT
    ano,
    mes,
    ano * 100 + mes AS competencia,
    id_municipio,
    COUNT(*) AS estabelecimentos,
    SUM(CASE WHEN indicador_vinculo_sus = 1 THEN 1 ELSE 0 END) AS estabelecimentos_sus,
    SUM(CASE WHEN indicador_atencao_hospitalar = 1 THEN 1 ELSE 0 END) AS estabelecimentos_hospitalares,
    SUM(
        COALESCE(quantidade_leito_cirurgico, 0)
        + COALESCE(quantidade_leito_clinico, 0)
        + COALESCE(quantidade_leito_complementar, 0)
    ) AS leitos_totais
FROM lakehouse_cnes.silver.cnes_silver
GROUP BY
    ano,
    mes,
    id_municipio;


-- Dimensão: cadastro atual dos estabelecimentos, publicado para consumo analítico
-- (contém CPF de pessoa física e dados bancários, protegidos por máscara no Unity Catalog)
CREATE OR REFRESH MATERIALIZED VIEW dim_estabelecimento
COMMENT 'Cadastro atual dos estabelecimentos CNES (RJ) a partir da SCD Tipo 1 - contém dados sensíveis'
TBLPROPERTIES (
    'quality' = 'gold'
)
AS
SELECT
    id_estabelecimento_cnes,
    id_municipio,
    cep,
    tipo_unidade,
    tipo_gestao,
    tipo_esfera_administrativa,
    id_natureza_juridica,
    tipo_pessoa,
    cpf_cnpj,
    cnpj_mantenedora,
    banco,
    agencia,
    conta_corrente,
    indicador_vinculo_sus,
    indicador_atencao_hospitalar,
    quantidade_leito_cirurgico,
    quantidade_leito_clinico,
    quantidade_leito_complementar,
    competencia_referencia
FROM lakehouse_cnes.silver.dim_estabelecimento_scd1;

-- Conclusão esperada: duas materialized views no schema gold, alimentadas por tabelas de outros pipelines