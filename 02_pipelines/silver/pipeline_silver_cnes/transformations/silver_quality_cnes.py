# Intenção: camada Silver (parte 1) - padronização, regras de qualidade e quarentena dos estabelecimentos CNES
from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, to_date, expr

BRONZE_TABLE = "lakehouse_cnes.bronze.cnes_estabelecimento_bronze"

# Escopo analítico da Silver: identificação, localização, atributos para SCD, capacidade e dados sensíveis
COLUNAS_TEXTO = [
    "id_estabelecimento_cnes",
    "sigla_uf",
    "id_municipio",
    "cep",
    "tipo_unidade",
    "tipo_gestao",
    "tipo_esfera_administrativa",
    "id_natureza_juridica",
    "tipo_prestador",
    "tipo_nivel_hierarquia",
    "tipo_turno",
    "tipo_pessoa",
    "cpf_cnpj",
    "cnpj_mantenedora",
    "banco",
    "agencia",
    "conta_corrente",
    "numero_alvara"
]

COLUNAS_NUMERICAS = [
    "ano",
    "mes",
    "ano_atualizacao",
    "mes_atualizacao",
    "indicador_vinculo_sus",
    "indicador_atencao_ambulatorial",
    "indicador_atencao_hospitalar",
    "indicador_leito_hospitalar",
    "quantidade_leito_cirurgico",
    "quantidade_leito_clinico",
    "quantidade_leito_complementar"
]

# Regras críticas: violação isola o registro na quarentena
REGRAS_CRITICAS = {
    "id_cnes_preenchido": "id_estabelecimento_cnes IS NOT NULL",
    "id_cnes_formato_7_digitos": "id_estabelecimento_cnes RLIKE '^[0-9]{7}$'",
    "competencia_valida": "ano IS NOT NULL AND mes BETWEEN 1 AND 12",
    "uf_rj": "sigla_uf = 'RJ'",
    "municipio_ibge_rj": "id_municipio RLIKE '^33[0-9]{5}$'",
    "tipo_unidade_preenchido": "tipo_unidade IS NOT NULL",
    "leitos_nao_negativos": (
        "COALESCE(quantidade_leito_cirurgico, 0) >= 0 "
        "AND COALESCE(quantidade_leito_clinico, 0) >= 0 "
        "AND COALESCE(quantidade_leito_complementar, 0) >= 0"
    ),
    "alvara_sem_data_futura": "data_expedicao_alvara IS NULL OR data_expedicao_alvara <= current_date()"
}

# Regras de alerta: violação é registrada nas métricas, mas o registro segue para a Silver
REGRAS_ALERTA = {
    "cep_valido": "cep IS NOT NULL AND cep RLIKE '^[0-9]{8}$' AND cep <> '99999999'",
    "documento_14_digitos": "cpf_cnpj IS NULL OR cpf_cnpj RLIKE '^[0-9]{14}$'"
}

# Lista das regras críticas violadas por registro (COALESCE evita que NULL escape das duas saídas)
EXPR_VIOLADAS = "concat_ws(',', " + ", ".join(
    f"CASE WHEN NOT COALESCE(({regra}), FALSE) THEN '{nome}' END"
    for nome, regra in REGRAS_CRITICAS.items()
) + ")"

PROPRIEDADES = {
    "quality": "silver",
    "delta.enableDeletionVectors": "true"
}


@dp.temporary_view(
    name="vw_cnes_padronizado",
    comment="Padronização da Bronze: escopo de colunas, nulos textuais ('nan', '') convertidos em NULL, datas tipadas"
)
def vw_cnes_padronizado():

    textos = [
        when(
            col(c).cast("string").isin("nan", ""),
            None
        ).otherwise(
            col(c).cast("string")
        ).alias(c)
        for c in COLUNAS_TEXTO
    ]

    return (
        spark.readStream
            .table(BRONZE_TABLE)
            .select(
                *textos,
                *[col(c) for c in COLUNAS_NUMERICAS],
                to_date(col("data_expedicao_alvara").cast("string")).alias("data_expedicao_alvara"),
                col("_ingestion_timestamp"),
                col("_source_file")
            )
    )


@dp.table(
    name="cnes_quality",
    comment="Estabelecimentos CNES com Expectations aplicadas e marcação de regras críticas violadas",
    table_properties=PROPRIEDADES
)
@dp.expect_all({**REGRAS_CRITICAS, **REGRAS_ALERTA})
def cnes_quality():

    return (
        spark.readStream
            .table("vw_cnes_padronizado")
            .withColumn(
                "_regras_violadas",
                expr(EXPR_VIOLADAS)
            )
            .withColumn(
                "is_quarantined",
                expr("_regras_violadas <> ''")
            )
    )


@dp.table(
    name="cnes_quarantine",
    comment="Registros CNES isolados por violarem ao menos uma regra crítica",
    table_properties=PROPRIEDADES
)
def cnes_quarantine():

    return (
        spark.readStream
            .table("cnes_quality")
            .filter("is_quarantined = true")
    )


@dp.table(
    name="cnes_silver",
    comment="Estabelecimentos CNES validados, particionados por competência e otimizados com Z-Order",
    partition_cols=["ano", "mes"],
    table_properties={
        **PROPRIEDADES,
        "pipelines.autoOptimize.zOrderCols": "id_estabelecimento_cnes,id_municipio"
    }
)
def cnes_silver():

    return (
        spark.readStream
            .table("cnes_quality")
            .filter("is_quarantined = false")
            .drop("is_quarantined", "_regras_violadas")
    )

# Conclusão esperada: cnes_quality = cnes_quarantine + cnes_silver, sem registros perdidos entre as saídas