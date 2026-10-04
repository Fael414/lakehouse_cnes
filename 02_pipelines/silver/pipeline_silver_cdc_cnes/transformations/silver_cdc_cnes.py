# Intenção: CDC por snapshot de competência sobre a Silver, gerando dimensões SCD Tipo 1 e Tipo 2
from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit

SILVER_TABLE = "lakehouse_cnes.silver.cnes_silver"

CHAVE = ["id_estabelecimento_cnes"]

# Atributos de negócio cuja mudança gera nova versão no SCD2
ATRIBUTOS_RASTREADOS = [
    "id_municipio",
    "cep",
    "tipo_unidade",
    "tipo_gestao",
    "tipo_esfera_administrativa",
    "id_natureza_juridica",
    "tipo_prestador",
    "tipo_pessoa",
    "cpf_cnpj",
    "cnpj_mantenedora",
    "indicador_vinculo_sus",
    "indicador_atencao_ambulatorial",
    "indicador_atencao_hospitalar",
    "indicador_leito_hospitalar",
    "quantidade_leito_cirurgico",
    "quantidade_leito_clinico",
    "quantidade_leito_complementar"
]

# Colunas que compõem a dimensão (atributos rastreados + descritivos sem histórico)
COLUNAS_DIMENSAO = CHAVE + ATRIBUTOS_RASTREADOS + [
    "sigla_uf",
    "tipo_nivel_hierarquia",
    "tipo_turno",
    "banco",
    "agencia",
    "conta_corrente",
    "numero_alvara",
    "data_expedicao_alvara",
    "ano_atualizacao",
    "mes_atualizacao"
]


def proximo_snapshot(ultima_versao):

    df = spark.read.table(SILVER_TABLE)

    # Competências disponíveis no formato AAAAMM, em ordem cronológica
    versoes = sorted(
        r["versao"]
        for r in df.select((col("ano") * 100 + col("mes")).alias("versao")).distinct().collect()
    )

    pendentes = [
        v for v in versoes
        if ultima_versao is None or v > ultima_versao
    ]

    # Nenhuma competência nova: encerra o fluxo
    if not pendentes:
        return None

    versao = pendentes[0]

    snapshot = (
        df
        .filter((col("ano") * 100 + col("mes")) == versao)
        .select(*COLUNAS_DIMENSAO)
        .withColumn(
            "competencia_referencia",
            lit(versao)
        )
    )

    return snapshot, versao


# SCD Tipo 1: estado atual de cada estabelecimento (sobrescreve mudanças e remove os que saíram do cadastro)
dp.create_streaming_table(
    name="dim_estabelecimento_scd1",
    comment="Estabelecimentos CNES (RJ) - estado atual, SCD Tipo 1",
    table_properties={
        "quality": "silver",
        "delta.enableDeletionVectors": "true"
    }
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_estabelecimento_scd1",
    source=proximo_snapshot,
    keys=CHAVE,
    stored_as_scd_type=1
)


# SCD Tipo 2: histórico de versões, com nova versão apenas quando um atributo de negócio muda
dp.create_streaming_table(
    name="dim_estabelecimento_scd2",
    comment="Estabelecimentos CNES (RJ) - histórico de atributos de negócio, SCD Tipo 2",
    table_properties={
        "quality": "silver",
        "delta.enableDeletionVectors": "true"
    }
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_estabelecimento_scd2",
    source=proximo_snapshot,
    keys=CHAVE,
    stored_as_scd_type=2,
    track_history_column_list=ATRIBUTOS_RASTREADOS
)

# Conclusão esperada: SCD1 com o estado de 2026/02; SCD2 com versões abertas e fechadas ao longo de 202512 → 202602