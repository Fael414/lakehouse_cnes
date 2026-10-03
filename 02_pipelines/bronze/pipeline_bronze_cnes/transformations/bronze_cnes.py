# Intenção: ingestão incremental dos arquivos JSON do CNES via Auto Loader (camada Bronze)
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

SOURCE_PATH = "/Volumes/lakehouse_cnes/bronze/raw_files/estabelecimento"

SCHEMA_HINTS = ", ".join([
    "id_estabelecimento_cnes STRING",
    "id_municipio STRING",
    "cep STRING",
    "cpf_cnpj STRING",
    "cnpj_mantenedora STRING",
    "agencia STRING",
    "conta_corrente STRING"
])


@dp.table(
    name="cnes_estabelecimento_bronze",
    comment="Estabelecimentos CNES (RJ) brutos, ingeridos via Auto Loader a partir do Volume",
    table_properties={
        "quality": "bronze",
        "delta.enableDeletionVectors": "true"
    }
)
def cnes_estabelecimento_bronze():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaHints", SCHEMA_HINTS)
            .option("pathGlobFilter", "*.json")
            .load(SOURCE_PATH)

            # Metadados técnicos de ingestão (rastreabilidade)
            .withColumn(
                "_ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "_source_file",
                col("_metadata.file_path")
            )
    )

# Conclusão esperada: streaming table com ~200 colunas da fonte + 2 colunas técnicas