# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Camada Bronze: ingestão bruta
# MAGIC
# MAGIC Regra da Bronze: **guardar exatamente o que chegou**, sem corrigir nada, e acrescentar metadados de rastreabilidade
# MAGIC (de qual arquivo veio e quando foi ingerido). Se algo der errado nas próximas camadas, dá para reprocessar a partir daqui.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "one_single_flow")
dbutils.widgets.text("raw_path", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW = dbutils.widgets.get("raw_path") or f"/Volumes/{CATALOG}/{SCHEMA}/raw"


def t(nome):
    return f"{CATALOG}.{SCHEMA}.{nome}"

# COMMAND ----------

from pyspark.sql import functions as F

FONTES = {
    # nome da fonte: formato do arquivo
    "montagem_1": "csv",
    "montagem_2": "csv",
    "desvios_manuais": "json",
    "reparos": "csv",
    "testes": "json",
    "expedicao": "csv",
}


def ler_bruto(nome, formato):
    leitor = spark.read
    if formato == "csv":
        df = leitor.option("header", True).csv(f"{RAW}/{nome}")  # tudo como texto: nada de inferir tipos na Bronze
    else:
        df = leitor.option("primitivesAsString", True).json(f"{RAW}/{nome}")
    return (
        df.select(
            "*",
            F.col("_metadata.file_path").alias("_arquivo_origem"),
            F.current_timestamp().alias("_ingerido_em"),
        )
    )


for nome, formato in FONTES.items():
    df = ler_bruto(nome, formato)
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t(f"bronze_{nome}"))
    print(f"bronze_{nome}: {spark.table(t(f'bronze_{nome}')).count()} linhas")

# COMMAND ----------

# Documentação no próprio catálogo (governança no Unity Catalog)
for nome in FONTES:
    spark.sql(f"COMMENT ON TABLE {t('bronze_' + nome)} IS 'Bronze: dado bruto da fonte {nome}, sem tratamento. Colunas _arquivo_origem e _ingerido_em para rastreabilidade.'")

# COMMAND ----------

display(spark.table(t("bronze_desvios_manuais")).limit(10))
