# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Camada Silver: limpeza, padronização e qualidade na origem
# MAGIC
# MAGIC O que esta camada resolve:
# MAGIC 1. **Um ID único por produto**: `" 1000123 "`, `"PRD-1000123"` e `"prd1000123"` viram `1000123`.
# MAGIC 2. **Datas em formatos diferentes** viram um único tipo `timestamp`.
# MAGIC 3. **Duplicidades** são removidas.
# MAGIC 4. **Texto livre vira código**: a descrição digitada à mão é ligada a um código padrão por uma tabela de sinônimos,
# MAGIC    e não pelo texto em si (que tem erros de digitação).
# MAGIC 5. **Quarentena**: registros sem ID válido não são descartados em silêncio, vão para uma tabela própria para correção.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "one_single_flow")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")


def t(nome):
    return f"{CATALOG}.{SCHEMA}.{nome}"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

ACENTOS_DE = "áàâãäéèêëíìîïóòôõöúùûüç"
ACENTOS_PARA = "aaaaaeeeeiiiiooooouuuuc"


def id_padrao(col):
    """Mantém só os dígitos. Um ID válido tem exatamente 7 dígitos; o resto vira nulo (e vai para a quarentena)."""
    so_digitos = F.regexp_replace(F.trim(col), "[^0-9]", "")
    return F.when(F.length(so_digitos) == 7, so_digitos)


def data_padrao(col):
    """Aceita os dois formatos que aparecem nas fontes. try_to_timestamp não quebra com valores inválidos."""
    return F.coalesce(
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(col, F.lit("dd/MM/yyyy HH:mm")),
    )


def texto_padrao(col):
    sem_acento = F.translate(F.lower(F.trim(col)), ACENTOS_DE, ACENTOS_PARA)
    return F.regexp_replace(sem_acento, r"\s+", " ")


quarentena = []  # lista de DataFrames com registros rejeitados


def separar_invalidos(df, fonte):
    """Devolve só os registros com ID válido e guarda os inválidos na quarentena."""
    df = df.withColumn("id", id_padrao(F.col("id_produto")))
    invalidos = df.filter(F.col("id").isNull()).select(
        F.lit(fonte).alias("fonte"),
        F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_") and c != "id"])).alias("registro"),
        F.lit("ID do produto inválido ou ausente").alias("motivo"),
        F.col("_arquivo_origem"),
    )
    quarentena.append(invalidos)
    return df.filter(F.col("id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Montagem (etapas 1 e 2 numa única tabela)

# COMMAND ----------

def preparar_montagem(nome_tabela, etapa):
    df = separar_invalidos(spark.table(t(nome_tabela)), nome_tabela)
    df = df.select(
        "id",
        F.lit(etapa).alias("etapa"),
        "linha",
        "posto",
        data_padrao(F.col("inicio")).alias("inicio"),
        data_padrao(F.col("fim")).alias("fim"),
        "operador",
        "_ingerido_em",
    )
    # Duplicidade: mesmo produto na mesma etapa → fica o primeiro registro
    w = Window.partitionBy("id", "etapa").orderBy(F.col("inicio").asc_nulls_last(), F.col("_ingerido_em"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_ingerido_em")


silver_montagem = preparar_montagem("bronze_montagem_1", "Montagem 1").unionByName(
    preparar_montagem("bronze_montagem_2", "Montagem 2")
)
silver_montagem.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_montagem"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Desvios: texto livre → código padrão

# COMMAND ----------

SINONIMOS = {
    "VAZ_OLEO": ("Vazamento de óleo", ["vazamento de oleo", "vazament de oleo", "vazamento oleo"]),
    "TORQUE": ("Torque fora do especificado", ["torque fora do especificado", "torque fora", "torq fora espec"]),
    "PECA_FALT": ("Falta de peça", ["falta de peca", "peca faltando", "falta peca"]),
    "RISCO": ("Risco na carcaça", ["risco na carcaca", "riscado", "risco carcaca"]),
    "CONECTOR": ("Conector solto ou mal encaixado", ["conector solto", "conector mal encaixado", "conect solto"]),
}

dim_codigo = spark.createDataFrame(
    [(codigo, nome) for codigo, (nome, _) in SINONIMOS.items()] + [("NAO_CLASSIFICADO", "Descrição sem padrão: revisar")],
    "codigo_desvio string, nome_desvio string",
)
dim_codigo.write.mode("overwrite").saveAsTable(t("dim_codigo_desvio"))

dim_sinonimo = spark.createDataFrame(
    [(sinonimo, codigo) for codigo, (_, lista) in SINONIMOS.items() for sinonimo in lista],
    "texto_normalizado string, codigo_desvio string",
)

desvios = separar_invalidos(spark.table(t("bronze_desvios_manuais")), "bronze_desvios_manuais")
silver_desvios = (
    desvios.withColumn("texto_normalizado", texto_padrao(F.col("descricao")))
    .join(F.broadcast(dim_sinonimo), "texto_normalizado", "left")
    .select(
        "id",
        "etapa",
        F.coalesce("codigo_desvio", F.lit("NAO_CLASSIFICADO")).alias("codigo_desvio"),
        F.col("descricao").alias("descricao_original"),
        data_padrao(F.col("registrado_em")).alias("registrado_em"),
        "operador",
    )
    .dropDuplicates(["id", "etapa", "codigo_desvio", "registrado_em"])
)
silver_desvios.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_desvios"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reparos, testes e expedição

# COMMAND ----------

reparos = separar_invalidos(spark.table(t("bronze_reparos")), "bronze_reparos")
silver_reparos = (
    reparos.select(
        "id",
        "origem",
        data_padrao(F.col("entrada")).alias("entrada"),
        data_padrao(F.col("saida")).alias("saida"),
        "resultado",
    )
    .dropDuplicates(["id", "entrada"])
    .withColumn("ciclo", F.row_number().over(Window.partitionBy("id").orderBy("entrada")))
)
silver_reparos.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_reparos"))

testes = separar_invalidos(spark.table(t("bronze_testes")), "bronze_testes")
silver_testes = (
    testes.select(
        "id",
        data_padrao(F.col("testado_em")).alias("testado_em"),
        F.upper(F.trim("status")).alias("status"),
        F.expr("try_cast(torque_nm AS DOUBLE)").alias("torque_nm"),
        "bancada",
    )
    .dropDuplicates(["id", "testado_em"])
    .withColumn("tentativa", F.row_number().over(Window.partitionBy("id").orderBy("testado_em")))
)
silver_testes.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_testes"))

expedicao = separar_invalidos(spark.table(t("bronze_expedicao")), "bronze_expedicao")
silver_expedicao = expedicao.select(
    "id",
    data_padrao(F.col("pronto_em")).alias("pronto_em"),
    data_padrao(F.col("expedido_em")).alias("expedido_em"),
).dropDuplicates(["id"])
silver_expedicao.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_expedicao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarentena

# COMMAND ----------

from functools import reduce

silver_quarentena = reduce(lambda a, b: a.unionByName(b), quarentena).withColumn("detectado_em", F.current_timestamp())
silver_quarentena.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_quarentena"))

for nome in ["silver_montagem", "silver_desvios", "silver_reparos", "silver_testes", "silver_expedicao", "silver_quarentena"]:
    print(f"{nome}: {spark.table(t(nome)).count()} linhas")

# COMMAND ----------

display(
    spark.table(t("silver_desvios"))
    .groupBy("codigo_desvio")
    .count()
    .orderBy(F.desc("count"))
)
