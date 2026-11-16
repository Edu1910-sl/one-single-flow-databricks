# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Camada Gold: o One Single Flow
# MAGIC
# MAGIC Aqui as fontes viram **uma única visão por produto** (a "storyline"): da montagem à expedição, com desvios,
# MAGIC ciclos de reparo e testes. É a **fonte única de verdade (SSOT)** que antes exigia consultar 6 lugares diferentes.
# MAGIC
# MAGIC Tabelas geradas:
# MAGIC - `gold_storyline_produto`: uma linha por produto, com o histórico completo e o status atual
# MAGIC - `gold_kpi_diario`: indicadores por dia e linha (produção, First Pass Yield, taxa de reparo, lead time)
# MAGIC - `gold_pareto_desvios`: quais desvios mais acontecem (onde atacar primeiro)

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

montagem = spark.table(t("silver_montagem"))
desvios = spark.table(t("silver_desvios"))
reparos = spark.table(t("silver_reparos"))
testes = spark.table(t("silver_testes"))
expedicao = spark.table(t("silver_expedicao"))

# Uma linha por produto com as duas etapas de montagem lado a lado
etapas = montagem.groupBy("id").agg(
    F.first("linha", ignorenulls=True).alias("linha"),
    F.min(F.when(F.col("etapa") == "Montagem 1", F.col("inicio"))).alias("inicio_montagem_1"),
    F.min(F.when(F.col("etapa") == "Montagem 1", F.col("fim"))).alias("fim_montagem_1"),
    F.min(F.when(F.col("etapa") == "Montagem 2", F.col("inicio"))).alias("inicio_montagem_2"),
    F.min(F.when(F.col("etapa") == "Montagem 2", F.col("fim"))).alias("fim_montagem_2"),
)

resumo_desvios = desvios.groupBy("id").agg(
    F.count("*").alias("qtd_desvios"),
    F.array_sort(F.collect_set("codigo_desvio")).alias("codigos_desvio"),
)

resumo_reparos = reparos.groupBy("id").agg(
    F.count("*").alias("qtd_ciclos_reparo"),
    F.round(F.sum((F.unix_timestamp("saida") - F.unix_timestamp("entrada")) / 3600), 2).alias("horas_em_reparo"),
    F.max("saida").alias("ultima_saida_reparo"),
)

ultimo_teste = Window.partitionBy("id").orderBy(F.desc("tentativa"))
resumo_testes = (
    testes.withColumn("_ultimo", F.row_number().over(ultimo_teste) == 1)
    .groupBy("id")
    .agg(
        F.count("*").alias("qtd_testes"),
        F.max(F.when(F.col("tentativa") == 1, F.col("status") == "APROVADO")).alias("aprovado_no_primeiro_teste"),
        F.max(F.when(F.col("_ultimo"), F.col("status"))).alias("status_teste_final"),
        F.max("testado_em").alias("ultimo_teste_em"),
    )
)

# COMMAND ----------

storyline = (
    etapas.join(resumo_desvios, "id", "left")
    .join(resumo_reparos, "id", "left")
    .join(resumo_testes, "id", "left")
    .join(expedicao, "id", "left")
    .fillna({"qtd_desvios": 0, "qtd_ciclos_reparo": 0, "qtd_testes": 0, "horas_em_reparo": 0.0})
    .withColumn(
        "status_atual",
        F.when(F.col("expedido_em").isNotNull(), "Expedido")
        .when(F.col("pronto_em").isNotNull(), "Aguardando expedição")
        .when(F.col("status_teste_final") == "APROVADO", "Aprovado no teste")
        .when(F.col("ultima_saida_reparo") > F.coalesce(F.col("ultimo_teste_em"), F.lit("1900-01-01").cast("timestamp")), "Aguardando reteste")
        .when(F.col("fim_montagem_2").isNotNull(), "Aguardando teste")
        .otherwise("Em montagem"),
    )
    # First Pass Yield: passou por tudo sem desvio, sem reparo e aprovado no primeiro teste
    .withColumn(
        "first_pass",
        (F.col("qtd_desvios") == 0) & (F.col("qtd_ciclos_reparo") == 0) & F.coalesce(F.col("aprovado_no_primeiro_teste"), F.lit(False)),
    )
    .withColumn(
        "lead_time_horas",
        F.round((F.unix_timestamp(F.coalesce("expedido_em", "pronto_em")) - F.unix_timestamp("inicio_montagem_1")) / 3600, 1),
    )
    .withColumn("data_producao", F.to_date("inicio_montagem_1"))
    .select(
        "id", "linha", "data_producao", "status_atual",
        "inicio_montagem_1", "fim_montagem_1", "inicio_montagem_2", "fim_montagem_2",
        "qtd_desvios", "codigos_desvio",
        "qtd_ciclos_reparo", "horas_em_reparo",
        "qtd_testes", "aprovado_no_primeiro_teste", "status_teste_final",
        "pronto_em", "expedido_em",
        "first_pass", "lead_time_horas",
    )
)
storyline.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("gold_storyline_produto"))

# COMMAND ----------

storyline = spark.table(t("gold_storyline_produto"))

kpi_diario = (
    storyline.groupBy("data_producao", "linha")
    .agg(
        F.count("*").alias("produtos_iniciados"),
        F.sum(F.col("fim_montagem_2").isNotNull().cast("int")).alias("produtos_montados"),
        F.round(100 * F.avg(F.col("first_pass").cast("int")), 1).alias("fpy_pct"),
        F.round(100 * F.avg((F.col("qtd_ciclos_reparo") > 0).cast("int")), 1).alias("taxa_reparo_pct"),
        F.round(F.avg("lead_time_horas"), 1).alias("lead_time_medio_h"),
    )
    .orderBy("data_producao", "linha")
)
kpi_diario.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("gold_kpi_diario"))

# COMMAND ----------

ordem = Window.orderBy(F.desc("qtd"), "codigo_desvio")
total = Window.partitionBy()

pareto = (
    desvios.groupBy("codigo_desvio")
    .agg(F.count("*").alias("qtd"))
    .join(spark.table(t("dim_codigo_desvio")), "codigo_desvio", "left")
    .withColumn("pct", F.round(100 * F.col("qtd") / F.sum("qtd").over(total), 1))
    .withColumn("pct_acumulado", F.round(100 * F.sum("qtd").over(ordem.rowsBetween(Window.unboundedPreceding, 0)) / F.sum("qtd").over(total), 1))
    .select("codigo_desvio", "nome_desvio", "qtd", "pct", "pct_acumulado")
    .orderBy(F.desc("qtd"))
)
pareto.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("gold_pareto_desvios"))

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {t('gold_storyline_produto')} IS 'Gold: uma linha por produto com o histórico ponta a ponta (fonte única de verdade).'")
spark.sql(f"COMMENT ON TABLE {t('gold_kpi_diario')} IS 'Gold: KPIs diários por linha (FPY, taxa de reparo, lead time).'")
spark.sql(f"COMMENT ON TABLE {t('gold_pareto_desvios')} IS 'Gold: Pareto dos desvios registrados.'")

display(spark.table(t("gold_storyline_produto")).orderBy(F.desc("qtd_desvios")).limit(20))
