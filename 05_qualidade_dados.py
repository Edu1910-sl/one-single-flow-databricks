# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Qualidade de dados
# MAGIC
# MAGIC Regras automáticas que rodam no fim do pipeline. O resultado de cada execução fica salvo na tabela
# MAGIC `dq_resultados` (histórico). Se uma regra **crítica** falhar, o notebook gera erro e o Job fica vermelho:
# MAGIC melhor parar do que publicar um dado errado no dashboard.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "one_single_flow")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")


def t(nome):
    return f"{CATALOG}.{SCHEMA}.{nome}"

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

storyline = spark.table(t("gold_storyline_produto"))
montagem = spark.table(t("silver_montagem"))
desvios = spark.table(t("silver_desvios"))
quarentena = spark.table(t("silver_quarentena"))

total_bronze = sum(
    spark.table(t(f"bronze_{n}")).count()
    for n in ["montagem_1", "montagem_2", "desvios_manuais", "reparos", "testes", "expedicao"]
)

resultados = []


def regra(nome, valor, limite, ok, critica=True):
    resultados.append((nome, float(valor), str(limite), "OK" if ok else ("FALHA" if critica else "ALERTA"), critica))

# COMMAND ----------

# 1. A storyline tem exatamente uma linha por produto
duplicados = storyline.groupBy("id").count().filter("count > 1").count()
regra("storyline: id único", duplicados, "= 0", duplicados == 0)

# 2. Nenhum produto sem ID na camada Silver
nulos = montagem.filter(F.col("id").isNull()).count()
regra("silver_montagem: id não nulo", nulos, "= 0", nulos == 0)

# 3. Fim nunca antes do início
datas_invertidas = montagem.filter(F.col("fim") < F.col("inicio")).count()
regra("silver_montagem: fim >= início", datas_invertidas, "= 0", datas_invertidas == 0)

# 4. Datas que não foram convertidas (formato desconhecido)
datas_nulas = montagem.filter(F.col("inicio").isNull() | F.col("fim").isNull()).count()
regra("silver_montagem: datas convertidas", datas_nulas, "= 0", datas_nulas == 0)

# 5. Todo desvio aponta para um produto que existe na montagem (integridade referencial)
orfaos = desvios.join(montagem.select("id").distinct(), "id", "left_anti").count()
regra("silver_desvios: produto existe", orfaos, "= 0", orfaos == 0)

# 6. Quarentena abaixo de 2% do volume bruto
pct_quarentena = 100 * quarentena.count() / max(total_bronze, 1)
regra("quarentena: % do volume bruto", round(pct_quarentena, 2), "< 2%", pct_quarentena < 2)

# 7. Desvios sem padrão (alerta, não bloqueia): sinal de que a tabela de sinônimos precisa crescer
pct_sem_padrao = 100 * desvios.filter("codigo_desvio = 'NAO_CLASSIFICADO'").count() / max(desvios.count(), 1)
regra("desvios: % não classificados", round(pct_sem_padrao, 2), "< 10%", pct_sem_padrao < 10, critica=False)

# COMMAND ----------

execucao = datetime.now(timezone.utc).replace(tzinfo=None)
df_resultados = spark.createDataFrame(
    resultados, "regra string, valor double, limite string, status string, critica boolean"
).withColumn("executado_em", F.lit(execucao).cast("timestamp"))

df_resultados.write.mode("append").saveAsTable(t("dq_resultados"))
display(df_resultados)

falhas = [r[0] for r in resultados if r[3] == "FALHA"]
if falhas:
    raise Exception(f"Regras críticas de qualidade falharam: {falhas}")
print("Todas as regras críticas passaram.")
