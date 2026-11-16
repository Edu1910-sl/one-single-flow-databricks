# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente
# MAGIC
# MAGIC Cria o **schema** e o **volume** no Unity Catalog onde o projeto vai guardar os arquivos brutos e as tabelas.
# MAGIC
# MAGIC No Databricks Free Edition o catálogo padrão se chama `workspace`. Se o seu tiver outro nome, troque no widget `catalog` (canto superior do notebook).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "one_single_flow")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Projeto One Single Flow: rastreabilidade ponta a ponta com arquitetura Medallion (dados fictícios)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw COMMENT 'Arquivos brutos simulando as fontes de origem'")

print(f"Schema pronto: {CATALOG}.{SCHEMA}")
print(f"Volume pronto: /Volumes/{CATALOG}/{SCHEMA}/raw")
